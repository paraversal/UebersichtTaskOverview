"""TaskOverview relay server.

Keeps the list of jobs, pushes it to every connected widget, and marks jobs stale
when the producer that owns them disappears or stops sending updates.

Run it with ``taskoverview-server`` or ``python -m taskoverview_notifier.server``.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
import uuid
from dataclasses import dataclass
from typing import Any, Literal

from pydantic import ValidationError
from websockets.asyncio.server import ServerConnection, broadcast, serve
from websockets.exceptions import ConnectionClosed

from .protocol import (
    PROTOCOL_VERSION,
    DismissJob,
    EndAllJobs,
    EndJob,
    Heartbeat,
    Hello,
    NewJob,
    Resync,
    UpdateJob,
    producer_message,
    widget_message,
)

logger = logging.getLogger(__name__)

DEFAULT_HOST = "localhost"
DEFAULT_PORT = 6789

# Ping clients often so a hung or killed producer is noticed within ~20s.
PING_INTERVAL = 10.0
PING_TIMEOUT = 10.0
HELLO_TIMEOUT = 10.0

StaleReason = Literal["disconnected", "timeout"]


def _now_ms() -> int:
    return int(time.time() * 1000)


@dataclass
class JobState:
    id: str
    owner: str
    title: str
    description: str
    progress: float | None
    stale_after: float | None
    created_at: int
    updated_at: int
    last_seen: float  # time.monotonic() of the last producer message for this job
    state: Literal["active", "stale"] = "active"
    stale_reason: StaleReason | None = None
    stale_since: int | None = None

    def touch(self) -> None:
        self.updated_at = _now_ms()
        self.last_seen = time.monotonic()

    def to_wire(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "progress": self.progress,
            "state": self.state,
            "staleReason": self.stale_reason,
            "staleAfter": self.stale_after,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
            "staleSince": self.stale_since,
        }


class ProtocolError(Exception):
    """A well-formed message that can't be applied (unknown job, wrong owner, …)."""


class Server:
    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        idle_timeout: float | None = None,
        check_interval: float = 1.0,
    ) -> None:
        self.host = host
        self.port = port
        self.idle_timeout = idle_timeout
        self.check_interval = check_interval

        self.jobs: dict[str, JobState] = {}
        self.widgets: set[ServerConnection] = set()
        self.producers: dict[str, set[ServerConnection]] = {}

        self._stop = asyncio.Event()
        self.ready = asyncio.Event()
        self._idle_since = time.monotonic()

    # --- lifecycle ------------------------------------------------------------

    async def run(self) -> None:
        async with serve(
            self._handle,
            self.host,
            self.port,
            ping_interval=PING_INTERVAL,
            ping_timeout=PING_TIMEOUT,
        ) as ws_server:
            # Resolve the real port (useful when started with port=0 in tests).
            self.port = next(iter(ws_server.sockets)).getsockname()[1]
            logger.info("Listening on ws://%s:%d", self.host, self.port)
            self.ready.set()
            watchdog = asyncio.create_task(self._watchdog())
            try:
                await self._stop.wait()
            finally:
                watchdog.cancel()
        logger.info("Server stopped")

    def stop(self) -> None:
        self._stop.set()

    # --- connections ----------------------------------------------------------

    async def _handle(self, conn: ServerConnection) -> None:
        try:
            raw = await asyncio.wait_for(conn.recv(), HELLO_TIMEOUT)
            hello = Hello.model_validate_json(raw)
        except (TimeoutError, ConnectionClosed):
            return
        except ValidationError as e:
            await self._send_error(conn, f"Expected a hello message first: {e}")
            await conn.close(code=1008, reason="missing hello")
            return

        if hello.protocol != PROTOCOL_VERSION:
            msg = f"Unsupported protocol {hello.protocol}, this server speaks {PROTOCOL_VERSION}"
            await self._send_error(conn, msg)
            await conn.close(code=1008, reason="protocol mismatch")
            return

        try:
            if hello.role == "widget":
                await self._serve_widget(conn)
            else:
                await self._serve_producer(conn, hello.client_id or uuid.uuid4().hex)
        except ConnectionClosed:
            pass

    async def _serve_widget(self, conn: ServerConnection) -> None:
        logger.info("Widget connected (%d total)", len(self.widgets) + 1)
        self.widgets.add(conn)
        try:
            await conn.send(self._snapshot())
            async for raw in conn:
                try:
                    msg = widget_message.validate_json(raw)
                except ValidationError as e:
                    await self._send_error(conn, str(e))
                    continue
                if isinstance(msg, DismissJob):
                    if msg.id in self.jobs:
                        logger.info("Widget dismissed job %r", msg.id)
                        self._remove(msg.id, "dismissed")
                elif isinstance(msg, Resync):
                    await conn.send(self._snapshot())
        finally:
            self.widgets.discard(conn)
            logger.info("Widget disconnected")

    async def _serve_producer(self, conn: ServerConnection, client_id: str) -> None:
        logger.info("Producer %s connected", client_id[:8])
        self.producers.setdefault(client_id, set()).add(conn)
        self._reclaim(client_id)
        try:
            async for raw in conn:
                try:
                    self._apply(client_id, producer_message.validate_json(raw))
                except (ValidationError, ProtocolError) as e:
                    logger.warning("Rejected message from %s: %s", client_id[:8], e)
                    await self._send_error(conn, str(e))
        finally:
            conns = self.producers.get(client_id, set())
            conns.discard(conn)
            if not conns:
                self.producers.pop(client_id, None)
                self._mark_stale([j for j in self.jobs.values() if j.owner == client_id], "disconnected")
            logger.info("Producer %s disconnected", client_id[:8])

    # --- state changes --------------------------------------------------------

    def _apply(self, client_id: str, msg: Any) -> None:
        if isinstance(msg, NewJob):
            spec = msg.job
            existing = self.jobs.get(spec.id)
            if existing and existing.owner != client_id and existing.state != "stale":
                raise ProtocolError(f"Job {spec.id!r} already exists and belongs to another producer")
            now = _now_ms()
            job = JobState(
                id=spec.id,
                owner=client_id,
                title=spec.title,
                description=spec.description,
                progress=spec.progress,
                stale_after=spec.stale_after,
                created_at=existing.created_at if existing else now,
                updated_at=now,
                last_seen=time.monotonic(),
            )
            self.jobs[spec.id] = job
            self._publish(job)
        elif isinstance(msg, UpdateJob):
            job = self._owned(client_id, msg.id)
            if "title" in msg.model_fields_set and msg.title is not None:
                job.title = msg.title
            if "description" in msg.model_fields_set and msg.description is not None:
                job.description = msg.description
            if "progress" in msg.model_fields_set:
                job.progress = msg.progress
            self._revive(job)
            self._publish(job)
        elif isinstance(msg, Heartbeat):
            job = self._owned(client_id, msg.id)
            was_stale = job.state == "stale"
            self._revive(job)
            if was_stale:
                self._publish(job)
        elif isinstance(msg, EndJob):
            if msg.id not in self.jobs:
                raise ProtocolError(f"Unknown job {msg.id!r}")
            self._remove(msg.id, "ended")
        elif isinstance(msg, EndAllJobs):
            for job_id in [j.id for j in self.jobs.values() if j.owner == client_id]:
                self._remove(job_id, "ended")

    def _owned(self, client_id: str, job_id: str) -> JobState:
        job = self.jobs.get(job_id)
        if job is None:
            raise ProtocolError(f"Unknown job {job_id!r}")
        if job.owner != client_id:
            raise ProtocolError(f"Job {job_id!r} belongs to another producer")
        return job

    @staticmethod
    def _revive(job: JobState) -> None:
        job.touch()
        job.state = "active"
        job.stale_reason = None
        job.stale_since = None

    def _reclaim(self, client_id: str) -> None:
        for job in self.jobs.values():
            if job.owner == client_id and job.state == "stale":
                logger.info("Job %r reclaimed by its producer", job.id)
                self._revive(job)
                self._publish(job)

    def _mark_stale(self, jobs: list[JobState], reason: StaleReason) -> None:
        for job in jobs:
            if job.state == "stale":
                continue
            logger.warning("Job %r is stale (%s)", job.id, reason)
            job.state = "stale"
            job.stale_reason = reason
            job.stale_since = _now_ms()
            self._publish(job)

    def _remove(self, job_id: str, reason: Literal["ended", "dismissed"]) -> None:
        if self.jobs.pop(job_id, None) is not None:
            broadcast(self.widgets, json.dumps({"type": "jobRemoved", "id": job_id, "reason": reason}))

    def _publish(self, job: JobState) -> None:
        broadcast(self.widgets, json.dumps({"type": "jobUpserted", "job": job.to_wire()}))

    def _snapshot(self) -> str:
        return json.dumps(
            {
                "type": "snapshot",
                "jobs": [j.to_wire() for j in self.jobs.values()],
                "serverTime": _now_ms(),
            }
        )

    @staticmethod
    async def _send_error(conn: ServerConnection, message: str) -> None:
        try:
            await conn.send(json.dumps({"type": "error", "message": message}))
        except ConnectionClosed:
            pass

    # --- periodic checks ------------------------------------------------------

    async def _watchdog(self) -> None:
        while True:
            await asyncio.sleep(self.check_interval)
            self.check_timeouts()
            if self.idle_timeout is not None:
                if self.producers or self.jobs:
                    self._idle_since = time.monotonic()
                elif time.monotonic() - self._idle_since > self.idle_timeout:
                    logger.info("Idle for %.0fs, shutting down", self.idle_timeout)
                    self.stop()
                    return

    def check_timeouts(self) -> None:
        now = time.monotonic()
        self._mark_stale(
            [
                j
                for j in self.jobs.values()
                if j.state == "active" and j.stale_after is not None and now - j.last_seen > j.stale_after
            ],
            "timeout",
        )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="taskoverview-server", description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=None,
        metavar="SECONDS",
        help="exit after this long with no producers and no jobs (default: run forever)",
    )
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(level=args.log_level, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    async def _run() -> None:
        server = Server(args.host, args.port, idle_timeout=args.idle_timeout)
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, server.stop)
        await server.run()

    try:
        asyncio.run(_run())
    except OSError as e:
        logger.error("Could not start server on %s:%d: %s", args.host, args.port, e)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

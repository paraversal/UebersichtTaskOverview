"""Producer clients: report jobs to the TaskOverview widget.

``Notifier`` is the asyncio client and ``SyncNotifier`` is the blocking one. Both keep one
persistent connection, reconnect automatically, and re-send their open jobs after a
reconnect, so a server restart doesn't lose cards.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass
from types import TracebackType
from typing import Any, Self

from websockets.asyncio.client import ClientConnection
from websockets.asyncio.client import connect as connect_async
from websockets.exceptions import ConnectionClosed, WebSocketException
from websockets.protocol import State
from websockets.sync.client import ClientConnection as SyncClientConnection
from websockets.sync.client import connect as connect_sync

from ._spawn import ensure_server
from .protocol import PROTOCOL_VERSION
from .server import DEFAULT_HOST, DEFAULT_PORT, PING_INTERVAL, PING_TIMEOUT

logger = logging.getLogger(__name__)

RECONNECT_MIN = 0.5  # seconds
RECONNECT_MAX = 10.0

# Errors that mean "the server isn't reachable right now".
_CONNECTION_ERRORS = (OSError, WebSocketException)


def _check_progress(progress: float | None) -> None:
    if progress is not None and not 0.0 <= progress <= 1.0:
        raise ValueError(f"progress must be between 0 and 1, got {progress!r}")


@dataclass
class _JobRecord:
    id: str
    title: str
    description: str
    progress: float | None
    stale_after: float | None

    def new_job_message(self) -> dict[str, Any]:
        return {
            "type": "newJob",
            "job": {
                "id": self.id,
                "title": self.title,
                "description": self.description,
                "progress": self.progress,
                "staleAfter": self.stale_after,
            },
        }


class _NotifierBase:
    """Message building and open-job bookkeeping shared by both clients."""

    def __init__(
        self,
        host: str = DEFAULT_HOST,
        port: int = DEFAULT_PORT,
        *,
        spawn_server: bool = True,
        client_id: str | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.spawn_server = spawn_server
        self.client_id = client_id or uuid.uuid4().hex
        self._jobs: dict[str, _JobRecord] = {}
        # Jobs ended while disconnected; their endJob is sent after reconnecting.
        self._unsent_ends: set[str] = set()
        self._closed = True
        self._reconnecting = False

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}"

    def _hello(self) -> str:
        return json.dumps(
            {"type": "hello", "role": "producer", "protocol": PROTOCOL_VERSION, "clientId": self.client_id}
        )

    def _resend_messages(self) -> list[str]:
        """Messages that restore this notifier's jobs on a fresh connection."""
        msgs = [json.dumps(job.new_job_message()) for job in self._jobs.values()]
        msgs += [json.dumps({"type": "endJob", "id": id}) for id in self._unsent_ends]
        return msgs

    def _end(self, ids: list[str]) -> None:
        for id in ids:
            self._jobs.pop(id, None)
        self._unsent_ends.update(ids)

    def _sent(self, msg: dict[str, Any]) -> None:
        if msg["type"] == "endJob":
            self._unsent_ends.discard(msg["id"])
        elif msg["type"] == "endAllJobs":
            self._unsent_ends.clear()

    def _offline(self, msg: dict[str, Any], error: BaseException | None = None) -> None:
        # Best effort: never crash the caller's script because the display is unreachable.
        # Open jobs (with their latest state) and pending ends are replayed on reconnect.
        if error is not None:
            logger.warning("TaskOverview server unreachable (%s); retrying in the background", error)
        else:
            logger.debug("Reconnecting; %s will be replayed later", msg["type"])

    def _new(
        self, title: str, description: str, progress: float | None, stale_after: float | None, id: str | None
    ) -> _JobRecord:
        _check_progress(progress)
        if stale_after is not None and stale_after <= 0:
            raise ValueError("stale_after must be positive")
        record = _JobRecord(id or uuid.uuid4().hex, title, description, progress, stale_after)
        self._jobs[record.id] = record
        return record

    def _update(
        self, id: str, title: str | None, description: str | None, progress: float | None
    ) -> dict[str, Any]:
        if title is None and description is None and progress is None:
            raise ValueError("update needs at least one of title, description or progress")
        _check_progress(progress)
        msg: dict[str, Any] = {"type": "updateJob", "id": id}
        record = self._jobs.get(id)
        for key, value in (("title", title), ("description", description), ("progress", progress)):
            if value is not None:
                msg[key] = value
                if record:
                    setattr(record, key, value)
        return msg

    @staticmethod
    def _log_server_message(raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        if msg.get("type") == "error":
            logger.warning("TaskOverview server rejected a message: %s", msg.get("message"))


# --- asyncio ------------------------------------------------------------------


class Job:
    """Handle for a job created with :meth:`Notifier.new_job`."""

    def __init__(self, notifier: Notifier, id: str) -> None:
        self._notifier = notifier
        self.id = id

    async def update(
        self, *, title: str | None = None, description: str | None = None, progress: float | None = None
    ) -> None:
        await self._notifier.update_job(self.id, title=title, description=description, progress=progress)

    async def heartbeat(self) -> None:
        await self._notifier.heartbeat(self.id)

    async def end(self) -> None:
        await self._notifier.end_job(self.id)

    def __repr__(self) -> str:
        return f"Job(id={self.id!r})"


class Notifier(_NotifierBase):
    """Asyncio client.

    >>> async with Notifier() as notifier:
    ...     async with notifier.job("Downloading", "model.bin") as job:
    ...         await job.update(progress=0.5)
    """

    _ws: ClientConnection | None = None
    _reader: asyncio.Task[None] | None = None
    _lock: asyncio.Lock | None = None

    async def __aenter__(self) -> Self:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def connect(self) -> None:
        """Connect, starting a server if needed. Raises if no server can be reached."""
        self._closed = False
        self._lock = asyncio.Lock()
        await self._ensure_connected()

    async def close(self) -> None:
        """Disconnect. Jobs that are still open will show up as stale on the widget."""
        self._closed = True
        if self._reader:
            self._reader.cancel()
            self._reader = None
        if self._ws:
            await self._ws.close()
            self._ws = None

    async def _ensure_connected(self) -> ClientConnection:
        assert self._lock is not None
        async with self._lock:
            if self._ws is not None and self._ws.state is State.OPEN:
                return self._ws
            if self._closed:
                raise RuntimeError("Notifier is closed")
            if self.spawn_server:
                await asyncio.to_thread(ensure_server, self.host, self.port)
            ws = await connect_async(self.url, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)
            await ws.send(self._hello())
            for data in self._resend_messages():
                await ws.send(data)
            self._unsent_ends.clear()
            self._ws = ws
            self._reader = asyncio.create_task(self._read_loop(ws))
            return ws

    async def _read_loop(self, ws: ClientConnection) -> None:
        with contextlib.suppress(ConnectionClosed):
            async for raw in ws:
                self._log_server_message(raw)
        if self._closed or self._ws is not ws:
            return
        # The server went away: reconnect in the background (restarting it if we may),
        # so the cards come back even if this script never sends another update.
        logger.warning("Lost connection to TaskOverview server, reconnecting")
        self._reconnecting = True
        delay = RECONNECT_MIN
        try:
            while not self._closed:
                try:
                    await self._ensure_connected()
                    logger.info("Reconnected to TaskOverview server")
                    return
                except _CONNECTION_ERRORS as e:
                    logger.debug("Reconnect failed: %s", e)
                    await asyncio.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX)
        finally:
            self._reconnecting = False

    async def _send(self, msg: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("Notifier is not connected; use `async with Notifier()` or call connect()")
        if self._reconnecting:
            return self._offline(msg)
        try:
            ws = await self._ensure_connected()
            await ws.send(json.dumps(msg))
        except _CONNECTION_ERRORS as e:
            return self._offline(msg, e)
        self._sent(msg)

    async def new_job(
        self,
        title: str,
        description: str = "",
        *,
        progress: float | None = None,
        stale_after: float | None = None,
        id: str | None = None,
    ) -> Job:
        """Show a new card. ``stale_after`` marks it stale if no update arrives for that many seconds."""
        record = self._new(title, description, progress, stale_after, id)
        await self._send(record.new_job_message())
        return Job(self, record.id)

    async def update_job(
        self,
        id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        progress: float | None = None,
    ) -> None:
        await self._send(self._update(id, title, description, progress))

    async def heartbeat(self, id: str) -> None:
        await self._send({"type": "heartbeat", "id": id})

    async def end_job(self, id: str) -> None:
        self._end([id])
        await self._send({"type": "endJob", "id": id})

    async def end_all_jobs(self) -> None:
        """End every job this notifier created."""
        self._end(list(self._jobs))
        await self._send({"type": "endAllJobs"})

    @contextlib.asynccontextmanager
    async def job(
        self,
        title: str,
        description: str = "",
        *,
        progress: float | None = None,
        stale_after: float | None = None,
        id: str | None = None,
    ) -> AsyncIterator[Job]:
        """Create a job and end it when the block exits, even on error."""
        job = await self.new_job(title, description, progress=progress, stale_after=stale_after, id=id)
        try:
            yield job
        finally:
            await job.end()


# --- blocking -----------------------------------------------------------------


class SyncJob:
    """Handle for a job created with :meth:`SyncNotifier.new_job`."""

    def __init__(self, notifier: SyncNotifier, id: str) -> None:
        self._notifier = notifier
        self.id = id

    def update(
        self, *, title: str | None = None, description: str | None = None, progress: float | None = None
    ) -> None:
        self._notifier.update_job(self.id, title=title, description=description, progress=progress)

    def heartbeat(self) -> None:
        self._notifier.heartbeat(self.id)

    def end(self) -> None:
        self._notifier.end_job(self.id)

    def __repr__(self) -> str:
        return f"SyncJob(id={self.id!r})"


class SyncNotifier(_NotifierBase):
    """Blocking client, for scripts that don't use asyncio.

    >>> with SyncNotifier() as notifier:
    ...     with notifier.job("Rendering") as job:
    ...         job.update(progress=0.5)
    """

    _ws: SyncClientConnection | None = None
    _stack: contextlib.ExitStack | None = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._lock = threading.RLock()

    def __enter__(self) -> Self:
        self.connect()
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def connect(self) -> None:
        """Connect, starting a server if needed. Raises if no server can be reached."""
        self._closed = False
        self._ensure_connected()

    def close(self) -> None:
        """Disconnect. Jobs that are still open will show up as stale on the widget."""
        self._closed = True
        with self._lock:
            if self._stack:
                self._stack.close()
            self._ws = self._stack = None

    def _ensure_connected(self) -> SyncClientConnection:
        with self._lock:
            if self._ws is not None and self._ws.state is State.OPEN:
                return self._ws
            if self._closed:
                raise RuntimeError("SyncNotifier is closed")
            if self._stack:
                self._stack.close()
            if self.spawn_server:
                ensure_server(self.host, self.port)
            stack = contextlib.ExitStack()
            ws = stack.enter_context(
                connect_sync(self.url, ping_interval=PING_INTERVAL, ping_timeout=PING_TIMEOUT)
            )
            ws.send(self._hello())
            for data in self._resend_messages():
                ws.send(data)
            self._unsent_ends.clear()
            self._ws, self._stack = ws, stack
            threading.Thread(
                target=self._read_loop, args=(ws,), daemon=True, name="taskoverview-reader"
            ).start()
            return ws

    def _read_loop(self, ws: SyncClientConnection) -> None:
        with contextlib.suppress(ConnectionClosed):
            for raw in ws:
                self._log_server_message(raw)
        if self._closed or self._ws is not ws:
            return
        logger.warning("Lost connection to TaskOverview server, reconnecting")
        self._reconnecting = True
        delay = RECONNECT_MIN
        try:
            while not self._closed:
                try:
                    self._ensure_connected()
                    logger.info("Reconnected to TaskOverview server")
                    return
                except _CONNECTION_ERRORS as e:
                    logger.debug("Reconnect failed: %s", e)
                    time.sleep(delay)
                    delay = min(delay * 2, RECONNECT_MAX)
        finally:
            self._reconnecting = False

    def _send(self, msg: dict[str, Any]) -> None:
        if self._closed:
            raise RuntimeError("SyncNotifier is not connected; use `with SyncNotifier()` or call connect()")
        if self._reconnecting:
            return self._offline(msg)
        try:
            ws = self._ensure_connected()
            ws.send(json.dumps(msg))
        except _CONNECTION_ERRORS as e:
            return self._offline(msg, e)
        self._sent(msg)

    def new_job(
        self,
        title: str,
        description: str = "",
        *,
        progress: float | None = None,
        stale_after: float | None = None,
        id: str | None = None,
    ) -> SyncJob:
        """Show a new card. ``stale_after`` marks it stale if no update arrives for that many seconds."""
        record = self._new(title, description, progress, stale_after, id)
        self._send(record.new_job_message())
        return SyncJob(self, record.id)

    def update_job(
        self,
        id: str,
        *,
        title: str | None = None,
        description: str | None = None,
        progress: float | None = None,
    ) -> None:
        self._send(self._update(id, title, description, progress))

    def heartbeat(self, id: str) -> None:
        self._send({"type": "heartbeat", "id": id})

    def end_job(self, id: str) -> None:
        self._end([id])
        self._send({"type": "endJob", "id": id})

    def end_all_jobs(self) -> None:
        """End every job this notifier created."""
        self._end(list(self._jobs))
        self._send({"type": "endAllJobs"})

    @contextlib.contextmanager
    def job(
        self,
        title: str,
        description: str = "",
        *,
        progress: float | None = None,
        stale_after: float | None = None,
        id: str | None = None,
    ) -> Iterator[SyncJob]:
        """Create a job and end it when the block exits, even on error."""
        job = self.new_job(title, description, progress=progress, stale_after=stale_after, id=id)
        try:
            yield job
        finally:
            job.end()

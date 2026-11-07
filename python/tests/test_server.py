from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from typing import Any

import pytest
from websockets.asyncio.client import ClientConnection, connect

from taskoverview_notifier import Notifier, SyncNotifier
from taskoverview_notifier.server import Server


@pytest.fixture
async def server() -> AsyncIterator[Server]:
    srv = Server("127.0.0.1", 0, check_interval=0.05)
    task = asyncio.create_task(srv.run())
    await srv.ready.wait()
    yield srv
    srv.stop()
    await task


async def recv(ws: ClientConnection, timeout: float = 2.0) -> dict[str, Any]:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def recv_until(ws: ClientConnection, pred: Any, timeout: float = 2.0) -> dict[str, Any]:
    async def _loop() -> dict[str, Any]:
        while True:
            msg = await recv(ws, timeout)
            if pred(msg):
                return msg

    return await asyncio.wait_for(_loop(), timeout)


async def widget(server: Server) -> ClientConnection:
    ws = await connect(f"ws://127.0.0.1:{server.port}")
    await ws.send(json.dumps({"type": "hello", "role": "widget", "protocol": 1}))
    snap = await recv(ws)
    assert snap["type"] == "snapshot"
    return ws


def notifier(server: Server, **kw: Any) -> Notifier:
    return Notifier("127.0.0.1", server.port, spawn_server=False, **kw)


async def test_job_lifecycle(server: Server) -> None:
    w = await widget(server)
    async with notifier(server) as n:
        job = await n.new_job("Download", "file.zip")
        msg = await recv(w)
        assert msg["type"] == "jobUpserted"
        assert msg["job"]["title"] == "Download"
        assert msg["job"]["progress"] is None

        await job.update(progress=0.0)  # 0 must not be ignored
        msg = await recv(w)
        assert msg["job"]["progress"] == 0.0

        await job.update(progress=0.5, description="halfway")
        msg = await recv(w)
        assert msg["job"]["progress"] == 0.5
        assert msg["job"]["description"] == "halfway"

        await job.end()
        assert await recv(w) == {"type": "jobRemoved", "id": job.id, "reason": "ended"}
    assert server.jobs == {}


async def test_snapshot_on_connect(server: Server) -> None:
    async with notifier(server) as n:
        job = await n.new_job("Already running")
        await asyncio.sleep(0.05)
        w = await connect(f"ws://127.0.0.1:{server.port}")
        await w.send(json.dumps({"type": "hello", "role": "widget", "protocol": 1}))
        snap = await recv(w)
        assert snap["type"] == "snapshot"
        assert [j["id"] for j in snap["jobs"]] == [job.id]


async def test_disconnect_marks_stale_and_reconnect_reclaims(server: Server) -> None:
    w = await widget(server)
    n = notifier(server, client_id="fixed")
    await n.connect()
    job = await n.new_job("Will crash")
    await recv(w)

    await n.close()  # simulates the process dying
    msg = await recv(w)
    assert msg["job"]["state"] == "stale"
    assert msg["job"]["staleReason"] == "disconnected"
    assert msg["job"]["staleSince"] is not None

    # Same client id reconnects -> job is active again.
    n2 = notifier(server, client_id="fixed")
    await n2.connect()
    msg = await recv(w)
    assert msg["job"]["id"] == job.id
    assert msg["job"]["state"] == "active"
    await n2.close()


async def test_timeout_marks_stale_and_heartbeat_revives(server: Server) -> None:
    w = await widget(server)
    async with notifier(server) as n:
        job = await n.new_job("Slow", stale_after=0.2)
        await recv(w)
        msg = await recv_until(w, lambda m: m.get("job", {}).get("state") == "stale")
        assert msg["job"]["staleReason"] == "timeout"

        await job.heartbeat()
        msg = await recv(w)
        assert msg["job"]["state"] == "active"
        await job.end()


async def test_widget_dismiss(server: Server) -> None:
    w = await widget(server)
    async with notifier(server) as n:
        job = await n.new_job("Dismiss me")
        await recv(w)
        await w.send(json.dumps({"type": "dismissJob", "id": job.id}))
        assert await recv(w) == {"type": "jobRemoved", "id": job.id, "reason": "dismissed"}
        assert job.id not in server.jobs


async def test_widget_resync(server: Server) -> None:
    w = await widget(server)
    await w.send(json.dumps({"type": "resync"}))
    assert (await recv(w))["type"] == "snapshot"


async def test_invalid_message_keeps_connection(server: Server) -> None:
    ws = await connect(f"ws://127.0.0.1:{server.port}")
    await ws.send(json.dumps({"type": "hello", "role": "producer", "protocol": 1, "clientId": "x"}))
    await ws.send(json.dumps({"type": "updateJob", "id": "nope", "progress": 2}))
    assert (await recv(ws))["type"] == "error"
    await ws.send(json.dumps({"type": "updateJob", "id": "nope", "progress": 0.5}))
    err = await recv(ws)
    assert err["type"] == "error" and "Unknown job" in err["message"]
    # still usable
    await ws.send(json.dumps({"type": "newJob", "job": {"id": "a", "title": "t"}}))
    await asyncio.sleep(0.05)
    assert "a" in server.jobs


async def test_missing_hello_is_rejected(server: Server) -> None:
    ws = await connect(f"ws://127.0.0.1:{server.port}")
    await ws.send(json.dumps({"type": "newJob", "job": {"id": "a", "title": "t"}}))
    assert (await recv(ws))["type"] == "error"
    assert server.jobs == {}


async def test_stale_job_can_be_taken_over(server: Server) -> None:
    n = notifier(server)
    await n.connect()
    await n.new_job("orig", id="shared")
    await n.close()
    await asyncio.sleep(0.05)
    assert server.jobs["shared"].state == "stale"

    async with notifier(server) as other:
        await other.new_job("restarted", id="shared")
        await asyncio.sleep(0.05)
        assert server.jobs["shared"].state == "active"
        assert server.jobs["shared"].title == "restarted"


async def test_active_job_id_conflict_is_rejected(server: Server) -> None:
    async with notifier(server) as a, notifier(server) as b:
        await a.new_job("a", id="dup")
        await asyncio.sleep(0.05)  # separate connections: make sure a's message lands first
        await b.new_job("b", id="dup")
        await asyncio.sleep(0.05)
        assert server.jobs["dup"].title == "a"


async def test_end_all_jobs_only_ends_own(server: Server) -> None:
    async with notifier(server) as a, notifier(server) as b:
        await a.new_job("a1")
        await a.new_job("a2")
        kept = await b.new_job("b1")
        await a.end_all_jobs()
        await asyncio.sleep(0.05)
        assert list(server.jobs) == [kept.id]


async def wait_for(pred: Any, timeout: float = 3.0) -> None:
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.02)


async def start_server(port: int = 0) -> tuple[Server, asyncio.Task[None]]:
    srv = Server("127.0.0.1", port, check_interval=0.05)
    task = asyncio.create_task(srv.run())
    await srv.ready.wait()
    return srv, task


async def test_client_restores_jobs_after_server_restart() -> None:
    srv, task = await start_server()
    port = srv.port
    n = Notifier("127.0.0.1", port, spawn_server=False)
    await n.connect()
    job = await n.new_job("survivor", progress=0.3)
    srv.stop()
    await task

    # Calls while the server is down must not raise...
    await job.update(progress=0.6)
    gone = await n.new_job("ended while offline")
    await gone.end()

    srv2, task2 = await start_server(port)
    # ...and the client reconnects by itself, replaying the latest state, without another send.
    await wait_for(lambda: job.id in srv2.jobs)
    assert srv2.jobs[job.id].progress == 0.6
    assert gone.id not in srv2.jobs
    await n.close()
    srv2.stop()
    await task2


async def test_sync_client_restores_jobs_after_server_restart() -> None:
    srv, task = await start_server()
    port = srv.port
    n = SyncNotifier("127.0.0.1", port, spawn_server=False)
    await asyncio.to_thread(n.connect)
    job = await asyncio.to_thread(n.new_job, "sync survivor")
    srv.stop()
    await task
    await asyncio.to_thread(job.update, progress=0.9)

    srv2, task2 = await start_server(port)
    await wait_for(lambda: job.id in srv2.jobs)
    await wait_for(lambda: srv2.jobs[job.id].progress == 0.9)
    await asyncio.to_thread(n.close)
    srv2.stop()
    await task2


async def test_sync_notifier(server: Server) -> None:
    w = await widget(server)

    def run() -> str:
        with SyncNotifier("127.0.0.1", server.port, spawn_server=False) as n:
            with n.job("sync job") as job:
                job.update(progress=1.0)
                return job.id

    job_id = await asyncio.to_thread(run)
    seen = [await recv(w) for _ in range(3)]
    assert [m["type"] for m in seen] == ["jobUpserted", "jobUpserted", "jobRemoved"]
    assert seen[2]["id"] == job_id


def test_progress_out_of_range_raises() -> None:
    n = Notifier(spawn_server=False)
    with pytest.raises(ValueError):
        n._new("x", "", 50, None, None)

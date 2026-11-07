# taskoverview-notifier

Show progress cards for long-running Python jobs on your macOS desktop, using the
[TaskOverview](https://github.com/paraversal/UebersichtTaskOverview) Übersicht widget.

```bash
pip install taskoverview-notifier
```

You also need [Übersicht](https://tracesof.net/uebersicht/) and the TaskOverview widget
(see the main repository's README).

## Usage

```python
import asyncio
from taskoverview_notifier import Notifier

async def main():
    async with Notifier() as notifier:
        async with notifier.job("Downloading", "model.bin", stale_after=30) as job:
            for i in range(101):
                await job.update(progress=i / 100, description=f"{i}%")
                await asyncio.sleep(0.05)

asyncio.run(main())
```

Without asyncio:

```python
from taskoverview_notifier import SyncNotifier

with SyncNotifier() as notifier:
    with notifier.job("Rendering frames") as job:
        for i, frame in enumerate(frames):
            render(frame)
            job.update(progress=(i + 1) / len(frames))
```

You can also manage jobs by hand: `job = await notifier.new_job(...)`, then
`job.update(...)`, `job.heartbeat()` and `job.end()`.

- `progress` is a float from `0` to `1`. Leave it out for a card with no progress bar.
- `notifier.job(...)` ends the card when the block exits, including on an exception.
- `end_all_jobs()` ends every job created by that notifier (not other processes' jobs).

## Stale jobs

If your process dies, gets killed or hangs while a job is open, the card is marked
**stale** instead of hanging around forever, and the widget shows **Refresh** and
**Dismiss** buttons on it.

- **Disconnected:** the connection to the server dropped. A crash is noticed
  immediately. A hung asyncio event loop is noticed after about 20 seconds, when
  WebSocket pings time out.
- **Timeout:** pass `stale_after=<seconds>` and the card goes stale if no update or
  `heartbeat()` arrives in that time. Use this to catch a process that is alive
  but stuck, for example in a blocking call.

A stale card becomes active again as soon as its process sends another update. If
a new process creates a job with the same `id` as a stale one, it takes the card over.

## The server

Widgets and scripts talk through a small local WebSocket server on `localhost:6789`.
The first `Notifier` that finds no server starts one as a detached background
process. That server exits after 10 minutes with no jobs and no connected scripts.
Logs go to `~/Library/Logs/taskoverview-notifier/server.log`.

To keep it running permanently instead, run `taskoverview-server`, for example as a
launchd agent. Use `taskoverview-server --help` to see the options.

The wire protocol is documented in
[PROTOCOL.md](https://github.com/paraversal/UebersichtTaskOverview/blob/main/PROTOCOL.md).

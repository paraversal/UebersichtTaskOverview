# Job Overview protocol (v1)

All traffic goes over a single WebSocket server (default `ws://localhost:6789`).
Every message is a JSON object with a `type` field.

There are two kinds of clients:

- **producers** – scripts that report jobs (the `taskoverview-notifier` Python package)
- **widgets** – the Übersicht widget, which only displays state

The **server owns the job list**. Widgets receive a full snapshot when they
connect and incremental changes afterwards, so a widget reload never loses state.

## Handshake

The first message on every connection must be `hello`:

```json
{"type": "hello", "role": "producer", "protocol": 1, "clientId": "3f2c…"}
{"type": "hello", "role": "widget",   "protocol": 1}
```

`clientId` identifies a producer across reconnects. When a producer reconnects
with the same `clientId`, its stale jobs become active again.

## Jobs

```json
{
  "id": "string",
  "title": "string",
  "description": "string",
  "progress": 0.42,            // 0..1, or null for "no progress bar"
  "state": "active",           // "active" | "stale"
  "staleReason": null,         // null | "disconnected" | "timeout"
  "staleAfter": 30,            // seconds without updates before "timeout", or null
  "createdAt": 1730000000000,  // epoch ms
  "updatedAt": 1730000000000,  // epoch ms of the last producer message for this job
  "staleSince": null           // epoch ms, or null
}
```

### Becoming stale

A job turns `stale` when:

- **`disconnected`** – the producer's connection dropped (the process crashed, was
  killed, or its event loop hung long enough for WebSocket pings to time out)
  while the job was still running.
- **`timeout`** – the job has a `staleAfter` and the producer has sent nothing for
  that job (no update or heartbeat) for that many seconds.

A stale job becomes `active` again if its producer reconnects (same `clientId`) or
sends an update or heartbeat for it. A stale job stays on screen until it is
dismissed or ended.

## Producer → server

| type          | fields                                                        |
|---------------|---------------------------------------------------------------|
| `newJob`      | `job: {id, title, description?, progress?, staleAfter?}`       |
| `updateJob`   | `id`, and at least one of `title`, `description`, `progress`  |
| `heartbeat`   | `id` – "still alive", resets the `staleAfter` timer            |
| `endJob`      | `id`                                                          |
| `endAllJobs`  | – ends every job owned by this producer                       |

## Widget → server

| type          | fields                                  |
|---------------|-----------------------------------------|
| `dismissJob`  | `id` – remove a job (usually a stale one) |
| `resync`      | – ask for a fresh `snapshot`             |

## Server → widget

| type          | fields                                  |
|---------------|-----------------------------------------|
| `snapshot`    | `jobs: Job[]`, `serverTime`             |
| `jobUpserted` | `job: Job`                              |
| `jobRemoved`  | `id`, `reason`: `"ended"` (its producer finished it) or `"dismissed"` (a widget removed it) |

## Server → any client

| type    | fields    |
|---------|-----------|
| `error` | `message` |

An invalid message gets an `error` reply. It does not close the connection.

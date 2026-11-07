import { React } from 'uebersicht'

// ─── Settings ────────────────────────────────────────────────────────────────

const SERVER_URL = 'ws://localhost:6789'
const POSITION = { top: '20px', left: '30px' }
const CARD_WIDTH = 320 // px
const GAP = 16 // px between cards

// A card stays on screen at least this long, even if its job finishes instantly…
const MIN_VISIBLE_MS = 10000
// …and always shows "Done" for at least this long before closing.
const DONE_LINGER_MS = 4000

// Gruvbox dark
const THEME = {
  cardBg: '#282828',
  textPrimary: '#ebdbb2',
  textSecondary: '#a89984',
  accent: '#458588',
  done: '#98971a',
  stale: '#d79921',
  track: 'rgba(235, 235, 245, 0.15)',
  radius: '22px',
  shadow: '0 0 15px #92837436',
}

// ─── Internals ───────────────────────────────────────────────────────────────

const PROTOCOL_VERSION = 1
const LEAVE_MS = 320
const RETRY_MIN_MS = 1000
const RETRY_MAX_MS = 2000 // localhost: retrying is cheap, and a fresh server shouldn't miss quick jobs
const TICK_MS = 1000 // keeps "closing in 5s" / "stale for 3m" current

export const refreshFrequency = false

export const className = `
  top: ${POSITION.top};
  left: ${POSITION.left};
`

// Shared across hot reloads of this file, so an old socket never lingers.
const runtime = (window.__taskOverview = window.__taskOverview || {})

const send = (msg) => {
  const ws = runtime.socket
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg))
    return true
  }
  return false
}

// Remember when each card first appeared, for MIN_VISIBLE_MS.
const markSeen = (jobs) => {
  const now = Date.now()
  for (const job of jobs) if (!(job.id in runtime.seen)) runtime.seen[job.id] = now
}

const connect = () => {
  clearTimeout(runtime.retryTimer)
  const current = runtime.socket
  if (current && current.readyState <= WebSocket.OPEN) return // connecting or open

  const dispatch = runtime.dispatch
  dispatch({ type: 'tov/connection', status: 'connecting' })

  const ws = new WebSocket(SERVER_URL)
  runtime.socket = ws

  ws.onopen = () => {
    runtime.retryDelay = RETRY_MIN_MS
    ws.send(JSON.stringify({ type: 'hello', role: 'widget', protocol: PROTOCOL_VERSION }))
    dispatch({ type: 'tov/connection', status: 'open' })
  }

  ws.onmessage = (event) => {
    let msg
    try {
      msg = JSON.parse(event.data)
    } catch (e) {
      console.error('taskOverview: unparseable message', event.data)
      return
    }
    switch (msg.type) {
      case 'snapshot':
        markSeen(msg.jobs)
        dispatch({ type: 'tov/snapshot', jobs: msg.jobs })
        break
      case 'jobUpserted':
        markSeen([msg.job])
        dispatch({ type: 'tov/upsert', job: msg.job })
        break
      case 'jobRemoved':
        if (msg.reason === 'dismissed') leave(msg.id)
        else finish(msg.id)
        break
      case 'error':
        console.error('taskOverview server error:', msg.message)
        break
      default:
        console.warn('taskOverview: unknown message', msg)
    }
  }

  ws.onclose = () => {
    if (runtime.socket !== ws) return // superseded by a newer socket
    runtime.socket = null
    dispatch({ type: 'tov/connection', status: 'closed' })
    const delay = runtime.retryDelay || RETRY_MIN_MS
    runtime.retryDelay = Math.min(delay * 2, RETRY_MAX_MS)
    runtime.retryTimer = setTimeout(connect, delay)
  }
}

// The job ended: show it as done, then close it once it has been visible long enough.
const finish = (id) => {
  const now = Date.now()
  const shownAt = runtime.seen[id] ?? now
  const removeAt = Math.max(shownAt + MIN_VISIBLE_MS, now + DONE_LINGER_MS)
  runtime.dispatch({ type: 'tov/finish', id, finishedAt: now, removeAt })
  // removeAt doubles as a token: if the job comes back meanwhile, this timer does nothing.
  setTimeout(() => leave(id, removeAt), removeAt - now)
}

// Animate a card out, then drop it from state.
const leave = (id, token) => {
  delete runtime.seen[id]
  runtime.dispatch({ type: 'tov/leave', id, token })
  setTimeout(() => runtime.dispatch({ type: 'tov/remove', id }), LEAVE_MS)
}

const dismiss = (id) => {
  // With a connection the server removes the job and tells every widget;
  // without one, just drop it locally.
  if (!send({ type: 'dismissJob', id })) leave(id)
}

const refresh = () => {
  if (!send({ type: 'resync' })) {
    runtime.retryDelay = RETRY_MIN_MS
    connect()
  }
}

export const init = (dispatch) => {
  // Tear down whatever a previous version of this file left behind.
  clearTimeout(runtime.retryTimer)
  clearInterval(runtime.tickTimer)
  if (runtime.socket) {
    const old = runtime.socket
    runtime.socket = null
    old.close()
  }

  runtime.dispatch = dispatch
  runtime.seen = {}
  runtime.retryDelay = RETRY_MIN_MS
  runtime.tickTimer = setInterval(() => dispatch({ type: 'tov/tick' }), TICK_MS)
  connect()
}

// ─── State ───────────────────────────────────────────────────────────────────

export const initialState = { jobs: [], connection: 'connecting', now: Date.now() }

// Cards the server no longer knows about but that are still on screen.
const isLocalOnly = (job) => job.leaving || job.finished

export const updateState = (event, state) => {
  switch (event.type) {
    case 'tov/connection':
      return { ...state, connection: event.status, now: Date.now() }

    case 'tov/snapshot': {
      // The server only knows running jobs; keep finished and closing cards until they're done.
      const fresh = event.jobs
      const local = state.jobs.filter((j) => isLocalOnly(j) && !fresh.some((f) => f.id === j.id))
      return { ...state, jobs: [...fresh, ...local], now: Date.now() }
    }

    case 'tov/upsert': {
      const exists = state.jobs.some((j) => j.id === event.job.id)
      const jobs = exists
        ? state.jobs.map((j) => (j.id === event.job.id ? event.job : j))
        : [...state.jobs, event.job]
      return { ...state, jobs, now: Date.now() }
    }

    case 'tov/finish':
      return {
        ...state,
        now: Date.now(),
        jobs: state.jobs.map((j) =>
          j.id === event.id
            ? {
                ...j,
                finished: true,
                finishedAt: event.finishedAt,
                removeAt: event.removeAt,
                progress: typeof j.progress === 'number' ? 1 : j.progress,
              }
            : j
        ),
      }

    case 'tov/leave':
      return {
        ...state,
        jobs: state.jobs.map((j) =>
          j.id === event.id && (event.token === undefined || j.removeAt === event.token)
            ? { ...j, leaving: true }
            : j
        ),
      }

    case 'tov/remove':
      return { ...state, jobs: state.jobs.filter((j) => !(j.id === event.id && j.leaving)) }

    case 'tov/tick': {
      // Only re-render when something on screen shows a running clock.
      const ticking = state.connection !== 'open' || state.jobs.some((j) => j.finished || j.state === 'stale')
      return ticking ? { ...state, now: Date.now() } : state
    }

    default:
      return state
  }
}

// ─── View ────────────────────────────────────────────────────────────────────

const ago = (ms) => {
  const s = Math.max(0, Math.round(ms / 1000))
  if (s < 60) return `${s}s`
  const m = Math.round(s / 60)
  if (m < 60) return `${m}m`
  return `${Math.round(m / 60)}h`
}

const staleInfo = (job, connection, now) => {
  if (connection !== 'open') return 'Server offline · reconnecting…'
  if (job.staleReason === 'timeout') return `No updates for ${ago(now - job.updatedAt)}`
  if (job.staleReason === 'disconnected') return `Disconnected ${ago(now - job.staleSince)} ago`
  return 'Not responding'
}

const Card = ({ job, connection, now }) => {
  const finished = !!job.finished
  const stale = !finished && (job.state === 'stale' || connection !== 'open')
  const hasProgress = typeof job.progress === 'number'
  const classes = [
    'tov-item',
    job.leaving && 'tov-leaving',
    stale && 'tov-is-stale',
    finished && 'tov-is-done',
  ]
    .filter(Boolean)
    .join(' ')

  return (
    <li className={classes}>
      <div className="tov-clip">
        <div className="tov-card">
          <div className="tov-head">
            <p className="tov-title">{job.title}</p>
            {stale && <span className="tov-badge">Stale</span>}
            {finished && <span className="tov-badge tov-badge-done">Done</span>}
          </div>
          {job.description && <p className="tov-desc">{job.description}</p>}
          {hasProgress && (
            <div className="tov-progress">
              <div className="tov-fill" style={{ width: `${Math.round(job.progress * 1000) / 10}%` }} />
            </div>
          )}
          {stale && (
            <div className="tov-footer">
              <span className="tov-footer-text">{staleInfo(job, connection, now)}</span>
              <div className="tov-actions">
                <button className="tov-btn" onClick={refresh} title="Ask the server for the latest state">
                  Refresh
                </button>
                <button className="tov-btn tov-btn-warn" onClick={() => dismiss(job.id)} title="Remove this card">
                  Dismiss
                </button>
              </div>
            </div>
          )}
          {finished && (
            <div className="tov-footer">
              <span className="tov-footer-text">
                Finished · closing in {Math.max(1, Math.ceil((job.removeAt - now) / 1000))}s
              </span>
              <div className="tov-actions">
                <button className="tov-btn" onClick={() => leave(job.id)} title="Close now">
                  Close
                </button>
              </div>
            </div>
          )}
          {finished && (
            <div className="tov-drain">
              <div className="tov-drain-bar" style={{ animationDuration: `${job.removeAt - job.finishedAt}ms` }} />
            </div>
          )}
        </div>
      </div>
    </li>
  )
}

export const render = ({ jobs, connection, now }) => (
  <div className="tov">
    <style>{CSS}</style>
    <ul className="tov-list">
      {jobs.map((job) => (
        <Card key={job.id} job={job} connection={connection} now={now} />
      ))}
    </ul>
  </div>
)

// Every selector is scoped under .tov so nothing leaks into other widgets.
const CSS = `
.tov {
  --tov-card-bg: ${THEME.cardBg};
  --tov-text: ${THEME.textPrimary};
  --tov-text-2: ${THEME.textSecondary};
  --tov-accent: ${THEME.accent};
  --tov-done: ${THEME.done};
  --tov-stale: ${THEME.stale};
  --tov-track: ${THEME.track};
  --tov-radius: ${THEME.radius};
  --tov-shadow: ${THEME.shadow};
  width: ${CARD_WIDTH}px;
  font-family: -apple-system, BlinkMacSystemFont, "Helvetica Neue", sans-serif;
  color: var(--tov-text);
  -webkit-font-smoothing: antialiased;
}
.tov *, .tov *::before, .tov *::after { box-sizing: border-box; }
.tov-list { list-style: none; margin: 0; padding: 0; }

/* Collapsing wrapper: animating grid rows 1fr -> 0fr shrinks the card's real height. */
.tov-item {
  display: grid;
  grid-template-rows: 1fr;
  transition: grid-template-rows ${LEAVE_MS}ms ease, opacity ${LEAVE_MS}ms ease, transform ${LEAVE_MS}ms ease;
}
.tov-clip { min-height: 0; }
.tov-leaving { grid-template-rows: 0fr; opacity: 0; transform: translateY(-12px); pointer-events: none; }
.tov-leaving .tov-clip { overflow: hidden; }

.tov-card {
  position: relative;
  overflow: hidden;
  margin-bottom: ${GAP}px;
  padding: 16px;
  background: var(--tov-card-bg);
  border-radius: var(--tov-radius);
  box-shadow: var(--tov-shadow);
  animation: tov-enter 600ms cubic-bezier(0.16, 1, 0.3, 1);
  transition: box-shadow 200ms ease;
}
.tov-is-stale .tov-card { box-shadow: var(--tov-shadow), inset 0 0 0 1px var(--tov-stale); }
.tov-is-done .tov-card { box-shadow: var(--tov-shadow), inset 0 0 0 1px var(--tov-done); }

.tov-head { display: flex; align-items: baseline; gap: 8px; }
.tov-title {
  flex: 1; min-width: 0; margin: 0;
  font-size: 15px; font-weight: 600; line-height: 1.3;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
.tov-desc {
  margin: 4px 0 0; font-size: 13px; line-height: 1.35; color: var(--tov-text-2);
  overflow-wrap: anywhere;
}
.tov-badge {
  flex: none; padding: 1px 8px; border-radius: 999px;
  font-size: 11px; font-weight: 600; letter-spacing: 0.02em;
  color: var(--tov-card-bg); background: var(--tov-stale);
}
.tov-badge-done { background: var(--tov-done); }

.tov-progress { margin-top: 12px; height: 4px; border-radius: 2px; overflow: hidden; background: var(--tov-track); }
.tov-fill { height: 100%; background: var(--tov-accent); transition: width 250ms cubic-bezier(0.65, 0, 0.35, 1), background-color 250ms ease; }
.tov-is-stale .tov-fill { background: var(--tov-stale); opacity: 0.6; }
.tov-is-done .tov-fill { background: var(--tov-done); }

.tov-footer { display: flex; align-items: center; gap: 8px; margin-top: 12px; }
.tov-footer-text { flex: 1; min-width: 0; font-size: 12px; color: var(--tov-stale); }
.tov-is-done .tov-footer-text { color: var(--tov-done); font-variant-numeric: tabular-nums; }
.tov-actions { display: flex; gap: 6px; flex: none; }
.tov-btn {
  padding: 3px 10px; border: 0; border-radius: 999px; cursor: pointer;
  font: inherit; font-size: 12px; font-weight: 500;
  color: var(--tov-text); background: var(--tov-track);
}
.tov-btn:hover { filter: brightness(1.3); }
.tov-btn:active { transform: scale(0.96); }
.tov-btn-warn { color: var(--tov-card-bg); background: var(--tov-stale); }

/* Countdown line along the bottom edge of a finished card. */
.tov-drain { position: absolute; left: 0; right: 0; bottom: 0; height: 3px; }
.tov-drain-bar {
  height: 100%; background: var(--tov-done); opacity: 0.7;
  transform-origin: left; animation-name: tov-drain; animation-timing-function: linear; animation-fill-mode: forwards;
}

@keyframes tov-enter {
  from { opacity: 0; transform: translateY(-10px); }
  to   { opacity: 1; transform: translateY(0); }
}
@keyframes tov-drain {
  from { transform: scaleX(1); }
  to   { transform: scaleX(0); }
}
`

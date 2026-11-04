# Runtime Workers

The backend has one long-lived Bun background Worker thread for Reflow and
`runWorker`, not a child process and not a pool. HTTP stays on main. The
`node:worker_threads.Worker` API is implemented by Bun; acceptance checks verify
that main and worker have the same process ID and different thread IDs.

## Startup And Ownership

1. Main opens the application database, applies application schema migrations,
   and starts the dedicated bootstrap with only an absolute database filename.
2. The worker opens its own application DB handle and settings repository. It
   initializes the existing execution runtime, copies/verifies legacy journal
   history, finalizes the legacy schema, and starts Reflow.
3. Only then does the worker send `ready`. Main constructs a **new** `BooksRepo`
   after that handshake, so it cannot retain a pre-migration cached legacy flag.
4. Main binds HTTP only after readiness. The worker continues the existing
   outbox scheduler and moves startup cover watermarking, metadata enqueueing,
   and pending curation off main. The existing asynchronous Plex keepalive stays
   on main; its credentials never cross the worker protocol.

The worker protocol carries readiness/stop/failure signals and scalar diagnostics,
not settings, credentials, native database handles, model providers, or serialized
job handlers. Logs remain worker stdout/stderr. Production uses the fixed local
`worker-entry.ts`; trusted bootstrap injection is only an in-code test seam.

`BackgroundWorker` permits one owner per configured app DB path in the current
process. Concurrent starts/restarts coalesce; another owner is rejected. This is
not a distributed deployment lock: continue running one backend per app database.

Reflow remains execution authority. Application jobs, durable outbox transactions,
command receipts, run IDs, generations, cancellation, owner-token fencing, and
the single worker's domain lanes are unchanged. HTTP continues writing intent to
the application outbox; it does not call or message an engine to enqueue jobs.

## Deadlines And Recovery

- Startup waits at most 60 seconds for actual runtime readiness, not merely worker
  creation. Failed startup cleans up the thread before reporting failure.
- Stop first requests cooperative drain (5 seconds), then bounded termination
  (2 seconds). Replacement is allowed only after the thread's exit is confirmed.
  If termination is unconfirmed, ownership remains held and replacement fails.
- The server retains an 8-second shutdown deadline. If cleanup cannot finish, it
  exits rather than spawning a second engine or closing a worker's native handle.
- An unexpected worker exit makes HTTP unavailable and exits the backend with a
  failure status. There is no automatic in-process crash loop. The existing
  container supervisor may restart the application. Explicit controller restart
  uses the same application/journal files after confirmed exit.
- Reflow recovers unfinished work under its existing leases and checkpoints.
  Production lease duration, retry policy, SQLite journal/reader 5-second busy
  timeouts, and outbox reconciliation behavior are not changed.

Both application handles opened through `openDatabase` set
`PRAGMA busy_timeout = 2000` before schema work. This is SQLite's built-in wait,
not a custom retry wrapper. No settings or scheduler framework were added.

## Verification And Limits

`test/integration/background-worker.test.ts` uses actual Bun Workers, the actual
Reflow runtime/scheduler and temporary file-backed SQLite databases. Fixtures
replace only job bodies/maintenance, without network or model calls. It covers
legacy migration, main-thread outbox execution, CPU work and a real worker write
lock while actual HTTP health reads remain responsive, 2-second app write timeout,
crash/checkpoint recovery with stable run/job/generation IDs, cancel/retry fences,
duplicate-owner rejection, restart, startup failure, and forced-stop deadlines.
Child-server checks also cover fatal worker loss (one engine, nonzero exit) and
cleanup after HTTP port binding fails.
The existing `server-execution.test.ts` also exercises the production bootstrap,
real import/publication, HTTP serving, and SIGTERM shutdown.

This isolates synchronous worker work from HTTP's JS event loop; it does not make
SQLite asynchronous or remove lock contention. A main-thread write can still
block for the configured two seconds, then fail. Deferred write transactions can
also surface `SQLITE_BUSY_SNAPSHOT`; there is intentionally no new retry policy.
WAL readers normally continue while the worker holds a write transaction.

Long synchronous work can still delay the worker's own Reflow heartbeat. A native
crash can affect the shared process, and forced termination cannot undo external
filesystem/network effects. Existing durable steps and fences remain essential.
Bun documents [Workers](https://bun.com/docs/runtime/workers) as experimental,
particularly termination; the local lifecycle tests are not a general guarantee
against every native termination edge case.

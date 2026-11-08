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
Repository transactions reserve write intent with `BEGIN IMMEDIATE` before
reading state. This lets the busy timeout apply before a snapshot is taken,
instead of failing a later read-to-write upgrade after another thread commits.
Transactions remain synchronous and bounded; network and filesystem awaits
stay outside them. Nested operations retain SQLite savepoint rollback semantics.
Application, movie, TV and torrent-use adapters share `immediateTransaction`.
Async callbacks are rejected before entry; returned promises/thenables roll back
instead of silently committing work before its continuation finishes.

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

## Resource Admission

Registration, canonical storage and publication use the shared owner-aware
`withKeyedMutex` primitive. Nested acquisition must follow registration -> storage
-> publication, then ascending key within a class. Reentry and order inversions
throw immediately. FIFO waiters can be cancelled without unlocking the active
callback; even an aborted owner retains the mutex until its callback exits.
`mutexDiagnostics` exposes held times and waiter counts for backend diagnostics.

Effect handlers execute in an async-local scope with a cancellation signal,
cooperative progress checkpoints and stable per-invocation operation identities.
Starting another workflow step or durable sleep inside a handler (or while holding
a keyed mutex) fails before reserving lanes. Transmission requests and queued
mutex waits inherit the effect's cancellation signal. Optional step deadlines
request cancellation but never race a still-running callback or free its locks.

The worker reports scalar liveness every second. Main reports a failed worker
after 60 seconds without a heartbeat and still refuses replacement until exit
is confirmed. A handler without a checkpoint for five minutes is diagnosed once,
not automatically unlocked or retried: a slow external operation is not proof
of deadlock. Internal runtime diagnostics identify the step, held mutexes and
waiter counts. Healthy heartbeats do not erase stalled-effect evidence.

Failed torrent-use calls retain their durable retirement protection. Replays of
the same step reuse the same admission identity, including after a process
restart, and remove it only after the operation succeeds. Sibling calls and new
cancelled-job generations have distinct identities. Old unknown admissions are
not released by a TTL, a worker heartbeat, or another operation's success.

Resource-lane contention parks a run with a durable Reflow sleep **before**
entering its next step handler. Waiting maintenance must not consume all 20 run
slots and starve unrelated acquisition, import, or reuse work. A lane is reserved
synchronously at admission and released when the actual handler finishes,
including retry backoff and cancellation. Cached step results retain their old
names and do not repeat effects. An immediate in-process retry must reacquire its
lane before doing work; it never runs an effect under another owner's lock.

Scheduled latest-state observations reuse an outstanding periodic job across
wall-clock slots and restarts. Existing obsolete periodic runs with no effects
are completed with a `coalescedInto` receipt. Runs with prior step history,
explicit commands, scoped jobs and backfill payloads are not discarded. This
drains old scheduler backlog without deleting journal history or cancelling
requested acquisitions.

Long synchronous work can still delay the worker's own Reflow heartbeat. A native
crash can affect the shared process, and forced termination cannot undo external
filesystem/network effects. Existing durable steps and fences remain essential.
Bun documents [Workers](https://bun.com/docs/runtime/workers) as experimental,
particularly termination; the local lifecycle tests are not a general guarantee
against every native termination edge case.

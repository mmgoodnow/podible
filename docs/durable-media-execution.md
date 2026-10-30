# Durable media execution

All background operations are Reflow function workflows, pinned to
`reflow-ts@0.11.0`: acquisition, download, import, reconciliation, full-library
refresh, metadata hydration, cover generation, and chapter analysis.
Handlers checkpoint meaningful stages rather than wrapping the entire operation
in one opaque durable step.

## Storage boundary

- Domain data stays in `podible.sqlite`.
- `jobs` remains the existing Activity/RPC record and transactional outbox.
- `job_executions` links that stable ID to a generic engine run and fences
  application writes from superseded executions.
- Reflow stores runs, steps, attempts, leases, and durable waits in a separate
  `execution.sqlite` alongside the application database. Back up both files
  while workers are stopped or using coordinated SQLite-aware snapshots, not
  independent live copies of the main files without WAL.
- Preserve configuration-directory transcript/curation artifacts and the
  library's generated-cover artifacts too. Recorded steps reference these files;
  restoring only the execution database is not a complete workflow backup.

The outbox bridge uses an enqueue idempotency key derived from job ID. A crash
before or after enqueue cannot lose the operation or create two engine runs.
Release/download creation and download/import handoffs commit atomically in
the domain database. These transactions do not pretend to include Reflow's
separate connection.

## External effects and publication

Execution is at least once, not exactly once. Before retrying a snatch, compare
the torrent's derived infohash with rTorrent's download list. Network errors
are not treated as evidence that a torrent is absent. Domain releases retain
their unique infohash and provider/GUID identities.

Each acquisition selection has a stable manifestation identity. Each import
job has a stable asset publication identity across branches and retries. Hardlinks can be prepared
again, but asset rows/files and derived book metadata publish together. The
write guard checks cancellation and the application ownership token after
asynchronous preparation, inside the publication transaction.

Completed decisions replay their saved results. Explicitly retrying a failed
job resumes its failed steps; restarting a cancelled operation gets a fresh
execution generation, fencing off the cancelled run. Starting a new acquisition also creates fresh research.
Successful existing assets and transcripts are never removed by migration.

## Execution and Compatibility

`runWorker` is now only the transactional-outbox bridge. There is no second
worker claiming rows or resetting running jobs at startup. Reflow owns retries,
leases, and durable waits. Job attempts and terminal failure remain public
Activity projections; explicit retries resume the recorded run. Existing job
IDs are preserved, and SQLite no longer constrains operation names with a
domain-specific type enum. Application code still validates registered types.
Do not run an old worker against the same database during rollout.

Normal five-attempt steps use one-second exponential backoff. Longer retry
budgets use thirty-second linear backoff: twenty attempts span 95 minutes of
waiting, rather than six days of uncapped exponential delays. A failed book's
metadata step does not prevent later books from hydrating; engine control-flow
signals still propagate without being turned into domain failures.

Resource lanes remain in-process policy for this single-server deployment:
an exclusive refresh cannot inspect hardlinks during import publication.
Parallel callbacks belonging to one execution share its lanes, so transcription
chunks and chapter-boundary agents retain their internal parallelism. Leases
fence stale owners, but these lanes are not distributed resource locks. Do not
horizontally scale workers without replacing the lane coordination.

Metadata hydration checkpoints each book's fetch, cover download, and metadata
publication. Refresh snapshots directory discovery and checkpoints each source's
probing/publication; manifestation and asset creation are atomic. Reconciliation
rechecks current releases and creates import handoffs transactionally.

Cover generation checkpoints the generated image, watermark, and publication.
Image bytes live in filesystem artifacts; publication uses execution-specific
paths and never replaces a cover selected while generation was in progress.
A crash after the remote API succeeds but before its response is saved can
still repeat the paid request; durable execution cannot make that remote effect
exactly-once without provider idempotency.

Chapter analysis checkpoints paid chunks, EPUB input, audible-node selection,
individual judged boundaries, and publication. Successful transcript/chapter
artifacts remain available while replacement work runs or fails. The journal
contains artifact references instead of full transcripts or agent evidence.
The transcript cache remains a domain artifact for reuse across independent
jobs, not a separate worker checkpoint protocol.

Acquisition is globally limited to one active workflow, imports and chapter analysis to two;
download waits release engine capacity. Cancellation cannot undo a torrent
already handed to rTorrent, but it prevents later publication by the cancelled
execution. Prepared, unpublished hardlinks may remain after a cancelled import.

## Acceptance

`bun test test/integration/media-execution-crash.test.ts` starts actual child
processes against file-backed SQLite, real application handlers and imports,
a fake model, and local indexer/rTorrent services. It kills/restarts after
search, selection, a partial multipart snatch, rTorrent success before the
checkpoint, import handoff, and asset commit before the checkpoint. It also
freezes a worker, allows another process to reclaim the lease, and resumes the
stale process before publication. Assertions cover search counts, external
loads, ordered manifestations, unique assets, cancellation and job status.

The HTTP end-to-end scenario runs against the durable worker, checking
downloadable EPUBs, audio range/stream URLs, feeds, and reconciliation. No
paid model calls or production downloads are used by these tests.

Additional acceptance tests cover all eight workflow types, retry exhaustion,
parallel sibling callbacks versus conflicting jobs, scanner rollback/replay,
cover/metadata checkpoints, chapter artifact replay, and successful transcript
preservation. Unit-level journal tests complement, rather than replace, the
file-backed Reflow and subprocess crash tests.

## Production Verification (2026-10-30)

Normal CD deployed the full migration and review fixes at `fbfe82e`.
Watchtower's obsolete Docker API default was repaired with API 1.44 in the
host's persistent Compose configuration. Shared CD commit `mmgoodnow/cd@600216b`
restricts update requests to the calling repository's image; the successful
rollout scanned and updated only Podible. No manual application deployment or
hotpatch was used.

The pre-rollout online backup remains at
`/opt/docker/config/podible/backups/pre-durable-20261030T1658Z`.
Production is at migration 32. Both databases report `integrity_check=ok` and
zero foreign-key violations. Counts remain 62 books, 142 manifestations, 126
assets, 759 asset files, and 138 releases.

A controlled authenticated `import.reconcile` RPC created job 193. It completed
as Reflow run `6f4ac96e-6631-44b8-9cee-da63b18a1e9e`, with persisted completed
`job-input`, `reconcile-targets`, and `operation-finish` steps, each attempted
once. The check first verified that no downloaded releases needed importing;
no downloads, paid calls, reimports, or retranscriptions were initiated, and
media counts were unchanged. The temporary verification session was removed.
Crash, retry, parallel transcription, and publication behavior are covered by
local file-backed acceptance tests rather than destructive production tests.

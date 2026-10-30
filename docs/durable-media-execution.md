# Durable media execution

Acquisition, download, and import are Reflow function workflows, pinned to
`reflow-ts@0.11.0`. They checkpoint individual searches, agent decisions,
manifestation creation, each snatch, download observations, import source,
file selection, and publication. A handler is not one opaque durable step.

## Storage boundary

- Domain data stays in `podible.sqlite`.
- `jobs` remains the existing Activity/RPC record and transactional outbox.
- `job_executions` links that stable ID to a generic engine run and fences
  application writes from superseded executions.
- Reflow stores runs, steps, attempts, leases, and durable waits in a separate
  `execution.sqlite` alongside the application database. Back up both files
  while workers are stopped or using coordinated SQLite-aware snapshots, not
  independent live copies of the main files without WAL.

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

## Rollout and remaining machinery

The legacy worker must exclude the three migrated types both when recovering
running jobs and when claiming queued jobs. Existing queued/running media jobs
can then be adopted by the bridge without changing their public IDs. Do not
run an old worker and the new runtime against the same database during rollout.

This is not a wholesale removal of `jobs`: refresh, reconciliation, metadata,
cover generation, and chapter analysis still use the legacy worker. Their
recovery and checkpointing have not been migrated. The existing job-type enum
also remains for API compatibility. Reflow itself has no domain-specific
schema or job-type enum. The bridge and public Activity projection are the
transitional compatibility boundary.

The server shares the existing in-process lane lock set between durable step
callbacks and legacy handlers. In particular, a filesystem refresh cannot scan
hardlinks while an import is preparing to publish them. This is transitional
single-server compatibility, not a new distributed lock service; the remaining
legacy worker is not safe to horizontally scale by itself.

Acquisition is globally limited to one active workflow, imports to two;
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

The existing HTTP end-to-end scenario runs against both workers, checking
downloadable EPUBs, audio range/stream URLs, feeds, and reconciliation. No
paid model calls or production downloads are used by these tests.

## Production verification status (2026-10-29)

A read-only production snapshot at schema 29 migrated locally to schema 31
with all original fields unchanged across 62 books, 142 manifestations, 126
assets, 759 asset files, 138 releases, 183 jobs, 17 stored transcripts and 38
chapter-analysis records. Foreign-key verification reported zero errors.
The deployed rTorrent adapter successfully returned its download list (9,358
hashes), validating the external reconciliation protocol without snatching.

Live workflow verification is blocked, not completed: GitHub's image build
succeeds but the CD Watchtower update fails. The production Watchtower logs
show Docker client API 1.25 rejected by a daemon requiring API 1.40 or newer.
Production is still revision `34cbb9a40b4bd1bd767d134a7065e66c2432a676`.
No manual rollout, hotpatch, production migration, or bulk refresh was made.
Repair normal CD before performing a controlled live workflow smoke test.

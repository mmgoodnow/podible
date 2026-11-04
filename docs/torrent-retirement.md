# Automatic Orphan Torrent Record Retirement

The continuous stewardship worker can retire a repeatedly unregistered **orphan
record** from Podible's private Transmission daemon. It never deletes seed files,
canonical content, bindings, releases, history, or other clients' records. There
is no cross-seed action and no direct HTTP removal mutation.

## Admission And Scope

`createOrphanRetirementFence(repo, {globalMaintenanceHeld})` is the concrete default
fence, not a caller-supplied eligibility boolean. Read-only `inspect` does not
require a held global lane; `claim` and `assert` do. The dispatcher must give
`torrent_stewardship_collect` the existing global `*` resource lane. The retirement
hook is invoked **outside** the completed collection step, then each
`retirement:execute` callback acquires its own global lane through Reflow. Checking
`activeLocks` between steps would incorrectly disable the hook.

Parent producer integration must remain installed:

- Before async daemon use, acquire `admitTorrentUse(repo, {clientId, infoHash},
  operationId)`. The parent `withTorrentUse` / `withDatabaseTorrentUse` wrappers
  hold this durable token across `ensureAdded` and binding commit. Success-only
  `finishTorrentUse` releases it; ambiguous failures retain it for reconciliation.
- Book reserve, attach and orphan reclaim, and movie/TV bindDownload call
  `assertTorrentBindingWritable` or the raw DB guard
  `assertTorrentDatabaseWritable(db, ref)` in their binding-write transaction.
  `assertTorrentAdmitted(db, clientId, hash)` is the equivalent scalar helper.
- Settings mutation calls `assertNoPendingTorrentRetirements(repo)` inside its
  write transaction. Relevant private-engine identity mutations must honor the
  same claim boundary. These guards perform no network I/O.

An orphan has no current book, attached release, or release referencing its hash;
or, for screen media, no movie/TV acquisition-download binding. Asset-file source,
identity or content paths below the seed directory also block eligibility. The
targeted consumer reader also checks related pending publications and explicit
version source references. `readConsumerCount(db, binding)` is bridged by
`BooksRepo.readTorrentRetirementConsumers(binding)`; it performs parameterized
COUNT/EXISTS/path-prefix queries, not full admin-table scans. More than 2,000
unrelated rows does not block an orphan. Unavailable schema/projection or an
overflow of the bounded relevant-job list still blocks; truncation never means
absence. Participating async producers
are counted from durable use tokens. The global lane excludes worker acquisition,
publication, import and maintenance callbacks throughout execution.

## Evidence

Retirement requires all of the following, freshly rechecked before dispatch:

- Exact private client identity, stable hash, expected numeric daemon record ID,
  media label, exact download directory and canonical ownership markers.
- Full completion: non-checking stopped/seed-queued/seeding state, zero wanted
  bytes remaining, `sizeWhenDone == totalSize == haveValid`, `haveUnchecked == 0`,
  and no local-storage error. A partial wanted-file download is not complete.
- At least 2,592,000 **actual daemon `secondsSeeding`**, never torrent age or
  elapsed wall time inferred by Podible. Missing or regressed counters block.
- Three durable qualified observations, each with every current tracker,
  including backup trackers, explicitly reporting unregistration. Every tracker's
  own announce timestamp must advance by at least 15 minutes between samples.
  Recollecting cached results, advancing only one tracker, timeouts, generic HTTP
  errors, missing metadata and mixed success cannot qualify. The final tracker
  set must match the certificate, and current timestamps cannot regress.
- Complete zero-domain-consumer proof, zero producer admissions, no competing
  retirement, and unchanged configuration/binding revisions under the global lane.

The existing primary-only stewardship streak is never removal authority.
Separate bounded all-tracker certificates are persisted atomically with the
collector's observations. Only opaque digests and typed timing/count facts are
stored; no tracker URL, passkey, raw tracker response, daemon password, session
token, metainfo, or seed filesystem path is exposed by the retirement API.

## Transactions And Replay

`app_state` holds per-client/hash admission state: `useRevision`, admitted users,
claim generation, unresolved claim, and a retired tombstone. Claim/index and the
`planned` retirement intent commit in one synchronous application transaction.
No daemon request runs under a SQLite writer. Claims never expire on a timer.

The state machine is `planned -> dispatching -> uncertain | retired`. A safe
pre-dispatch cancellation or daemon-session change may produce `aborted` instead.
`dispatching` is persisted **before** a removal may enter the network. From then
on a lost acknowledgment, crash or cancellation cannot open admission. A retry
first reads the exact record: confirmed absence completes retirement without
another remove; the same unchanged record may be retried under the original
claim. A changed daemon identity/session or replacement numeric record is not a
broader replay target. Cancellation after dispatch permits read-only completion
reconciliation but does not issue another remove.

A `planned` intent may be aborted after session change only after proving its
original binding, identity and owned fence are unchanged. No remove had been
dispatched, so clearing that claim is safe. Session change alone never clears
`dispatching`/`uncertain` claims.

Transport performs a single `torrent-remove` with the pinned numeric ID and
`delete-local-data:false`. The captured daemon session token is sent directly;
HTTP 409 is **not** automatically renegotiated/retried for removal. Read-only
handshakes may renegotiate. This uses the official
[Transmission RPC session and removal contract](https://github.com/transmission/transmission/blob/4.0.6/docs/rpc-spec.md).

Confirmed retirement preserves a permanent per-hash tombstone: existing missing
record detection cannot silently admit/re-add the retired hash. This slice does
not implement tombstone clearing or broad retirement of linked media. Intent,
binding and observation history remain available for inspection.

## API And Verification

Admin-only typed tRPC / JSON-RPC methods are `torrentRetirement.preview({source?,
offset?,limit?})` and `torrentRetirement.get({operationId})`. Preview is read-only,
uses the default concrete non-owning fence, and reports private-engine
configuration, all-tracker evidence qualification, eligibility and blockers.
The RPC factory explicitly installs this fence without requiring a held worker
lock; preview neither claims admission nor contacts or mutates the daemon.
Get returns only the sanitized intent phase, timestamps and record-only outcome.
The automatic worker examines the collected batch, reconciles at most five
outstanding intents, and starts at most two new retirements per batch.

Focused tests exercise the concrete default fence, real producer wrapper,
transaction rollback, full-completion/seed/tracker gates, durable intent before
I/O, lost acknowledgment reconciliation, cancellation and restart fences,
read-only API behavior, protocol session/ID pinning, and the real default
dispatcher/Reflow hook. All daemon commands in tests use fixtures; no production
access, deployment, or live torrent controls are part of implementation testing.

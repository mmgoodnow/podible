# Retention and Recovery Foundation

This track records shared lifecycle facts and supplies a pure, conservative deletion audit.
It does not delete files, evict automatically, enqueue acquisition jobs, inspect disks or clients,
register migrations, or add settings, RPC, or worker behavior. TV, movies and books can adopt it
independently without a media-specific policy framework.

## Integration Contract

- Schema export: `RETENTION_SCHEMA_SQL` from `src/media/retention-schema.ts` creates
  `retention_resources` and `retention_events`. The parent registers a migration, then generates
  the shared DB types. This track does not edit the generated schema.
- Repository: `new RetentionRepo(db, beforeWrite)` from `src/media/retention-repo.ts` uses the
  **same caller-owned `bun:sqlite` connection** and its nested transaction/savepoint semantics.
  It implements the exported structural `TransactionalRepository` interface, `transaction<T>(fn: () => T): T`.
  Compile-only Kysely `dbSql.withTables` supplies local table types until shared codegen includes
  the tables; `typedGet`/`typedAll` execute synchronously on that connection. There is no second
  connection, Kysely execution driver, or independent migration runner.
- Resource identity: `{ namespace: string, id: string }`. Namespaces use lowercase kebab case,
  maximum 64 characters, and are open rather than an enum: `movie-target`, `tv-request`,
  `book-manifestation`, or future namespaces. IDs are opaque strings, 1-256 characters, with
  no edge whitespace/control characters. Convert numeric IDs explicitly; never reuse an ID
  for a different resource. Keys are not paths or filesystem ownership claims.
- `get(key): RetentionResource | null` and `events(key): RetentionEvent[]` are read-only.
  Events are ordered by resource revision, not timestamp.
- `request(key, change)`, `evict(key, change)`, `withdraw(key, change)` and
  `observe(key, { ...change, actualPresence })` return the new resource snapshot.
  `change` is `{ expectedRevision: number, reason: string }`; reasons are required, nonblank,
  control-free text up to 1000 characters. Use human-readable causal reasons, not secrets.
  Expected revision `0` means **create only**. Existing resources start at `1` and every call,
  even repeated intent/withdrawal/observation, appends an event and advances the revision.
  Stale/missing revisions throw `RetentionRevisionConflictError` with `key`, `expectedRevision`,
  `actualRevision` (zero when absent), without committing an event or state change.
- `beforeWrite` runs inside each write transaction, before its callback; reads do not invoke it.
  Observations and withdrawals require existing explicit intent. Initial `request` or `evict`
  initializes actual presence to `unknown`, never a guessed disk state.

The state row has `namespace`, `resource_id`, `desired_presence`, `actual_presence`, `revision`,
`updated_at`. Each event has the same key/revision/presence snapshot plus `action`, `reason`,
`created_at`. Event primary key is `(namespace, resource_id, revision)`; event rows reference
the state row with deletion restricted. There are **no request/book/movie/TV foreign keys**
and no request deletion cascade. The API has no resource or history deletion operation.

## Lifecycle Facts

| Operation | Desired presence | Actual presence |
| --- | --- | --- |
| `request` / re-request | `present` | unchanged (initially `unknown`) |
| `evict` | `evicted` | unchanged (initially `unknown`) |
| `observe` | unchanged | explicit `present`, `missing`, or `unknown` |
| `withdraw` | unchanged | unchanged |

`evict` is deliberate intent, not a report of successful deletion. `present` observations must
mean the domain's required manifestation/files are verified available, not merely that a path
or database row exists. Partial/inconclusive checks are `unknown`. Missing observations must
not reset desired presence. A withdrawal does not imply eviction: the parent determines
remaining demand and records a separate explicit eviction intent if appropriate. History
survives withdrawal or deletion of the external request, and is not reset on re-request.

`recoveryDecision(resource)` returns `{ action, reason }`:

- Desired `evicted`: `none`, regardless of observed presence. Never automatically reacquire.
- Desired `present`, actual `missing`: `recover`, describing accidental loss.
- Desired `present`, actual `present`: `none`.
- Untracked resource or unknown actual presence: `inspect`, not blind recovery.

This is a desired-recovery recommendation, not enqueue authorization. Parent recovery callers
must re-check the latest revision/intent, remaining demand, and active acquisitions in the
transaction that persists recovery intent. No stale snapshot should trigger acquisition after
a newer eviction. Re-request deliberately restores `present`, allowing recovery if still missing.

## Atomic Intent Boundary

Use the same connection for retention, domain requests, and durable outbox writes:

```ts
const retention = new RetentionRepo(db, beforeWrite);
retention.transaction(() => {
  // Domain request and outbox repositories must also use this exact db connection.
  requests.restore(requestId);
  retention.request({ namespace: "tv-request", id: String(requestId) }, {
    expectedRevision: revision,
    reason: "User re-requested withdrawn TV request",
  });
  outbox.persistAcquisitionIntent(requestId);
});
```

These names are illustrative adapters, not new APIs in this track. A thrown request/outbox
write, guard, CAS conflict, or event append rolls back all nested state/event writes. A parent
`BooksRepo.transaction` can likewise surround retention calls when it owns the same connection.
Callbacks must be synchronous; returned thenables are rejected and synchronous writes rolled
back. Never start asynchronous work inside them: a later promise continuation cannot be
rolled back. Persist the outbox before commit and dispatch external effects afterward.

## Pure Deletion Audit

`auditDeletion(input)` returns `eligible`, operator-readable `summary`, structured `blockers`
with codes/messages, `reclaimableBytes`, and `desiredRecovery`. It audits the entire proposed
removal set, with no partial-plan savings: any blocker yields `eligible: false` and zero
reclaimable bytes. Eligibility means only that the supplied evidence passes; it is not a
deletion authorization, durable execution plan, lock, or fresh filesystem verification.

The caller must establish **all** of these facts from authoritative current inventories:

- Intent is deliberately `evicted`, actual presence is verified `present`, watch/consumption
  state is known `watched` (including read/listened where applicable), and the count of active
  acquisitions is verified zero. Queued/retrying/running acquisition or import/publication work
  that can touch this set counts as active. Unknown watch state is never treated as watched;
  this track has no unwatched-deletion override.
- Every file is `verified-owned`: durable managed ownership and current identity agree.
  A filename, matching content hash, library location, or discovered torrent alone is not proof.
  Foreign/unknown files block. The module does not discover or adopt files.
- `fileId` uniquely identifies one physical allocation unit in this removal set, typically a
  verified filesystem/device+inode identity, not a pathname/content hash. Duplicate identities
  block instead of double-counting bytes. Callers must account for mounts, inode reuse and the
  full proposed unlink set; the string is an identity assertion, not independently verified here.
- `references: verified-unshared` means a complete logical reference **and** filesystem link/
  allocation inventory proves no reference outside the removal set. Include all media targets,
  versions, requests, torrent clients, publications/library views, hardlinks and other consumers,
  not just one media table. Unknown links/references and known sharing block. Removing a library
  hardlink while a seed hardlink survives is shared storage, not reclaimed allocation.
- `verifiedReclaimableBytes` is a nonnegative safe-integer count of actual allocation freed by
  the complete removal set, after hardlinks, sparse allocation, reflinks/clones, snapshots and
  filesystem-specific sharing are accounted for. It is **not** logical file size or torrent size.
  If these properties cannot be verified, supply `null`. Total must be safely countable and
  positive. Do not infer reclaimability from `nlink` alone on clone/snapshot filesystems.

These are evidence contracts, not facts currently gathered by this module. There is no
timestamp freshness heuristic: a future destructive consumer must revalidate under appropriate
coordination immediately before effects. If any inventory cannot be made complete, mark unknown.

### Seeding Evidence

`seeding` is `unknown`, `verified-not-applicable`, or `obligations` containing **all** affected
seed obligations. An empty obligation list is not proof of inapplicability. Use verified
inapplicability only when the complete inventory proves this removal affects no seed obligation.

For each obligation, provide its stable `id`, `remainingSeedableCopies`,
`verifiedSecondsSeeded`, `requiredSecondsSeeded`, and `otherRequirements`:

- A positive `remainingSeedableCopies` must prove another copy remains seedable **for that
  exact torrent/client/tracker obligation after this removal**, with its registration and
  seeding capability preserved. Swarm seed counts, a peer, a library hardlink included in the
  removal set, or an unregistered duplicate are not evidence. This can preserve an obligation
  without deleting its last seed copy even when past seeding duration is unknown.
- Unknown surviving-copy inventory blocks. Losing the last seed copy requires at least
  **2,592,000 verified seconds seeded (30 days)** and any stricter verified tracker time rule.
  `verifiedSecondsSeeded` must come from trusted, torrent-specific accumulated seeding history;
  neither time since added/completed, current `seeding` status, retention timestamps, nor upload
  bytes prove duration. Unknown/reset/incomplete history is `null` and blocks last-copy deletion.
- `requiredSecondsSeeded` is the verified tracker time rule (`0` only when explicitly none),
  not an assumed 30-day tracker policy. Unknown is `null` and blocks last-copy deletion.
  `otherRequirements` must be `verified-satisfied`; unknown or unmet ratios, tracker rules,
  retention promises, or other obligations block last-copy deletion. No tracker rules are
  guessed by namespace, path, age, or media kind. Thirty days is this module's floor, not a
  claim that every tracker's requirements end after thirty days.

## Verification and Deferred Work

Run `bun test test/unit/retention.test.ts test/unit/retention-repo.test.ts`. Fixtures install
the exported schema in isolated SQLite databases without requiring parent migration edits.
Tests cover lifecycle transitions, intentional versus accidental loss, withdrawals, revision
conflicts, persistence/reopen, guards, journal failure, same-connection request/outbox rollback,
physical sharing/bytes, consumption/acquisitions, and seed duration/history/requirements.

Parent-owned integration: migration registration, shared DB type generation, `BooksRepo`
accessor, settings/RPC/worker wiring, authoritative fact collectors and recovery dispatch.
Actual deletion and automatic eviction are intentionally absent from this first track.

## Read-Only Sidecar

`src/media/retention-service.ts` adds `RetentionService(repo, serverOnlyAdapters?)` using the
parent-owned `BooksRepo.retention` getter and registered schema (migration 47). Do not install
`RETENTION_SCHEMA_SQL` again after `runMigrations`. The sidecar does not own migration/type
generation, the RPC root, tRPC registration, UI wiring, or TV's unfinished repository contract.

`retentionRouter` from `src/rpc/retention-router.ts` exports only admin, read-only methods:

- `status({ afterTargetId?: number, limit?: number })` returns paginated resource details,
  `supportedNamespaces: ["movie-target"]` and explicitly unsupported TV/book namespaces.
  The default page is 20, maximum 100. `nextAfterTargetId` is null on the last page.
- `audit({ namespace: "movie-target", id: "123" })` accepts only a positive safe-integer
  target ID encoded as a string. Unknown keys/parameters, paths, desired-state changes, seed
  statistics and client-authored evidence are rejected. There is no eviction mutation.

Both return `mode: audit-only` and `deletionSupported: false`. Reads do not persist intent,
observations, jobs, events, or even missing settings defaults. They report `storedIntent`
separately from `observation` and calculate recovery against current observed presence without
writing it back. `displayState` distinguishes `present`, `missing`, `unknown`,
`intentional-absence` (eviction intent AND verified missing), and `eviction-intent-only`
(eviction intent but bytes still present). Never label intent alone as completed eviction.
No "enabled" setting or second credential surface is introduced.

### Request and Recovery Helpers

`ensurePresentIntent(repo, key, reason)` records explicit request/re-request intent with the
current CAS revision inside `repo.transaction`. Apply it to movie request/re-request and future
native TV request paths within the SAME transaction as domain requests and the durable outbox.
Do not call it from status, automatic recovery, reconcile, RSS matching, or worker retry: doing
so would incorrectly undo intentional eviction. Every explicit call records its causal event.

`allowRecovery(repo, key)` is a read-only **intent gate**, true only for stored desired `present`.
Untracked and deliberately evicted resources return false; it does not silently initialize
legacy state. Parent recovery/reacquisition guards must still check actual loss, active demand,
acquisition deduplication, and execution fencing. Apply the gate in the intent/outbox transaction
and again before effects. Explicit user re-request must restore intent BEFORE that gate.
This sidecar exports the helpers but does not edit movie/TV request or acquisition paths.

### Authoritative Filesystem Evidence

The first sidecar scope is **canonical movie objects only**, not download/seed-store removal,
retirement of library links, or a whole-resource deletion plan. It inventories all retained
versions acquired for a target plus its selected version. Presence is determined from the
complete selected-version manifest; no selected version or an incomplete manifest is unknown,
not proof of accidental loss. Missing an old retained version does not falsely hide a present
selected version, but still blocks auditing the full candidate object set as reclaimable.

Ownership requires a canonical absolute `movies.managedRoot`, the existing pinned movie-content
mount identity and `.podible-content.json`, a `managed_content_files` DB record with a UUID ID,
and its exact canonical `objects/files/XX/UUID.ext` layout. Each ancestor is checked without
following symlinks; objects are opened with `O_NOFOLLOW` and stat identity/size verified. Only
`ENOENT` under that pinned mount is reported as missing. Root/marker failure, symlinks, permission
errors, wrong file types, altered sizes, and unsafe DB paths produce unknown/inspection errors.
The root, file identities/link/allocation counters, view manifests, settings, intent and DB inventory are checked
again after asynchronous adapters; changes invalidate the observation. This is not a destructive
operation lock or a substitute for revalidation at a future execution boundary.

Bounded inventories use existing public `adminDbTableNames`/`adminDbTablePage` reads, never private
connection access or another SQLite connection. Any relevant table exceeding 5,000 rows or an
incomplete/changing inventory blocks the audit rather than treating omitted references as zero.
All tables with a `file_id` column participate in reference checks, including newly registered
TV tables without importing TV's unfinished types. Additional selected movie targets, pending
publications, book asset paths, managed view/pending manifests and real inode links are protected.
`databaseReferences` counts direct `file_id` rows; indirect target/view references are separate
blocker evidence. Missing/unreadable view ownership inventories remain unknown. Acquisitions
are checked conservatively across the inventoried media acquisition tables, not just this target;
queued/running work (even if an acquisition row says finished), absent jobs, or unknown lifecycle
data prevent a zero-active-work claim. This can over-block unrelated work in this first dry run.

This assumes durable DB registrations and managed manifests contain every *managed* logical
consumer. It cannot discover arbitrary unregistered symlinks outside managed views, establish
exclusive ownership of foreign trees, or prove global filesystem allocation exclusivity.
Hardlinks or remaining references outside canonical-only scope report zero reclaimable bytes.
Otherwise `allocatedBytes` comes from `stat.blocks * 512`, but `verifiedReclaimableBytes` defaults
to **null**, not logical file size or allocated blocks. A trusted server-only
`reclaimableAllocation(file)` adapter must independently account for snapshots/clones and
allocation sharing; its result must be a safe nonnegative count no larger than observed blocks.
There is no platform-independent verifier enabled by default. Aggregate verified bytes are null
if any candidate is unknown; the pure audit's executable-plan byte count remains zero whenever
any blocker exists. A positive storage count does not authorize deletion or change intent.

### Private Transmission and Plex

The default seed reader reuses the existing private Transmission identity and loopback RPC
credentials; it issues only `torrent-get`. It requires exact persisted `client_id` and infohash,
the exact `Podible Movies` label, the persisted `movies` download location, canonical real paths,
complete registered file lengths/bytes, and acceptable complete/stopped/seeding engine state.
Duration reads use Transmission's accumulated `secondsSeeding`, never `addedDate`/completion age
([Transmission 4 RPC specification](https://github.com/transmission/transmission/blob/4.0.6/docs/rpc-spec.md)).
The trusted server seam can inject a read-only RPC adapter; RPC clients cannot configure it.
Unknown identity/store, client/label/path mismatch, missing files or stats remain unknown.

Canonical adoption copies bytes out of the writable seed layout. Only when *all* bound torrents
retain verified complete files with different physical identities does the sidecar assert
`verified-independent-seed-store` and one surviving seedable copy per exact obligation.
This is not last-seed-copy deletion: the canonical-only audit would not remove seed paths or
registrations. Distinct inodes do NOT prove distinct disk extents; reclaimability still needs
allocation evidence. If that independent-copy proof is absent, no last-copy safety is assumed.
Unknown seed history blocks last-copy deletion; tracker requirements remain unknown, never
guessed from Transmission limits, age, labels, or the 30-day local floor. This sidecar does not
implement seed-store deletion or a tracker-policy integration.

Watch history intentionally defaults to **unknown** on the first dry run. A server-only
`watchHistory` adapter receives movie identity and the existing shared `movies.plexUrl`,
`movies.plexToken`, `movies.plexSectionId`. It should use GET global Plex history with verified
coverage across users; there is no Tautulli dependency. Only explicit `global-all-users`
coverage is accepted. Owner-only `viewCount`, unauthenticated/failed/partial history and unknown
coverage are never converted to all-user watched or to zero views. Adapter failures return
sanitized unknown reasons without echoing upstream messages or credentials. `createRetentionRouter`
can receive these trusted adapters during server composition; no adapter is populated from RPC.

Run `bun test test/unit/retention-service.test.ts` for real migrated SQLite/FS reference and
presence tests, atomic helper rollback, root/path fencing, seed identity/history/independence,
watch-history coverage, byte accounting, bounded paging and admin read-only RPC validation.

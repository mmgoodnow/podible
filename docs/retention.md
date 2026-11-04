# Retention and Recovery

Retention now includes a bounded disk-pressure cleanup loop for Podible-owned canonical movie
objects. The original lifecycle repository and pure audit remain reusable foundations; neither
deletes by itself. Only the durable movie execution path below performs deletion. TV objects,
books, torrent registrations, and download/seed stores are outside its destructive scope.

## Automatic Movie Cleanup

The scheduler queues one global `retention_cleanup` job per fifteen-minute slot when movie storage
is configured. It observes the movie filesystem first. Healthy or unknown space is a no-op:
no per-title Plex history lookup, provider search, seed inspection, intent change, or deletion.
This is not a periodic release-search/upgrade loop.

`AppSettings.retention` has just two settings:

```ts
{ minimumFreeBytes: 20 * 1024 ** 3, protectedMovieTargetIds: [] }
```

When available space is below the reserve, inspect at most 20 selected targets per pass by
default, rank the eligible inspected candidates by lowest global play count, then oldest last
viewing, then stable target ID. A persisted scan cursor lets later passes inspect other targets;
this is not a claim to have globally ranked unseen history. Execute at most three whole-target
plans per job and measure space again after each. Stop as soon as the observed reserve is met,
space becomes unknown, no inspected candidate is eligible, or the target budget is exhausted.
The loop never treats expected file bytes as newly available space.

Both automatic and explicit operator eviction require all of these conditions:

- Existing tracked retention intent, a selected owned movie version, and a complete bounded
  object/reference inventory. Untracked legacy content is never silently adopted or evicted.
- Target is not in `protectedMovieTargetIds`. The most recent explicit request, considering
  both movie request rows and retention request events across users, is at least 30 days old.
  A fresh request is protected even when another user had already requested the same target.
- Known watched history with verified all-user Plex visibility, positive known retained play
  count, known last viewing, and a fresh `activePlayback: none`. Unwatched, owner-only, partial,
  unknown, currently playing, or unresolvable playback evidence blocks action. Watched means
  at least one completed history event, not that every authorized user has watched.
- No relevant unfinished acquisition, active import/library refresh, pending publication, or
  sharing outside this target's complete retained-version removal set. Repeated references
  inside the same complete target set are not external sharing; other targets/media domains,
  book asset paths, unknown view inventories, and extra hardlinks block.
- Pinned content mount and exact registered UUID object layout, regular files, stable physical
  identities, known positive reclaimable allocation, and independent surviving seed data.
  Current source/seed bytes must match canonical SHA-256 evidence; cached Transmission
  completion counters and same-size independent inodes alone are not surviving-copy proof.

Seed obligations are scoped through the removal set's exact file/version/acquisition bindings,
not every historical torrent ever associated with the target. Unrelated retired-version
sources are not affected by deleting a newly acquired object. All bindings of each affected
acquisition still participate, including retained failed/stalled siblings; those are not
mistaken for the verified source merely because the acquisition eventually produced a version.

No operator override turns unknown or unwatched evidence into permission. The 30-day last-seed-
copy floor and unknown tracker obligations remain unchanged. V1 never deletes a last seed copy:
it requires the independent owned seed store to survive with byte-backed coverage of every
canonical object being removed. Seed-store paths, torrent records, resume state, and tracker
registrations are not removed or controlled.

### Filesystem Space Evidence

The default action adapter supports Linux ext4 only. It combines the verified filesystem type
with complete logical-reference checks, a single inode link, pinned open-file identity, stable
size/timestamps, and observed allocated blocks. Sparse files are counted by allocation, not
logical length. Unsupported filesystems or unverified sharing remain unknown; there is no
generic `file size = freed bytes` fallback or client-authored reclaimability claim.

These are filesystem allocator bytes, not a promise of underlying thin-pool, hardware, or
block-device snapshot savings. Results retain filesystem availability before/after and
`observedAvailableDeltaBytes` separately from planned allocation. `attributedFreedBytes` stays
null because concurrent volume activity can increase or decrease the observed delta. Available
space after each action, not that delta or an expected byte sum, controls the pressure loop.

A predeployment read-only probe found the configured production movie root on ext4 (`0xef53`)
with roughly 59 TB available, comfortably above the default reserve. This supports a healthy
no-op expectation; it is not evidence that production deletion was exercised.

### Intent, Effects, and Recovery

Schema 50 installs `RETENTION_EXECUTION_SCHEMA_SQL` from `retention-execution-schema.ts`.
`retention_evictions` records an immutable resource/file plan, job linkage, creation time, and
eventual domain outcome. It does not duplicate Reflow status, attempts, leases, or retry fields.
The existing `BooksRepo.retention` uses the same application connection as requests and outbox.

An operator plan atomically records eviction intent, clears the selected movie version with
revision fencing, creates the durable job/outbox command, and stores the exact plan. A pressure
job stores each plan under its already durable job. Movie/request/version/file DB identities
survive; explicit re-request can restore desired presence and acquire or reuse bytes normally.
Eviction intent alone never sets actual presence to missing and never proves space was freed.

Reflow captures original per-object SHA receipts before the destructive execution step. The
global filesystem lane covers execution, while `withEvictionFence` additionally obtains an
immediate application SQLite writer transaction around only short synchronous guarded
rename/unlink effects. HTTP request/settings writes cannot land between the intent check and
those syscalls. No network request, hash, awaited operation, or custom retry loop runs under
that writer transaction; async/thenable effect callbacks are rejected.

Execution retires only the owned declarative movie view and revalidates the pinned mount,
intent, object identities, references, acquisition state, fresh watch/playback, allocation,
and independent seed bytes before deletion. Candidates move through an owned private staging
directory under the canonical content root and must still match their original durable SHA.
Staging ownership is published as a marker-bearing temporary directory via atomic rename;
an arbitrary preexisting unmarked directory is never blessed. Process interruption during
initialization, after presentation retirement, after staging, or after unlink is replayable.

A newer present request supersedes the eviction. Proven staged bytes are restored without
overwriting a new canonical file or a newer selected version. `supersedeEviction` preserves
that newer intent, records verified present/absent identities, restores the prior selection
only when its required files are available and no newer selection/publication exists, and
closes the old plan. Already-unlinked identities are retained as known absence for explicit
request recovery, not abandoned unfinished work or falsely restored bytes. Only verified
absence IDs become retirement tombstones; restored IDs do not. Later whole-resource planning
can therefore exclude intentionally absent historical objects without deleting their DB IDs.

Completion observes missing only after canonical and staged members are verified absent on
the still-pinned mount. A blocked, staged, or superseded operation is not reported as successful
eviction merely because its old desired state was evicted.

### Operator and Composition Contract

- Admin `retention.preview` accepts `{ targetId?, afterTargetId?, limit? }`; default limit 20,
  maximum 100. A selected target can be previewed even when overall space is healthy. Reads do
  not write intent, settings defaults, events, jobs, cursor, or files.
- Admin `retention.evict` accepts only `{ targetId, expectedRevision, reason }`, re-observes
  server evidence, and returns `{ evictionId, jobId }`. It accepts no path, watch/seed facts,
  byte estimate, or force flag. Existing job controls expose durable progress/retry.
- `previewRetention`, `queueMovieEviction`, and `RetentionAdapters` come from
  `src/media/retention-policy.ts`; `runRetentionCleanup(ctx, job, adapters?)` is in
  `src/media/retention-execution.ts`. A job payload is `{}` for pressure or `{ evictionId }`
  for a precommitted operator plan. Schemas/defaults are in `retention-policy-types.ts`.
- `BooksRepo.readSettings()` is typed and noninitializing. Adapter injection is server-only.
  Preview may use bounded Plex read-model caches; action validation uses
  `createPlexWatchHistoryReader({ cacheTtlMs: 0 })`. Current sessions are always fresh.

Run `bun test test/unit/retention*.test.ts test/integration/retention-execution.test.ts`.
Acceptance uses actual SQLite, owned temporary files/symlinks, exact fake private-client
inventory, and real Reflow restart tests. It covers healthy/dry-run no-op, ranking and measured
space stopping, atomic outbox rollback, protected/fresh/untracked/unwatched/unknown/shared
resources, byte-corrupt seed copies, cross-connection writer fencing, supersession, and crash
recovery. No production pause, deletion, seed-store mutation, or torrent control is part of
this verification.

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

## Foundation Verification

Run `bun test test/unit/retention.test.ts test/unit/retention-repo.test.ts`. Fixtures install
the exported schema in isolated SQLite databases without requiring parent migration edits.
Tests cover lifecycle transitions, intentional versus accidental loss, withdrawals, revision
conflicts, persistence/reopen, guards, journal failure, same-connection request/outbox rollback,
physical sharing/bytes, consumption/acquisitions, and seed duration/history/requirements.

The parent owns migration registration, shared DB type generation, `BooksRepo` accessors,
settings/RPC/worker wiring, and recovery dispatch. These foundation tests do not execute
deletion; the action-loop modules and acceptance tests described above do.

## Read-Only Sidecar

`src/media/retention-service.ts` adds `RetentionService(repo, serverOnlyAdapters?)` using the
parent-owned `BooksRepo.retention` getter and registered schema (migration 47). Do not install
`RETENTION_SCHEMA_SQL` again after `runMigrations`. The sidecar does not own migration/type
generation, the RPC root, tRPC registration, UI wiring, or TV's unfinished repository contract.

`retentionRouter` from `src/rpc/retention-router.ts` retains these admin, read-only methods
alongside the new preview/evict action endpoints described above:

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
Logical DB/view references block deletion but do not prove zero physically reclaimable bytes:
a published symlink does not retain an unlinked inode. Those bytes remain **null**. Zero is
verified only when an inode's link count exceeds the entire canonical candidate count, proving
at least one link must survive this removal set. Multiple links entirely within the candidate
set do not prove that; duplicate physical identities remain blocked, not counted as savings.
`allocatedBytes` comes from `stat.blocks * 512`, but `verifiedReclaimableBytes` defaults
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

An unconfigured `RetentionService` leaves watch history **unknown**. Application composition
now supplies the global Plex reader described above. A server-only
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

### Production Read-Only Validation (2026-11-03)

The deployed service was inspected with a SQLite read-only/query-only connection, repository
write guard, filesystem metadata reads and private `torrent-get` only. No intent, jobs, files,
configuration or download registrations were changed; the connection recorded zero changes.
The initial running build was `79737b5`; a final read-only audit independently confirmed the
deployed build was `1c0db9b`, with the same retention-service source and unchanged movie counts.
The byte-accounting correction above remains local and was not deployed by this sidecar.

- Four movie targets: three verified present; one unknown with no selected canonical version.
  All four movie intents were legacy/untracked; recovery stayed `inspect`, not auto-reacquire.
- The three canonical regular files matched the DB sizes and pinned content mount. Each had
  one inode link and one real published-view symlink resolving to the expected object. These
  logical references block canonical-only deletion; they are not proof of zero freed allocation.
- Three exact owned Transmission bindings retained complete, separately-inoded seed files
  under the canonical download root, with matching private client identity and exact owned
  label. Final observed seed durations were 25,007-25,168 seconds (about 7 hours), not 30 days. Surviving
  seed copies apply only to canonical-object removal, never seed-store deletion; tracker time
  and other requirements remained unknown, and separate inodes do not prove separate extents.
- Watch history was unknown for all four; no global/all-user Plex adapter is installed. No
  allocation-sharing verifier is installed, so the corrected present-object byte counts remain
  unknown, not file lengths or observed blocks. Every deletion audit was blocked.
- Concurrent TV canaries changed the global active-acquisition count between read samples
  (two, then three). The audit intentionally over-blocks on that activity; it does not infer
  target-specific use or claim the whole library was a transactionally frozen snapshot.

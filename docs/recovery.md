# Owned Movie And TV Recovery

Recovery is an explicit administrative operation, not a scheduled title-search
loop. Inventory and preview are read-only: they never initialize storage or
settings, alter retention intent, create requests/jobs, inspect torrent clients,
or contact Plex/indexers/catalogs. JSON-RPC and typed tRPC expose the service;
the admin SPA is at `/recovery`. Recovery does not install a worker or schedule
automatic downloads.

## API Contract

```ts
import { RecoveryService } from "../src/media/recovery";
import { recoveryRouter } from "../src/rpc/recovery-router";
// Schemas and RecoveryKey/RecoveryResource/RecoveryResult:
// ../src/media/recovery-types

const service = new RecoveryService(repo);
const page = service.inventory({
  namespace: "movie-target", // or "tv-target"
  afterTargetId: 0,          // optional; defaults to 0
  limit: 20,                 // optional; maximum 20
});
// { resources: RecoveryResource[], nextAfterTargetId: number | null }

const preview = service.preview({
  targets: [{ namespace: "tv-target", id: "1" }],
});
// { resources: RecoveryResource[] }

const result = service.recover({
  operationId: crypto.randomUUID(),
  targets: [{
    key: { namespace: "tv-target", id: "1" },
    expectedRevision: preview.resources[0]!.revision,
    action: "repair-view", // or "queue-acquisition"; must match preview.action
  }],
});
// { operationId, results: RecoveryResult[] }
```

All service calls are synchronous. The router exposes `inventory`, `preview`,
and `recover`, all administrator-only. The first two are marked read-only.
All request objects are strict. IDs are canonical positive safe-integer strings;
duplicate keys and batches over 20 are rejected. `movie-target:1` and
`tv-target:1` are unrelated resources. Request references use `movie-request`
and `tv-request` namespaces and retain their original IDs/withdrawal state.

Each resource returns `key`, `title`, opaque SHA-256 `revision`,
`targetRevision`, `intentRevision`, `desiredPresence`, namespaced `requestIds`,
`mount`, `canonical`, `view`, `activeAcquisitions`, `action`, and `reasons`.
Canonical files include `selected`, relative path, expected size, and observed
presence. View files include relative path and present/missing/conflict/unknown.
No credentials or upstream error bodies are returned.

Preview actions are `none`, `repair-view`, `queue-acquisition`, or `blocked`.
Command results contain `key`, `status` (`repaired`, `queued`, `unchanged`, or
`blocked`), `revision`, `jobIds`, and structured `{code,message}` reasons.
The result's revision identifies the **pre-action** evidence accepted by the
command, not a promise of post-action availability. Refresh preview afterward.
A replay returns the saved original result, not a fresh availability claim.

## Evidence And Actions

The mount check uses the existing retention/movie storage pin:
`movie-content-mount:sha256(resolve(contentRoot))`, matched against
`objects/.podible-content.json`. TV uses this same existing storage mechanism.
Root/ancestor canonicality, mount identity, and marker identity are rechecked.
A directory existing at a mountpoint is not evidence that owned storage is
mounted. Missing/replaced/uninitialized markers or symlinked ancestors block
recovery and make canonical presence **unknown**, never verified missing.
No recovery path calls `prepareMovieStorage` or initializes a new marker.

Retained `desiredPresence=evicted` always blocks recovery. Missing intent does
not imply demand. Withdrawn requests are never resurrected. Active acquisitions
and pending per-target publications block competing operations. Inventory does
not rewrite the retention repository's last persisted presence observation.

`repair-view` reuses selected, surviving owned canonical files and the existing
movie/TV declarative view renderers. It adds missing directories/symlinks and
durably merges their ownership into `.podible-view.json`. It does not remove
stale links, replace existing files/links, modify media bytes, promote another
version, scan Plex, control torrents, or invoke general movie/TV maintenance.
Foreign/conflicting paths, an unmarked nonempty view, mismatched view identity,
or a pending view manifest block repair. A missing empty view tree can be rebuilt.
`repaired` means local declarative links were verified, not Plex availability.
Presence checks establish canonical regular-file type/size/identity, not a fresh
media decode or content digest.

`queue-acquisition` requires retained present intent, active original requests,
a verified mount, no competing acquisition/publication, and no unsupported reuse
case. It calls `queueMovieAcquisition(..., "recovery")` or
`queueTvAcquisition(originalRequestId, "recovery")` inside the existing transaction.
It preserves original requests and intent and atomically commits acquisition,
job/outbox command, and recovery receipt. TV queues requests with unselected aired
episodes, including coverage invalidated by confirmed loss. This command itself performs no search or download;
explicitly queued existing acquisition may later perform its ordinary search.
There are no periodic searches or automatic recovery/download actions.

### Confirmed Selected-Content Loss

On a verified mount, absent selected canonical paths can be recovered through the
same explicit `queue-acquisition` action. Migration 51 adds `media_content_losses`,
a durable per-file absence record. `ContentLossRepo` fences target revision,
active demand, competing acquisition/publication and shared selections. In the
same transaction as the acquisition/outbox/receipt, it clears the missing movie
selection or only the exact lost TV file's episode coverage, observes missing
presence without changing desired presence, and queues view reconciliation.
Multi-episode coverage is invalidated together. Existing requests, versions,
surviving canonical bytes, torrent records and seed data are retained.

Old publication retries cannot reselect a tombstoned version. New acquisition may
retry a previously successful source whose adopted copy was lost, while prior
rejected releases remain excluded. Movies no longer require a quality upgrade
to replace absent bytes. TV reuses verified surviving pack data through its
existing exact source/provenance checks and imports into new canonical file IDs.
Unknown/missing mounts and changed/wrong-size/noncanonical files are not absence
evidence and remain blocked. Shared active selections require joint invalidation
and are blocked rather than partially updated. Repaired bytes appearing before
the command invalidate its preview revision.

### Unsupported Cases

Surviving nonselected versions still
block replacement acquisition when republication/reselection cannot be safely
verified through the existing APIs. Historical torrent/seed-store reuse is not
invented from size, title, last-known completion, or a pathname.

## Bounds And Fences

The narrow `BooksRepo.readRecoveryInventory` bridge delegates only read-only
queries to `recovery-query.ts`. Inventory pagination uses target ID keyset
`WHERE id > ? ORDER BY id LIMIT ?`. Preview uses target-scoped joins with explicit
LIMITs, not global admin table scans/counts. More than 5,000 unrelated episodes
does not disable recovery. One preview target is bounded to 20 requests,
100 acquisitions/versions, and 500 canonical/file-link/episode rows. Oversized
targets return a blocker rather than a partial absence claim.

Mutation is stricter: at most 20 canonical files, view paths, file links,
acquisitions, and versions per target; database/manifest metadata is capped at
64 KiB. Full inspection, domain rendering, and revision hashing occur **outside**
the writer transaction. Inside, one target-scoped database read (8 movie or 13 TV
fixed SELECTs), bounded job lifecycle reads, current intent/settings, mount
marker, and at most 20 exact canonical/view paths are revalidated. No tree scan,
media hashing, network I/O, async wait, or full `inspect()` runs under the writer.
The operation binding is written before this fence to acquire the SQLite writer
even in WAL mode, preventing concurrent application intent commits through the
filesystem effects. Each target has its own transaction, not one library-wide
writer transaction. Filesystem fsync latency still depends on storage; the local
focused test enforces a 250 ms writer ceiling, not a production latency guarantee.

An operation UUID is durably bound to its exact ordered batch, expected revisions,
and actions. Reusing it with different input is `operation-conflict`. Successful
per-target results are stored in `app_state` in the action's transaction; reopen
and lost-response replay return those receipts without repeating effects.
Blocked results are not cached as success. Storage errors remain errors; an
outbox failure rolls back the acquisition, job, and success receipt together.
The operation binding survives a failed target so an exact retry remains valid.
Filesystem effects cannot be rolled back with SQLite: if a crash/error occurs
after additive links but before receipt commit, fresh preview may be required.
No success is fabricated, no bytes are removed, and a fresh command can converge
the additive view state.

Receipt storage has a hard limit of 256 operation bindings, each with at most 20
results. When full, new UUIDs return `operation-budget`; existing operations stay
retryable. Receipts are not silently expired/forgotten, which could defeat
idempotency. Receipt archival/compaction is deliberately unsupported here.

## Verification

```sh
bun test test/unit/recovery.test.ts test/integration/recovery.test.ts
bun run typecheck
```

Tests use real SQLite and temporary owned files/markers/views, including reopened
receipts, modern transactional outbox rollback/retry, movie/TV ID collisions,
intentional eviction, withdrawal, missing/replaced mounts, actual canonical loss,
foreign/symlinked paths, surviving-content reuse, strict admin APIs, large
unrelated episode inventory, query/writer budgets, and exclusion of another WAL
writer during the filesystem fence. They do not claim production Plex recovery,
production network acquisition success. Real Reflow fixtures verify equal-quality
movie recovery and exact TV episode reimport without a new snatch, plus atomic
loss/outbox rollback and retained request/history/surviving-file identities.

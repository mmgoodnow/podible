# Whole-Season TV Retention

This slice supports **explicit durable eviction of a complete single-season TV
target**. Every selected and originally requested episode for that target must
belong to the same complete canonical season. It does not remove a watched subset
of a season or one season from a multi-season target. Movie retention is unchanged.

## Integration And API

Parent composition registers `TV_RETENTION_SCHEMA_SQL` from
`src/media/tv-retention-repo.ts` in a migration, exposes
`BooksRepo.tvRetention = new TvRetentionRepo(connection, beforeWrite, getJob)`, and
registers `tv_retention_cleanup` with `runTvRetentionCleanup(ctx, job, adapters?)`.
The job uses the same global `"*"` managed-storage lane as movie retention. No
migration number is assumed by these modules. Existing `media_content_losses`
capability is checked by table name; missing loss evidence blocks removal.

`createTvRetentionRouter(adapters?)` exports administrator-only `audit`, `preview`
and `evict`. Parent owns JSON-RPC/tRPC, scheduler and UI route registration.
`TvRetentionAudit({client?})` is the standalone UI; its default client uses typed
`rpc.tvRetention.audit.query` and `.evict.mutate`. It has no polling or automatic
actions and retains the same operation UUID when retrying an unacknowledged write.

```ts
const service = new TvRetentionService(repo, serverOwnedAdapters);
const scope = { targetId: 12, scheme: "tvdb:aired", seasonNumber: 1 };
const audit = await service.audit(scope); // read-only
const preview = await service.preview({ seasons: [scope] }); // maximum 3
const queued = await service.evict({
  ...scope,
  expectedRevision: audit.revision,
  operationId: crypto.randomUUID(),
  reason: "Administrator explicitly requested whole-season target eviction",
});
// {status: "queued" | "blocked", evictionId, jobId, blockers}
```

Scopes and commands are strict. RPC callers cannot submit paths, watched/seed
claims, skip guards, or force eviction. The audit reports `safetyEligible`,
`removalSupported`, `eligible`, structured safety/execution blockers, complete
membership, exact episode history, owned canonical files and seed obligations.
`mode: "audit-only"` describes the read operation, not a disabled executor.
Only `eligible=true` exposes the explicit eviction action. Queueing is not a
claim that the season has already been removed.

## Safety Evidence

- Whole-season membership is resolved against a fresh authoritative catalog and
  compared by stable work/episode keys, not titles or ordinal equality. Unknown,
  future, ephemeral, duplicate or partial membership blocks the entire season.
- The complete season must be requested and selected. Every selected/requested
  episode for the **whole target** must be within that season. Another season,
  including an unselected/future request, blocks whole-target retirement.
- Fresh or unknown demand for any member/policy target protects the whole season
  for the existing 30-day grace period. Original request IDs are never deleted or
  withdrawn by retention.
- A settings hash fences configuration edits between planning and removal.
- The existing pinned content marker and canonical root must match. Missing,
  replaced, uninitialized or symlinked storage is unknown, not removable bytes.
- Canonical UUID layout, exact file coverage, physical identity/size/timestamps,
  current loss tombstones, cross-target/domain references and inode links are
  checked. A file spanning another season is never split or partially removed.
- Reclaimable allocation requires the existing ext4 proof or an explicit trusted
  server verifier. File length or allocated blocks alone are not storage savings.
- Plex history is GET-only and paginated with completeness/change checks. A
  multi-account global-authority witness is required; owner `viewCount` is not
  global history. Every exact episode ID needs a retained play across all users,
  not just some watched episodes. Duplicate work copies across Plex sections are
  included. Unknown/active playback blocks removal. No aired-ordinal fallback is
  used for retention episode identity.
- Every relevant exact private Transmission binding, ownership label, hash,
  location and complete seed file is verified. Canonical and seed byte digests
  must match in independent storage. Tracker requirements remain unknown, and
  time since added is never seed history. Last-copy removal stays blocked;
  verified independent seed copies are required. No torrent control/removal RPC
  is sent, and the seed-store files/registrations remain intact.

## Durable Queue And Execution

The final audit's private context freezes the target/intent revisions, complete
inventory, demand fingerprint, settings/mount identity, selected mappings and
verified seed-member proofs. After the awaited audit, re-reading new revisions
does **not** bless new demand. The context is compared again before and after file
inspection, and queueing checks the frozen revisions inside an immediate SQLite
transaction. Re-request or another-season races leave intent/selection untouched.

Queueing atomically records the existing `tv-target` eviction intent, retires its
exact whole-target selected episode mappings, inserts the eviction plan and
creates the existing job/outbox command. Acquisition/RSS already honor this
whole-target intent. The operation UUID binds the exact command; repeated input
returns the same job, while changed input conflicts. Outbox failure rolls all
domain changes and the plan back together.

Real Reflow checkpoints every original canonical SHA-256 proof before entering a
single globally locked destructive/recovery step. Only exact owned view symlinks
are retired. Content is moved into a same-filesystem, content-marker-owned staging
directory and fsynced before unlink. Unmarked/wrong-owner staging and foreign
view files are not adopted or removed.

Before each file's staging and unlink, the executor checks whole-season canonical
membership, original demand/selection/configuration, exact bindings, current
file's independent matching seed members, and fresh global history/playback.
The durable plan stores exact seed-member names and byte proofs discovered by the
initial whole-plan audit. Consequently fresh seed hashing is linear in season
bytes, not an entire-season rehash for every file. Playback is observed after
the slow seed hashing. Short synchronous SQLite fences protect rename/unlink
against concurrent application demand changes; no network or media hash runs
inside those writer fences. Staged bytes are rehashed against the durable proof,
never blessed by a resumed pathname/size alone.

Restart resumes the original plan and completed proof checkpoints. New explicit
present demand supersedes eviction: proven staged bytes are exclusively linked
back without overwriting newer canonical content, and surviving healthy episode
selection is restored when still compatible. Already unlinked members remain
honestly missing, with the new demand retained and independent seed store intact.
Completion records verified absent object IDs and filesystem availability; free
space deltas are not attributed as physical pool savings.

## Bounds And Limits

Preview is at most three seasons. A season has at most 50 members and 20 selected
files. Read inventory uses target/season-scoped joins and explicit LIMITs, not
global row-count ceilings. Execution additionally requires bounded request and
acquisition histories. Unknown/oversized inventories block rather than authorize
a partial removal.

The default byte-proof budget is **32 GiB**, with a **60-second** deadline per
bounded proof operation (server adapters may lower it; timeout is capped at two
minutes). Reads use 1 MiB buffers. The initial audit/proof covers the full plan;
each deletion revalidates only that file's exact independent seed members while
still checking whole-season demand/membership/playback. Multiple relevant source
bindings must each pass. Budget/deadline exhaustion blocks safely and may require
operator review on slow/large storage.

Unsupported scopes remain explicitly blocked: multi-season targets, incomplete
seasons, missing/lost/foreign/shared bytes, unknown actual reclaimability,
unverified global history/playback or seed independence, and absent durable
repository/execution integration.

## Bounded TV Pressure

The parent-owned `tv-retention-pressure.ts` / `tv_retention_pressure` path runs
every five minutes under the same global storage lane. It first observes fresh
disk and pinned mount state: healthy disk, unknown disk, or unavailable mount is
a no-op. Under pressure it considers at most three supported single-season
targets, performs these same whole-season audits, ranks eligible low-play/older
last-watch candidates, and calls the same revision-fenced `service.evict` to
queue the durable executor. The shared threshold is
`retention.minimumFreeBytes`.
There is no pressure bypass for complete membership, watch/playback, seed
proofs or whole-target scope. This does not introduce periodic title searches or
partial-season/multi-season removal.

## Verification

```sh
bun test test/unit/tv-retention.test.ts test/integration/tv-retention.test.ts
bun run typecheck
```

Tests use actual SQLite, owned temporary bytes/views/seed copies and the real
Reflow journal. They cover atomic queue rollback/deduplication, complete eviction,
restart after view retirement/staging creation/file staging/unlink, newer demand
before queue and during execution, preservation of surviving healthy episodes,
playback appearing after staging, exact-ID/watch/seed/mount/loss blockers, and
linear hash-byte/read counts. A sparse 513 MiB file exercises the realistic default
hash budget without allocating a season-sized in-memory buffer. Plex/Transmission
and catalog responses are synthetic upstream seams, not production verification.

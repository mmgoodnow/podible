# Cross-Seeding POC

## Implemented Scope

Book, movie and TV RSS entries also route to retained content, independently of
missing formats, episodes and quality upgrades. The semantic agent identifies plausible works;
the server then fetches the original retained candidate metainfo and builds its
actual file layout from registered canonical objects. There are no virtual-season
torrents, periodic per-title searches, arbitrary client download URLs, or hardcoded
release-group preferences.

Movie and TV acquisition selectors also identify likely copies of the selected
release during search. `assessCrossSeedability` counts distinct normalized tracker
names in agent-proposed groups, rather than counting duplicate search results.
Semantic grouping must preserve encode, edition, tracks and episode coverage;
size and identity diagnostics are estimates, not proof of matching payload bytes.
Among otherwise equivalent viable options, greater estimated tracker coverage is
preferred. Within that group, known lower download-volume factors/freeleech can
break a source tie. This is not a quality improvement and cannot replace an
equivalent existing playback version.

`movies.crossSeedPreference` is `"tie-break"` by default or `"off"` to disable
this ranking for both movie and TV selectors. It is a selection setting, not part
of quality-policy/target identity. No particular group (including NTb) gets a bonus.
The selector still records known matching copies when ranking is off.

The chosen source and other matching offers are captured server-side before the
download starts. After canonical import, **before waiting for Plex verification**,
each alternate gets an idempotent reuse job. Those jobs use the same optimistic
executor as RSS and do not block publication on unsuccessful/unsupported reuse.
They survive app restarts without depending on an RSS row or a repeated search.
Retained TV packs carry that source association across later import batches.
Book selectors retain equivalent candidates separately for each ordered release
part, preserving translation, narration, edition and part coverage. Audio and
ebook use separate book-scoped inventories. Their source association is the
book release ID, and import replay queues retained copies idempotently without
creating additional manifestations or changing existing selections. Manual
snatches do not invent matching copies absent server-owned selection evidence.
Book copy evidence is committed independently before client dispatch, then
associated atomically with the published release/download job. A crash before
publication can recover it even without Reflow or the original runtime input.
Already-imported books keep consuming category feeds for reuse, without starting
new title searches or missing-format acquisitions.
Copies skipped before registration can be reconsidered once registered file coverage grows;
the same content revision cannot enqueue duplicate attempts. Pending, successful
and filesystem-unsupported attempts are not requeued for every import batch.
Already registered skipped/stalled attempts retain their original operation and
layout ownership; automatic recovery of those transfers is not implemented here.
Snapshots keep provider download URLs private; commands reference opaque retained
IDs. A failed/unplayable primary download does not dispatch its copies.

These are complementary triggers: completed acquisitions cover new local bytes,
and RSS covers new indexer releases against existing local bytes. This is not an
exhaustive discovery guarantee: search omissions, RSS gaps, undiscovered older
copies and unsupported storage/client capabilities remain possible.

## Explicit Library Backfill

On November 8, normal CD deployed optimistic reuse as running build `6b74b4a`.
The authenticated `crossSeed.startBackfill` call at 17:39 UTC created request
`9a544937-e239-4a06-afda-c17d00ffafb7`, job 34787, with 150 results per provider.
Its durable snapshot contains 141 eligible format/content targets. This records
the start of the explicit pass, not a claim that every search or seed completed.

Activity's **Find more seeds** control starts a durable, admin-only library
backfill. It snapshots every eligible retained audiobook/ebook, movie target and
TV target, including libraries larger than 100 targets. The snapshot and inbox
high-water mark survive retries and restarts; cancellation/retry does not quietly
expand the original scope.

It first considers retained candidates in 25-release batches, then searches the
configured indexers explicitly. Identical media/query/provider searches are
shared across targets (for example, seasons of the same show). Search results
and per-target/provider progress are saved incrementally. Provider failures are
isolated: healthy formats/providers and retained candidates continue, while
temporary failures sleep durably until their retry time. Completed searches are
not repeated when retrying unfinished work. Repeated start clicks reuse the
active backfill.

A new explicit pass can reconsider old failed/skipped attempts. Pending attempts
and seeding receipts with an existing owning registration are not queued again.
Candidates still go through the normal metainfo, ownership, seed-only admission
and client safeguards; **queued candidates are not successful seeds**. Stopping
the search does not cancel already queued reuse jobs or remove torrents.

Searches default to at most 150 results per query/provider, configurable from
1 to 150 in the start command. The existing reuse-policy and supported-client
bounds remain in force. This is an explicit bounded historical search, not an
exhaustive tracker archive crawl or a scheduled per-title search. No RSS cursor
or historical gap is reset or declared repaired by this operation.

The reuse command snapshots a versioned policy. Defaults require 80% reusable
bytes, at most 512 MiB of missing pieces, and at most a 100 GiB candidate. Admin
RPC callers can tune these independently per command. Those are POC defaults,
not an asserted optimal objective function. Identity, path ownership, isolation,
retained demand and publication rules are safety checks,
not tunable scores. Feed matching queues all matches durably; execution limits
active reuse transfers to two independent targets, with same-target work
serialized. There is no initial full payload hash pass. Per-client/hash registration is serialized separately so concurrent
targets cannot overwrite each other's seed-only intent or layout ownership.

## Storage And Verification

New initial downloads also use canonical-first torrent symlink views, rather than
copying a completed movie/TV into canonical storage. Their objects remain private
while downloading and surrender writer authority before becoming reuse sources.
See [canonical downloads](canonical-downloads.md). Existing initial downloads are
not migrated automatically.

Transmission receives a paused torrent in an isolated symlink layout, with every
file unwanted. The executor does not hash payloads or request a daemon recheck.
It requires every candidate member to have an exact-size source and waits until
file mtimes satisfy Transmission's native fast-seed admission. Transmission
checks the first piece on admission, then checks other pieces when peers request
them. This is **optimistic seeding**, not independent full-payload verification.
Incomplete layouts and missing sidecars are explicitly unsupported by this
no-bulk-check adapter; it never silently falls back to a scan or full copy.

Completed, sealed canonical download members can enter the reuse pool before
every member has been imported or published. The inventory opens only registered
owned layouts and requires finalized-object or saved source-identity evidence;
it does not enumerate arbitrary directories or adopt legacy regular downloads.
Sources must be complete. A partially published pack is not treated as a
partially downloaded pack.

The server journals source mount/inode/size/mtime/ctime and every published link;
replay validates these identities without rereading payloads. Receipts explicitly
mark admission as `verification: "assumed"`; assumed reuse does not inflate the
independently verified download-savings counter. Existing explicit verification
helpers remain available to tests, but automatic reuse does not invoke them.

Scheduled stewardship monitors precise lazy piece-mismatch errors, not ordinary
tracker warnings or corrupt network downloads. It persists a candidate/source-plan
blacklist before stopping and removing that exclusively owned reuse registration.
Removal is reconciled across crashes and never deletes files or primary torrent
bindings. Changed source identity produces a new plan and may be tried again.

An independent per-hash seed-only intent is fsynced before torrent addition.
Before every supervised daemon launch, the startup guard verifies metainfo
identity and restores unwanted flags, destination and ownership labels. Missing
or damaged resume files are rebuilt paused, without inventing piece proofs.
This is an application safeguard, **not OS-enforced read-only access**. Symlink
permissions do not protect their targets; bypassing the private application's
control path can invalidate these assumptions.

Adding another torrent does not create or promote a library version. Plex keeps
its explicitly selected existing version, including equivalent-version seniority.
The original torrent, canonical bytes and Plex view remain intact. This adapter
drops incompatible registrations rather than repairing shared files.

Plans and outcomes are persisted as domain receipts; Reflow owns steps, sleeps,
leases and retries. Lost add acknowledgments reconcile the same owned hash and
layout. Registrations retain their owning operation, feed retries retain their
matched target snapshot, and live daemon state is rechecked rather than treating
a saved start/completion observation as current truth. Replay revalidates layout
receipts, and changed configuration/demand fences execution. Daemon admission is
not represented as independent piece verification.

## RPC

The non-CoW Linux application lab (`test/lab/symlink-reuse-inner.ts`) executes
the actual selected-copy/Reflow path: exact symlink publication, zero downloaded
bytes, unchanged canonical bytes and library selection, then supervised restart.
It is a required runtime CI gate. The separate two-daemon
`symlink-seeding-inner.ts` lab also proves actual peer upload, no repair of a
corrupted shared target, and safe pre-spawn restoration of damaged DND/labels,
missing resume files and malformed resume files. These are local synthetic
checks, not production download evidence.
The same application lab also runs with `--audio` and `--ebook`: actual book
canonical hardlinks, format-scoped source inventory and book ownership go
through the same planner, materializer, client and restart guard. Both passed
with zero downloaded bytes and unchanged manifests on a non-CoW Linux tmpfs.

All methods below require admin authentication. The library-backfill methods
also have typed tRPC bindings for Activity. Operation/report UUIDs are immutable idempotency
keys. Supply exactly one of `releaseId` (a server-retained RSS inbox ID) or
`selectionCopyId` (a server-retained acquisition-copy UUID), never a URL.

```ts
crossSeed.preview({ operationId, scope: { media: "movie" | "tv", targetId }, releaseId, policy? })
// Read-only candidate plan; no layout or daemon writes.
crossSeed.create({ operationId, scope, releaseId, policy? })
// Durable intent/outbox receipt with jobId.
crossSeed.create({ operationId, scope, selectionCopyId, policy? })
// The same executor for a copy retained during release selection.
crossSeed.status({ operationId?, limit? })
// Saved intents/outcomes only; no daemon I/O.
crossSeed.reportNuked({ reportId, sourceTorrentId, reason })
// Trusted defect evidence; queues normal movie replacement where demand remains.
crossSeed.startBackfill({ requestId, limitPerProvider? })
// Saved library-search intent/progress. Repeated clicks share active work.
crossSeed.backfillLatest({})
// Latest summary, including after reloading Activity.
crossSeed.backfillStatus({ requestId, offset?, limit? })
// Per-target/provider progress, paginated independently of search execution.
crossSeed.cancelBackfill({ requestId })
// Stop searching; already queued reuse attempts continue.
jobs.retry({ jobId })
// Resume failed/cancelled backfill without repeating completed provider searches.
```

Confirmed nuke reports preserve source lineage, exclude the defective hash/offer
and its canonical bytes from new reuse, and allow a verified non-downgrading
same-tier movie replacement. Current playback remains available until exact Plex
publication succeeds. A warning, temporary tracker outage, or unregistered torrent
is **not** enough to assert a nuke. Automatic nuke discovery and TV nuke replacement
are not implemented by this POC.

## Verified Local Evidence

Current no-bulk-check acceptance uses the Linux application and two-peer labs
above. The following November 5 evidence describes the former explicit-verification
CoW implementation, not the current automatic executor; partial repair is not
supported by the current optimistic adapter.

`bun run test/lab/cross-seed-host.ts` runs a real stable Transmission 4.1.4 daemon
in Docker against host-native APFS CoW files, with synthetic metainfo, HTTP
trackers and a webseed. It uses only temporary isolated roots and removes its
container and files afterward. The image defaults to `podible-ci-lab:verify`.
`PODIBLE_LAB_DISCOVERY=selection bun run test/lab/cross-seed-host.ts` exercises
the persisted search-selection/completion trigger instead of the RSS candidate
receipt. The selected-copy variant was also verified November 5 with the same
2,097,152 reused / 238 downloaded bytes and unchanged canonical/Plex selection.

November 5, 2026, repeated successful runs demonstrated:

- One renamed media file differs from the original by one byte at its tail;
  the candidate adds a private tracker sidecar.
- **2,097,152 verified bytes reused; 238 bytes downloaded**, confirmed by the
  daemon's `downloadedEver`, out of 2,097,390 candidate bytes.
- The candidate completes and seeds. One tracker announces successfully while
  another is unavailable; mixed coverage is not treated as total failure.
- Original canonical bytes, selected version, exact Plex-facing symlink and the
  old incomplete torrent record remain unchanged.

SQLite/Reflow tests additionally cover crash after add before checkpoint, false
completion/corrupt bytes, authentication, immutable commands, retained-only input,
unsupported CoW, all-match RSS fanout, target-scoped locking, and late nuke fences.
The layout tests assemble episode files from multiple sources against an actual
season-pack manifest, including piece boundaries crossing files.

## Deliberate POC Limits

- The runtime file pool is registered book/movie/TV canonical content. Unpublished
  partial-download bytes and newly created seed-only variants are not yet fed
  back into discovery. The rescue canary uses retained canonical bytes while
  another torrent is incomplete; it does not establish arbitrary partial-torrent
  rescue.
- Candidate matching is conservative for ambiguous equal-sized files. It will
  skip a candidate rather than guess an unproven association.
- Existing confirmed-orphan record retirement remains in force; this change does
  not detach library-bound registrations, delete seed bytes, or claim physical
  CoW extent/reclaimability accounting.
- Discovery comes from RSS, acquisition search/selection, explicit retained
  candidate commands and bounded library backfill, not an exhaustive tracker crawl.
- An already-owned infohash is not registered twice. Automatically merging
  additional private tracker announcements into that registration is not yet
  supported; such attempts report `skipped`, not new tracker coverage.
- Planner identities leave room for v1/v2/hybrid; actual metainfo validation,
  piece proof and Transmission execution remain v1-only.

These limits distinguish a working P16/P20/P21/P25/P26 proof from full torrent
stewardship, v2 support, or zero-recheck migration at scale.

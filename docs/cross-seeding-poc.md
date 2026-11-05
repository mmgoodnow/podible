# Cross-Seeding POC

## Implemented Scope

Movie and TV RSS entries also route to retained content, independently of missing
episodes and quality upgrades. The semantic agent identifies plausible works;
the server then fetches the original retained candidate metainfo and builds its
actual file layout from registered canonical objects. There are no virtual-season
torrents, periodic per-title searches, arbitrary client download URLs, or new
release-group preferences.

The reuse command snapshots a versioned policy. Defaults require 80% reusable
bytes, at most 512 MiB of missing pieces, and at most a 100 GiB candidate. Admin
RPC callers can tune these independently per command. Those are POC defaults,
not an asserted optimal objective function. Identity, path ownership, isolation,
verified completion, retained demand, and publication rules are safety checks,
not tunable scores. Feed matching queues all matches durably; execution limits
active reuse transfers to one without globally locking unrelated acquisitions.

## Storage And Verification

Transmission receives a paused torrent in an isolated derived layout. Media is
cloned using **forced native CoW**, never a writable hardlink or an ordinary-copy
fallback. Sidecars belong to the candidate. Unsupported filesystems produce an
explicit `unsupported` result before torrent addition. Production filesystem
capability must be checked separately from the successful APFS canary below.
Production mount inspection on November 5 identified both media roots as ext4.
An isolated two-byte forced-clone probe on the TV mount returned `ENOTSUP`, with
all probe files cleaned up. Writable reuse is therefore a verified unsupported
constraint there, not merely a filesystem-type inference. A reflink-capable
storage arrangement or a separately proven safe client/storage strategy is needed
to use this path there; no full-copy fallback was used.

This POC independently hashes the bounded candidate pieces, runs the daemon's
candidate recheck, applies the missing-piece budget, and independently verifies
completion. It does not recheck the entire library, but it is **not yet a
recheck-free implementation**. The pure core supports verified read-only
symlinks; the Transmission runtime intentionally does not advertise the no-write
guarantee necessary to use them.

Adding another torrent does not create or promote a library version. Plex keeps
its explicitly selected existing version, including equivalent-version seniority.
The original torrent, canonical bytes and Plex view remain intact. Repair writes
can affect only the candidate's private CoW variant.

Plans and outcomes are persisted as domain receipts; Reflow owns steps, sleeps,
leases and retries. Lost add acknowledgments reconcile the same owned hash and
layout. Registrations retain their owning operation, feed retries retain their
matched target snapshot, and live daemon state is rechecked rather than treating
a saved start/completion observation as current truth. Replay revalidates layout
receipts, changed configuration/demand fences execution, and a false daemon
completion fails independent piece verification.

## RPC

All methods below require admin authentication and use both JSON-RPC and typed
tRPC through the existing router. Operation/report UUIDs are immutable idempotency
keys. `releaseId` is a server-retained RSS inbox ID, not a URL.

```ts
crossSeed.preview({ operationId, scope: { media: "movie" | "tv", targetId }, releaseId, policy? })
// Read-only candidate plan; no layout or daemon writes.
crossSeed.create({ operationId, scope, releaseId, policy? })
// Durable intent/outbox receipt with jobId.
crossSeed.status({ operationId?, limit? })
// Saved intents/outcomes only; no daemon I/O.
crossSeed.reportNuked({ reportId, sourceTorrentId, reason })
// Trusted defect evidence; queues normal movie replacement where demand remains.
```

Confirmed nuke reports preserve source lineage, exclude the defective hash/offer
and its canonical bytes from new reuse, and allow a verified non-downgrading
same-tier movie replacement. Current playback remains available until exact Plex
publication succeeds. A warning, temporary tracker outage, or unregistered torrent
is **not** enough to assert a nuke. Automatic nuke discovery and TV nuke replacement
are not implemented by this POC.

## Verified Local Evidence

`bun run test/lab/cross-seed-host.ts` runs a real stable Transmission 4.1.4 daemon
in Docker against host-native APFS CoW files, with synthetic metainfo, HTTP
trackers and a webseed. It uses only temporary isolated roots and removes its
container and files afterward. The image defaults to `podible-ci-lab:verify`.

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

- The runtime file pool is registered movie/TV canonical content. Unpublished
  partial-download bytes and newly created seed-only variants are not yet fed
  back into discovery. The rescue canary uses retained canonical bytes while
  another torrent is incomplete; it does not establish arbitrary partial-torrent
  rescue.
- Candidate matching is conservative for ambiguous equal-sized files. It will
  skip a candidate rather than guess an unproven association.
- Existing confirmed-orphan record retirement remains in force; this change does
  not detach library-bound registrations, delete seed bytes, or claim physical
  CoW extent/reclaimability accounting.
- New discovery comes from RSS or explicit retained-candidate commands, not an
  exhaustive historical cross-tracker search.
- Planner identities leave room for v1/v2/hybrid; actual metainfo validation,
  piece proof and Transmission execution remain v1-only.

These limits distinguish a working P16/P20/P21/P25/P26 proof from full torrent
stewardship, v2 support, or zero-recheck migration at scale.

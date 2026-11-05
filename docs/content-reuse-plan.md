# Content Reuse And Torrent Lifecycle Plan

Proposed next phase, not implemented or production-verified. First build anime
confidence through the normal One Piece request path (see P14 in the
[roadmap](feedback-and-roadmap.md)). No new download, daemon change or migration
is authorized by this document.

## Desired Outcomes

One set of rules should support cross-seeding, shared-byte stall rescue, episode
to season-pack consolidation, nuke-driven replacement and safe retirement.
Library quality, retained demand and seed obligations are constraints, not scores
that extra tracker coverage can override.

Optimize useful live seed coverage while limiting new downloads, physical storage
and churn. Do not maximize raw torrent size: overlapping packs can count the same
bytes repeatedly and reward unnecessarily large encodes. Diminishing benefit from
extra trackers must preserve the accepted policy that four healthy trackers and
one unavailable is generally fine.

Season packs should be ordinary candidate layouts over existing content, not a
separate upgrade subsystem. Episode-torrent unregistration can motivate a new
layout, but is not required: a useful pack can appear while episode torrents are
still registered. A packaging improvement need not replace a better-quality
library version.

## Separate The Decisions

1. Discover plausible reuse using content/episode identity, sizes, names and
   known torrent metadata. Episode files may come from several existing torrents.
2. Produce an explicit file-to-layout plan with estimated reusable bytes, missing
   data, verification evidence, new storage and tracker benefit. Likely matches
   and verified matches remain distinguishable.
3. Materialize the candidate's names/layout without exposing shared media bytes
   to writes. Small tracker-specific sidecars may be independent. Client
   capabilities govern whether optimistic/lazy verification is available.
4. Reconcile the owned client record durably; observe verification, completion and
   registration separately. Repair/download work requires safe writable storage.
5. Select the preferred media variant and retire obsolete registrations or bytes
   only after reference, demand and seed-obligation checks. No inference that an
   accepted add command proves matching bytes, or that record removal frees data.

Exact matches should share one stored media file across layouts. For media that
differs even by one byte, the latest preference permits CoW variants to gain seed
coverage rather than forcing a single physical variant. A write to one variant
must not change another variant's bytes. Verify actual filesystem clone support
and client write behavior; do not silently fall back to allocating a full copy.
Shared CoW extents also require honest incremental-storage and reclaimability
accounting. Do not patch a shared inode through a new torrent layout.

Plex always receives the one version selected by the library's durable record,
through the existing declarative view. A new torrent variant does not change that
selection. Break equivalent-version ties by seniority (oldest verified retained
version by persisted identity, not filesystem mtime; stable ID as a final
tie-break), avoiding publication churn for tracker-only gains. An explicit
quality upgrade can still promote a newer
verified version. Keep torrent registrations, byte variants and library selection
as distinct associations; CoW adds a variant, not a second authority for Plex.

Avoid routine full-library rechecks. Reuse valid prior evidence and support
client-side lazy verification where its actual behavior is established. Hash
checks remain necessary when evidence is insufficient; filename/size equality is
not a proof. A hash mismatch can affect only one piece despite identical lengths.
Do not assume every client reports a bad optimistic match as a prompt local error.

## BitTorrent v2

Support v1, pure v2 and hybrid identities explicitly. Retain full SHA-256 v2
infohashes and associate both hashes of a hybrid without duplicating its torrent
record. Client-native identifiers remain separate from protocol identities.
Preserve original metainfo and validate file trees, piece layers and hybrid
consistency rather than stripping unfamiliar fields.

v2's per-file Merkle roots and file-aligned pieces make renamed/repacked identical
files easier to identify independently of the enclosing torrent. They do not
prove that unverified local bytes match, and the roots are not ordinary whole-file
SHA-256 digests. Different roots can still describe largely matching files;
safe CoW reuse can retain both variants while library selection remains stable.
See [BEP 52](https://www.bittorrent.org/beps/bep_0052.html).

Current Podible is v1-only: `src/media/metainfo.ts` rejects `meta version`,
`src/media/transmission-client.ts` advertises `metainfoVersions: [1]`, and several
identity validators require 40 hexadecimal characters. Full v2 client support
must be demonstrated, not inferred from accepting hybrid metainfo; Transmission's
[v2 feature request](https://github.com/transmission/transmission/issues/458)
remains open at the time of this review. Keep Transmission for now; qBittorrent
can be a future private-client adapter, not an immediate migration. Protocol
identities, reuse plans and library selection must not depend on Transmission's
RPC shape. Advertise real protocol, layout and verification capabilities rather
than pretend all backends implement them. Do not change the bundled daemon as
part of planning.

## Ideas To Reuse From Cross-Seed

Inspected the local checkout at `/Users/mmgoodnow/src/cross-seed/cross-seed/src`:

- `searchee.ts`: `createVirtualSeasonSearchee` assembles episode-file candidates
  across torrents; Podible should use its existing explicit episode identities
  rather than make title parsing the primary domain model.
- `decide.ts`: strict, name-independent and partial file matching provide useful
  candidate discovery. Its partial size/piece calculation is a reuse estimate,
  not cryptographic verification.
- `action.ts`: `linkAllFilesInMetafile` builds the candidate's expected layout.
  Podible should separate a pure layout plan from filesystem/client effects and
  keep them recoverable through its existing durable execution boundary.
- `clients/TorrentClient.ts`: `shouldRecheck` permits skipping checks for full or
  size-only matches when configured, but still checks partial matches. Skipping
  initial checks and repairing a partially matching torrent are distinct paths.
- `clients/QBittorrent.ts`: the adapter selects the supported skip/seed-mode field
  by client version. Treat optimistic verification as a tested client capability,
  not a universal assumption. Libtorrent's [seed mode](https://www.libtorrent.org/reference-Core.html#torrent_flags_t)
  verifies requested pieces lazily and falls back to a full check on mismatch.

These are implementation references, not a decision to port the modules, global
configuration, orchestration or database schema wholesale.

## Required Acceptance Cases

- Renamed identical episodes assemble into a pack without duplicate media bytes.
- Same-length files differing only at the tail repair an isolated CoW variant;
  all original torrent and library bytes remain unchanged.
- Lazy verification failure follows the client's documented recovery behavior,
  including an automatic full recheck where required, without unbounded downloads.
- CoW unsupported/failed paths do not silently allocate a full media duplicate.
- Adding a tracker or variant never changes Plex's equivalent selected version;
  an explicit quality upgrade still can.
- Pure v2 and hybrid fixtures preserve protocol identities, validate piece layers
  and reconcile client records without duplicating a hybrid as two downloads.
- Crash/replay, concurrent eviction, and seed/reference changes cannot delete
  still-needed bytes or turn stale verification into current authority.

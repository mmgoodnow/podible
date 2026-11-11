# Seed Consolidation Planner

`src/media/seed-consolidation.ts` is a pure, cross-media policy POC. It does not
read or write files, query a database, contact trackers, control a client, change
library selections, or authorize deletion. Every result says `mode: "plan-only"`
and `destructiveActionsAllowed: false`.

## Input Contract

- Verified canonical objects use existing `ReuseFileIdentity` device/inode/size
  snapshots. IDs represent physical objects, not filenames, matching sizes or
  matching hashes. Two distinct inodes remain two objects even with equal hashes;
  ambiguous aliases of one inode under multiple IDs block the snapshot.
- `librarySelections` names the selected library IDs, their existing media/target
  scopes and exact object references. The planner copies these unchanged. It
  never promotes a pack, different encode, translation or edition.
- `requestedCoverage` names semantic units in an existing `CrossSeedScope`, such
  as a specific TV episode. A registration's coverage proof identifies all objects
  needed for the complete unit. Upstream semantic mapping and byte verification
  are prerequisites, not something this planner guesses from release names.
- Registrations use `TorrentRef`, exact object references, ownership/currentness,
  active-use state, actual seed seconds and tracker eligibility. Tracker IDs must
  already be normalized authoritative identities, not announce URLs or passkeys.
  `repeatedUnregistration` means independently established repeated proof for the
  current tracker set; this module does not validate the underlying announcements.
- All four inventory-completeness flags must be true. The caller must account for
  every selection, registration, active reader/writer and object reference in the
  coherent snapshot. Pagination/truncation is not complete inventory.

The TypeScript API is internal, not an untrusted JSON/RPC validation boundary.
Missing references, unverified objects, duplicate identities, invalid counts and
incomplete inventory produce a blocked plan with no retirement/reclaim candidates.
Unknown individual uses and stale/unverified registration evidence protect their
registrations/objects; they do not become evidence supporting another retirement.

## Selected Season Pack Evidence

The TV-only override is opt-in through `seasonPacks`. Each claim joins one exact
registration to one TV target, authoritative season ID and publication version:

- `expectedEpisodeIds` is the complete canonical season roster, not the requested
  subset. `seasonInventoryComplete` attests to that roster. Separately,
  `packEpisodeInventoryComplete` attests that the registration's coverage lists
  all of the pack's episodes. The two scoped episode sets must match exactly.
- `fullTorrentVerified` attests to current full-torrent byte completion, including
  unwanted files, not just an import receipt or downloaded wanted files. Ownership,
  current registration evidence, verified whole-episode coverage and verified
  objects with no active or unknown writer are also required.
- For every roster episode, exactly one `librarySelections` row with
  `tvPublication` must match the target, episode, publication version and
  `sourceRegistrationId` from the version/acquisition binding. It must be active
  (promoted, not staged), currently verified by Plex at the exact
  episode identity and paths, and name the same full set of canonical objects as
  the pack's episode mapping. The same object union across the season, or another
  registration of the same objects, is not enough. Ambiguous selections or season
  claims do not qualify.
- A candidate must carry `singleTvEpisode`, attesting to the sole episode in its
  entire release. Its only coverage row must be verified and match that scoped
  episode. One requested episode or one currently mapped episode is not evidence
  that a torrent contains only one. No release titles are inspected.

Absent, stale or mismatched proof does not grant the override. It leaves the
generic rules in place; malformed object references still block the whole plan.

## Policy

1. Active or unknown registration use, active or unknown object writers,
   unverified ownership, stale evidence and unverified coverage block retirement.
   Active or unknown object use blocks generic retirement; see rule 5 for the
   narrow exception for a known independent object reader.
2. Live/unknown tracker registrations require the existing conservative minimum
   of 30 **actual** seed days. Age since registration is irrelevant. Unknown or
   regressed seed counters remain protected.
3. Current repeated unregistration proof from **every** tracker, with a complete
   nonempty tracker set, bypasses only the seed-time obligation. One unknown or
   live tracker, missing tracker inventory or a single unregistration observation
   cannot grant this exception. It never bypasses use or coverage protection.
4. A retained, currently verified registration must still cover each previously
   covered requested unit. Generic retirement also preserves every requested
   live tracker/unit pair and retains registrations with no storage benefit,
   except for the existing all-trackers-unregistered exception to the storage
   rule. An older pack supplying an episode absent from a newer/partial pack
   remains retained when that episode is still needed.
5. A proved Plex-selected complete pack stays in the plan and supersedes proved
   singles for its exact episodes after 30 actual seed days, even when the single
   saves no bytes or supplies a unique live tracker pair. Repeated unregistration
   does not waive the minimum on this path. The plan names the replacement pack
   and still reports the tracker's lost coverage. Known independent object readers
   can keep using the object after registration retirement; active or unknown
   registration use, unknown object use and active or unknown writers still block.
6. Decisions are sequential, newest registration first, then ID for timestamp
   ties. Two variants cannot both retire by relying on each other's availability.
   This tie-break never changes library-selection seniority.
7. Objects become reclaim candidates only after the proposed retained-registration
   set is known, and only with no selection, retained registration, active reader,
   active writer or unknown-use reference. An unregistered torrent record can
   retire while its selected/shared objects remain protected.

All omitted/unknown demand must be distinguished upstream from known absent
coverage. `uncoveredRequestedCoverage` reports requested units without a retained
verified registration; the planner cannot acquire or invent their contents.

## Accounting And Execution Boundary

`objectBytes` and reclaim-candidate bytes sum canonical object sizes once.
`registrationReferenceBytes` deliberately counts each torrent's references;
`duplicateRegistrationReferenceBytes` reports the overlap, **not** extra storage
or saved disk space. The reported allocation upper bound may be null and is not
an exclusive-extent proof. Hardlinks, CoW sharing, sparse files and filesystem
metadata mean neither logical size nor reported allocation proves actual bytes
freed by unlinking. Counter overflow/incomplete inventories do not become zero
known storage totals.

A future executor must revalidate current identities/revisions, references,
ownership, tracker proofs and obligations under appropriate locks, quiesce client
uses, persist retirement receipts and recover crashes before considering any
unlink. This module supplies none of that production deletion authority. It has
no DB adapter, durable workflow, UI, tracker discovery, pack acquisition, byte
reuse/materialization, quality scoring, or physical-space measurement.

Tests: `bun test test/unit/seed-consolidation.test.ts`. Regression fixtures cover
shared and distinct objects, partial packs, stale/staged/mismatched Plex evidence,
the actual-seed boundary and independent object-use protection. Generated graphs
check that whole requested coverage survives, tracker pairs disappear only for
proved supersession, and no reclaim candidate is still referenced. The generic
graph tests continue to require exact tracker-pair preservation.

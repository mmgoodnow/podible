# Canonical-First Downloads

The original download is a torrent view, not the owner of a second payload copy.
New movie, TV and book acquisitions on the private Transmission adapter use:

```text
metainfo -> persisted reservation -> canonical regular objects
                                     ^             ^
                                     |             |
                           torrent symlinks   published library symlinks
```

## Lifecycle

1. Validate bounded metainfo, safe paths, client identity and the canonical mount
   marker. Each media member receives a new UUID, never an identity inferred from
   filename or size. No existing published object becomes a writable target.
2. Persist the intent under `contentRoot/.podible-downloads/` before creating
   payload objects. Files start empty; expected lengths remain in the intent,
   avoiding a full-sized zero placeholder for the client to hash. A ready
   manifest records their device/inode and exact torrent-member mapping before
   any client add. Sidecars remain local to the torrent directory.
3. Add the torrent paused, bind the acquisition, then resume. Partial data belongs
   to the reservation but is absent from publication and cross-seed inventories.
   A replay validates the existing reservation and links without truncation.
   Missing torrent symlinks can be rematerialized from that intent; missing ready
   canonical bytes cannot be mistaken for a missing disposable view.
4. After actual complete/checked client evidence, stop the writer and persist its
   per-hash seed-only intent. Mark all files unwanted, verify the flags, then
   resume seeding. The supervisor's existing pre-spawn guard also restores these
   flags and ownership after restart. Only then mark the reservation sealed.
5. Probe and identify the media normally. Finalization records the sealed
   file's device, inode, size and timestamps without rereading the payload.
   Movie/TV versions and book assets reference that exact object. Exact Plex
   identity/path verification remains required for movie/TV activation.

The private adapter advertises `canonicalDownloads` and implements `sealDownload`.
Unsupported clients and already-bound legacy downloads retain their old paths.
In-flight execution receipts are not interpreted as permission to convert an
existing directory. No production data migration or transcript invalidation is
required by this change.

## Safety And Limits

- There is one writable torrent registration per newly reserved object. Shared
  verified objects are never selected for initial-download repair writes.
- Symlink traversal is allowed only at declared torrent leaves targeting the
  manifest's exact canonical inode. Directory aliases, arbitrary symlinks,
  replaced objects and changed mount identities fail closed.
- Finalization requires a sealed reservation. Fingerprint changes invalidate a
  saved receipt. Application seed-only protection is not an OS write sandbox.
- A missing ready object is not silently recreated or made writable on replay.
  Recovery must not redownload into surviving published/shared objects. Legacy
  independent seed-store recovery remains supported by its original path.
- Removing a torrent record/layout does not remove canonical bytes. Existing
  retention requires independent seed-store evidence before deleting canonical
  payload; a symlink to the same bytes cannot satisfy that proof. Such content
  remains protected until coordinated seed-store reclamation supports it.
- This change does not merge byte-identical objects from unrelated initial
  downloads, relocate existing payload, or implement arbitrary partial symlink
  repair. Cross-seed partial repair still requires isolated CoW storage.

## Verification

Unit and application integration tests cover reservation replay, changed
links/objects, active-writer rejection, movie/TV/book publication using the same
inode, legacy receipts, and unrequested pack members remaining unpublished.

`test/lab/canonical-download-inner.ts` exercises the actual helpers and managed
adapter against stable Transmission 4.1.4, using synthetic multi-file v1 metainfo,
a local webseed, nonaligned content/sidecar boundaries, partial-download restart,
exact payload hashes and post-completion seed-only restart. It asserts that the
torrent leaves remain symlinks, canonical inodes do not change, and the torrent
directory contains no regular media copy or `.part` payload. A separate
single-file M4B download exercises Transmission's directory-free member layout
through the same reservation, sealing, finalization and guarded restart.

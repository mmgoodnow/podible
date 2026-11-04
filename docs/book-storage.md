# Book storage

`libraryRoot` is a presentation view, not the identity of a book file. This is
the normal import, scanner, and reconciliation path, independent of the one-off
rTorrent transfer.

```
libraryRoot/
  .podible/
    downloads/<infohash>/<original torrent layout>
    content/files/<prefix>/<object identity>/content.<extension>
  <Author>/<Title>/<Title> [m<manifestation>-a<asset>].<extension>
```

Downloads and canonical content are regular files sharing an inode through a
hardlink. Presentation files are relative symlinks to canonical content. Removing
a download directory or rebuilding the view cannot remove the canonical link.
Hardlink adoption requires the same filesystem; there is no implicit copying or
archive extraction.

## Publication And Recovery

1. Claim a stable content object and fsync its identity before hardlinking bytes.
2. Publish the asset and canonical file paths together in the application DB.
3. Derive desired view entries from current book, manifestation, asset, and file
   records; reconcile links without changing target bytes.

Replaying an import repairs its view instead of publishing another asset. Worker
maintenance runs at startup and hourly; full library refresh also reconciles.
Changing naming code or book metadata therefore converges without a separate
filesystem migration. The scanner imports loose regular files and ignores private
storage and managed symlinks, preventing self-imports.

Existing DB files are adopted by the same maintenance path. Their IDs, byte and
duration offsets, timestamps, and transcript records stay unchanged. A retained
`identity_path` preserves the exact old transcript/remux fingerprint input, while
`path` points to canonical storage. It is not used to open audio. Old presentation
hardlinks can only be retired after matching their inode/device/size to canonical
content. Foreign files and symlinks fail closed. Pending view ownership records
make interrupted replacements recoverable.

## One-Off rTorrent Transfer

`scripts/migrate-book-torrents.ts --help` describes the separate operator tool.
It defaults to a read-only plan and only considers DB-linked releases tagged
`Podible` or `LazyLibrarian`. Back up the application DB, execution DB, and torrent
session configuration before applying. Workers must remain stopped during apply;
the private Transmission daemon must remain available.

The tool uses cached, hash-verified metainfo, stops the source, adds the target
paused, hardlinks the original layout, and requests a complete piece recheck.
Only fully verified bytes permit atomic DB backend/source-path cutover and
resumption. Its durable report allows interrupted runs to be reconciled; report
success never substitutes for a fresh verification. Failed verification cannot
fall back to downloading. Original data and stopped rTorrent records are retained,
not deleted. This command is not a background task or a new configuration surface.

### Production Preparation (2026-11-04 UTC)

At deployed build `8344dbb`, the read-only plan contains all **144** eligible
Podible/LazyLibrarian releases with **zero exceptions**. They contain about
47.2 GB of original torrent data. This is source/layout validation, not completed
Transmission piece verification or an applied transfer.

Five initial exceptions were not missing/corrupt data: four old tracker messages
coexisted with complete byte counters, and XML-RPC's default whitespace trimming
removed a trailing space from a real directory name. The client now preserves
typed strings exactly. Neither fix bypasses the target's complete piece recheck;
the real-daemon acceptance covers the trailing-space layout, zero downloaded
bytes, corrupt-byte refusal, restart/replay and retained source records.

Before transfer, all 17 successful transcript fingerprints still exactly match
`/config/backups/book-storage-2026-11-04T01-36-26-903Z/inventory.json`.
Repeat that comparison after transfer, and independently inspect the resulting
bindings, Transmission completion/ownership and original stopped records.

The production container supervises the application and Transmission separately.
A local Docker rehearsal confirmed that a Docker-host `SIGSTOP` to the exact
container's PID 1 suspends the supervisor only; the application can then exit
gracefully while Transmission keeps serving RPC and seeding. Resuming PID 1 with
`SIGCONT` lets the existing supervisor restart the application. Do not use
`docker pause`, which freezes the engine too. A confirmed application exit,
private application/execution/client backups, an independent restore watchdog,
and unconditional supervisor restoration are prerequisites for using this
maintenance procedure. Pin the container ID, avoid overlapping deployments, and
do not proceed on an unconfirmed worker shutdown. The website is unavailable
during this window.

### Completed Production Transfer (2026-11-04 UTC)

The approved transfer is complete on build `e581960`: **144 torrents**, including
120 Podible and 24 LazyLibrarian sources, totaling **47,201,179,188 bytes**.
All 144 now have verified private Transmission bindings and are in its seeding
state. Four report `torrent not registered with this tracker` (release IDs 6, 8,
64 and 138); that is a tracker-availability problem, not missing local data.
No tracker records, original source files or rTorrent records were removed.
Original rTorrent records are stopped and retained.

The main window ran from 15:45:24 to 15:48:20 UTC. Ten first-pass verifications
reported partial completion and were safely paused without a binding or download.
Sampled original pieces matched their metainfo, and fresh full rechecks then
verified all ten completely. Their final cutover used those fresh verification
receipts, current complete engine/file observations, exact original hardlink
identities and the existing atomic repository cutover. Tracker errors were kept
distinct from storage errors. Finalization finished at 15:57:34 UTC after a
second brief window. This does not establish the root cause of the initial
verification-state failures or claim a general verifier fix.

Independent final checks confirmed:

- 144 complete engine records, zero unchecked/missing bytes and zero bytes downloaded.
- 1,174 migrated source files retain their exact original inodes and sizes.
- All 765 canonical book files retain their IDs, paths, inode/device, sizes and mtimes.
- All 765 declarative presentation symlinks resolve to their expected canonical files.
- All 17 successful transcript rows, artifact hashes and live fingerprints are unchanged.
- The application restarted while the same private torrent daemon stayed running;
  the public SPA returned HTTP 200 after the window.

Private application/execution database backups passed SQLite integrity checks.
Initial client session backups and the first-pass report are in
`/config/backups/book-transfer-2026-11-04T15-45Z/`. The final report,
independent verification and another pre-cutover database snapshot are in
`/config/backups/book-transfer-2026-11-04T16-00Z/`. These contain private operational
state and must not be committed. The earlier 15:25Z directory is an aborted
operator preflight; it changed no torrents or database rows.

### Future Relocation Requirement

This one-off full-recheck procedure is not the desired routine relocation API.
Ordinary same-client moves and derived-view changes should preserve verified
torrent state and stable content identity, reconcile paths declaratively, and
avoid hashing all bytes again. A cross-client transfer additionally needs a
validated translation of resume/verification state; an existing client's
`complete` flag alone is not that proof. Until that mechanism exists, retain the
conservative recheck requirement rather than silently trusting arbitrary paths.

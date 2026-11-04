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

# Book Download Ownership

New book snatches use the same private, Docker-supervised Transmission daemon as
movies. The adapter uses a separate `Podible Books` ownership label and a `books`
download root. Client credentials and instance identity remain internal; no new
torrent-client settings are required.

Before adding a torrent, Podible persists its hash, owning book, client identity,
and destination in `book_torrents`. A retry reconciles that intent by infohash
rather than starting another download. The release association and downstream
download job commit together. Ownership records remain after a book is deleted:
deleting library metadata must not silently delete seeding data.

An existing release without a Transmission binding is a legacy rTorrent release.
Progress polling and reimport resolve the backend from that binding, never by
guessing from whichever client is currently configured. Existing rTorrent
downloads and seeding records are not automatically moved, stopped, or re-added.
The separate [one-off transfer](book-storage.md#one-off-rtorrent-transfer) can
cut over explicitly verified existing releases, including LazyLibrarian entries.

## Files

Managed downloads live under `libraryRoot/.podible/downloads/<infohash>`. The
scanner excludes this hidden directory. The download root shares the library
filesystem. Imports hardlink download bytes into canonical content and derive
`Author/Title` symlink views from the asset records; they do not copy the book or
audiobook again.

There is no archive extraction stage in normal book or movie acquisition.
Books require supported audio, EPUB, or PDF files; movies require supported
playable video containers. Reading an EPUB's internal ZIP structure for metadata
is distinct from unpacking an archived torrent release.

Canonical content and declarative symlink publication are the normal path, not a
one-off migration convention. See [book storage](book-storage.md) and
[shared RSS recovery](indexer-feeds.md).

## Verification

The offline Docker acceptance in `test/lab/transmission-inner.ts` starts the real
bundled daemon and serves synthetic torrent data from a loopback webseed. It
checks book snatch, download-to-import handoff, unchanged bytes, matching source
and library inodes, persistent ownership after daemon restart, and idempotent
snatch. It also retains the movie transfer and supervisor-restart checks.

Unit/integration and browser tests use explicit `TorrentClient` injections, not a
production fallback to rTorrent for new snatches. Live tracker availability and
production acquisition outcomes are separate from these local acceptance tests.

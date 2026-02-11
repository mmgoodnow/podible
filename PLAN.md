# Kindling Backend Plan (Podible/Bun)

This document is a build plan for a new Kindling backend implemented inside the `podible` codebase using Bun. It replaces LL. It is written so an agent can implement the entire system end-to-end with realistic tests.

## Goals

- Provide a reliable, deterministic backend for Kindling (macOS + iOS).
- Support both ebooks and audiobooks end-to-end.
- Automatically attempt to acquire both ebook and audiobook for every book by default.
- Keep search, snatch, download, import, streaming, and feed generation in one service.
- Make the system resilient to duplicate events, partial failures, and provider quirks.
- Provide a realistic test harness with mocked Torznab and rTorrent so regressions are caught early.

## Non-Goals

- Maintain LL feature parity or LL-specific semantics.
- Support every provider or every downloader. Initial support: Torznab + rTorrent only.
- Cloud sync. Design with future CloudKit support in mind, but do not implement now.

## High-Level Architecture

Process layout:

1. HTTP API server (Bun.serve + existing `server.ts`)
2. Background worker loop (jobs)
3. SQLite storage (Bun.sqlite)
4. Optional file watcher (library root scan)
5. Streaming + feeds endpoints (existing podible behavior preserved)

Key subsystems:

- Search: Torznab query + normalize results (audio + ebook categories)
- Snatch: create Release + enqueue download job
- Download: rTorrent integration + status polling
- Import: detect files, compute assets, link into library, update book
- Streaming + feed: reuse podible range streaming, chapters, feeds (audio only)
- Ebooks: store file assets, expose download URLs (no streaming semantics needed)
- Acquisition loop: auto search/snatch/download/import for audio + ebook per book

## Tech Constraints

- Bun runtime (TypeScript supported by Bun)
- HTTP server built into `server.ts` using `Bun.serve`
- Bun.sqlite for storage
- node:child_process for rTorrent/ffmpeg/ffprobe
- No version prefix in URLs
- Raw SQL + thin repository layer (no ORM)
- Typechecking required for all TS (`tsc --noEmit`)

## Persistence Migration (Podible JSON -> SQLite)

Current podible persistence relies on JSON files in `dataDir`:

- `library-index.json`
- `transcode-status.json`
- `probe-cache.json`
- `api-key.txt`

Move all persistence to SQLite. During transition, it is acceptable to:

- Read existing JSON files once to seed SQLite (optional)
- Stop writing JSON files after SQLite is introduced
- Delete JSON files in test/dev environments

## Library Layout (Filesystem)

Single configurable library root. Organize as:

`/LibraryRoot/Author/Book Title/`

Assets live under the book directory:

- `Book Title.m4b` (preferred audio)
- `Book Title.epub` or `Book Title.pdf` (ebook)
- `Book Title.jpg` (cover)
- `Book Title/` (folder of mp3s only if no m4b)

SQLite is the source of truth. The filesystem only stores assets referenced by the database.

## Data Model (SQLite)

Tables:

### books
Represents the canonical logical book record in Kindling (title/author + metadata). A book can have multiple assets over time (multiple releases or formats).

- id INTEGER PRIMARY KEY AUTOINCREMENT
- title TEXT NOT NULL (display title)
- author TEXT NOT NULL (display author)
- cover_path TEXT NULL (resolved cover file path; cacheable)
- duration_ms INTEGER NULL (for audio; derived from preferred audio asset)
- added_at TEXT NOT NULL (when first seen/imported)
- updated_at TEXT NOT NULL
- published_at TEXT NULL (best-effort from metadata)
- description TEXT NULL (plain text)
- description_html TEXT NULL (rich text if available)
- language TEXT NULL (BCP-47 if known)
- isbn TEXT NULL (best-effort)
- identifiers_json TEXT NULL (JSON map of provider IDs)

### releases
Represents a specific acquisition attempt (a chosen search result). Releases track downloader state and connect a provider result to the eventual asset(s).

- id INTEGER PRIMARY KEY AUTOINCREMENT
- book_id INTEGER NOT NULL (foreign key to book)
- provider TEXT NOT NULL (torznab source/provider name)
- title TEXT NOT NULL (release title as returned by provider)
- media_type TEXT NOT NULL (audio|ebook)
- info_hash TEXT NOT NULL
- size_bytes INTEGER NULL (raw size from provider)
- url TEXT NOT NULL (download/magnet URL)
- snatched_at TEXT NOT NULL (when acquisition requested)
- status TEXT NOT NULL (snatched|downloading|downloaded|imported|failed)
- error TEXT NULL (last failure reason)
- updated_at TEXT NOT NULL
- FOREIGN KEY(book_id) REFERENCES books(id)

### assets
Represents a concrete file set that can be played or downloaded (single audio file, multi-part audio, or a single ebook file). Assets are immutable.

- id INTEGER PRIMARY KEY AUTOINCREMENT
- book_id INTEGER NOT NULL (foreign key to book)
- kind TEXT NOT NULL (single|multi|ebook)
- mime TEXT NOT NULL (audio/mpeg, audio/mp4, application/epub+zip, application/pdf)
- total_size INTEGER NOT NULL (bytes)
- duration_ms INTEGER NULL (audio only)
- source_release_id INTEGER NULL (release that produced this asset)
- created_at TEXT NOT NULL (when asset was created)
- updated_at TEXT NOT NULL
- FOREIGN KEY(book_id) REFERENCES books(id)
- FOREIGN KEY(source_release_id) REFERENCES releases(id)

### asset_files
Represents individual files that make up an asset, including byte offsets for stitched audio streaming and per-file duration for chapter mapping.

- id INTEGER PRIMARY KEY AUTOINCREMENT
- asset_id INTEGER NOT NULL (foreign key to asset)
- path TEXT NOT NULL (absolute or library-relative path)
- size INTEGER NOT NULL (bytes)
- start INTEGER NOT NULL (byte offset in stitched stream)
- end INTEGER NOT NULL (byte offset in stitched stream)
- duration_ms INTEGER NOT NULL (per-file duration for audio)
- title TEXT NULL (chapter title or file-derived title)
- updated_at TEXT NOT NULL
- FOREIGN KEY(asset_id) REFERENCES assets(id)

### jobs
Represents background work. Jobs provide visibility and retries for scan, download, import, transcode, and reconcile flows.

- id INTEGER PRIMARY KEY AUTOINCREMENT
- type TEXT NOT NULL (scan|download|import|transcode|reconcile)
- status TEXT NOT NULL (queued|running|succeeded|failed|cancelled)
- book_id INTEGER NULL (optional target book)
- release_id INTEGER NULL (optional target release)
- payload_json TEXT NULL (job-specific params)
- error TEXT NULL (last failure reason)
- attempt_count INTEGER NOT NULL DEFAULT 0
- max_attempts INTEGER NOT NULL DEFAULT 5
- next_run_at TEXT NULL
- created_at TEXT NOT NULL
- updated_at TEXT NOT NULL

### settings
Single-row settings storage.

- id INTEGER PRIMARY KEY (always 1)
- value_json TEXT NOT NULL


## ID Format

- Use SQLite `INTEGER PRIMARY KEY AUTOINCREMENT` for all IDs.
- Expose IDs as numbers in the API.

## Status Derivation (books.status)

Do not persist `books.status`. Derive it on read, and expose per-media states.

Per-media status (audio, ebook):

1. `imported` if an asset of that media exists
2. `downloading` if any release of that media is `downloading`
3. `downloaded` if at least one release of that media is `downloaded` but no asset exists
4. `snatched` if any release of that media is `snatched`
5. `error` if all releases of that media failed and no asset exists
6. `open` otherwise

Overall `books.status`:

- `imported` only if both audio and ebook are imported
- `partial` if exactly one media is imported
- otherwise the “highest” non-imported state across both media

This avoids regressing from `imported` to `snatched` on transient errors.

## Asset Selection Heuristics

When a book has multiple assets, select one for playback/feed using deterministic heuristics:

1. Prefer audio assets over ebooks for feeds/streaming.
2. Prefer m4b single-file audio over multi-mp3.
3. Prefer the most recently imported asset.
4. For audio, prefer longer duration if timestamps tie.

No persisted “favorite” or active flag.

## Acquisition Loop (Auto)

When a book is added, the system should automatically attempt to acquire both media types:

1. Search audio results, rank, snatch, download, import.
2. Search ebook results, rank, snatch, download, import.

If import fails due to mismatch or bad content, mark the release as failed and try the next result. Stop after N attempts (configurable) and surface a “needs manual selection” state.

This keeps correctness without heavy orchestration.

## Job Execution Semantics

- Use jobs only for long-running tasks: download, import, transcode, scan.
- Snatch should attempt the rTorrent add inline and fail fast if the add fails.
- Jobs are best-effort and can be retried.
- Retries use exponential backoff based on `attempt_count` and `next_run_at`.
- Import/snatch/download transitions are wrapped in DB transactions.
- Search and snatch are synchronous when possible; long-running work is async.

## Indexes (required)

- `books(added_at)`
- `releases(book_id, status)`
- `releases(book_id, media_type)`
- `releases(info_hash)`
- `releases(url)`
- `assets(book_id, created_at)`
- `asset_files(asset_id, start)`
- `jobs(status, type, updated_at)`

## State Machine

Per-media state machine:

`open -> snatched -> downloading -> downloaded -> imported`

Overall book state:

- Derived from `audio_status` and `ebook_status`.
- `imported` only when both media are imported.
- `partial` when exactly one media is imported.
- Otherwise derived from the highest non-imported state across media.

Rules:

- Per-media transitions are monotonic unless explicit user action.
- Failures do not erase last known good state. Store `error` on release/job.
- Multiple assets per book are allowed. Playback/feed selection is derived by heuristic.

Release transition mirrors book but can fail independently:

`snatched -> downloading -> downloaded -> imported` or `failed`

Idempotency:

- All external-triggered actions (snatch/download/import) must be idempotent.
- Use unique constraints on `releases.info_hash` to prevent duplicate snatches.
- Deduplicate by infohash before contacting rTorrent.

## API Surface (Versionless)

### Health + Server

- `GET /health`
- `GET /server`

### Library

- `GET /library?limit=&cursor=&q=`
- `GET /library/{bookId}`
- `POST /library` -> create a book (title, author) and trigger acquisition loop
- `POST /library/refresh`

### Search + Snatch

- `POST /search` -> `{ query, media: audio|ebook }`, returns normalized Torznab results
- `POST /snatch` -> requires `bookId`, creates release and download job
- `GET /releases?bookId=`

### Downloads + Import

- `GET /downloads` -> mapped from jobs/releases
- `GET /downloads/{jobId}` (download job id)
- `POST /downloads/{jobId}/retry`
- `/downloads` responses include both `job_id` and `release_id`
- `POST /import/reconcile`
- `GET /assets?bookId=`

### Streaming + Feeds

- `GET /stream/{assetId}.{ext}` (range supported, audio only)
- `GET /chapters/{assetId}.json` (audio only)
- `GET /covers/{bookId}.jpg`
- `GET /feed.xml` (audio feed)
- `GET /feed.json` (audio feed)
- `GET /ebook/{assetId}` (direct download, ebook only)

Feeds are sorted by `added_at` (fixed).

### Settings

- `GET /settings`
- `PUT /settings`

Settings shape (stored in SQLite as a single JSON row):

```
{
  "torznab": [
    { "name": "prowlarr", "baseUrl": "...", "apiKey": "...", "categories": { "audio": "audio", "ebook": "book" } }
  ],
  "rtorrent": { "transport": "http-xmlrpc", "url": "...", "username": "...", "password": "..." },
  "libraryRoot": "/media/library",
  "polling": { "rtorrentMs": 5000, "scanMs": 30000 },
  "transcode": { "enabled": true, "format": "mp3", "bitrateKbps": 64 },
  "feed": { "title": "Kindling", "author": "..." },
  "auth": { "mode": "apikey", "key": "..." }
}
```

## Upstream Integrations

### Torznab

- Endpoint: `GET /api?t=search&q=...` or `t=search&cat=...`
- Parse RSS/Atom results
- Normalize fields: title, size, download url, provider, seed/leech
- Use `cat=audio` for audiobooks, `cat=book` for ebooks (when supported)

### rTorrent

Use XML-RPC or `rpc` interface. Needed calls:

- `load.raw_start` with torrent bytes (no filesystem path dependence)
- `d.name`, `d.hash`, `d.complete`, `d.get_base_path`
- `d.get_bytes_done`, `d.get_size_bytes`

Default transport: HTTP XML-RPC only. Do not support SCGI.

If a search result does not include `info_hash`, fetch the `.torrent` file first and compute the hash before snatch.

### Open Library

- Search by title+author or ISBN
- Fetch metadata and cover
- Must be optional: import should not fail on missing metadata

## Metadata Strategy

- Open Library is the primary metadata source.
- Store raw provider payloads for reproducibility.
- Manual overrides in Kindling should take precedence.

## Search Result Ranking (including box sets)

Default behavior favors single-book matches:

1. Require strong title + author match for the specific book title.
2. Penalize results containing set markers: "box set", "collection", "complete", "omnibus", "books 1-7", "1-3", "series".
3. Prefer exact title match and smaller total size/duration.

If only box sets are returned, mark as ambiguous and stop after N attempts. This can later be resolved by an AI-assisted selection step.

## Import & Linking Strategy

- Do not delete or move source torrents
- Use hardlinks only; if hardlink fails (EXDEV), surface a clear error
- Compute asset(s) as first-class objects
- Replace means swapping the preferred asset by heuristic, not deleting history
- Ebook import stores the file as a single asset with `kind=ebook`

## Streaming + Feed Parity (Podible)

Maintain existing podible behaviors:

- Single-file m4b can be transcoded to mp3
- Multi-mp3 is stitched with correct range handling
- ID3 chapter tag injection for multi assets
- Xing header patching for concatenated streams
- JSON feed + RSS feed with cover/chapters
Feed uses the asset selection heuristic (audio only).
Ebooks are not part of the podcast feed. They are exposed via direct download.

## Auth + Security

- Default to API key auth in `Authorization: Bearer` or `X-API-Key`.
- Allow `auth.mode=local` to disable auth for localhost-only development.
- Validate and sanitize all file/path inputs for stream/download endpoints.

## Open Defaults (Explicit)

- `POST /snatch` requires a `bookId`.
- Hardlinks only; no configurable fallback.
- Feeds include only audio assets; sort is fixed to `added_at`.
- Use a migration system, but during early development it is acceptable to drop test databases and update the initial migration.
- Migration from LL/podible data is out of scope for v1.

## Observability

- Structured logs with `request_id`, `job_id`, `book_id`, `release_id` fields.
- Basic metrics in `/health` or `/status`: counts by job state, release state, queue size.

## Admin UI

- Use server-rendered HTML (old school).
- Keep client-side JS minimal.
- Use medium CSS and rely on browser defaults where possible.

## Testing Plan

Tests must be added alongside each implementation step. Do not defer testing to the end.

### Unit Tests

- State machine transitions
- Infohash dedup behavior
- Asset construction from file layout
- Ebook asset creation and download endpoint behavior

### Integration Tests

Use local mock services:

1. Mock Torznab
   - Bun server with static RSS responses
   - Variants: valid results, empty results, malformed response

2. Mock rTorrent
   - Bun server with XML-RPC endpoint that simulates:
     - success
     - duplicate hash
     - timeout
     - complete state changes over time

3. File fixtures
   - Single m4b
   - Multi mp3 book
   - Ebook (epub + pdf)
   - Mismatched metadata

### End-to-End Tests

- Search -> snatch -> download -> import -> asset selected -> stream
- Duplicate snatch attempt should be idempotent
- rTorrent timeout should not corrupt state
- Reconcile should recover downloaded-but-not-imported
- Ebook search -> snatch -> download -> import -> direct download works

### Additional Edge Tests

- Restart in the middle of a running job (ensure job can be retried cleanly)
- Concurrent snatch requests for the same release
- SQLite busy/lock contention under parallel jobs
- Malformed or unsatisfiable Range requests
- Very large multi-file audio (stream stitching correctness)

### Test Runner

- `bun test` (or `node --test` if Bun test is insufficient)
- Start mock services during tests
- Use tmp dirs for filesystem side effects

## Implementation Order

1. Open Library integration + book persistence, with tests (mocked or live)
2. Torznab search normalization, with mocked Torznab tests
3. Snatch flow (requires bookId), with mocked Torznab tests
4. Downloading via rTorrent, with mocked rTorrent or Docker-backed rTorrent tests
5. Import pipeline + asset creation, with mocked rTorrent or Docker-backed tests
6. Streaming + feeds, with range/chapters/feed tests
7. Harden mock services and add regression tests

## Deliverables

- API server with stable behavior
- Mock services in `test/mocks`
- E2E test suite runnable in CI
- README with setup instructions

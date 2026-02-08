# Kindling Backend Plan (Podible/Bun)

This document is a build plan for a new Kindling backend implemented inside the `podible` codebase using Bun. It replaces LL. It is written so an agent can implement the entire system end-to-end with realistic tests.

## Goals

- Provide a reliable, deterministic backend for Kindling (macOS + iOS).
- Support both ebooks and audiobooks end-to-end.
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

## Tech Constraints

- Bun runtime (TypeScript supported by Bun)
- HTTP server built into `server.ts` using `Bun.serve`
- Bun.sqlite for storage
- node:child_process for rTorrent/ffmpeg/ffprobe
- No version prefix in URLs

## Data Model (SQLite)

Tables:

### books

- id TEXT PRIMARY KEY (stable internal ID)
- title TEXT NOT NULL (display title)
- author TEXT NOT NULL (display author)
- status TEXT NOT NULL (open|snatched|downloading|downloaded|imported|error, monotonic)
- media_type TEXT NOT NULL (audio|ebook|mixed; derived from assets/releases)
- primary_asset_id TEXT NULL (current active asset for playback/feed)
- cover_path TEXT NULL (resolved cover file path; cacheable)
- duration_ms INTEGER NULL (for audio; sum of active asset)
- added_at TEXT NOT NULL (when first seen/imported)
- published_at TEXT NULL (best-effort from metadata)
- description TEXT NULL (plain text)
- description_html TEXT NULL (rich text if available)
- language TEXT NULL (BCP-47 if known)
- isbn TEXT NULL (best-effort)
- identifiers_json TEXT NULL (JSON map of provider IDs)

### releases

- id TEXT PRIMARY KEY (stable internal ID)
- book_id TEXT NOT NULL (foreign key to book)
- provider TEXT NOT NULL (torznab source/provider name)
- title TEXT NOT NULL (release title as returned by provider)
- media_type TEXT NOT NULL (audio|ebook)
- info_hash TEXT NULL (if available from downloader)
- size_bytes INTEGER NULL (raw size from provider)
- url TEXT NOT NULL (download/magnet URL)
- snatched_at TEXT NOT NULL (when acquisition requested)
- status TEXT NOT NULL (snatched|downloading|downloaded|imported|failed)
- error TEXT NULL (last failure reason)
- FOREIGN KEY(book_id) REFERENCES books(id)

### assets

- id TEXT PRIMARY KEY (stable internal ID)
- book_id TEXT NOT NULL (foreign key to book)
- kind TEXT NOT NULL (single|multi|ebook)
- mime TEXT NOT NULL (audio/mpeg, audio/mp4, application/epub+zip, application/pdf)
- total_size INTEGER NOT NULL (bytes)
- duration_ms INTEGER NULL (audio only)
- active INTEGER NOT NULL DEFAULT 0 (1 = active for playback/feed)
- source_release_id TEXT NULL (release that produced this asset)
- created_at TEXT NOT NULL (when asset was created)
- FOREIGN KEY(book_id) REFERENCES books(id)
- FOREIGN KEY(source_release_id) REFERENCES releases(id)

### asset_files

- id TEXT PRIMARY KEY (stable internal ID)
- asset_id TEXT NOT NULL (foreign key to asset)
- path TEXT NOT NULL (absolute or library-relative path)
- size INTEGER NOT NULL (bytes)
- start INTEGER NOT NULL (byte offset in stitched stream)
- end INTEGER NOT NULL (byte offset in stitched stream)
- duration_ms INTEGER NOT NULL (per-file duration for audio)
- title TEXT NULL (chapter title or file-derived title)
- FOREIGN KEY(asset_id) REFERENCES assets(id)

### jobs

- id TEXT PRIMARY KEY (stable internal ID)
- type TEXT NOT NULL (scan|search|snatch|download|import|transcode|reconcile)
- status TEXT NOT NULL (queued|running|succeeded|failed|cancelled)
- book_id TEXT NULL (optional target book)
- release_id TEXT NULL (optional target release)
- payload_json TEXT NULL (job-specific params)
- error TEXT NULL (last failure reason)
- created_at TEXT NOT NULL
- updated_at TEXT NOT NULL

### operations

- id TEXT PRIMARY KEY (stable internal ID)
- key TEXT NOT NULL UNIQUE (idempotency key)
- status TEXT NOT NULL (started|succeeded|failed)
- created_at TEXT NOT NULL
- updated_at TEXT NOT NULL

Idempotency keys should be derived as `book_id + info_hash + action` where possible, or include a stable provider URL.

## State Machine

Books transition through (both media types):

`open -> snatched -> downloading -> downloaded -> imported`

Rules:

- Transitions are monotonic. No backward transition unless explicit user action.
- Failures do not erase last known good state. Store `error` on release/job.
- Multiple assets per book are allowed. `active` asset is for playback and feeds.

Release transition mirrors book but can fail independently:

`snatched -> downloading -> downloaded -> imported` or `failed`

Idempotency:

- All external-triggered actions (snatch/download/import) must be idempotent.
- Use operations table to block duplicate work.
- Deduplicate by infohash or url before contacting rTorrent.

## API Surface (Versionless)

### Health + Server

- `GET /health`
- `GET /server`

### Library

- `GET /library?limit=&cursor=&q=`
- `GET /library/{bookId}`
- `POST /library/refresh`
- `GET /library/changes?since=`

### Search + Snatch

- `POST /search` -> returns normalized Torznab results (include `media_type`)
- `POST /snatch` -> creates release and download job
- `GET /releases?bookId=`

### Downloads + Import

- `GET /downloads` -> mapped from jobs/releases
- `GET /downloads/{id}`
- `POST /downloads/{id}/retry`
- `POST /import/reconcile`
- `GET /assets?bookId=`
- `POST /assets/{assetId}/activate`

### Playback

- `PUT /playback/position`
- `GET /playback/positions?since=`

### Streaming + Feeds

- `GET /stream/{assetId}.{ext}` (range supported, audio only)
- `GET /chapters/{assetId}.json` (audio only)
- `GET /covers/{bookId}.jpg`
- `GET /feed.xml` (audio feed)
- `GET /feed.json` (audio feed)
- `GET /ebook/{assetId}` (direct download, ebook only)

### Settings

- `GET /settings`
- `PUT /settings`

## Upstream Integrations

### Torznab

- Endpoint: `GET /api?t=search&q=...` or `t=search&cat=...`
- Parse RSS/Atom results
- Normalize fields: title, size, download url, provider, seed/leech
- Use `cat=audio` for audiobooks, `cat=book` for ebooks (when supported)

### rTorrent

Use XML-RPC or `rpc` interface. Needed calls:

- `load.start` (or equivalent) with magnet/url
- `d.name`, `d.hash`, `d.complete`, `d.get_base_path`
- `d.get_bytes_done`, `d.get_size_bytes`

### Open Library

- Search by title+author or ISBN
- Fetch metadata and cover
- Must be optional: import should not fail on missing metadata

## Import & Linking Strategy

- Do not delete or move source torrents
- Create hardlink when same filesystem, reflink when supported, else symlink
- Compute asset(s) as first-class objects
- Replace means swapping active asset, not deleting history
- Ebook import stores the file as a single asset with `kind=ebook`

## Streaming + Feed Parity (Podible)

Maintain existing podible behaviors:

- Single-file m4b can be transcoded to mp3
- Multi-mp3 is stitched with correct range handling
- ID3 chapter tag injection for multi assets
- Xing header patching for concatenated streams
- JSON feed + RSS feed with cover/chapters
Ebooks are not part of the podcast feed. They are exposed via direct download.

## Testing Plan

### Unit Tests

- State machine transitions
- Idempotency key behavior
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

- Search -> snatch -> download -> import -> asset active -> stream
- Duplicate snatch attempt should be idempotent
- rTorrent timeout should not corrupt state
- Reconcile should recover downloaded-but-not-imported
- Ebook search -> snatch -> download -> import -> direct download works

### Test Runner

- `bun test` (or `node --test` if Bun test is insufficient)
- Start mock services during tests
- Use tmp dirs for filesystem side effects

## Implementation Order

1. Storage + migrations + schema
2. Search + Torznab normalization
3. Snatch + rTorrent integration
4. Import pipeline + asset creation
5. Streaming + feeds
6. Mock services + tests

## Deliverables

- API server with stable behavior
- Mock services in `test/mocks`
- E2E test suite runnable in CI
- README with setup instructions

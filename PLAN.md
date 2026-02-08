# Kindling Backend Plan (Podible/Bun)

This document is a build plan for a new Kindling backend implemented inside the `podible` codebase using Bun. It replaces LL. It is written so an agent can implement the entire system end-to-end with realistic tests.

## Goals

- Provide a reliable, deterministic backend for Kindling (macOS + iOS).
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
3. SQLite storage (node:sqlite)
4. Optional file watcher (library root scan)
5. Streaming + feeds endpoints (existing podible behavior preserved)

Key subsystems:

- Search: Torznab query + normalize results
- Snatch: create Release + enqueue download job
- Download: rTorrent integration + status polling
- Import: detect files, compute assets, link into library, update book
- Streaming + feed: reuse podible range streaming, chapters, feeds

## Tech Constraints

- Bun runtime (TypeScript supported by Bun)
- HTTP server built into `server.ts` using `Bun.serve`
- node:sqlite for storage
- node:child_process for rTorrent/ffmpeg/ffprobe
- No version prefix in URLs

## Data Model (SQLite)

Tables:

### books

- id TEXT PRIMARY KEY
- title TEXT NOT NULL
- author TEXT NOT NULL
- status TEXT NOT NULL (open|snatched|downloading|downloaded|imported|error)
- primary_asset_id TEXT NULL
- cover_path TEXT NULL
- duration_ms INTEGER NULL
- added_at TEXT NOT NULL
- published_at TEXT NULL
- description TEXT NULL
- description_html TEXT NULL
- language TEXT NULL
- isbn TEXT NULL
- identifiers_json TEXT NULL

### releases

- id TEXT PRIMARY KEY
- book_id TEXT NOT NULL
- provider TEXT NOT NULL
- title TEXT NOT NULL
- info_hash TEXT NULL
- size_bytes INTEGER NULL
- url TEXT NOT NULL
- snatched_at TEXT NOT NULL
- status TEXT NOT NULL (snatched|downloading|downloaded|imported|failed)
- error TEXT NULL
- FOREIGN KEY(book_id) REFERENCES books(id)

### assets

- id TEXT PRIMARY KEY
- book_id TEXT NOT NULL
- kind TEXT NOT NULL (single|multi)
- mime TEXT NOT NULL
- total_size INTEGER NOT NULL
- duration_ms INTEGER NULL
- active INTEGER NOT NULL DEFAULT 0
- source_release_id TEXT NULL
- created_at TEXT NOT NULL
- FOREIGN KEY(book_id) REFERENCES books(id)
- FOREIGN KEY(source_release_id) REFERENCES releases(id)

### asset_files

- id TEXT PRIMARY KEY
- asset_id TEXT NOT NULL
- path TEXT NOT NULL
- size INTEGER NOT NULL
- start INTEGER NOT NULL
- end INTEGER NOT NULL
- duration_ms INTEGER NOT NULL
- title TEXT NULL
- FOREIGN KEY(asset_id) REFERENCES assets(id)

### jobs

- id TEXT PRIMARY KEY
- type TEXT NOT NULL (scan|search|snatch|download|import|transcode|reconcile)
- status TEXT NOT NULL (queued|running|succeeded|failed|cancelled)
- book_id TEXT NULL
- release_id TEXT NULL
- payload_json TEXT NULL
- error TEXT NULL
- created_at TEXT NOT NULL
- updated_at TEXT NOT NULL

### operations

- id TEXT PRIMARY KEY
- key TEXT NOT NULL UNIQUE
- status TEXT NOT NULL (started|succeeded|failed)
- created_at TEXT NOT NULL
- updated_at TEXT NOT NULL

Idempotency keys should be derived as `book_id + info_hash + action` where possible, or include a stable provider URL.

## State Machine

Books transition through:

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

- `POST /search` -> returns normalized Torznab results
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

- `GET /stream/{assetId}.{ext}` (range supported)
- `GET /chapters/{assetId}.json`
- `GET /covers/{bookId}.jpg`
- `GET /feed.xml`
- `GET /feed.json`

### Settings

- `GET /settings`
- `PUT /settings`

## Upstream Integrations

### Torznab

- Endpoint: `GET /api?t=search&q=...` or `t=search&cat=...`
- Parse RSS/Atom results
- Normalize fields: title, size, download url, provider, seed/leech

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

## Streaming + Feed Parity (Podible)

Maintain existing podible behaviors:

- Single-file m4b can be transcoded to mp3
- Multi-mp3 is stitched with correct range handling
- ID3 chapter tag injection for multi assets
- Xing header patching for concatenated streams
- JSON feed + RSS feed with cover/chapters

## Testing Plan

### Unit Tests

- State machine transitions
- Idempotency key behavior
- Asset construction from file layout

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
   - Mismatched metadata

### End-to-End Tests

- Search -> snatch -> download -> import -> asset active -> stream
- Duplicate snatch attempt should be idempotent
- rTorrent timeout should not corrupt state
- Reconcile should recover downloaded-but-not-imported

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

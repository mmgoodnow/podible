# Podible Backend (Bun)

Bun-based backend implemented in the `podible` codebase.

The service provides:

- SQLite-backed library, releases, assets, jobs, and settings.
- Torznab search.
- rTorrent snatch/download polling.
- Import pipeline with hardlinking into the configured library root.
- Open Library search + identifier-based library creation.
- Audio streaming, chapters, RSS/JSON feeds.
- Ebook direct download endpoint.
- Mock Torznab/rTorrent and end-to-end tests.

## Requirements

- Bun `1.3+`
- `ffprobe` (used for audio duration/chapters)
- `ffmpeg` (used by existing podible media behaviors)

## Install

```bash
bun install
```

## Run

```bash
bun run server.ts
```

Optional: pass a library root override as the first arg.

```bash
bun run server.ts /path/to/library
```

Runtime state is stored in `DATA_DIR` (default `${TMPDIR:-/tmp}/podible-transcodes`) and includes:

- `kindling.sqlite` (main app DB + `app_state` cache state)

## Auth

Settings default to API key auth (`Authorization: Bearer <key>` or `X-API-Key`).

On boot, the server logs the active API key from settings.

For localhost development, set settings `auth.mode` to `local` via `settings.update` RPC.

## Key Endpoints

- `POST /rpc` (control/data APIs via JSON-RPC 2.0)
- `GET /rpc/{namespace}/{method}` (read-only convenience bridge, query params -> RPC params)
- `GET /assets?bookId=`
- `GET /stream/{assetId}.{ext}`
- `GET /chapters/{assetId}.json`
- `GET /covers/{bookId}.jpg`
- `GET /feed.xml`
- `GET /feed.json`
- `GET /ebook/{assetId}`

Removed REST control routes now return `404`:

- `/health`, `/server`
- `/settings`
- `/openlibrary/search`
- `/library`, `/library/{bookId}`, `/library/refresh`
- `/search`, `/snatch`
- `/releases`, `/downloads`, `/downloads/{id}`, `/downloads/{id}/retry`
- `/import/reconcile`

## JSON-RPC Methods (v1)

- `system.health`
- `system.server`
- `settings.get`
- `settings.update`
- `openlibrary.search`
- `library.list`
- `library.get`
- `library.create`
- `library.refresh`
- `library.rehydrate`
- `search.run`
- `snatch.create`
- `releases.list`
- `downloads.list`
- `downloads.get`
- `downloads.retry`
- `jobs.list`
- `jobs.get`
- `import.reconcile`

RPC request shape:

```json
{
  "jsonrpc": "2.0",
  "id": 1,
  "method": "settings.get",
  "params": {}
}
```

Read-only bridge examples:

```bash
curl "http://localhost/rpc/system/health"
curl "http://localhost/rpc/library/list?limit=20&q=dune"
curl "http://localhost/rpc/library/get?bookId=1"
```

Bridge constraints:

- Read-only methods only (`settings.update`, `library.create`, `snatch.create`, etc. are blocked).
- Responses still use JSON-RPC envelopes with `id: null`.
- Canonical control/data write path remains `POST /rpc`.

## Settings Shape

`settings.get` / `settings.update` use:

```json
{
  "torznab": [
    {
      "name": "prowlarr",
      "baseUrl": "http://localhost:9696",
      "apiKey": "...",
      "categories": { "audio": "audio", "ebook": "book" }
    }
  ],
  "rtorrent": {
    "transport": "http-xmlrpc",
    "url": "http://127.0.0.1/RPC2",
    "username": "",
    "password": ""
  },
  "libraryRoot": "/media/library",
  "polling": { "rtorrentMs": 5000, "scanMs": 30000 },
  "transcode": { "enabled": true, "format": "mp3", "bitrateKbps": 64 },
  "feed": { "title": "Kindling", "author": "Unknown" },
  "auth": { "mode": "apikey", "key": "..." }
}
```

## Open Library Flows

Search across Open Library:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"openlibrary.search","params":{"q":"Hyperion Dan Simmons","limit":10}}'
```

Add by Open Library work key:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"library.create","params":{"openLibraryKey":"/works/OL45804W"}}'
```

Rehydrate metadata for existing books (all or one):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"library.rehydrate","params":{}}'
```

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":4,"method":"library.rehydrate","params":{"bookId":123}}'
```

Inspect background jobs and scan outcomes:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":5,"method":"jobs.list","params":{"limit":20}}'
```

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":6,"method":"jobs.get","params":{"jobId":42}}'
```

## Testing

Run all tests:

```bash
bun test
```

Typecheck:

```bash
bun run typecheck
```

Current suites include:

- unit tests (schema, repo, status, auth, torznab, rtorrent, media)
- integration HTTP tests
- end-to-end flow tests with mocks in `test/mocks`

## Notes

- Idempotency is enforced by globally unique `releases.info_hash`.
- Job worker uses queue claim/requeue semantics with retry backoff.
- Scanner and `library.rehydrate` hydrate missing metadata from Open Library (work id/language/publish date/description/cover where available).
- Import strategy uses hardlinks only; cross-device `EXDEV` is surfaced as an error.
- Snatch requires `.torrent` URLs and explicit `infoHash`; magnet links are out of scope.
- JSON-RPC batch requests are intentionally unsupported in v1.
- Playback position APIs are intentionally out of scope for this phase.

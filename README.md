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

For localhost development, set settings `auth.mode` to `local` via `PUT /settings`.

## Key Endpoints

- `GET /health`
- `GET /server`
- `GET /settings`
- `PUT /settings`
- `GET /openlibrary/search?q=&limit=`
- `GET /library?limit=&cursor=&q=`
- `POST /library` (title/author OR `openLibraryKey` OR `isbn`)
- `GET /library/{bookId}`
- `POST /library/refresh`
- `POST /search`
- `POST /snatch` (`infoHash` required)
- `GET /releases?bookId=`
- `GET /downloads`
- `GET /downloads/{jobId}`
- `POST /downloads/{jobId}/retry`
- `POST /import/reconcile`
- `GET /assets?bookId=`
- `GET /stream/{assetId}.{ext}`
- `GET /chapters/{assetId}.json`
- `GET /covers/{bookId}.jpg`
- `GET /feed.xml`
- `GET /feed.json`
- `GET /ebook/{assetId}`

## Settings Shape

`GET /settings` / `PUT /settings` use:

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
curl "http://localhost/openlibrary/search?q=Hyperion%20Dan%20Simmons&limit=10"
```

Add directly by Open Library key:

```bash
curl -X POST http://localhost/library \
  -H "Content-Type: application/json" \
  -d '{"openLibraryKey":"/works/OL45804W"}'
```

Add directly by ISBN:

```bash
curl -X POST http://localhost/library \
  -H "Content-Type: application/json" \
  -d '{"isbn":"9780553283686"}'
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
- Scanner hydrates missing metadata from Open Library on discovered books.
- Import strategy uses hardlinks only; cross-device `EXDEV` is surfaced as an error.
- Snatch requires `.torrent` URLs and explicit `infoHash`; magnet links are out of scope.
- Playback position APIs are intentionally out of scope for this phase.

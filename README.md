# Kindling Backend (Podible/Bun)

Bun-based backend for Kindling, implemented in the `podible` codebase.

The service provides:

- SQLite-backed library, releases, assets, jobs, and settings.
- Torznab search.
- rTorrent snatch/download polling.
- Import pipeline with hardlinking into the Kindling library root.
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

- `kindling.sqlite`
- migrated/legacy podible cache artifacts if present

## Auth

Settings default to API key auth (`Authorization: Bearer <key>` or `X-API-Key`).

On boot, the server logs the active API key from settings.

For localhost development, set settings `auth.mode` to `local` via `PUT /settings`.

## Key Endpoints

- `GET /health`
- `GET /server`
- `GET /settings`
- `PUT /settings`
- `GET /library?limit=&cursor=&q=`
- `POST /library`
- `GET /library/{bookId}`
- `POST /library/refresh`
- `POST /search`
- `POST /snatch`
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
- Import strategy uses hardlinks only; cross-device `EXDEV` is surfaced as an error.
- Playback position APIs are intentionally out of scope for this phase.

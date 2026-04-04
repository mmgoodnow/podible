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

Runtime bootstrap uses one directory:

- `CONFIG_DIR` (default `${TMPDIR:-/tmp}/podible-config`) for `podible.sqlite`

`libraryRoot` is stored in application settings (`settings.get` / `settings.update`).

Open Library covers are stored alongside each imported book inside `libraryRoot`.

## Auth

Settings default to Plex browser sign-in.

- `auth.mode = "plex"` enables the normal browser login flow.
- `auth.mode = "local"` is intended for localhost development and tests only.
- Browser routes use Podible session cookies.
- App clients can use the app-login flow: `auth.beginAppLogin` -> browser sign-in -> `auth.exchange` for a bearer token.
- JSON-RPC methods are now scoped by auth level (`public`, `user`, `admin`).

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
- `auth.beginAppLogin`
- `auth.exchange`
- `auth.me`
- `auth.logout`
- `settings.get`
- `settings.update`
- `openlibrary.search`
- `library.list`
- `library.get`
- `library.create`
- `library.delete`
- `library.refresh`
- `library.acquire`
- `library.reportImportIssue`
- `library.rehydrate`
- `search.run`
- `agent.search.plan`
- `snatch.create`
- `releases.list`
- `downloads.list`
- `downloads.get`
- `downloads.retry`
- `jobs.list`
- `jobs.get`
- `agent.import.plan`
- `import.reconcile`
- `import.inspect`
- `import.manual`

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
- Browser login bootstrap still uses normal HTTP redirect routes behind the scenes.

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
    "password": "",
    "downloadPath": ""
  },
  "libraryRoot": "/media/library",
  "polling": { "rtorrentMs": 5000, "scanMs": 30000 },
  "recovery": { "stalledTorrentMinutes": 10 },
  "feed": { "title": "Books", "author": "Unknown" },
  "auth": {
    "mode": "plex",
    "appRedirectURIs": ["kindling://auth/podible"],
    "plex": {
      "productName": "Podible",
      "ownerToken": "",
      "machineId": "",
      "machineName": ""
    }
  },
  "agents": {
    "enabled": false,
    "provider": "openai-responses",
    "model": "gpt-5-mini",
    "apiKey": "",
    "lowConfidenceThreshold": 0.45,
    "timeoutMs": 30000
  },
  "notifications": {
    "pushover": {
      "enabled": false,
      "apiToken": "",
      "userKey": ""
    }
  }
}
```

Agent behavior:

- Deterministic ranking/selection remains the default behavior.
- Responses API is used only when `agents.enabled=true` and a trigger condition is met (`forceAgent`, prior failure, or low confidence).
- Missing/failed agent calls fall back to deterministic selection.

Download recovery behavior:

- Download jobs continuously watch rTorrent state; this is the stalled-torrent watcher.
- If rTorrent reports any error on an incomplete torrent, Podible cancels that download job and queues a forced agent reacquire for the same media while rejecting the failed torrent URL/guid/infohash.
- If the forced reacquire job later exhausts retries or produces no usable candidate, Podible sends a notification.
- `recovery.stalledTorrentMinutes` controls how long an incomplete torrent can sit with no progress before Podible treats it as stalled and auto-reacquires.
- Pushover delivery is best-effort and requires `notifications.pushover.enabled=true` plus `apiToken` and `userKey`.

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

Re-trigger auto-acquire for an existing book:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":21,"method":"library.acquire","params":{"bookId":123}}'
```

Target only one media type:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":22,"method":"library.acquire","params":{"bookId":123,"media":["ebook"]}}'
```

Force agent-powered reacquire (user-triggered recovery):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":23,"method":"library.acquire","params":{"bookId":123,"media":["audio"],"forceAgent":true,"priorFailure":true}}'
```

Report an imported file as wrong (attempt forced agent re-import first, then queue forced agent reacquire if needed):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":24,"method":"library.reportImportIssue","params":{"bookId":123,"mediaType":"audio"}}'
```

Rehydrate metadata for existing books (all or one):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":3,"method":"library.rehydrate","params":{}}'
```

Start an app login attempt for Kindling:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":30,"method":"auth.beginAppLogin","params":{"redirectUri":"kindling://auth/podible"}}'
```

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":4,"method":"library.rehydrate","params":{"bookId":123}}'
```

Inspect background jobs and acquire outcomes:

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

Inspect a local download path before manual import:

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":7,"method":"import.inspect","params":{"path":"/data/downloads/box-set"}}'
```

Manual import with explicit file selection (useful for box sets):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":8,"method":"import.manual","params":{"bookId":123,"mediaType":"audio","path":"/data/downloads/box-set","selectedPaths":["/data/downloads/box-set/Disc 1/01.mp3","/data/downloads/box-set/Disc 1/02.mp3"]}}'
```

Plan search candidate selection (no side effects):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":9,"method":"agent.search.plan","params":{"query":"Dune Frank Herbert","media":"audio","bookId":123}}'
```

Plan manual import file selection (no side effects):

```bash
curl -X POST http://localhost/rpc \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":10,"method":"agent.import.plan","params":{"path":"/data/downloads/box-set","mediaType":"audio","bookId":123}}'
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
- Job type split: `acquire` is targeted auto-search/snatch for one book, while `full_library_refresh` scans and imports existing filesystem content.
- Scanner and `library.rehydrate` hydrate missing metadata from Open Library (work id/language/publish date/description/cover where available).
- Import strategy uses hardlinks only; cross-device `EXDEV` is surfaced as an error.
- Snatch requires `.torrent` URLs (magnet links are out of scope).
- Snatch computes canonical infohash from downloaded `.torrent` bytes; Torznab `infohash` attrs are optional.
- JSON-RPC batch requests are intentionally unsupported in v1.
- Playback position APIs are intentionally out of scope for this phase.

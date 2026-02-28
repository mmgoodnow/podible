# Books Backend Notes

## Progress

- [x] Branch created: `feat/books-backend`
- [x] SQLite schema + migration system
- [x] Settings + auth migration off JSON files
- [x] Torznab search and normalization
- [x] Snatch + rTorrent integration
- [x] Download/import worker loop
- [x] Asset streaming/feed parity
- [x] Mock services and e2e tests
- [x] Review feedback pass (rTorrent methods, infohash handling, snatch behavior)
- [x] Legacy JSON cache state moved into SQLite (`app_state`)
- [x] Open Library search endpoint + add-to-library by OL key/ISBN
- [x] README refresh
- [x] Manual import controls for box sets (`import.inspect` + file selection)
- [x] Optional Responses-API decision scaffolding (`agent.search.plan`, `agent.import.plan`) with deterministic fallback
- [x] Import failure recovery path: deterministic import -> forced agent import -> forced agent reacquire scan; user can trigger via `library.acquire`
- [x] User-reported semantic import issue path via `library.reportImportIssue` (forced agent import attempt -> forced agent reacquire)

## Open Questions / Decisions

- None currently. Any blockers encountered during implementation will be recorded here with context and fallback action.

## Current Focus

- API surface replaced in `server.ts` and `/src/books/http.ts`.
- Worker handles `download`, `import`, `scan`, and `reconcile` with retry/backoff.
- Next: continue API/tests hardening and close any follow-up review comments.

## Assumptions

- Playback position APIs are intentionally out of scope in v1 per `PLAN.md`.
- rTorrent transport is HTTP XML-RPC only.
- During early development we can recreate the local test DB when schema changes.

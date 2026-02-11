# Kindling Backend Notes

## Progress

- [x] Branch created: `feat/kindling-backend`
- [x] SQLite schema + migration system
- [x] Settings + auth migration off JSON files
- [x] Torznab search and normalization
- [x] Snatch + rTorrent integration
- [x] Download/import worker loop
- [x] Asset streaming/feed parity
- [x] Mock services and e2e tests
- [ ] README refresh

## Open Questions / Decisions

- None currently. Any blockers encountered during implementation will be recorded here with context and fallback action.

## Current Focus

- API surface replaced in `server.ts` and `/src/kindling/http.ts`.
- Worker handles `download`, `import`, `scan`, and `reconcile` with retry/backoff.
- Next: refresh README to match Kindling backend endpoints and setup.

## Assumptions

- Playback position APIs are intentionally out of scope in v1 per `PLAN.md`.
- rTorrent transport is HTTP XML-RPC only.
- During early development we can recreate the local test DB when schema changes.

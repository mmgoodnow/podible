# Kindling Backend Notes

## Progress

- [x] Branch created: `feat/kindling-backend`
- [ ] SQLite schema + migration system
- [ ] Settings + auth migration off JSON files
- [ ] Torznab search and normalization
- [ ] Snatch + rTorrent integration
- [ ] Download/import worker loop
- [ ] Asset streaming/feed parity
- [ ] Mock services and e2e tests
- [ ] README refresh

## Open Questions / Decisions

- None currently. Any blockers encountered during implementation will be recorded here with context and fallback action.

## Assumptions

- Playback position APIs are intentionally out of scope in v1 per `PLAN.md`.
- rTorrent transport is HTTP XML-RPC only.
- During early development we can recreate the local test DB when schema changes.

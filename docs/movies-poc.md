# Native Movies POC

## Status

This is an incremental implementation, not a completed movie-request vertical.
The foundations below are implemented; no production movie request endpoint or
movie acquisition workflow has been enabled yet. Existing book routes and jobs
remain in use. The SPA currently displays the existing book library.

## Implemented

- Bun 1.4.4 runtime, native typechecking, HTML/React bundling and packaged assets.
- Typed tRPC/TanStack Query SPA, sharing schemas, authorization and handlers with
  the existing JSON-RPC API. `/rpc` remains a separate supported wire protocol.
- Native movie, quality target, user request, acquisition, version, content-file
  and semantic timeline records. Requests with identical policies share a target; incompatible
  cut/quality requirements do not overwrite each other. No movie columns were
  added to application jobs or book models.
- TMDB adapter with normalized IDs/metadata, bounded caching, timeouts and explicit
  upstream errors. It accepts catalog IDs, not client-authored fetch URLs.
- Explainable quality eligibility, ordered tiers, hard language/cut/codec/HDR
  constraints and a stable upgrade comparator. Unknown facts remain unknown.
- Torrent client port with opaque client identity, file indexes, paused/idempotent
  add, ownership checks, observation, recheck, resume, quiescence and record-only
  removal. rTorrent is the first adapter; client details stay inside it.
- Canonical content adoption uses independent, readonly files rather than sharing
  a writable inode with the torrent client. Publication uses relative symlinks.
  Content IDs are independent of the client's current save path.
- Declarative view reconciliation with ownership records, interrupted-publication
  recovery, path/collision checks and refusal to overwrite foreign files.
- Publication intent survives process restarts. Both old and pending links exist
  until Plex verifies the candidate's movie GUID and exact content paths. Only
  then is the selected version promoted and the old presentation link retired.
  Old versions, content bytes and torrent records are not automatically deleted.

`movies` settings are persisted and admin-only alongside the existing settings.
They default to disabled. They describe the intended vertical configuration;
setting `enabled` alone does not create an acquisition worker or movie UI.

## Verification

The ordinary test suite covers transaction rollback, shared requests, execution
fencing, policy evaluation, unknown facts, file adoption/cancellation, view
recovery and publication failure/replay. The Plex HTTP contract test requires
both the correct provider GUID and exact part path, not merely a successful scan.

The browser acceptance uses real Chrome, the packaged SPA and typed HTTP API.
It checks filtering without reload, mobile overflow, missing resources and an
expired session. This is book-library SPA acceptance, not movie UI acceptance.

The isolated Docker lab runs real Plex, rTorrent and Prowlarr with private volumes
and no production credentials or media. See [the lab guide](../test/lab/README.md).

- Real Plex indexed relative symlinks, served byte ranges, and retained its item
  identity and watched state after a declarative rename. This uses a synthetic
  personal-video library; authenticated real-movie GUID matching is not proven.
- Real rTorrent verified paused/idempotent add, existing-byte recheck, state,
  resume/quiescence, file mapping and record-only removal. It also downloaded a
  fresh synthetic payload from a local tracker/peer and verified a multi-file
  feature/nested-NFO layout against metainfo indexes.
- rTorrent 0.9.8 crashed during lab runs that immediately removed a freshly
  transferred record before the next operation. The cause is not established.
  The passing transfer test retains/pauses the completed record, matching the
  intended acquisition retention policy. This does not prove safe removal of
  freshly completed transfers.
- Prowlarr has been started, but an actual Prowlarr search contract is not tested.
- No production SSH, manual deployment, production downloads or agent API calls
  were performed for this POC work.

## Remaining Vertical

1. Implement movie search/selection through Prowlarr, with server-owned release
   references and persisted concise agent reasoning. Use the existing text model
   constant, not a new model configuration surface or brittle title filters.
2. Compose a native movie acquisition Reflow workflow: durable selection and
   metainfo intent, client reconciliation, download observation, probe, canonical
   adoption, Plex publication and semantic request events. Enqueue through the
   existing transactional job-command outbox; do not duplicate execution state.
3. Add typed movie search/detail/request screens and the request tracker. Keep
   availability separate from optional upgrade progress, and explain blocked
   states with a user-actionable retry/manual selection path.
4. Add bounded stalled-download replacement and scheduled automatic upgrade
   searches. Searches for upgrades must be new acquisitions, not memoized retries
   of a previous search. Keep the current version available on any failure.
5. Reconcile desired views on startup/code changes without a one-off filesystem
   migration. Recover pending publication intents before accepting replacements.
6. Prove the whole browser-to-Prowlarr-to-transfer-to-Plex path, upgrade, restart,
   cancellation and stalled replacement against the local stack. Add a small
   second-client adapter contract to verify the port is genuinely portable.
7. Configure an authorized TMDB credential and verify real catalog/movie matching.
   Before displaying TMDB metadata in a released UI, include the provider's
   required attribution and approved branding.

Cross-seeding, shared-byte rescue and torrent-specific symlink materialization
remain out of scope. The separate content identities, client roots and view
renderer leave room for them without coupling canonical files to tracker names.

# Native Movies POC

## Scope

The native movie vertical is implemented at `/app/movies` and `/app/requests`.
Existing book HTTP and JSON-RPC routes remain supported. Movies default to
disabled; this work does not enable or configure movies in production.

This is not a Radarr frontend. Podible owns movie requests, policy, release
selection, acquisition, canonical files and declarative Plex views. Cross-seeding,
TV/anime, subtitles and torrent-specific derived views are out of scope.

## Request to Plex

1. A user searches TMDB and requests a catalog ID. The server fetches metadata;
   callers cannot supply arbitrary catalog or torrent URLs.
2. Compatible requests share a policy-snapshot target and active acquisition.
   Incompatible cut/quality policies cannot overwrite each other's selection.
3. An application transaction persists request/acquisition identity and a job
   command. Reflow owns steps, retries, leases and sleeps in the existing separate
   execution database; movie tables do not duplicate execution state.
4. Prowlarr/Torznab supplies releases. The existing shared text-agent model reviews
   identity, cut and quality, with optional bounded metainfo inspection. There is
   no title-regex prefilter that can hide all candidates from the agent. Concise
   selection/import explanations are saved on the acquisition.
5. An owned torrent-client adapter reconciles a paused add, resumes the download,
   observes progress and retains stalled downloads before trying another release.
   At most three alternatives are attempted. Rate-limit waits are durable and
   honor bounded Retry-After delays.
6. Actual ffprobe streams and duration accompany an agent's main-feature review.
   Probe-derived facts override release-title claims. Unsupported multipart-only
   contents are rejected rather than publishing only part of a movie.
   Rejected/unplayable contents or post-probe policy mismatches trigger another
   release, not just stalled downloads. Storage/permission/Plex failures instead
   retry or surface a blocked operation without blaming the downloaded release.
7. Verified bytes are independently copied into a readonly canonical content
   store. They are not writable hardlinks to the client's seeding files. Adoption
   checks path safety, size/hash identity, source changes and disk headroom.
8. Publication stages an intent and relative symlinks alongside the old version.
   Plex must report the expected TMDB/IMDb GUID and exact file paths before
   promotion. A scan's HTTP 200 alone is not success. Section scans are coalesced;
   background thumbnail analysis does not block checking an already indexed file.
9. Only after verification does the selected version change and the obsolete
   presentation link disappear. Old versions, canonical bytes and torrent records
   remain. Failed upgrades leave the working movie available.

Startup and hourly maintenance reconcile desired views, recover/retire interrupted
publication intents, and enqueue fresh search/upgrade acquisitions when due.
Retrying a failed step does not rerun already successful transfers or adoption.
Withdrawing the last request cancels unneeded work and pauses incomplete owned
downloads, but does not delete bytes or stop completed seeding. Shared requests
continue for remaining users.

## UI and API

The Bun-built React SPA uses tRPC/TanStack Query with the same schemas,
authorization and handlers as JSON-RPC. Request pages show semantic stages,
byte progress, availability separately from upgrades, activity and explanations.
Users can retry, withdraw, request an upgrade check or choose a release manually.

- `movies.status`, `movies.search`, `movies.get`, `movies.request`.
- `requests.list`, `requests.get`, `requests.retry`, `requests.checkUpgrade`,
  `requests.withdraw`, `requests.searchReleases`, `requests.selectRelease`.

All require a user session. Request access is owner-scoped. Manual release
searches return a SQLite-backed `searchId` plus result indexes valid for 30
minutes and scoped to that user/target. Download URLs and tracker credentials do
not go to the browser. Expired/cross-user references are rejected without
performing another expensive search. Generic book/admin RPC behavior is unchanged.

## Configuration

Use the existing administrator settings JSON; no new model configuration surface
was added. Configure:

- `movies.enabled`, `movies.tmdbAccessToken` (TMDB API read-access bearer token).
- `movies.managedRoot`: an app-owned absolute directory visible at the same path
  inside the app container. Contains `downloads`, `objects`, `views/plex/movies`.
- `movies.downloadGroupId`: optional shared group for client-writable download
  directories; defaults to null. The app needs permission to assign that group.
- `movies.plexUrl`, `movies.plexToken`, `movies.plexSectionId`.
- `movies.plexViewPath`: the Plex-visible path for `views/plex/movies`. Plex must
  also mount the canonical content with the same relative relationship, since
  presentation links are relative. Mount the whole managed tree, not just views.
- Existing `torznab` (Prowlarr sources) and `agents.apiKey` settings. Book downloads
  continue using the existing `rtorrent` settings; native movies use the private
  Docker-managed Transmission engine, not those settings.
- `movies.qualityPolicy`, `automaticUpgrades`, `searchIntervalHours` (default 24),
  and `maximumReleaseBytes` (default 40 GiB).

Use a dedicated Plex movie section and managed root. Existing foreign files and
missing/replaced content mounts are errors, not permission to bulldoze data.
Do not change managed roots/client identity during an acquisition; retry only
after addressing the configuration mismatch.

The default policy requires the theatrical cut, English audio, SDR and allowed
720p/1080p tiers. Unknown facts need verification; they are not silently assumed.
Quality is an ordered tier plus bounded within-tier preference, not a weighted
sum that can override hard constraints. Equal/lower quality is not an upgrade.

### Private torrent engine

The image bundles Debian's pinned, security-maintained `transmission-daemon`
package (`4.1.0~beta2+dfsg-3+deb13u2`). `scripts/container.ts` supervises Bun and
the foreground daemon independently, restarts failed processes with bounded
backoff, and stops the app before the daemon. Allow a 30-second Docker stop
grace period. Failed engine readiness does not block the web UI.

`/config/transmission` persists the private identity, generated authentication,
metainfo and resume state. Back up the whole config volume. RPC binds only to
`127.0.0.1:9091`, requires authentication and is never published/proxied. There
is no user/admin torrent-control API or extra torrent-client settings surface.
Peer traffic is separate (`51413` TCP/UDP); publishing that port is optional and
is not permission to publish `9091`. Existing book/rTorrent work is unchanged.
Old native-movie executions from another client identity fail closed rather
than silently being replayed against Transmission. Local non-Docker developers
can use `bun run scripts/container.ts` with `transmission-daemon` installed.

### Radarr shadow mode

See [movie-shadow.md](movie-shadow.md) for the independent read-only observer,
admin audit UI and settings. Live movie acquisition can remain disabled while
shadow mode evaluates existing Radarr downloads. Cross-seeding, Seerr attribution
and automated action on shadow disagreements are intentionally not implemented.

## Verification and Limits

On 2026-11-02 the full local live-agent run passed: three real Prowlarr searches,
three retained torrent records (one intentionally stalled), both actual media
transfers, and promotion from version 1 to version 2 after Plex verification.
The synthetic run ID was `80407337-a72d-4ffd-adce-6da32e01de41`.
The final regression run passed 444 tests, both Chrome scripts, native Bun
typechecking/build, runtime-script validation and the packaged-image auth/asset
smoke. The separate real rTorrent single/multifile contract also passed.

`bun test` covers native repository transactions, auth and opaque search
references, real Reflow dispatch/recovery, stalled replacement, upgrades,
cancellation/sharing, canonical adoption and publication replay. The generic
file-backed execution harness also exercises SIGKILL/restart failure windows.

Both browser acceptance scripts use Chrome/Chromium, built assets and actual
typed HTTP calls, including mobile layout, deep links and session expiry. The
movie script tests blocked/manual recovery, retained availability and upgrades.

See [the isolated lab guide](../test/lab/README.md) for full-stack acceptance and
the explicitly opt-in live-agent run. The lab uses generated media and private
local services, not production trackers or user media. Its offline personal-video
Plex library verifies file paths, payload hashes and byte-range reads; this is
**not** proof of authenticated online TMDB/Plex GUID matching. That contract has
fixture tests, but still needs an authorized TMDB credential and online Plex
movie-library smoke test before claiming that deployment-specific integration.

rTorrent 0.9.8 crashed in earlier lab experiments removing a just-transferred
record. The passing transfer path retains/pauses records, matching acquisition
retention. Safe removal of freshly completed transfers is not established.

The neutral torrent port separates client identity and download layout from
canonical content and library views. rTorrent is the only production adapter;
an independent fixture client tests orchestration without rTorrent assumptions.
No cross-seed discovery, shared-byte rescue or derived torrent layouts are
implemented. Those can be added without making tracker names canonical identity.

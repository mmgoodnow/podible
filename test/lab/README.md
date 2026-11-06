# Isolated movie lab

The services have private named volumes and an internal-only Docker network.
No production configuration, Plex account, tracker passkeys or existing media
mounts are used. No service ports are published. Plex trusts only this private
network; this is not a production configuration.

## Full Movie Request

Requires Bun 1.4.4, Docker, ffmpeg and local Chrome (or Playwright Chromium in CI).

1. `bun install --frozen-lockfile`
2. `bun run build`
3. `docker build -t podible-movies-poc:local .`
4. `bun run test/lab/movie-stack.ts`

This opens the built SPA in real Chrome, requests a synthetic movie through
tRPC, searches a real Prowlarr Generic Torznab indexer backed by a local fixture
tracker, deliberately replaces a stalled torrent, transfers actual bytes through
rTorrent, probes them with ffprobe, adopts them into canonical storage, publishes
relative symlinks and verifies real Plex indexing plus a byte-range read. It then
requests and verifies a 720p-to-1080p upgrade. Old media/torrent records remain.
All identity/quality checks other than the fixture decider use production code.

To exercise the actual movie selection/import agents instead of the deterministic
fixture decider, explicitly authorize a local `OPENAI_API_KEY`, then run:

```sh
bun run test/lab/movie-stack.ts --live-agent
```

This costs real API usage. The key stays on the host; model calls are relayed over
a private stdin/stdout bridge, not by exposing a credential to Plex/rTorrent or
giving the Docker network internet access. No real tracker is contacted. The
runner never prints the key or raw model payloads. The normal test suite does
not call a paid model.

Artifacts are in gitignored `tmp/movies-lab`: `movie-stack-acceptance.json`,
`movie-stack-mobile.png`, and generated fixture clips. The report distinguishes
live agents from fixture decisions and explicitly records synthetic identity.
The Plex library is an offline personal-video section: exact paths and generated
payload hashes are verified, but no fake online TMDB GUID is injected. Real
online catalog matching/authentication requires separate authorized credentials.

## Smaller Contracts

- `test/lab/transfer-metrics-inner.ts` (inside the runtime container, network none):
  saved metrics from real private movie/TV/book transfers, paused-copy exclusion,
  and unknown-not-zero outage handling. No external tracker or credentials.
- `bun run test/lab/plex-acceptance.ts`: relative links, indexing and byte ranges,
  plus retained Plex identity/watch-state after declarative naming changes.
- `bun run test/lab/torrent-acceptance.ts`: neutral adapter against rTorrent,
  paused/idempotent add, recheck, file mapping and record-only removal of existing
  records; then actual transfer and multifile feature/nested-NFO layouts.
- `bun run scripts/smoke-movie-agent.ts --live`: small opt-in semantic-selection
  smoke using the authorized host key, without any real tracker download.
- `bun run test/browser/movies.ts`: faster fixture-backed full SPA flow, blocked
  acquisition/manual recovery, upgrade, mobile layout, withdrawal and expiry.

The full runner temporarily creates its own Prowlarr indexer and removes it at
shutdown. Synthetic Plex sections and paused torrent/content records remain for
inspection, because deletion of just-transferred records is not a safe cleanup
assumption for the lab's rTorrent 0.9.8. `docker compose -f test/lab/compose.yml down`
stops the services without deleting volumes. Never mount production paths here.
No cross-seed service is included.

## Seed-Only Symlink Reuse

Build the current runtime image, then run the real application/Reflow path with
tmpfs forcing native CoW to be unavailable. All metainfo and bytes are synthetic;
no production paths, credentials or external network are used.

```sh
docker run --rm --network none --tmpfs /tmp:rw,size=128m -v "$PWD/test:/app/test:ro" --entrypoint bun podible-movies-poc:local run test/lab/symlink-reuse-inner.ts
```

This checks exact-byte symlink fallback, zero downloads, unchanged canonical
bytes/selected version, and seed-only restart through the actual startup guard.
CI runs it against the freshly built runtime image.
Pass `--audio` or `--ebook` to exercise the same pipeline using actual registered
book canonical hardlinks, format-scoped inventory, and book Transmission ownership.
Both book modes also run in CI and leave manifestations/selected editions intact.

The lower-level peer-to-peer probe additionally proves a second daemon can
download the linked bytes, and corruption does not cause the seeder to repair
shared files. It requires a disposable bridge network (Transmission filters
loopback peers); its tracker, webseed and peers are entirely local.

```sh
docker run --rm --network bridge -e TRANSMISSION_BINARY=/usr/local/bin/transmission-daemon -v "$PWD/test:/app/test:ro" --entrypoint bun podible-movies-poc:local run test/lab/symlink-seeding-inner.ts
```

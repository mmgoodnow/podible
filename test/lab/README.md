# Isolated movie lab

These services use separate named volumes and an internal-only network. No production
configuration, Plex account, tracker passkeys or existing media mounts are used.
The Docker network has no external connectivity. Plex trusts only the lab's
private Docker network; this configuration is not for production.

1. `docker build -t podible-movies-poc:local .`
2. `bun run test/lab/plex-acceptance.ts`
3. `bun run test/lab/torrent-acceptance.ts`
4. `docker compose -f test/lab/compose.yml down` to stop services.

The acceptance script generates a short movie using ffmpeg, applies the actual
symlink reconciler inside Linux, creates a Plex personal-video library, checks
indexing/byte-range reads, marks it watched, changes the view naming, then checks
Plex identity/watch-state retention. Artifacts go to ignored `tmp/movies-lab`.
It also starts real rTorrent and Prowlarr; startup alone does not establish their
search/transfer contracts. The separate torrent acceptance verifies the neutral
adapter against real rTorrent with private synthetic torrents: paused/idempotent
add, existing-byte recheck, resume, quiesce, file layout, and record-only removal.
It then transfers a fresh payload from a tiny local BitTorrent fixture peer and
tracker, checks byte equality, and verifies a multi-file feature/nested-NFO layout
against its metainfo indexes. It does not yet prove the Prowlarr search contract.
No cross-seed service is included.

No service ports are published. The Bun acceptance runner joins the private
Plex container's network namespace to use its unclaimed, loopback-only setup API
without borrowing an account token. Configuration volumes persist
between runs. `down` does not delete them or the test content.
Never mount production paths into this lab. Real metadata matching and authenticated
movie-provider integration need a separate authorized online smoke test.

# Private Transmission Runtime

Podible builds the official **stable Transmission 4.1.4** daemon instead of
installing Debian 13's `4.1.0~beta2+dfsg-3+deb13u2` package.
That exact package pin was introduced in commit `f22407b` ("Bundle private
Transmission and supervise movie torrent execution"); it was not an upstream
latest-release lookup.

The reported production Berserk canary received BTN's exact tracker response,
`Your client is not on the whitelist`, with the beta daemon. A separate AlphaRatio
canary was accepted. That did not establish stable-version compatibility; the
production verification below separately observed a successful BTN announce with
4.1.4. No peer-ID or user-agent spoofing is used.

## Source And Build

- [Official release](https://github.com/transmission/transmission/releases/tag/4.1.4).
- [Complete source asset](https://github.com/transmission/transmission/releases/download/4.1.4/transmission-4.1.4.tar.xz),
  including upstream third-party sources, not GitHub's incomplete generated archive.
- SHA256: `86e7aa020cdf64826447034bc85f4d3b7c848d8c0891d9dc9b9fddfb2c805d22`.
  Docker verifies this before extraction or compilation; a mismatch fails the build.
- Builder and runtime both use `oven/bun:1.4.4`, Debian 13/trixie, avoiding a
  cross-distribution libc/OpenSSL ABI mismatch. No compiler is copied to runtime.
- CMake builds only the `transmission-daemon` target in Release mode. GTK, Qt,
  macOS, CLI/utilities, unit tests, translations, web assets, documentation, and
  systemd integration are disabled. The existing supervisor owns daemon lifecycle.
- System curl/OpenSSL, libevent, libdeflate, and libpsl are used. Other bundled
  libraries come from the checksum-verified source. Upstream DHT, NAT traversal,
  and micro-transport defaults remain enabled; upstream identity is unchanged.
- Runtime receives the stripped daemon at `/usr/local/bin/transmission-daemon`,
  upstream `COPYING`, CA certificates, and its dynamic runtime libraries. A version
  smoke check during the final image build also catches missing shared libraries.

The build follows upstream's
[Debian prerequisites](https://github.com/transmission/transmission/blob/4.1.4/docs/Building-Transmission.md)
and [CMake options](https://github.com/transmission/transmission/blob/4.1.4/CMakeLists.txt).
`TRANSMISSION_BUILD_JOBS` defaults to `2` to bound compile concurrency in CI; it can
be increased locally. The source stage is independent of app source changes, so
Docker can reuse it when only application code changes.

The client interface, RPC authentication, loopback binding, daemon arguments,
config/identity/resume/torrent directories, ownership tags, and `/books` default
are unchanged. This change does not rewrite any production settings or torrents.

## Local Verification

```sh
docker build --build-arg TRANSMISSION_BUILD_JOBS=4 -t podible-transmission-stable .
docker run --rm --network none --entrypoint transmission-daemon podible-transmission-stable --version
docker run --rm --network none -v "$PWD/test:/app/test:ro" --entrypoint bun podible-transmission-stable run test/lab/transmission-inner.ts
docker run --rm --network none -v "$PWD/test:/app/test:ro" --entrypoint bun podible-transmission-stable run test/lab/book-migration-inner.ts
bun test test/unit/transmission-packaging.test.ts
```

The real-daemon labs use loopback webseed fixtures and temporary container-local
state, with external networking disabled. They exercise paused adds, exact bytes,
multi-file paths, restart persistence, book import/hardlinks, TV ownership and
non-destructive removal, supervisor restart/authentication, and legacy-book
migration with piece verification and corrupt-source rejection. They do not
announce to BTN or AlphaRatio and cannot prove either tracker's whitelist policy.

Local validation of this change passed on native Linux arm64 (Docker Desktop):

- Official archive SHA256 independently matched the pin above.
- Source verification/configuration/compilation took 96.8 seconds with four jobs;
  this is a local measurement, not a CI timing guarantee.
- Final image: `sha256:09e3b4869161edc508735abb9b5c123b0d965a8c24270f0c827a67b119dab127`.
  The daemon reports `transmission-daemon 4.1.4 (a6d907d9ea)`; `ldd` resolves all
  libraries, build tools are absent, and the license file is present.
- Both real-daemon labs above exited successfully with `--network none`, including
  book/TV transfers, restart/supervision, and migration/corruption checks.
- Six focused packaging/client tests and repository typechecking passed.

The frontend build stage also copies shared `src` files: the current auth UI
imports the pure redirect helper from there. This fixes the full-image build
without modifying the admin UI or the torrent client interface.

The existing Docker workflow builds the image and runs both labs before its normal
CD image push. Deployment stays on that path: no manual production binary swap,
container restart, config migration, or torrent re-add is part of this task.

After normal CD deployment, verify the running daemon/RPC reports 4.1.4, then
observe an authorized BTN canary announce. Retain the distinction between a
successful image build, a passing local transfer, and tracker acceptance. If BTN
still rejects the real stable identity, investigate its supported-client policy
rather than changing the advertised identity or interpreting another tracker's
success as evidence of BTN acceptance.

## Production Verification (2026-11-03)

Normal CD deployed `1c0db9b`. The private RPC reported
`4.1.4 (a6d907d9ea)` with the existing client identity and torrent registrations.
Its identity/settings/resume directory was backed up first under
`/config/backups/transmission-stable-2026-11-04T04-07-55-092Z/`.

The retained, paused Berserk BTN canary was ownership-checked, briefly started
for one bounded announce, then returned to its original paused state. A new
tracker timestamp reported `Success`, `lastAnnounceSucceeded=true`, and 102
seeders. The final client state was stopped with no tracker error. No torrent
registration or files were removed, and the independently selected fallback
download was left running. This proves acceptance of the real stable client by
that tracker at that time, not completion of the TV import or universal tracker
compatibility.

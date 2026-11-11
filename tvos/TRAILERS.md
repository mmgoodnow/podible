# Apple TV trailers

Podible can play one explicitly approved **local Plex trailer** per TMDB movie
or TV series. It does not play features or episodes, request a transcode, use
Plex watch-history APIs, or play trailers for titles without an approved extra.
Playback uses AVKit with a temporary MP4. The clip must finish downloading
before playback starts; no Plex URL or token reaches Apple TV.

## Enable

1. Add an authorized H.264/AAC MP4, at most 1080p, 32 MiB and five minutes, as a
   local Plex extra: [movie extras][movie-extras] or [series extras][show-extras].
   Enable local assets and refresh metadata in Plex. Series extras must be at
   show level, not season or episode level.
2. Using the [public PMS API][pms] on the same server Podible already uses for
   its movie/TV libraries, read `/identity` for the machine identifier. Check
   the title with `/library/metadata/{parent}?includeGuids=1`, and review the
   local trailer and its rating key via `/library/metadata/{parent}/extras`.
   The parent must be in Podible's configured section and match the exact
   TMDB ID; confirm that the configured Plex integration token can download the
   part. Review the actual clip before sharing it.
3. Set `PODIBLE_PLEX_TRAILERS` in the Podible server environment and restart it.
   For example, with illustrative Plex IDs:

   ```json
   {
     "machineIdentifier": "0123456789abcdef0123456789abcdef01234567",
     "titles": [
       { "kind": "movie", "tmdbId": "10378", "parent": "101", "extra": "102" },
       { "kind": "show", "tmdbId": "2316", "parent": "201", "extra": "202" }
     ]
   }
   ```

This is explicit publication to **all signed-in Podible users**, not a claim
that they have Plex access. Only approve clips you are willing and authorized
to share with those viewers. Empty, malformed or duplicate-title manifests
disable playback. Recheck approvals if extras are replaced or the library is
rebuilt. Remove the entry and restart Podible to withdraw an approval.

On a connected title screen, **Watch Trailer** appears only after Podible
rechecks the PMS identity, library section, exact parent TMDB GUID, trailer
type, local file and a byte-range probe of the Plex MP4. On Select it rechecks
the association and fetches the complete file with the existing Podible
session. Errors after selection are shown explicitly; Back or completion
returns to the title. The temporary file is discarded after playback.

## Why this scope

- [TMDB movie videos][tmdb-movie] and [TV videos][tmdb-tv] describe clips via
  provider keys. The YouTube IDs in the documented examples are not AVPlayer
  assets. Podible does not fetch or extract streams from those links.
- Plex's [automatically sourced extras][plex-cloud] are licensed, streamed
  extras. This implementation does not infer native playback rights or
  availability from those records or use private Plex web APIs.
- The [PMS API][pms] documents extras, media parts and download mode.
  [AVPlayerViewController][avkit] provides the native tvOS controls. For wider
  coverage, use a provider explicitly licensed to deliver direct media
  (or a supported provider player), not a link-to-stream fallback.

## Verification

`bun test test/unit/trailer-routes.test.ts` covers session checks, PMS/title
identity, local trailer and codec filtering, range/MIME validation, and
rejection of redirects. The opt-in `TrailerPlaybackTests` suite exercises a
synthetic catalog, a real PMS local extra and actual AVKit timeline advance.

For the simulator fixture, use a disposable loopback PMS with synthetic MP4s
under `Trailers/` for Big Buck Bunny (TMDB 10378) and The Office (TMDB 2316).
Use Plex Movie / Plex TV Series with local assets enabled. Set the fixture
manifest from that PMS, with the actual rating keys. Create a short-lived
localhost TLS certificate and trust its CA only on the test simulator.

```sh
bun run test/lab/trailer-fixture.ts KEY CERT PLEX_PORT MOVIE_SECTION SHOW_SECTION
TEST_RUNNER_PODIBLE_TEST_TRAILER_URL=https://localhost:43243 xcodebuild \
  -project tvos/PodibleTV.xcodeproj -scheme PodibleTV \
  -destination 'platform=tvOS Simulator,id=SIMULATOR_ID' \
  -parallel-testing-enabled NO -only-testing:PodibleTVUITests/TrailerPlaybackTests \
  SWIFT_ACTIVE_COMPILATION_CONDITIONS=PODIBLE_TRAILER_TEST_FIXTURE \
  CODE_SIGNING_ALLOWED=NO test
```

[movie-extras]: https://support.plex.tv/articles/local-files-for-trailers-and-extras/
[show-extras]: https://support.plex.tv/articles/local-files-for-tv-show-trailers-and-extras/
[plex-cloud]: https://support.plex.tv/articles/202934883-cinema-trailers-extras/
[pms]: https://developer.plex.tv/pms/
[tmdb-movie]: https://developer.themoviedb.org/reference/movie-videos.md
[tmdb-tv]: https://developer.themoviedb.org/reference/tv-series-videos.md
[avkit]: https://developer.apple.com/documentation/avkit/avplayerviewcontroller

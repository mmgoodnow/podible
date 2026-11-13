# Podible for Apple TV

Browse what's already available, request what's missing, and play supported movies and episodes without leaving the TV. Plex controls access to playback.

## Design references

Use [Plex's Apple TV preview](https://www.plex.tv/blog/new-year-same-mission/) for artwork-led shelves and pre-play hierarchy, not its branding, assets, or custom keyboard. Follow [Apple's focus guidance](https://developer.apple.com/design/human-interface-guidelines/focus-and-selection): moving focus previews a title; pressing Select opens it.

![Plex Apple TV browse reference with featured artwork and poster shelves](https://www.plex.tv/wp-content/uploads/2025/01/Shows-Conners-scaled.jpg)

![Plex Apple TV movie detail reference with a prominent primary action](https://www.plex.tv/wp-content/uploads/2025/01/Preplay-Contractor-scaled.jpg)

[Plex Apple TV search reference](https://www.plex.tv/wp-content/uploads/2025/01/tvOS-Search-scaled.jpg)

[Plex later moved navigation back to the left](https://forums.plex.tv/t/navigation-updates-coming-to-preview-users/941751/1).
Podible uses a floating left navigation pill that expands when focused. Press Left from the first poster to enter it,
Up or Down to choose a destination, and Select to open it. Home uses larger portrait rails under the full-bleed hero.

## Prototype

- Browse movie and TV shelves, search, open title details, request a movie or specific seasons, and revisit your requests.
- Demo mode needs no server; its requests are temporary and its player uses an Apple sample stream.
- Settings pairs with an HTTPS Podible origin. Scan the QR code and approve on your phone. Sign in with Plex first if prompted. The TV remembers the server address separately from its revocable Keychain session.
- Playback uses the viewer's access to the selected Plex server and requires a verified publication. If a Plex server is selected, QR pairing requires that server's viewer access before approval.
- Compatible H.264/HEVC and AAC MP4 files direct play. Other single-file editions can use Plex's HLS fallback when the server can supply a supported H.264/AAC stream. Combined episode files are not supported. Playback control uses JSON-RPC; AVKit fetches the video through short-lived, session-bound Podible URLs.
- Connected titles can play explicitly shared [local Plex trailers](TRAILERS.md). Catalog availability is not playback authorization.

```sh
xcodebuild -project tvos/PodibleTV.xcodeproj -scheme PodibleTV \
  -destination 'generic/platform=tvOS Simulator' build
```

Leave Xcode's default signing enabled when testing real pairing in the simulator.
The unsigned UI test build does not have the app identifier entitlement required to save a session in Keychain.

Run `bun run generate:tvos-wire` after changing the pairing, catalog, request, or playback RPC schemas. CI runs
`bun run check:tvos-wire` to catch stale Swift types.

## Simulator focus and screenshots

With an Apple TV simulator installed, substitute its ID from `xcrun simctl list devices available`.
The UI tests use Siri Remote actions and capture Home, expanded navigation, inline Search, detail heroes and the TV season request flow.
The `--demo --demo-rails` fixture adds enough titles to exercise multiple populated Home rails.

```sh
xcodebuild -project tvos/PodibleTV.xcodeproj -scheme PodibleTV \
  -destination 'platform=tvOS Simulator,id=SIMULATOR_ID' \
  -parallel-testing-enabled NO -resultBundlePath /tmp/podible-tv-focus.xcresult \
  CODE_SIGNING_ALLOWED=NO test
xcrun xcresulttool export attachments --path /tmp/podible-tv-focus.xcresult \
  --output-path /tmp/podible-tv-screens --filter '*.png'
```

Demo artwork is bundled in `PodibleTV/DemoArtwork`. It was generated for the fictional demo titles,
using a grid of six cinematic poster illustrations and a widescreen lighthouse backdrop.

## Next milestones

1. Verify phone pairing, movie requests and season requests with a non-admin account on a real server.
2. Verify AVKit direct play and MKV playback via Plex HLS on physical Apple TV.
3. Report playback progress back to Plex and use it for Continue Watching. Keep transfer progress, Plex publication, and Plex-verified readiness distinct.

# Podible for Apple TV

Browse what's already available and request what's missing without leaving the TV. Podible owns discovery and requests; Plex remains the feature and episode playback and watch-history authority.

## Design references

Use [Plex's Apple TV preview](https://www.plex.tv/blog/new-year-same-mission/) for artwork-led shelves and pre-play hierarchy, not its branding, assets, or custom keyboard. Follow [Apple's focus guidance](https://developer.apple.com/design/human-interface-guidelines/focus-and-selection): moving focus previews a title; pressing Select opens it.

![Plex Apple TV browse reference with featured artwork and poster shelves](https://www.plex.tv/wp-content/uploads/2025/01/Shows-Conners-scaled.jpg)

![Plex Apple TV movie detail reference with a prominent primary action](https://www.plex.tv/wp-content/uploads/2025/01/Preplay-Contractor-scaled.jpg)

[Plex Apple TV search reference](https://www.plex.tv/wp-content/uploads/2025/01/tvOS-Search-scaled.jpg)

[Plex later moved navigation back to the left](https://forums.plex.tv/t/navigation-updates-coming-to-preview-users/941751/1).
Podible uses a left sidebar that expands when focused. Press Left from the first poster to enter the
sidebar, Up or Down to choose a destination, and Select to open it.

## Prototype

- Browse movie and TV shelves, search, open title details, request a movie or specific seasons, and revisit your requests.
- Demo mode needs no server; its requests are temporary and its player uses an Apple sample stream.
- Settings can connect to an HTTPS Podible origin with an existing user bearer token stored in the device Keychain. Catalog and request features use existing JSON-RPC methods, not admin endpoints.
- Connected titles can play explicitly shared [local Plex trailers](TRAILERS.md). Catalog availability is not playback authorization.

```sh
xcodebuild -project tvos/PodibleTV.xcodeproj -scheme PodibleTV \
  -destination 'generic/platform=tvOS Simulator' CODE_SIGNING_ALLOWED=NO build
```

## Simulator focus and screenshots

With an Apple TV simulator installed, substitute its ID from `xcrun simctl list devices available`.
The UI test uses Siri Remote actions and captures Home, sidebar focus, Search, and the TV season request flow.

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

1. Add phone-based linking so viewers never type session tokens on TV. Verify movie and season requests with a non-admin account on a test instance.
2. Link the viewer's Plex account, explicitly select the Plex server, and resolve titles to exact server-scoped movie and episode identities. Verify AVKit direct play and Plex progress reporting before adding a tested transcode fallback.
3. Add season and episode playback, continue watching, and live request status. Keep transfer progress, Plex publication, and Plex-verified readiness distinct. Test remote focus restoration, sign-in, and playback on physical Apple TV before distribution.

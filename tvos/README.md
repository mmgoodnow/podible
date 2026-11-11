# Podible for Apple TV

Browse what's already available and request what's missing without leaving the TV. Podible owns discovery and requests; Plex remains the playback and watch-history authority.

## Design references

Use [Plex's Apple TV preview](https://www.plex.tv/blog/new-year-same-mission/) for artwork-led shelves and pre-play hierarchy, not its branding, assets, or custom keyboard. Follow [Apple's focus guidance](https://developer.apple.com/design/human-interface-guidelines/focus-and-selection): moving focus previews a title; pressing Select opens it.

![Plex Apple TV browse reference with featured artwork and poster shelves](https://www.plex.tv/wp-content/uploads/2025/01/Shows-Conners-scaled.jpg)

![Plex Apple TV movie detail reference with a prominent primary action](https://www.plex.tv/wp-content/uploads/2025/01/Preplay-Contractor-scaled.jpg)

[Plex Apple TV search reference](https://www.plex.tv/wp-content/uploads/2025/01/tvOS-Search-scaled.jpg)

## Prototype

- Browse movie and TV shelves, search, open title details, request a movie or specific seasons, and revisit your requests.
- Demo mode needs no server; its requests are temporary and its player uses an Apple sample stream.
- Settings can connect to an HTTPS Podible origin with an existing user bearer token stored in the device Keychain. This uses existing JSON-RPC methods, not admin endpoints. It does not stream from Plex or treat catalog availability as playback authorization.

```sh
xcodebuild -project tvos/PodibleTV.xcodeproj -scheme PodibleTV \
  -destination 'generic/platform=tvOS Simulator' CODE_SIGNING_ALLOWED=NO build
```

## Next milestones

1. Add phone-based linking so viewers never type session tokens on TV. Verify movie and season requests with a non-admin account on a test instance.
2. Link the viewer's Plex account, explicitly select the Plex server, and resolve titles to exact server-scoped movie and episode identities. Verify AVKit direct play and Plex progress reporting before adding a tested transcode fallback.
3. Add season and episode playback, continue watching, and live request status. Keep transfer progress, Plex publication, and Plex-verified readiness distinct. Test remote focus restoration, sign-in, and playback on physical Apple TV before distribution.

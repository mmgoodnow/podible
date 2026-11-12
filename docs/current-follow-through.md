# Current Follow-Through

## Settings And Integrations

- [x] Remove the movie download group setting and its filesystem override.
- [x] Remove the automatic movie upgrades switch. Keep RSS-driven upgrades gated by the requested quality policy and verified publication.
- [x] Group Plex sign-in and publishing, Seerr, TMDB, Radarr and Sonarr under Integrations. Move the read-only library preview out of Storage.
- [ ] Combine Prowlarr and manual Torznab sources for search and feeds. Deduplicate endpoint aliases, keep discovery status separate, and do not let one integration outage stop the other.
- [ ] Replace the Resend-specific notification transport with SMTP. Match Seerr's connection and TLS settings without displaying its credentials; preserve email for users, Pushover for the owner, and dry-run delivery in production.
- [ ] Keep cross-seed coverage as a tie-breaker, not a quality or viability override. Remove its settings toggle once that behavior is enforced.
- [ ] Prefer proper/repack releases by default without a toggle. A correction label must not waive eligibility or regress quality.
- [ ] Add freeform video selection guidance to Decisions and its agent fallback. Keep ordered quality tiers and hard playback constraints enforceable. Replace soft scoring controls only after testing stable upgrade decisions.

## Acquisition And Production Proof

- [ ] Expand Decisions with escalation to book, movie and TV selection and import. Use bounded evidence, never pass credentials, and measure fallback rate, latency and cost.
- [ ] Make the ten-minute stall threshold internal. Check connectivity before recovery; preserve current transfers through outages and keep the only viable download eligible for import. Shorten the threshold only after those safeguards are proven.
- [ ] Run the Khom Khlang season-pack pilot on real production inbox offers. Record reuse, download and import outcomes; verify exact Plex identities and paths if the pack is promoted.
- [ ] Implement automatic retirement of provably superseded episode torrent registrations once the pack is verified and seed obligations are met. Keep any referenced payload bytes.
- [ ] Capture a fresh production download-complete-to-Plex trace, verify the open UI updates on its own, and compare each step in Grafana.

## Apple TV

- [ ] Generate Swift wire types from the shared JSON-RPC schemas at build time and check contract drift. Reuse the transport Kindling already calls.
- [ ] Add QR sign-in via the self-hosted Podible origin and existing account authorization. Use a short-lived pairing grant and a separate revocable TV session.
- [ ] Add movie and episode playback against the viewer's Plex access, exact server/title identities, and verified progress reporting. Test in the simulator and on device.
- [ ] Expand trailer discovery beyond approved local Plex clips using playable, authorized sources.

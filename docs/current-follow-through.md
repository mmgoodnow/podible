# Current Follow-Through

## Settings And Integrations

- [x] Remove the movie download group setting and its filesystem override.
- [x] Remove the automatic movie upgrades switch. Keep RSS-driven upgrades gated by the requested quality policy and verified publication.
- [x] Group Plex sign-in and publishing, Seerr, TMDB, Radarr and Sonarr under Integrations. Move the read-only library preview out of Storage.
- [x] Combine Prowlarr and manual Torznab sources for search and feeds. Deduplicate endpoint aliases and keep discovery status separate.
- [ ] Let direct sources keep working during discovery outages without treating the unavailable Prowlarr sources as searched or caught up.
- [x] Replace the Resend-specific notification transport with SMTP, preserving user email, owner Pushover and dry run. Require TLS and do not resend ambiguous deliveries.
- [ ] Configure Podible with the same authenticated port 587 relay as Seerr after normal CD. Verify TLS/authentication without sending mail and confirm dry run stays enabled.
- [x] Keep cross-seed coverage as a tie-breaker, not a quality or viability override. Remove its settings toggle.
- [x] Prefer proper/repack releases without a toggle. A correction label does not waive eligibility or regress quality. Retain the old serialized policy field solely to avoid splitting existing targets.
- [ ] Add freeform video selection guidance to Decisions and its agent fallback. Keep ordered quality tiers and hard playback constraints enforceable. Replace soft scoring controls only after testing stable upgrade decisions.

## Acquisition And Production Proof

- [ ] Expand Decisions with escalation to book, movie and TV selection and import. Use bounded evidence, never pass credentials, and measure fallback rate, latency and cost.
- [ ] Make the ten-minute stall threshold internal. Check connectivity before recovery; preserve current transfers through outages and keep the only viable download eligible for import. Shorten the threshold only after those safeguards are proven.
- [x] Finish the Khom Khlang season-pack pilot on real production inbox offers. Match job 63190 dispatched upgrade job 63191. See the live verification and timing in [TV](tv.md).
- [ ] Implement automatic retirement of provably superseded episode torrent registrations once the pack is verified and seed obligations are met. Keep any referenced payload bytes.
- [x] Verify production Prometheus receives the acquisition timing samples used by Grafana. The Khom Khlang phase deltas matched the backend trace.
- [ ] Verify the open UI updates on its own during a production publication.

## Apple TV

- [ ] Generate Swift wire types from the shared JSON-RPC schemas at build time and check contract drift. Reuse the transport Kindling already calls.
- [ ] Add QR sign-in via the self-hosted Podible origin and existing account authorization. Use a short-lived pairing grant and a separate revocable TV session.
- [ ] Add movie and episode playback against the viewer's Plex access, exact server/title identities, and verified progress reporting. Test in the simulator and on device.
- [ ] Expand trailer discovery beyond approved local Plex clips using playable, authorized sources.

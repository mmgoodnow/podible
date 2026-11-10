# Library home rails

The unfiltered All view uses bounded horizontal rails. Searching replaces them with existing local/catalog results; individual media tabs retain their grids. Rail tracks support keyboard focus, Left/Right and Home/End, as well as ordinary link/tab navigation and labelled scroll buttons.

## Evidence and boundaries

- Your requests uses the existing authenticated own-request read models. Partly available TV requests remain visible. The rail shows up to ten movie and ten TV cards, with counted media links for the rest.
- Coming up uses only the viewer's unfulfilled requests with known dates in the next 14 days (UTC). Movie dates are catalog release dates, not confirmed streaming or disc dates. Existing movies without a saved date remain absent until the catalog supplies one. The rail never starts a search.
- Newly added books use the first persisted asset import timestamp, not book release dates, IDs, metadata updates or pending additions. Only books with imported assets are included.
- Newly added movies/TV use the first persisted verified publication event and require current selected content. An upgrade does not make an old title newly added. Rows with no historical publication event are omitted rather than assigned an invented date. Known content-loss tombstones are excluded.
- Recently aired, in your library uses real episode air dates within the preceding 30 calendar days and exact selected healthy file coverage. Future/undated episodes are omitted. This is not a recently-downloaded claim. There is no per-episode publication timestamp, so a separate recently-available episode rail is intentionally omitted.
- Availability is the persisted publication/selection state, not a fresh disk/Plex probe. These read-only projections do not perform network, filesystem or hashing work, or initiate acquisitions.

`catalog.recent({limit?:1..20})` returns `{books,movies,tv,episodes,upcoming}` from bounded SQL projections. `catalog.discovery({})` returns `{configured,movies:{items,unavailable},tv:{items,unavailable}}`. Both require authentication and return only card metadata, never ownership paths or private acquisition diagnostics.

## Discovery

Trending uses two fixed TMDB weekly list endpoints, reusing the stored movie TMDB key/token. There are no per-card lookups or periodic title searches. Public metadata is cached server-side for ten minutes, coalesced across simultaneous callers; failures are cached for one minute. Credential hashes partition the bounded cache. Each list is limited to 20 cards, 512 KiB and an eight-second timeout. One failed list does not discard the other. Upstream error bodies/URLs/credentials are never returned. Browser artwork URLs are restricted to normalized official TMDB image paths.

Discovery is sent separately from local RPC batches, so remote latency cannot gate local library cards. No discovery polling occurs. The browser independently queries own requests and recent publications; new remote queries do not replace authorization or shared-library boundaries.

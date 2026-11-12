# Unified Library

The primary Library screen combines books, published shared movies/TV, and the
current user's movie/TV requests. One search field filters local content while
remote book, movie and TV results arrive below it. Media filters keep the same
screen. Detail-page back links return to the relevant Library filter. Existing
`/add`, `/movies` and `/requests` SPA URLs still resolve, but are not separate
primary navigation destinations.

The authenticated book bootstrap is unchanged: one request supplies identity and
the first book page without a separate authentication waterfall. Filtering and
pagination retain their own query keys. Shared published movie/TV search uses
bounded local SQL reads through user `catalog.available({ query?, limit? })`.
Other users' pending/withdrawn requests, decision traces, download paths and
credentials are not included. `catalog.tvEntry({ entryId })` exposes show
metadata, episode progress, and cross-seed coverage for a published show or a
show the viewer requested. Coverage comes from reuse plans matched to exact
episode file mappings. Full-season cross-seeds appear on the season; partial
coverage appears on the matched episodes. The counts are registrations, not
live upload activity. For older registrations without a reuse plan, stewardship
recovers exact mappings from owned torrent layouts without reading media bytes.

Remote queries are debounced 350 ms and cached per authenticated actor. Each
provider has independent loading, failure and retry state. Remote search calls
use independent typed tRPC HTTP links, not a batch response barrier that would
make a slow Open Library request hold back movie/TV results. Remote failure is
not rendered as a successful empty catalog. Existing local movie details/follow
actions reuse stored metadata instead of requiring an available remote catalog.

Native user TV details live at `/tv/:tmdbId`; users explicitly select complete
canonical seasons. `/tv/requests/:requestId` shows verified episode counts,
future intent and sanitized acquisition progress, and allows own-request
withdrawal. `/tv/library/:entryId` displays shared published work metadata without
exposing its requesters. See [TV identities and catalog](tv.md).

Admin `/recovery` is separate from ordinary discovery: it previews physical
presence and ownership, then requires an explicit bounded recovery command.
Loading the screen never starts downloads or deletes anything.

Browser acceptance covers independent results with the book provider blocked,
local filtering/pagination/bootstrap, season selection, durable request/deep
link/withdrawal, actor isolation, recovery and mobile layout. These checks run
in normal CI; they do not manufacture a production upgrade or episode arrival.

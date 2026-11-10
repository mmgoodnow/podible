import { useDeferredValue, useEffect, useState } from "react";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import { useAcquisitionEvents } from "./acquisition-events";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { AddCandidate, Failure, Library } from "./books";
import { Link, useLocation } from "./router";
import { ScreenCard } from "./screen-card";
import { movieBadge, movieDetail, tvBadge, tvDetail } from "./screen-status";
import { ContentRail, RailBook } from "./content-rail";

type Media = "all" | "books" | "movies" | "tv";
const mediaTypes: Media[] = ["all", "books", "movies", "tv"];
type MovieRequest = inferRouterOutputs<AppRouter>["requests"]["list"]["requests"][number];
type TvRequest = inferRouterOutputs<AppRouter>["tv"]["requestCards"]["requests"][number];

export function UnifiedLibrary({ initialPage }: {
  initialPage?: inferRouterOutputs<AppRouter>["library"]["bootstrap"];
}) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const route = useLocation();
  const params = new URLSearchParams(route.split("?")[1]);
  const initialMedia = params.get("media") as Media;
  const [q, setQ] = useState(params.get("q") ?? "");
  const [media, setMedia] = useState<Media>(mediaTypes.includes(initialMedia) ? initialMedia : "all");
  const [remoteQ, setRemoteQ] = useState("");
  const localQ = useDeferredValue(q.trim());
  useEffect(() => {
    const p = new URLSearchParams(route.split("?")[1]);
    setQ(p.get("q") ?? "");
    const m = p.get("media") as Media;
    setMedia(mediaTypes.includes(m) ? m : "all");
  }, [route]);
  useEffect(() => {
    const timer = setTimeout(() => setRemoteQ(q.trim().length >= 2 ? q.trim() : ""), 350);
    return () => clearTimeout(timer);
  }, [q]);
  const booksShown = media === "all" || media === "books";
  const moviesShown = media === "all" || media === "movies";
  const tvShown = media === "all" || media === "tv";
  const homeRails = media === "all" && !q.trim();
  const recent = useQuery(trpc.catalog.recent.queryOptions({ limit: 12 }, { enabled: homeRails, staleTime: 60_000 }));
  const discovery = useQuery(trpc.catalog.discovery.queryOptions({}, { enabled: homeRails, staleTime: 10 * 60_000, retry: false }));
  const movies = useQuery(trpc.requests.list.queryOptions({}, { enabled: moviesShown }));
  const tv = useQuery(trpc.tv.requestCards.queryOptions({}, { enabled: tvShown }));
  const available = useQuery(trpc.catalog.available.queryOptions({ query: localQ, limit: 50 }, { enabled: !homeRails && (moviesShown || tvShown) }));
  useAcquisitionEvents(moviesShown || tvShown, () => {
    if (moviesShown) void cache.invalidateQueries({ queryKey: trpc.requests.list.pathKey() });
    if (tvShown) void cache.invalidateQueries({ queryKey: trpc.tv.requestCards.pathKey() });
    if (homeRails) void cache.invalidateQueries({ queryKey: trpc.catalog.recent.pathKey() });
    else void cache.invalidateQueries({ queryKey: trpc.catalog.available.pathKey() });
  });
  const movieStatus = useQuery(trpc.movies.status.queryOptions({}, { enabled: moviesShown && Boolean(remoteQ) }));
  const remoteBooks = useQuery(trpc.openlibrary.search.queryOptions({ q: remoteQ, limit: 20 }, { enabled: booksShown && Boolean(remoteQ), staleTime: 10 * 60_000 }));
  const remoteMovies = useQuery(trpc.movies.search.queryOptions({ query: remoteQ }, { enabled: moviesShown && Boolean(remoteQ) && movieStatus.data?.catalogConfigured === true, staleTime: 10 * 60_000 }));
  const remoteTv = useQuery(trpc.tv.search.queryOptions({ query: remoteQ }, { enabled: tvShown && Boolean(remoteQ), staleTime: 10 * 60_000 }));
  const term = q.trim().toLocaleLowerCase();
  const movieItems = movies.data?.requests.filter(r => r.state !== "withdrawn" && r.movie.title.toLocaleLowerCase().includes(term)) ?? [];
  const tvItems = tv.data?.requests.filter(r => r.entry.title.toLocaleLowerCase().includes(term)) ?? [];
  const requestedMovies = new Set(movieItems.map(r => r.movie.tmdbId));
  const requestedTv = new Set(tvItems.map(r => r.entry.key));
  const sharedMovies = available.data?.movies.filter(movie => !requestedMovies.has(movie.tmdbId)) ?? [];
  const publishedTv = available.data?.tv ?? [];
  const publishedTvKeys = new Set(publishedTv.map(entry => entry.key));
  const tvRequestByShow = new Map(tvItems.map(request => [request.entry.key, request]));
  const readyMovies = movieItems.filter(request => request.available);
  const pendingMovies = movieItems.filter(request => !request.available);
  const readyTv = tvItems.filter(request => request.availableEpisodes > 0 && !request.intentionallyAbsent && !publishedTvKeys.has(request.entry.key));
  const pendingTv = tvItems.filter(request => request.availableEpisodes === 0 || request.intentionallyAbsent);
  const outstandingTv = tvItems.filter(request => request.intentionallyAbsent || request.availableEpisodes < request.requestedEpisodes);
  const hasRequests = pendingMovies.length > 0 || outstandingTv.length > 0;
  const loadingRequests = !hasRequests && (movies.isPending || tv.isPending);
  const movieCard = (request: MovieRequest) => <ScreenCard key={`request:movie:${request.id}`} title={request.movie.title} year={request.movie.year}
    posterUrl={request.movie.posterUrl} href={`/movies/${request.movie.tmdbId}`} media="movie" badge={movieBadge(request)} source="Your request"
    detail={movieDetail(request)}
    progress={!request.available && request.state === "running" && request.stage === "downloading" && request.download?.bytesDone != null && request.download.sizeBytes != null ? {
      value: request.download.bytesDone, max: request.download.sizeBytes, label: `${request.movie.title} download progress`,
    } : undefined} />;
  const tvCard = (request: TvRequest) => <ScreenCard key={`request:tv:${request.requestId}`} title={request.entry.title} year={request.entry.year} posterUrl={request.entry.posterUrl ?? null}
    href={`/tv/library/${request.entryId}`} media="tv" badge={tvBadge(request)} source={`Season${request.seasonNumbers.length === 1 ? "" : "s"} ${request.seasonNumbers.join(", ")}`}
    detail={tvDetail(request)}
    progress={request.intentionallyAbsent ? undefined : { value: request.availableEpisodes, max: request.requestedEpisodes, label: `${request.entry.title} available episodes` }} />;
  const searching = remoteQ === q.trim() && Boolean(remoteQ);
  const currentBookKeys = new Set(initialPage?.library.items.map(b => b.identifiers.openlibrary).filter(Boolean) ?? []);
  return <>
    <div className="library-toolbar">
      <h1>Library</h1>
      <label className="search">Search library and catalogs
        <input type="search" placeholder="Books, movies or TV" value={q} maxLength={200}
          onChange={e => setQ(e.target.value)} />
      </label>
      <Link className="button secondary" href="/add">Find a book</Link>
    </div>
    <div className="media-filters" role="group" aria-label="Media type">
      {mediaTypes.map(m => <button className="secondary" key={m} aria-pressed={media === m} onClick={() => setMedia(m)}>
        {m === "all" ? "All" : m === "tv" ? "TV" : m === "books" ? "Books" : "Movies"}
      </button>)}
    </div>
    {homeRails ? <div className="home-rails">
      {(loadingRequests || hasRequests) && <ContentRail title="Your requests" variant="requests" loading={loadingRequests} more={[
        ...(pendingMovies.length ? [{ href: "/library?media=movies", label: `Movies (${pendingMovies.length})` }] : []),
        ...(outstandingTv.length ? [{ href: "/library?media=tv", label: `TV (${outstandingTv.length})` }] : []),
      ]}>
        {loadingRequests ? [0, 1, 2, 3].map(i => <div key={i} className="content-rail__placeholder" aria-hidden="true" />)
          : <>{pendingMovies.slice(0, 10).map(movieCard)}{outstandingTv.slice(0, 10).map(tvCard)}</>}
      </ContentRail>}
      {movies.isError && <Failure error={movies.error} retry={() => void movies.refetch()} />}
      {tv.isError && <Failure error={tv.error} retry={() => void tv.refetch()} />}
      {recent.isPending && <p role="status">Loading library...</p>}
      {recent.isError && <Failure error={recent.error} retry={() => void recent.refetch()} />}
      {!!recent.data?.books.length && <ContentRail title="Newly added books" more={{ href: "/library?media=books", label: "All books" }}>
        {recent.data.books.map(book => <RailBook key={book.id} book={book} />)}
      </ContentRail>}
      {!!recent.data?.movies.length && <ContentRail title="Newly added movies" more={{ href: "/library?media=movies", label: "All movies" }}>
        {recent.data.movies.map(movie => <ScreenCard key={movie.tmdbId} title={movie.title} year={movie.year} posterUrl={movie.posterUrl}
          href={`/movies/${movie.tmdbId}`} media="movie" badge={{ label: "Available", tone: "available" }} detail="Ready in Plex" />)}
      </ContentRail>}
      {!!recent.data?.tv.length && <ContentRail title="Newly added TV" more={{ href: "/library?media=tv", label: "All TV" }}>
        {recent.data.tv.map(entry => <ScreenCard key={entry.entryId} title={entry.title} year={entry.year} posterUrl={entry.posterUrl}
          href={`/tv/library/${entry.entryId}`} media="tv" badge={{ label: "Available", tone: "available" }} detail="Episodes ready in Plex" />)}
      </ContentRail>}
      {!!recent.data?.episodes.length && <ContentRail title="Recently aired episodes" more={{ href: "/library?media=tv", label: "All TV" }}>
        {recent.data.episodes.map(ep => <ScreenCard key={`${ep.entryId}:${ep.episodeId}`} title={ep.title} posterUrl={ep.posterUrl}
          href={`/tv/library/${ep.entryId}`} media="tv" badge={{ label: "Available", tone: "available" }}
          source={ep.season != null && ep.episode != null ? `S${ep.season} E${ep.episode}` : ep.episodeTitle}
          detail={`Aired ${ep.airDate}`} />)}
      </ContentRail>}
      {!recent.isPending && !recent.isError && recent.data && !recent.data.books.length && !recent.data.movies.length && !recent.data.tv.length &&
        <p className="muted">No newly published titles yet. Browse a media tab or search above.</p>}
      {discovery.isPending && <p role="status">Loading trending titles...</p>}
      {discovery.isError && <p className="muted">Trending titles are unavailable right now.</p>}
      {(["movies", "tv"] as const).map(kind => {
        const section = discovery.data?.[kind];
        return section?.items.length ? <ContentRail key={kind} title={kind === "movies" ? "Trending movies" : "Trending TV"}>
          {section.items.map(item => <ScreenCard key={item.tmdbId} title={item.title} year={item.year} posterUrl={item.posterUrl}
            href={kind === "movies" ? `/movies/${item.tmdbId}` : `/tv/${item.tmdbId}`} media={kind === "movies" ? "movie" : "tv"}
            badge={{ label: kind === "movies" ? "Movie" : "TV series", tone: "catalog" }} detail={kind === "movies" ? "View movie" : "Choose seasons"} />)}
        </ContentRail> : section?.unavailable ? <p className="muted" key={kind}>Trending {kind === "movies" ? "movies" : "TV"} unavailable right now.</p> : null;
      })}
    </div> : <section aria-label="In the library">
      {booksShown && <section className="media-section library-books" aria-label="Your books">
        <h2 className="visually-hidden">Books</h2><Library initialPage={initialPage} query={q} embedded />
      </section>}
      {moviesShown && (!term || movieItems.length > 0 || (available.data?.movies.length ?? 0) > 0 || movies.isPending || movies.isError || available.isError) && <section className="media-section screen-shelf" aria-label="Your movies">
        <div className="screen-shelf__heading"><h2>Movies</h2><span>{readyMovies.length + sharedMovies.length} available</span></div>
        {available.isError && <Failure error={available.error} retry={() => void available.refetch()} />}
        <div className="screen-grid">
          {readyMovies.map(movieCard)}
          {sharedMovies.map(movie => <ScreenCard key={`shared:${movie.tmdbId}`} title={movie.title} year={movie.year} posterUrl={movie.posterUrl}
            href={`/movies/${movie.tmdbId}`} media="movie" badge={{ label: "Available", tone: "available" }} detail="Ready in Plex" />)}
        </div>
        {pendingMovies.length > 0 && <section className="screen-requests" aria-label="Movie requests">
          <div className="screen-shelf__heading"><h3>Movie requests</h3><span>{pendingMovies.length} not available</span></div>
          <div className="screen-grid">{pendingMovies.slice(0, 6).map(movieCard)}</div>
          {pendingMovies.length > 6 && <details className="screen-requests__more">
            <summary>Show all requests <span>{pendingMovies.length - 6} more</span></summary>
            <div className="screen-grid">{pendingMovies.slice(6).map(movieCard)}</div>
          </details>}
        </section>}
        {available.data?.movieTruncated && <p className="muted">Showing the first 50 published movies. Refine your search for more.</p>}
        {movies.isError ? <Failure error={movies.error} retry={() => void movies.refetch()} /> : movies.isPending ? <p role="status">Loading your movies...</p> : !movieItems.length && !sharedMovies.length ?
          <p className="muted">{term ? "No matching movie requests." : "Search above to find and request a movie."}</p> : null}
      </section>}
      {tvShown && (!term || tvItems.length > 0 || publishedTv.length > 0 || tv.isPending || tv.isError || available.isError) && <section className="media-section screen-shelf" aria-label="TV">
        <div className="screen-shelf__heading"><h2>TV</h2><span>{readyTv.length + publishedTv.length} available</span></div>
        {available.isError && <Failure error={available.error} retry={() => void available.refetch()} />}
        <div className="screen-grid">
          {readyTv.map(tvCard)}
          {publishedTv.map(entry => {
            const request = tvRequestByShow.get(entry.key);
            return <ScreenCard key={`show:${entry.entryId}`} title={entry.title} year={entry.year} posterUrl={entry.posterUrl}
              href={`/tv/library/${entry.entryId}`} media="tv" badge={{ label: "Available", tone: "available" }}
              progress={request && !request.intentionallyAbsent && request.availableEpisodes < request.requestedEpisodes ? {
                value: request.availableEpisodes, max: request.requestedEpisodes, label: `${entry.title} available episodes`,
              } : undefined}
              detail={`${entry.availableEpisodes} episode${entry.availableEpisodes === 1 ? "" : "s"} ready`} />;
          })}
        </div>
        {pendingTv.length > 0 && <section className="screen-requests" aria-label="TV requests">
          <div className="screen-shelf__heading"><h3>TV requests</h3><span>{pendingTv.length} not available</span></div>
          <div className="screen-grid">{pendingTv.slice(0, 6).map(tvCard)}</div>
          {pendingTv.length > 6 && <details className="screen-requests__more">
            <summary>Show all requests <span>{pendingTv.length - 6} more</span></summary>
            <div className="screen-grid">{pendingTv.slice(6).map(tvCard)}</div>
          </details>}
        </section>}
        {available.data?.tvTruncated && <p className="muted">Showing the first 50 published TV works. Refine your search for more.</p>}
        {tv.isError ? <Failure error={tv.error} retry={() => void tv.refetch()} /> : tv.isPending ? <p role="status">Loading TV requests...</p> : !tvItems.length && !publishedTv.length ?
          <p className="muted">{term ? "No matching TV requests." : "Search above to find TV and choose seasons."}</p> : null}
      </section>}
    </section>}
    {searching && <section className="remote-results" aria-label="Catalog results">
      <h2>Catalog matches</h2>
      <div className="remote-columns unified-catalogs">
      {booksShown && <section className="media-section" aria-label="Book catalog"><h3>Books</h3>
        {remoteBooks.isError ? <Failure error={remoteBooks.error} retry={() => void remoteBooks.refetch()} /> : remoteBooks.isPending ? <p role="status">Searching book catalog...</p> : <>
          {remoteBooks.data.results.filter(b => !currentBookKeys.has(b.openLibraryKey)).map(b => <AddCandidate key={b.openLibraryKey} candidate={b} compact />)}
          {!remoteBooks.data.results.length && <p>No book catalog matches.</p>}
        </>}
      </section>}
      {moviesShown && <section className="media-section" aria-label="Movie catalog"><h3>Movies</h3>
        {movieStatus.isError ? <Failure error={movieStatus.error} retry={() => void movieStatus.refetch()} /> : movieStatus.isPending ? <p role="status">Checking movie catalog...</p> : !movieStatus.data.catalogConfigured ? <p>Movie catalog is not configured.</p> : remoteMovies.isError ? <Failure error={remoteMovies.error} retry={() => void remoteMovies.refetch()} /> : remoteMovies.isPending ? <p role="status">Searching movie catalog...</p> : <div className="screen-grid">
          {remoteMovies.data.results.map(m => <ScreenCard key={m.tmdbId} title={m.title} posterUrl={m.posterUrl} year={m.year} media="movie"
            badge={{ label: "Movie", tone: "catalog" }} detail="View movie" href={`/movies/${m.tmdbId}`} />)}
          {!remoteMovies.data.results.length && <p>No movie catalog matches.</p>}
        </div>}
      </section>}
      {tvShown && <section className="media-section" aria-label="TV catalog"><h3>TV</h3>
        {remoteTv.isError ? <Failure error={remoteTv.error} retry={() => void remoteTv.refetch()} /> : remoteTv.isPending ? <p role="status">Searching TV catalog...</p> : remoteTv.data.unavailableReason ? <p role="alert" className="notice">{remoteTv.data.unavailableReason}</p> : <div className="screen-grid">
          {remoteTv.data.results.map(t => <ScreenCard key={t.tmdbId} title={t.title} posterUrl={t.posterUrl} year={t.year} media="tv"
            badge={{ label: "TV series", tone: "catalog" }} detail="Choose seasons" href={`/tv/${t.tmdbId}`} />)}
          {!remoteTv.data.results.length && <p>No TV catalog matches.</p>}
        </div>}
      </section>}
      </div>
    </section>}
  </>;
}

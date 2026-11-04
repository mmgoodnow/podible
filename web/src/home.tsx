import { useDeferredValue, useEffect, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { AddCandidate, Failure, Library } from "./books";
import { Link, useLocation } from "./router";

type Media = "all" | "books" | "movies" | "tv";
const mediaTypes: Media[] = ["all", "books", "movies", "tv"];
function CatalogCard({ title, image, detail, href }: { title: string; image: string | null; detail: string; href: string }) {
  return <Link className="search-result-card" href={href}>
    <div className="jacket">{image ? <img src={image} alt="" loading="lazy" /> : <span>{title}</span>}</div>
    <h3>{title}</h3><p>{detail}</p>
  </Link>;
}

export function UnifiedLibrary({ initialPage }: {
  initialPage?: inferRouterOutputs<AppRouter>["library"]["bootstrap"];
}) {
  const trpc = useTRPC();
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
  const movies = useQuery(trpc.requests.list.queryOptions({}, { enabled: moviesShown, refetchInterval: 15_000 }));
  const tv = useQuery(trpc.tv.myRequests.queryOptions({}, { enabled: tvShown, refetchInterval: 15_000 }));
  const available = useQuery(trpc.catalog.available.queryOptions({ query: localQ, limit: 50 }, { enabled: moviesShown || tvShown, refetchInterval: 15_000 }));
  const movieStatus = useQuery(trpc.movies.status.queryOptions({}, { enabled: moviesShown && Boolean(remoteQ) }));
  const remoteBooks = useQuery(trpc.openlibrary.search.queryOptions({ q: remoteQ, limit: 20 }, { enabled: booksShown && Boolean(remoteQ), staleTime: 10 * 60_000 }));
  const remoteMovies = useQuery(trpc.movies.search.queryOptions({ query: remoteQ }, { enabled: moviesShown && Boolean(remoteQ) && movieStatus.data?.catalogConfigured === true, staleTime: 10 * 60_000 }));
  const remoteTv = useQuery(trpc.tv.search.queryOptions({ query: remoteQ }, { enabled: tvShown && Boolean(remoteQ), staleTime: 10 * 60_000 }));
  const term = q.trim().toLocaleLowerCase();
  const movieItems = movies.data?.requests.filter(r => r.state !== "withdrawn" && r.movie.title.toLocaleLowerCase().includes(term)) ?? [];
  const tvItems = tv.data?.requests.filter(r => !r.withdrawn && r.entry.title.toLocaleLowerCase().includes(term)) ?? [];
  const requestedMovies = new Set(movieItems.map(r => r.movie.tmdbId));
  const requestedTv = new Set(tvItems.map(r => r.entry.key));
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
    <section aria-label="In your library">
      {booksShown && <section className="media-section library-books" aria-label="Your books">
        <h2 className="visually-hidden">Books</h2><Library initialPage={initialPage} query={q} embedded />
      </section>}
      {moviesShown && (!term || movieItems.length > 0 || (available.data?.movies.length ?? 0) > 0 || movies.isPending || movies.isError || available.isError) && <section className="media-section" aria-label="Your movies">
        <h2>Movies</h2>
        {available.isError && <Failure error={available.error} retry={() => void available.refetch()} />}
        {available.data?.movies.filter(m => !requestedMovies.has(m.tmdbId)).map(m => <Link className="request-row" key={m.tmdbId} href={`/movies/${m.tmdbId}`}>
          <div><h3>{m.title}</h3><p>{m.year ?? ""} · Available in shared library</p></div><span>Open</span>
        </Link>)}
        {available.data?.movieTruncated && <p className="muted">Showing the first 50 published movies. Refine your search for more.</p>}
        {movies.isError ? <Failure error={movies.error} retry={() => void movies.refetch()} /> : movies.isPending ? <p role="status">Loading your movies...</p> : movieItems.length ?
          <div className="request-list">{movieItems.map(r => <Link className="request-row" key={r.id} href={`/requests/${r.id}`}>
            <div><h3>{r.movie.title}</h3><p>{r.movie.year ?? ""} · {r.available ? "Available in Plex" : r.state === "blocked" ? "Waiting for a suitable release" : r.state}</p></div>
            <span>{r.available ? "Open" : "Track request"}</span>
          </Link>)}</div> : <p className="muted">{term ? "No matching movie requests." : "Search above to find and request a movie."}</p>}
      </section>}
      {tvShown && (!term || tvItems.length > 0 || (available.data?.tv.length ?? 0) > 0 || tv.isPending || tv.isError || available.isError) && <section className="media-section" aria-label="Your TV">
        <h2>TV</h2>
        {available.isError && <Failure error={available.error} retry={() => void available.refetch()} />}
        {available.data?.tv.filter(t => !requestedTv.has(t.key)).map(t => <Link className="request-row" key={t.entryId} href={`/tv/library/${t.entryId}`}>
          <div><h3>{t.title}</h3><p>{t.availableEpisodes} episodes in shared library</p></div><span>Open</span>
        </Link>)}
        {available.data?.tvTruncated && <p className="muted">Showing the first 50 published TV works. Refine your search for more.</p>}
        {tv.isError ? <Failure error={tv.error} retry={() => void tv.refetch()} /> : tv.isPending ? <p role="status">Loading your TV...</p> : tvItems.length ?
          <div className="request-list">{tvItems.map(r => <Link className="request-row" key={r.requestId} href={`/tv/requests/${r.requestId}`}>
            <div><h3>{r.entry.title}</h3><p>Seasons {r.seasonNumbers.join(", ")} · {r.availableEpisodes}/{r.requestedEpisodes} episodes ready{r.futureEpisodes ? ` · ${r.futureEpisodes} not yet aired` : ""}</p></div><span>View request</span>
          </Link>)}</div> : <p className="muted">{term ? "No matching TV requests." : "Search above to find TV and choose seasons."}</p>}
      </section>}
    </section>
    {searching && <section className="remote-results" aria-label="Catalog results">
      <h2>Find more</h2><p className="muted">Catalog results for “{remoteQ}” arrive independently. Your library stays above.</p>
      <div className="remote-columns">
      {booksShown && <section className="media-section" aria-label="Book catalog"><h3>Books</h3>
        {remoteBooks.isError ? <Failure error={remoteBooks.error} retry={() => void remoteBooks.refetch()} /> : remoteBooks.isPending ? <p role="status">Searching book catalog...</p> : <>
          {remoteBooks.data.results.filter(b => !currentBookKeys.has(b.openLibraryKey)).map(b => <AddCandidate key={b.openLibraryKey} candidate={b} compact />)}
          {!remoteBooks.data.results.length && <p>No book catalog matches.</p>}
        </>}
      </section>}
      {moviesShown && <section className="media-section" aria-label="Movie catalog"><h3>Movies</h3>
        {movieStatus.isError ? <Failure error={movieStatus.error} retry={() => void movieStatus.refetch()} /> : movieStatus.isPending ? <p role="status">Checking movie catalog...</p> : !movieStatus.data.catalogConfigured ? <p>Movie catalog is not configured.</p> : remoteMovies.isError ? <Failure error={remoteMovies.error} retry={() => void remoteMovies.refetch()} /> : remoteMovies.isPending ? <p role="status">Searching movie catalog...</p> : <div className="catalog-grid">
          {remoteMovies.data.results.map(m => <CatalogCard key={m.tmdbId} title={m.title} image={m.posterUrl} detail={String(m.year ?? "Movie")} href={`/movies/${m.tmdbId}`} />)}
          {!remoteMovies.data.results.length && <p>No movie catalog matches.</p>}
        </div>}
      </section>}
      {tvShown && <section className="media-section" aria-label="TV catalog"><h3>TV</h3>
        {remoteTv.isError ? <Failure error={remoteTv.error} retry={() => void remoteTv.refetch()} /> : remoteTv.isPending ? <p role="status">Searching TV catalog...</p> : remoteTv.data.unavailableReason ? <p role="alert" className="notice">{remoteTv.data.unavailableReason}</p> : <div className="catalog-grid">
          {remoteTv.data.results.map(t => <CatalogCard key={t.tmdbId} title={t.title} image={t.posterUrl} detail={String(t.year ?? "TV")} href={`/tv/${t.tmdbId}`} />)}
          {!remoteTv.data.results.length && <p>No TV catalog matches.</p>}
        </div>}
      </section>}
      </div>
    </section>}
  </>;
}

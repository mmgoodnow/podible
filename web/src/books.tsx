import { useDeferredValue, useState } from "react";
import {
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from "@tanstack/react-query";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { Link, navigate, useLocation } from "./router";

export type Book = inferRouterOutputs<AppRouter>["library"]["get"]["book"];
export function bytes(value: number) {
  return value >= 1_000_000_000
    ? `${(value / 1_000_000_000).toFixed(1)} GB`
    : `${(value / 1_000_000).toFixed(1)} MB`;
}
type Candidate =
  inferRouterOutputs<AppRouter>["openlibrary"]["search"]["results"][number];

export function Failure({
  error,
  retry,
}: {
  error: { message: string };
  retry?: () => void;
}) {
  return (
    <div className="notice error" role="alert">
      {error.message}{" "}
      {retry && (
        <button className="secondary" onClick={retry}>
          Try again
        </button>
      )}
    </div>
  );
}

export function BookCard({ book }: { book: Book }) {
  return (
    <Link className="book-card" href={`/book/${book.id}`}>
      <div className="jacket">
        {book.coverUrl ? (
          <img src={book.coverUrl} alt="" loading="lazy" />
        ) : (
          <span>{book.title}</span>
        )}
      </div>
      <h2>{book.title}</h2>
      <p>{book.author}</p>
      <span className="state">
        {[book.playback.audio && "Audio", book.playback.ebook && "Ebook"]
          .filter(Boolean)
          .join(" + ") ||
          (book.status === "error"
            ? "Needs attention"
            : "Finding your editions")}
      </span>
    </Link>
  );
}

export function Library() {
  const trpc = useTRPC();
  const [q, setQ] = useState(
    new URLSearchParams(location.search).get("q") ?? "",
  );
  const deferredQ = useDeferredValue(q);
  const books = useInfiniteQuery(
    trpc.library.list.infiniteQueryOptions(
      { q: deferredQ, limit: 48 },
      { getNextPageParam: (last) => last.nextCursor, refetchInterval: 15_000 },
    ),
  );
  const items = books.data?.pages.flatMap((page) => page.items) ?? [];
  return (
    <>
      <div className="library-toolbar">
        <h1>Library</h1>
        <label className="search">
          Filter library
          <input
            type="search"
            value={q}
            onChange={(e) => setQ(e.target.value)}
            placeholder="Title or author"
          />
        </label>
        <Link className="button" href="/add">Find a book</Link>
      </div>
      {books.isPending ? (
        <p role="status">Loading your library...</p>
      ) : books.isError ? (
        <Failure error={books.error} retry={() => void books.refetch()} />
      ) : items.length === 0 ? (
        <section className="empty-shelf">
          <h2>
            {q ? "No matching books." : "No books yet."}
          </h2>
          <p>Find a title and we will look for audio and ebook editions.</p>
          <Link className="button" href="/add">
            Explore the catalog
          </Link>
        </section>
      ) : (
        <>
          <div className="book-grid" aria-busy={q !== deferredQ}>
            {items.map((book) => (
              <BookCard key={book.id} book={book} />
            ))}
          </div>
          {books.hasNextPage && (
            <div className="load-more">
              <button
                disabled={books.isFetchingNextPage}
                onClick={() => void books.fetchNextPage()}
              >
                {books.isFetchingNextPage
                  ? "Loading..."
                  : "Load more"}
              </button>
            </div>
          )}
        </>
      )}
    </>
  );
}

function AddCandidate({ candidate }: { candidate: Candidate }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const add = useMutation(
    trpc.library.create.mutationOptions({
      onSuccess: (result) => {
        void cache.invalidateQueries({ queryKey: trpc.library.list.pathKey() });
        if (result.book) navigate(`/book/${result.book.id}`);
      },
    }),
  );
  return (
    <article className="catalog-result">
      <div className="catalog-spine" aria-hidden="true">
        {candidate.title.slice(0, 1)}
      </div>
      <div>
        <span className="eyebrow">
          {candidate.publishedAt || "OPEN LIBRARY"}
        </span>
        <h2>{candidate.title}</h2>
        <p>{candidate.author}</p>
        {candidate.series.map((series) => (
          <Link
            className="series-link"
            key={`${series.key}:${series.name}`}
            href={seriesHref(series)}
          >
            {series.name}
            {series.position ? ` · ${series.position}` : ""}
          </Link>
        ))}
        {add.isError && <Failure error={add.error} />}
        {add.isSuccess && !add.data.book && (
          <p role="status">
            Added. Acquisition job {add.data.acquisition_job_id} is queued.{" "}
            <Link href="/library">Open library</Link>
          </p>
        )}
      </div>
      <button
        disabled={add.isPending || add.isSuccess}
        onClick={() => add.mutate({ openLibraryKey: candidate.openLibraryKey })}
      >
        {add.isPending
          ? "Adding..."
          : add.isSuccess
            ? "Added"
            : "Add to library"}
      </button>
    </article>
  );
}

export function seriesHref(series: { key: string | null; name: string }) {
  const params = new URLSearchParams({ name: series.name });
  if (series.key) params.set("key", series.key);
  return `/series?${params}`;
}

export function Catalog() {
  const trpc = useTRPC();
  const route = useLocation();
  const params = new URLSearchParams(route.split("?")[1]);
  const query = params.get("q") ?? "";
  const [draft, setDraft] = useState(query);
  const search = useQuery(
    trpc.openlibrary.search.queryOptions(
      { q: query, limit: 30 },
      { enabled: Boolean(query.trim()) },
    ),
  );
  return (
    <>
      <section className="catalog-intro">
        <h1>Find books</h1>
      </section>
      <form
        className="catalog-search"
        onSubmit={(e) => {
          e.preventDefault();
          if (draft.trim())
            navigate(`/add?${new URLSearchParams({ q: draft.trim() })}`);
        }}
      >
        <label className="search">
          Search the book catalog
          <input
            type="search"
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            placeholder="A title or author..."
            required
          />
        </label>
        <button disabled={!draft.trim()}>Search books</button>
      </form>
      {!query ? null : search.isPending ? (
        <p role="status">Searching Open Library...</p>
      ) : search.isError ? (
        <Failure error={search.error} retry={() => void search.refetch()} />
      ) : (
        <>
          <p className="results-label">
            {search.data.results.length} matches for <strong>{query}</strong>
          </p>
          {search.data.results.length ? (
            search.data.results.map((candidate) => (
              <AddCandidate
                key={candidate.openLibraryKey}
                candidate={candidate}
              />
            ))
          ) : (
            <p>No books found. Try another title or author.</p>
          )}
        </>
      )}
    </>
  );
}

export function BookCollection({ kind }: { kind: "series" | "author" }) {
  const trpc = useTRPC();
  const route = useLocation();
  const params = new URLSearchParams(route.split("?")[1]);
  const name = params.get("name") ?? "";
  const series = useQuery(
    trpc.library.series.queryOptions(
      {
        seriesKey: params.get("key") ?? undefined,
        seriesName: name,
        limit: 100,
      },
      { enabled: kind === "series" && Boolean(name || params.get("key")) },
    ),
  );
  const author = useQuery(
    trpc.library.author.queryOptions(
      { authorName: name, limit: 100 },
      { enabled: kind === "author" && Boolean(name) },
    ),
  );
  const result = kind === "series" ? series : author;
  const localKeys = new Set(
    result.data?.libraryBooks.map((book) => book.identifiers.openlibrary),
  );
  return (
    <>
      <Link className="back" href="/library">
        Back to library
      </Link>
      <h1>{name || params.get("key") || "Choose a collection"}</h1>
      {result.isPending && <p role="status">Finding books...</p>}
      {result.isError && (
        <Failure error={result.error} retry={() => void result.refetch()} />
      )}
      {result.data && (
        <>
          <h2 className="editorial-heading">In your library</h2>
          {result.data.libraryBooks.length ? (
            <div className="book-grid">
              {result.data.libraryBooks.map((book) => (
                <BookCard key={book.id} book={book} />
              ))}
            </div>
          ) : (
            <p>No books in your library yet.</p>
          )}
          <h2 className="editorial-heading">Explore the collection</h2>
          {result.data.openLibraryBooks
            .filter((book) => !localKeys.has(book.openLibraryKey))
            .map((candidate) => (
              <AddCandidate
                key={candidate.openLibraryKey}
                candidate={candidate}
              />
            ))}
          {!result.data.openLibraryBooks.length && (
            <p>No additional catalog matches found.</p>
          )}
        </>
      )}
    </>
  );
}

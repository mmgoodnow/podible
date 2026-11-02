import { createRoot } from "react-dom/client";
import { useState } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import { rpc, TRPCProvider, useTRPC } from "./rpc";
import { Link, useLocation } from "./router";
import {
  MovieSearch,
  MovieDetail,
  MovieRequests,
  MovieRequestDetail,
  MovieCredits,
} from "./movies";

const queryClient = new QueryClient({
  defaultOptions: {
    queries: { retry: false, staleTime: 15_000 },
    mutations: { retry: false },
  },
});

function Library() {
  const trpc = useTRPC();
  const [q, setQ] = useState("");
  const books = useQuery(
    trpc.library.list.queryOptions({ q, limit: 100, cursor: undefined }),
  );
  return (
    <>
      <div className="section-heading">
        <div>
          <span className="eyebrow">YOUR COLLECTION</span>
          <h1>A good story awaits.</h1>
        </div>
        <label className="search">
          Filter library
          <input
            type="search"
            value={q}
            onChange={(event) => setQ(event.target.value)}
            placeholder="Title or author"
          />
        </label>
      </div>
      {books.isPending ? (
        <p role="status">Loading your library...</p>
      ) : books.isError ? (
        <p role="alert">
          Could not load your library.{" "}
          <button onClick={() => books.refetch()}>Try again</button>
        </p>
      ) : books.data.items.length === 0 ? (
        <p>
          No books found. <a href="/add">Add a book</a>
        </p>
      ) : (
        <div className="book-grid">
          {books.data.items.map((book) => (
            <a className="book-card" href={`/book/${book.id}`} key={book.id}>
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
                {book.playback.audio || book.playback.ebook
                  ? "Available"
                  : "In progress"}
              </span>
            </a>
          ))}
        </div>
      )}
    </>
  );
}

function App() {
  const route = useLocation();
  const trpc = useTRPC();
  const session = useQuery(trpc.auth.me.queryOptions({}));
  return (
    <div className="shell">
      <header>
        <a href="/app" className="wordmark">
          podible<span>THE LIBRARY</span>
        </a>
        <nav aria-label="Main navigation">
          <Link
            href="/app/library"
            aria-current={
              route === "/app/library" || route === "/app" ? "page" : undefined
            }
          >
            Library
          </Link>
          <Link
            href="/app/movies"
            aria-current={route.startsWith("/app/movies") ? "page" : undefined}
          >
            Movies
          </Link>
          <Link
            href="/app/requests"
            aria-current={
              route.startsWith("/app/requests") ? "page" : undefined
            }
          >
            Requests
          </Link>
          <a href="/">Classic view</a>
        </nav>
        {session.data && <a href="/logout">Sign out</a>}
      </header>
      <main>
        {session.isPending ? (
          <p role="status">Checking your session...</p>
        ) : session.isError ? (
          <section className="welcome">
            <span className="eyebrow">YOUR NEXT CHAPTER</span>
            <h1>Make yourself at home.</h1>
            {session.error.data?.code === "UNAUTHORIZED" ? (
              <>
                <p>Sign in to open your library.</p>
                <a
                  className="button"
                  href={`/login?redirectTo=${encodeURIComponent(location.pathname + location.search)}`}
                >
                  Sign in
                </a>
              </>
            ) : (
              <>
                <p role="alert">
                  The server is unavailable. Your session may still be valid.
                </p>
                <button onClick={() => session.refetch()}>Try again</button>
              </>
            )}
          </section>
        ) : (
          <ActorLibrary key={session.data.user.id} />
        )}
      </main>
      <footer>
        Stories worth finding.
        <MovieCredits />
      </footer>
    </div>
  );
}

function ActorLibrary() {
  const route = useLocation().split("?")[0]!;
  const [actorClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { retry: false, staleTime: 15_000 } },
      }),
  );
  return (
    <QueryClientProvider client={actorClient}>
      <TRPCProvider trpcClient={rpc} queryClient={actorClient}>
        {/^\/app\/movies\/\d+$/.test(route) ? (
          <MovieDetail key={route} tmdbId={Number(route.split("/").at(-1))} />
        ) : route === "/app/movies" ? (
          <MovieSearch />
        ) : /^\/app\/requests\/\d+$/.test(route) ? (
          <MovieRequestDetail
            key={route}
            requestId={Number(route.split("/").at(-1))}
          />
        ) : route === "/app/requests" ? (
          <MovieRequests />
        ) : ["/app", "/app/", "/app/library"].includes(route) ? (
          <Library />
        ) : (
          <p>
            Page not found. <Link href="/app/library">Open library</Link>.
          </p>
        )}
      </TRPCProvider>
    </QueryClientProvider>
  );
}

createRoot(document.getElementById("root")!).render(
  <QueryClientProvider client={queryClient}>
    <TRPCProvider trpcClient={rpc} queryClient={queryClient}>
      <App />
    </TRPCProvider>
  </QueryClientProvider>,
);

import { createRoot } from "react-dom/client";
import { useState } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import { rpc, TRPCProvider, useTRPC } from "./rpc";

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
  const trpc = useTRPC();
  const session = useQuery(trpc.auth.me.queryOptions({}));
  return (
    <div className="shell">
      <header>
        <a href="/app" className="wordmark">
          podible<span>THE LIBRARY</span>
        </a>
        <nav aria-label="Main navigation">
          <a href="/app/library" aria-current="page">
            Library
          </a>
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
      <footer>Books worth keeping. Stories worth finding.</footer>
    </div>
  );
}

function ActorLibrary() {
  const [actorClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { retry: false, staleTime: 15_000 } },
      }),
  );
  return (
    <QueryClientProvider client={actorClient}>
      <TRPCProvider trpcClient={rpc} queryClient={actorClient}>
        <Library />
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

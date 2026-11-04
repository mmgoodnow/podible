import { createRoot } from "react-dom/client";
import { useState } from "react";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import { rpc, TRPCProvider, useTRPC } from "./rpc";
import { Link, useLocation } from "./router";
import { MovieSync } from "./movie-sync";
import { TvSync } from "./tv-sync";
import { RetentionAudit } from "./retention";
import { SettingsPage, UsersPage, OpsPage } from "./admin";
import { LoginScreen } from "./auth";
import { Library, Catalog, BookCollection } from "./books";
import { BookDetail } from "./book-detail";
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

function App() {
  const route = useLocation().split("?")[0]!.replace(/\/+$/, "") || "/";
  const trpc = useTRPC();
  const session = useQuery(trpc.auth.me.queryOptions({}));
  if (route === "/login" || route === "/login/plex/loading" || route === "/login/plex/result")
    return <LoginScreen key={route} mode={route.endsWith("/loading") ? "loading" : route.endsWith("/result") ? "result" : "login"} />;
  return (
    <div className="shell">
      <header>
        <Link href="/" className="wordmark">
          podible<span>THE LIBRARY</span>
        </Link>
        <nav aria-label="Main navigation">
          <Link
            href="/library"
            aria-current={
              route === "/library" || route === "/" ? "page" : undefined
            }
          >
            Library
          </Link>
          <Link
            href="/add"
            aria-current={route === "/add" ? "page" : undefined}
          >
            Discover
          </Link>
          <Link
            href="/movies"
            aria-current={route.startsWith("/movies") ? "page" : undefined}
          >
            Movies
          </Link>
          <Link
            href="/requests"
            aria-current={route.startsWith("/requests") ? "page" : undefined}
          >
            Requests
          </Link>
          {session.data?.user.isAdmin && (
            <Link
              href="/movie-dual-write"
              aria-current={route === "/movie-dual-write" ? "page" : undefined}
            >
              Movie sync
            </Link>
          )}
          {session.data?.user.isAdmin && <Link href="/settings" aria-current={route === "/settings" ? "page" : undefined}>Settings</Link>}
          {session.data?.user.isAdmin && <Link href="/ops" aria-current={route === "/ops" ? "page" : undefined}>Ops</Link>}
          {session.data?.user.isAdmin && <Link href="/tv-sync" aria-current={route === "/tv-sync" ? "page" : undefined}>TV sync</Link>}
          {session.data?.user.isAdmin && <Link href="/retention" aria-current={route === "/retention" ? "page" : undefined}>Retention</Link>}
        </nav>
        {session.data && <form method="post" action="/logout"><button type="submit">Sign out</button></form>}
      </header>
      <main
        className={
          (["/", "/library", "/add", "/movie-dual-write", "/tv-sync", "/retention", "/settings", "/users", "/ops"].includes(route) || route.startsWith("/requests"))
            ? "compact-main"
            : undefined
        }
      >
        {session.isPending ? (
          <p role="status">Checking your session...</p>
        ) : session.isError ? (
          <section className="welcome">
            <div className="library-toolbar"><h1>{session.error.data?.code === "UNAUTHORIZED" ? "Sign in" : "Connection unavailable"}</h1></div>
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
          <ActorLibrary
            key={session.data.user.id}
            isAdmin={session.data.user.isAdmin}
          />
        )}
      </main>
      <footer>
        <MovieCredits />
      </footer>
    </div>
  );
}

function ActorLibrary({ isAdmin }: { isAdmin: boolean }) {
  const route =
    useLocation()
      .split("?")[0]!
      .replace(/\/$/, "") || "/";
  const [actorClient] = useState(
    () =>
      new QueryClient({
        defaultOptions: { queries: { retry: false, staleTime: 15_000 } },
      }),
  );
  return (
    <QueryClientProvider client={actorClient}>
      <TRPCProvider trpcClient={rpc} queryClient={actorClient}>
        {["/settings", "/users", "/ops"].includes(route) ? (
          isAdmin ? (route === "/settings" ? <SettingsPage /> : route === "/users" ? <UsersPage /> : <OpsPage />)
            : <p role="alert">Administrator access required.</p>
        ) : route === "/retention" ? (
          isAdmin ? <RetentionAudit /> : <p role="alert">Administrator access required.</p>
        ) : route === "/tv-sync" ? (
          isAdmin ? <TvSync /> : <p role="alert">Administrator access required.</p>
        ) : route === "/movie-dual-write" ? (
          isAdmin ? (
            <MovieSync />
          ) : (
            <p role="alert">Administrator access required.</p>
          )
        ) : /^\/movies\/\d+$/.test(route) ? (
          <MovieDetail key={route} tmdbId={Number(route.split("/").at(-1))} />
        ) : route === "/movies" ? (
          <MovieSearch />
        ) : /^\/requests\/\d+$/.test(route) ? (
          <MovieRequestDetail
            key={route}
            requestId={Number(route.split("/").at(-1))}
          />
        ) : route === "/requests" ? (
          <MovieRequests />
        ) : /^\/book\/\d+$/.test(route) ? (
          <BookDetail
            key={route}
            bookId={Number(route.split("/").at(-1))}
            isAdmin={isAdmin}
          />
        ) : route === "/add" ? (
          <Catalog />
        ) : route === "/series" || route === "/author" ? (
          <BookCollection
            key={location.pathname + location.search}
            kind={route === "/series" ? "series" : "author"}
          />
        ) : ["/", "/library"].includes(route) ? (
          <Library />
        ) : (
          <p>
            Page not found. <Link href="/library">Open library</Link>.
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

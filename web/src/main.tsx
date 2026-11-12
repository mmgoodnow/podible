import { createRoot } from "react-dom/client";
import { useEffect, useState } from "react";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import {
  QueryClient,
  QueryClientProvider,
  useQuery,
} from "@tanstack/react-query";
import { rpc, TRPCProvider, useTRPC } from "./rpc";
import { Link, useLocation } from "./router";
import { RetentionAudit } from "./retention";
import { SettingsPage, UsersPage, OpsPage } from "./admin";
import { LoginScreen } from "./auth";
import { Catalog, BookCollection } from "./books";
import { BookDetail } from "./book-detail";
import { UnifiedLibrary } from "./home";
import { TvDetail, TvLibraryDetail, TvRequestDetail } from "./tv";
import { RecoveryPage } from "./recovery";
import { TvRetentionAudit, type TvRetentionClient } from "./tv-retention";
import { TorrentRetirementStatus } from "./torrent-retirement";
import { StewardshipPage } from "./stewardship";
import type { StewardshipPanelApi } from "../../src/media/torrent-stewardship-types";
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
const stewardshipApi: StewardshipPanelApi = {
  status: input => rpc.stewardship.status.query(input),
  collect: input => rpc.stewardship.collect.mutate(input),
};
const tvRetentionApi: TvRetentionClient = {
  audit: input => rpc.tvRetention.audit.query(input),
  evict: input => rpc.tvRetention.evict.mutate(input),
};

function App() {
  const currentLocation = useLocation();
  const route = currentLocation.split("?")[0]!.replace(/\/+$/, "") || "/";
  const retiredDiagnostic = /^\/(?:movie-shadow(?:\/\d+)?|movie-dual-write|tv-sync)$/.test(route);
  useEffect(() => {
    if (retiredDiagnostic) location.replace("/ops");
  }, [retiredDiagnostic]);
  const libraryRoute = route === "/" || route === "/library";
  const trpc = useTRPC();
  const identity = useQuery(trpc.auth.me.queryOptions({}, { enabled: !libraryRoute && !retiredDiagnostic }));
  const bootstrap = useQuery(trpc.library.bootstrap.queryOptions({
    q: new URLSearchParams(currentLocation.split("?")[1] ?? "").get("q") ?? "", limit: 48,
  }, { enabled: libraryRoute }));
  const session = libraryRoute ? bootstrap : identity;
  if (retiredDiagnostic) return <p role="status">Opening activity...</p>;
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
          {session.data?.user.isAdmin && <Link href="/ops" aria-current={route === "/ops" ? "page" : undefined}>Activity</Link>}
          {session.data?.user.isAdmin && <Link href="/settings" aria-current={route === "/settings" ? "page" : undefined}>Settings</Link>}
        </nav>
        {session.data && <form method="post" action="/logout"><button type="submit">Sign out</button></form>}
      </header>
      <main
        className={
          (["/", "/library", "/add", "/retention", "/tv-retention", "/recovery", "/stewardship", "/settings", "/users", "/ops"].includes(route) || route.startsWith("/requests"))
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
            initialLibrary={libraryRoute ? bootstrap.data : undefined}
          />
        )}
      </main>
      <footer>
        <MovieCredits />
      </footer>
    </div>
  );
}

function ActorLibrary({ isAdmin, initialLibrary }: {
  isAdmin: boolean;
  initialLibrary?: inferRouterOutputs<AppRouter>["library"]["bootstrap"];
}) {
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
        {isAdmin && ["/retention", "/tv-retention", "/recovery", "/stewardship"].includes(route) &&
          <Link className="back" href="/ops">Back to activity</Link>}
        {["/settings", "/users", "/ops"].includes(route) ? (
          isAdmin ? (route === "/settings" ? <SettingsPage /> : route === "/users" ? <UsersPage /> : <OpsPage />)
            : <p role="alert">Administrator access required.</p>
        ) : route === "/stewardship" ? (
          isAdmin ? <><StewardshipPage api={stewardshipApi} /><TorrentRetirementStatus /></> : <p role="alert">Administrator access required.</p>
        ) : route === "/recovery" ? (
          isAdmin ? <RecoveryPage /> : <p role="alert">Administrator access required.</p>
        ) : route === "/retention" || route === "/tv-retention" ? (
          isAdmin ? <>
            <nav className="actions" aria-label="Retention media"><Link href="/retention">Movies</Link><Link href="/tv-retention">TV seasons</Link></nav>
            {route === "/tv-retention" ? <TvRetentionAudit client={tvRetentionApi} /> : <RetentionAudit />}
          </> : <p role="alert">Administrator access required.</p>
        ) : /^\/tv\/library\/\d+$/.test(route) ? (
          <TvLibraryDetail key={route} entryId={Number(route.split("/").at(-1))} />
        ) : /^\/tv\/requests\/\d+$/.test(route) ? (
          <TvRequestDetail key={route} requestId={Number(route.split("/").at(-1))} />
        ) : /^\/tv\/\d+$/.test(route) ? (
          <TvDetail key={route} tmdbId={Number(route.split("/").at(-1))} />
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
          <UnifiedLibrary initialPage={initialLibrary} />
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

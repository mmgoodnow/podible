import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Link, navigate } from "./router";
import type { MovieRequestView } from "../../src/movies/contracts";
import tmdbLogo from "./assets/tmdb-logo.svg" with { type: "file" };

function Failure({ message }: { message: string }) {
  return (
    <p role="alert" className="notice error">
      {message} <a href="/login?redirectTo=%2Fapp%2Fmovies">Sign in again</a> if
      your session expired.
    </p>
  );
}
function bytes(n: number | null) {
  return n === null
    ? "Size unknown"
    : n > 1024 ** 3
      ? `${(n / 1024 ** 3).toFixed(1)} GiB`
      : `${(n / 1024 ** 2).toFixed(1)} MiB`;
}
export function MovieSearch() {
  const trpc = useTRPC();
  const initial = new URLSearchParams(location.search).get("q") ?? "";
  const [draft, setDraft] = useState(initial);
  const [query, setQuery] = useState(initial);
  const status = useQuery(trpc.movies.status.queryOptions({}));
  const results = useQuery({
    ...trpc.movies.search.queryOptions({ query }),
    enabled: Boolean(
      query.trim() && status.data?.catalogConfigured,
    ),
  });
  return (
    <>
      <span className="eyebrow">MOVIE NIGHT, SORTED</span>
      <h1>Find your next feature.</h1>
      <form
        className="movie-search"
        onSubmit={(event) => {
          event.preventDefault();
          const q = draft.trim();
          if (q === query) void results.refetch();
          else setQuery(q);
          history.replaceState(
            null,
            "",
            `/movies${q ? `?q=${encodeURIComponent(q)}` : ""}`,
          );
        }}
      >
        <label className="search">
          Search movies
          <input
            type="search"
            value={draft}
            placeholder="Movie title"
            onChange={(event) => setDraft(event.target.value)}
            required
            maxLength={200}
          />
        </label>
        <button
          disabled={!draft.trim() || results.isFetching}
          aria-busy={results.isFetching}
        >
          {results.isFetching ? "Searching..." : "Search"}
        </button>
      </form>
      {status.isError ? (
        <Failure message={status.error.message} />
      ) : status.isPending ? (
        <p role="status">Checking movie catalog...</p>
      ) : !status.data.catalogConfigured ? (
        <p className="notice">
          The movie catalog needs a TMDB credential. Ask an administrator to
          configure movies.
        </p>
      ) : results.isError ? (
        <Failure message={results.error.message} />
      ) : results.isFetching ? (
        <p role="status">Searching the catalog...</p>
      ) : results.data ? (
        <>
          <p className="muted">
            {results.data.results.length} results for &ldquo;{query}&rdquo;
          </p>
          <div className="book-grid movie-grid">
            {results.data.results.map((movie) => (
              <Link
                className="book-card"
                key={movie.tmdbId}
                href={`/movies/${movie.tmdbId}`}
              >
                <div className="jacket">
                  {movie.posterUrl ? (
                    <img src={movie.posterUrl} alt="" loading="lazy" />
                  ) : (
                    <span>{movie.title}</span>
                  )}
                </div>
                <h2>{movie.title}</h2>
                <p>{movie.year ?? "Year unknown"}</p>
              </Link>
            ))}
          </div>
          {!results.data.results.length && (
            <p>No movies found. Try a shorter title or a different spelling.</p>
          )}
        </>
      ) : (
        <p className="muted">
          Search by title, then request the movie. We will keep you updated from
          search to Plex.
        </p>
      )}
    </>
  );
}
export function MovieDetail({ tmdbId }: { tmdbId: number }) {
  const trpc = useTRPC();
  const queryClient = useQueryClient();
  const details = useQuery(trpc.movies.get.queryOptions({ tmdbId }));
  const request = useMutation(
    trpc.movies.request.mutationOptions({
      onSuccess: ({ request }) => {
        void queryClient.invalidateQueries({
          queryKey: trpc.requests.list.queryKey(),
        });
        navigate(`/requests/${request.id}`);
      },
    }),
  );
  if (details.isError) return <Failure message={details.error.message} />;
  if (!details.data) return <p role="status">Loading movie...</p>;
  const { movie, requests } = details.data;
  const existing = requests.find((entry) => entry.state !== "withdrawn");
  return (
    <>
      <Link href="/movies" className="back">
        Back to movies
      </Link>
      <section className="movie-hero">
        <div className="jacket">
          {movie.posterUrl ? (
            <img src={movie.posterUrl} alt="" />
          ) : (
            <span>{movie.title}</span>
          )}
        </div>
        <div>
          <span className="eyebrow">
            {movie.year ?? "YEAR UNKNOWN"} /{" "}
            {movie.originalLanguage.toUpperCase()}
          </span>
          <h1>{movie.title}</h1>
          <p className="synopsis">
            {movie.overview || "No description available."}
          </p>
          {existing ? (
            <Link className="button" href={`/requests/${existing.id}`}>
              {existing.available ? "Available in Plex" : "Track request"}
            </Link>
          ) : (
            <button
              disabled={request.isPending}
              onClick={() => request.mutate({ tmdbId })}
            >
              {request.isPending ? "Requesting..." : "Request movie"}
            </button>
          )}
          {request.isError && <Failure message={request.error.message} />}
        </div>
      </section>
    </>
  );
}
export function MovieRequests() {
  const trpc = useTRPC();
  const requests = useQuery({
    ...trpc.requests.list.queryOptions({}),
    refetchInterval: (query) =>
      query.state.data?.requests.some(
        (r) => ["queued", "running"].includes(r.state) || r.upgrading,
      )
        ? 5000
        : false,
  });
  return (
    <>
      <span className="eyebrow">FROM WANTED TO WATCHING</span>
      <h1>Your requests.</h1>
      {requests.isError ? (
        <Failure message={requests.error.message} />
      ) : !requests.data ? (
        <p role="status">Loading requests...</p>
      ) : requests.data.requests.length ? (
        <div className="request-list">
          {requests.data.requests.map((request) => (
            <Link
              href={`/requests/${request.id}`}
              key={request.id}
              className="request-row"
            >
              <div>
                <h2>{request.movie.title}</h2>
                <p>
                  {request.movie.year} /{" "}
                  {request.available
                    ? request.upgrading
                      ? "Available; looking for an upgrade"
                      : "Available in Plex"
                    : request.state}
                </p>
              </div>
              <span className={`state ${request.available ? "ready" : ""}`}>
                {request.available
                  ? "Ready to watch"
                  : (request.stage ?? "Withdrawn")}
              </span>
            </Link>
          ))}
        </div>
      ) : (
        <p>
          No movie requests yet. <Link href="/movies">Find a movie</Link>.
        </p>
      )}
    </>
  );
}
function ReleaseSearch({
  requestId,
  onSelected,
}: {
  requestId: number;
  onSelected: () => void;
}) {
  const trpc = useTRPC();
  const [draft, setDraft] = useState("");
  const [submitted, setSubmitted] = useState<string>();
  const results = useQuery({
    ...trpc.requests.searchReleases.queryOptions({
      requestId,
      query: submitted,
    }),
    enabled: submitted !== undefined,
    staleTime: 0,
  });
  const selection = useMutation(
    trpc.requests.selectRelease.mutationOptions({ onSuccess: onSelected }),
  );
  return (
    <details className="panel manual-search">
      <summary>Choose a release manually</summary>
      <p className="muted">
        Search results are private references valid for 30 minutes. The selected
        release still goes through identity, quality, and file verification.
      </p>
      <form
        className="movie-search"
        onSubmit={(event) => {
          event.preventDefault();
          if (submitted === draft.trim()) void results.refetch();
          else setSubmitted(draft.trim());
        }}
      >
        <label className="search">
          Release query (optional)
          <input
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            placeholder="Use the movie title and year"
            maxLength={200}
          />
        </label>
        <button disabled={results.isFetching} aria-busy={results.isFetching}>
          {results.isFetching ? "Searching releases..." : "Search releases"}
        </button>
      </form>
      {results.isFetching && <p role="status">Searching indexers...</p>}
      {results.isError && <Failure message={results.error.message} />}
      {results.data?.warnings.map((warning) => (
        <p className="notice" key={warning}>
          {warning}
        </p>
      ))}
      {results.data?.results.map((result) => (
        <div key={result.index} className="release-row">
          <div>
            <h3>{result.title}</h3>
            <p>
              {result.provider} / {bytes(result.sizeBytes)} /{" "}
              {result.seeders ?? "Unknown"} seeders
            </p>
          </div>
          <button
            disabled={selection.isPending}
            onClick={() =>
              selection.mutate({
                requestId,
                searchId: results.data!.searchId,
                resultIndex: result.index,
              })
            }
          >
            Choose release
          </button>
        </div>
      ))}
      {results.data && !results.data.results.length && (
        <p>No releases found. Try another query.</p>
      )}
      {selection.isError && <Failure message={selection.error.message} />}
    </details>
  );
}
const stages = [
  { label: "Requested", values: ["requested"] },
  { label: "Search", values: ["searching", "selecting"] },
  { label: "Download", values: ["downloading", "replacing"] },
  { label: "Verify", values: ["verifying"] },
  { label: "Plex", values: ["publishing", "available"] },
];
function Progress({ request }: { request: MovieRequestView }) {
  const download = request.download;
  const percent =
    download?.bytesDone !== null &&
    download?.bytesDone !== undefined &&
    download.sizeBytes
      ? Math.min(
          100,
          Math.max(0, (100 * download.bytesDone) / download.sizeBytes),
        )
      : null;
  return (
    <section className="panel progress-panel" aria-label="Request progress">
      <div className="progress-heading">
        <h2>
          {request.state === "withdrawn"
            ? "Request withdrawn"
            : request.available
              ? "Ready to watch in Plex"
              : request.state === "blocked"
                ? "Needs attention"
                : "Working on your movie"}
        </h2>
        <span
          className={`state ${request.available && request.state !== "withdrawn" ? "ready" : ""}`}
        >
          {request.upgrading ? "Upgrade in progress" : request.state}
        </span>
      </div>
      <ol className="stage-tracker">
        {stages.map((stage) => (
          <li
            key={stage.label}
            className={
              request.events.some((event) => stage.values.includes(event.stage))
                ? "reached"
                : ""
            }
          >
            <span />
            {stage.label}
          </li>
        ))}
      </ol>
      {request.upgrading && (
        <p>
          Your current version stays available while we look for something
          better.
        </p>
      )}
      {request.available && request.job?.status === "failed" && (
        <p className="notice">
          The upgrade failed; your current version is still available. Retry or
          choose another release.
        </p>
      )}
      {!request.available && request.state !== "withdrawn" && (
        <p>
          {request.job?.error ??
            request.events.at(-1)?.message ??
            "Your request is queued."}
        </p>
      )}
      {request.stage === "downloading" && download && (
        <div>
          <progress
            max={100}
            value={percent ?? undefined}
            aria-label="Download progress"
          />
          <p className="muted">
            {percent === null
              ? "Progress unavailable"
              : `${percent.toFixed(0)}%`}{" "}
            / {bytes(download.sizeBytes)} / {bytes(download.bytesPerSecond)}/s
          </p>
        </div>
      )}
    </section>
  );
}
export function MovieRequestDetail({ requestId }: { requestId: number }) {
  const trpc = useTRPC();
  const queryClient = useQueryClient();
  const query = useQuery({
    ...trpc.requests.get.queryOptions({ requestId }),
    refetchInterval: (q) =>
      q.state.data &&
      (["queued", "running"].includes(q.state.data.request.state) ||
        q.state.data.request.upgrading)
        ? 2000
        : false,
  });
  const refresh = () => {
    void queryClient.invalidateQueries({
      queryKey: trpc.requests.get.queryKey({ requestId }),
    });
    void queryClient.invalidateQueries({
      queryKey: trpc.requests.list.queryKey(),
    });
  };
  const retry = useMutation(
    trpc.requests.retry.mutationOptions({ onSuccess: refresh }),
  );
  const upgrade = useMutation(
    trpc.requests.checkUpgrade.mutationOptions({ onSuccess: refresh }),
  );
  const withdraw = useMutation(
    trpc.requests.withdraw.mutationOptions({ onSuccess: refresh }),
  );
  if (query.isError) return <Failure message={query.error.message} />;
  if (!query.data) return <p role="status">Loading request...</p>;
  const request = query.data.request;
  const working =
    ["queued", "running"].includes(request.state) || request.upgrading;
  return (
    <>
      <Link href="/requests" className="back">
        All requests
      </Link>
      <span className="eyebrow">
        REQUEST {request.id} / {request.movie.year}
      </span>
      <h1>{request.movie.title}</h1>
      <Progress request={request} />
      <div className="actions">
        {request.state === "blocked" && (
          <button
            disabled={retry.isPending}
            onClick={() => retry.mutate({ requestId })}
          >
            Retry acquisition
          </button>
        )}
        {request.available && (
          <button
            disabled={working || upgrade.isPending}
            onClick={() => upgrade.mutate({ requestId })}
          >
            Check for upgrade
          </button>
        )}
        {request.state !== "withdrawn" && (
          <button
            className="secondary"
            disabled={withdraw.isPending}
            onClick={() => withdraw.mutate({ requestId })}
          >
            Withdraw my request
          </button>
        )}
        <Link href={`/movies/${request.movie.tmdbId}`}>Movie details</Link>
      </div>
      {[retry.error, upgrade.error, withdraw.error]
        .filter(Boolean)
        .map((error, i) => (
          <Failure key={i} message={error!.message} />
        ))}
      {request.state !== "withdrawn" && !working && (
        <ReleaseSearch requestId={requestId} onSelected={refresh} />
      )}
      <div className="request-columns">
        <section className="panel">
          <h2>Activity</h2>
          <ol className="timeline">
            {request.events.map((event) => (
              <li key={event.id}>
                <span className="state">{event.stage}</span>
                <p>{event.message}</p>
                <time dateTime={event.createdAt}>
                  {new Date(event.createdAt).toLocaleString()}
                </time>
              </li>
            ))}
          </ol>
        </section>
        <aside>
          <section className="panel">
            <h2>Selection &amp; quality</h2>
            <p>{request.decisionNote ?? "No release selected yet."}</p>
            {request.quality && (
              <p className="muted">
                Policy eligibility: {request.quality.eligibility}. Quality tier:{" "}
                {(request.quality.tier ?? 0) + 1}.{" "}
                {request.quality.reasons.join("; ")}
              </p>
            )}
            <p className="muted">
              Withdrawing does not delete media or interrupt an acquisition
              needed by another user.
            </p>
          </section>
        </aside>
      </div>
    </>
  );
}
export function MovieCredits() {
  return (
    <details className="credits">
      <summary>Movie data &amp; credits</summary>
      <a href="https://www.themoviedb.org/" target="_blank" rel="noreferrer">
        <img
          width="130"
          alt="TMDB"
          src={tmdbLogo}
        />
      </a>
      <p>
        This product uses the TMDB API but is not endorsed or certified by TMDB.
      </p>
    </details>
  );
}

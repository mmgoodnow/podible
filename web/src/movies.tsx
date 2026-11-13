import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useAcquisitionEvents } from "./acquisition-events";
import { NotifyMe } from "./notifications";
import { useTRPC } from "./rpc";
import { Link, navigate } from "./router";
import type { MovieRequestView } from "../../src/movies/contracts";
import { StageTracker } from "./stage-tracker";
import { ContentRail } from "./content-rail";
import { ScreenCard } from "./screen-card";
import { ScreenIssuePanel } from "./screen-issues";
import type { MoviePageExtras } from "../../src/movies/types";
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
      <div className="library-toolbar"><h1>Movies</h1></div>
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
  useAcquisitionEvents(true, () => {
    void queryClient.invalidateQueries({ queryKey: trpc.movies.get.queryKey({ tmdbId }) });
    void queryClient.invalidateQueries({ queryKey: trpc.notifications.watchStatus.queryKey({ kind: "movie", tmdbId }) });
    void queryClient.invalidateQueries({ queryKey: trpc.requests.get.pathKey() });
  });
  const extra = useQuery(trpc.movies.pageExtras.queryOptions({ tmdbId }, {
    enabled: details.isSuccess, staleTime: 10 * 60_000, retry: false,
  })).data;
  const request = useMutation(
    trpc.movies.request.mutationOptions({
      onSuccess: ({ request }) => {
        queryClient.setQueryData(trpc.movies.get.queryKey({ tmdbId }), current => current && ({
          ...current, requests: [request, ...current.requests.filter(r => r.id !== request.id)],
        }));
        queryClient.setQueryData(trpc.requests.get.queryKey({ requestId: request.id }), { request });
        void queryClient.invalidateQueries({
          queryKey: trpc.requests.list.queryKey(),
        });
      },
    }),
  );
  if (details.isError) return <Failure message={details.error.message} />;
  if (!details.data) return <p role="status">Loading movie...</p>;
  const { movie, requests, availableInLibrary } = details.data;
  const existing = requests.find((entry) => entry.state !== "withdrawn");
  const tracked = requests.some(entry => entry.state !== "withdrawn")
    ? requests.filter(entry => entry.state !== "withdrawn") : requests.slice(0, 1);
  return (
    <>
      <Link href="/library" className="back">
        Back to library
      </Link>
      <section className="movie-hero">
        <div className="jacket">
          {movie.posterUrl ? (
            <img src={movie.posterUrl} alt={`${movie.title} movie poster`} width={300} height={450} />
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
          {availableInLibrary ? <p className="state ready">Available in Plex</p> : !existing && (
            <button
              disabled={request.isPending}
              onClick={() => request.mutate({ tmdbId })}
            >
              {request.isPending ? "Requesting..." : "Request movie"}
            </button>
          )}
          {request.isError && <Failure message={request.error.message} />}
          <NotifyMe kind="movie" tmdbId={tmdbId} />
          <MovieFacts runtimeMinutes={extra?.runtimeMinutes ?? movie.runtimeMinutes ?? null}
            genres={extra?.genres ?? []} directors={extra?.directors ?? []} />
        </div>
      </section>
      <MovieVersionRequest tmdbId={tmdbId} />
      {tracked.map(entry => <section key={entry.id} aria-label={`${entry.versionLabel} version`}>
        {tracked.length > 1 && <h2>{entry.versionLabel}</h2>}
        <MovieRequestPanel initialRequest={entry} />
      </section>)}
      {(availableInLibrary || existing) && <ScreenIssuePanel key={tmdbId} title={{ media: "movie", tmdbId }} />}
      {extra && (extra.cast.length > 0 || extra.related.length > 0) && <div className="movie-extras">
        {extra.cast.length > 0 && <ContentRail title="Cast" variant="cast">
          {extra.cast.map(actor => <CastPortrait key={actor.id} actor={actor} />)}
        </ContentRail>}
        {extra.related.length > 0 && <ContentRail title="Related movies">
          {extra.related.map(related => <ScreenCard key={related.tmdbId} title={related.title}
            year={related.year} posterUrl={related.posterUrl} href={`/movies/${related.tmdbId}`} media="movie" />)}
        </ContentRail>}
      </div>}
    </>
  );
}
function MovieVersionRequest({ tmdbId }: { tmdbId: number }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const [open, setOpen] = useState(false);
  const [resolution, setResolution] = useState("");
  const [cut, setCut] = useState("");
  const [audioLanguage, setAudioLanguage] = useState("");
  const [sdrOnly, setSdrOnly] = useState(false);
  const request = useMutation(trpc.movies.request.mutationOptions({ onSuccess: async () => {
    await cache.invalidateQueries({ queryKey: trpc.movies.get.queryKey({ tmdbId }) });
    await cache.invalidateQueries({ queryKey: trpc.requests.list.queryKey() });
  } }));
  return <section className="media-section" aria-label="Specific movie version">
    <button className="secondary" aria-expanded={open} aria-controls="movie-version-form" onClick={() => setOpen(!open)}>Request a specific version</button>
    {open && <form id="movie-version-form" className="panel" onSubmit={event => {
      event.preventDefault();
      request.mutate({ tmdbId, version: { ...(resolution ? { resolution: Number(resolution) as 1080 | 2160 } : {}),
        ...(cut.trim() ? { cut: cut.trim() } : {}), ...(audioLanguage.trim() ? { audioLanguage: audioLanguage.trim() } : {}),
        ...(sdrOnly ? { sdrOnly: true as const } : {}) } });
    }}>
      <h2>Keep a separate version</h2>
      <p>This version is acquired and upgraded independently. Other requested versions are not replaced.</p>
      <div className="actions">
        <label>Resolution <select aria-label="Resolution" value={resolution} onChange={event => setResolution(event.target.value)}>
          <option value="">Library default</option><option value="1080">1080p only</option><option value="2160">4K only</option>
        </select></label>
        <label>Cut <input value={cut} onChange={event => setCut(event.target.value)} placeholder="Any cut" maxLength={100} /></label>
        <label>Audio language <input value={audioLanguage} onChange={event => setAudioLanguage(event.target.value)} placeholder="Default (e.g. en or ja)" maxLength={35} /></label>
        <label><input type="checkbox" checked={sdrOnly} onChange={event => setSdrOnly(event.target.checked)} /> SDR only</label>
      </div>
      <button disabled={request.isPending || !(resolution || cut.trim() || audioLanguage.trim() || sdrOnly)}>
        {request.isPending ? "Requesting version..." : "Request version"}
      </button>
      {request.isError && <p role="alert">{request.error.message}</p>}
    </form>}
  </section>;
}
function MovieFacts({ runtimeMinutes, genres, directors }: Pick<MoviePageExtras, "runtimeMinutes" | "genres" | "directors">) {
  const facts = [...(runtimeMinutes ? [`${runtimeMinutes} min`] : []), ...genres];
  if (!facts.length && !directors.length) return null;
  return <div className="movie-facts">
    {facts.length > 0 && <p>{facts.join(" · ")}</p>}
    {directors.length > 0 && <p>Directed by <strong>{directors.join(", ")}</strong></p>}
  </div>;
}

function CastPortrait({ actor }: { actor: MoviePageExtras["cast"][number] }) {
  const [broken, setBroken] = useState<string | null>(null);
  const initials = actor.name.split(/\s+/u).slice(0, 2).map(word => Array.from(word)[0]).join("");
  return <article className="cast-portrait">
    <div className="cast-portrait__image">{actor.profileUrl && actor.profileUrl !== broken ?
      <img src={actor.profileUrl} alt="" loading="lazy" decoding="async" width="185" height="278"
        onError={() => setBroken(actor.profileUrl)} /> : <span aria-hidden="true">{initials}</span>}</div>
    <h3>{actor.name}</h3>
    {actor.character && <p>{actor.character}</p>}
  </article>;
}
export function MovieRequests() {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const requests = useQuery(trpc.requests.list.queryOptions({}));
  useAcquisitionEvents(true, () => {
    void cache.invalidateQueries({ queryKey: trpc.requests.list.pathKey() });
  });
  return (
    <>
      <div className="library-toolbar"><h1>Requests</h1></div>
      {requests.isError ? (
        <Failure message={requests.error.message} />
      ) : !requests.data ? (
        <p role="status">Loading requests...</p>
      ) : requests.data.requests.length ? (
        <div className="request-list">
          {requests.data.requests.map((request) => (
            <Link
              href={`/movies/${request.movie.tmdbId}`}
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
                    : request.state === "waiting-for-release" ? "No matching release found yet" : request.state}
                </p>
              </div>
              <span className={`state ${request.available ? "ready" : ""}`}>
                {request.available
                  ? "Ready to watch"
                  : request.state === "waiting-for-release" ? "Watching for releases" : (request.stage ?? "Withdrawn")}
              </span>
            </Link>
          ))}
        </div>
      ) : (
        <p>
          No movie requests yet. <Link href="/library?media=movies">Find a movie</Link>.
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
const movieStages = [
  ["requested"],
  ["searching", "selecting"],
  ["downloading", "replacing"],
  ["verifying"],
  ["publishing", "available"],
];
function Progress({ request }: { request: MovieRequestView }) {
  return (
    <section className="panel progress-panel" aria-label="Request progress">
      <div className="progress-heading">
        <h2>
          {request.state === "withdrawn"
            ? "Request withdrawn"
            : request.available
              ? "Ready to watch in Plex"
              : request.state === "waiting-for-release"
                ? "Watching for a release"
                : request.state === "blocked"
                ? "Needs attention"
                : "Working on your movie"}
        </h2>
        <span
          className={`state ${request.available && request.state !== "withdrawn" ? "ready" : ""}`}
        >
          {request.upgrading ? "Upgrade in progress" : request.state === "waiting-for-release" ? "Watching for releases" : request.state}
        </span>
      </div>
      <StageTracker reached={movieStages.map(stages => request.events.some(event => stages.includes(event.stage)))}
        transfer={request.stage === "downloading" ? { media: "movie", id: request.id } : undefined} />
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
          {request.state === "waiting-for-release"
            ? "No matching release has been found yet. Podible is watching incoming indexer releases and will try automatically when one appears. You don't need to do anything."
            : request.job?.error ??
            request.events.at(-1)?.message ??
            "Your request is queued."}
        </p>
      )}
    </section>
  );
}
export function MovieRequestDetail({ requestId }: { requestId: number }) {
  const trpc = useTRPC();
  const query = useQuery(trpc.requests.get.queryOptions({ requestId }));
  const tmdbId = query.data?.request.movie.tmdbId;
  useEffect(() => {
    if (tmdbId) navigate(`/movies/${tmdbId}`, { replace: true });
  }, [tmdbId]);
  if (query.isError) return <Failure message={query.error.message} />;
  return <p role="status">Opening movie...</p>;
}

function MovieRequestPanel({ initialRequest }: { initialRequest: MovieRequestView }) {
  const requestId = initialRequest.id;
  const trpc = useTRPC();
  const queryClient = useQueryClient();
  const query = useQuery({
    ...trpc.requests.get.queryOptions({ requestId }),
    initialData: { request: initialRequest },
  });
  const refresh = () => {
    void queryClient.invalidateQueries({
      queryKey: trpc.movies.get.queryKey({ tmdbId: initialRequest.movie.tmdbId }),
    });
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
        {request.available && request.state !== "withdrawn" && (
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

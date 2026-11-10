import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Failure } from "./books";
import { Link, navigate } from "./router";
import { metricBytes } from "./transfer-metrics-model";
import { StageTracker } from "./stage-tracker";
import { useAcquisitionEvents } from "./acquisition-events";
import "./tv.css";

function ShowRedirect({ entryId }: { entryId: number }) {
  useEffect(() => navigate(`/tv/library/${entryId}`, { replace: true }), [entryId]);
  return <p role="status">Opening show...</p>;
}

export function TvRequestDetail({ requestId }: { requestId: number }) {
  const trpc = useTRPC();
  const detail = useQuery(trpc.tv.myRequest.queryOptions({ requestId }));
  if (detail.isError) return <Failure error={detail.error} retry={() => void detail.refetch()} />;
  if (!detail.data) return <p role="status">Opening show...</p>;
  const request = detail.data.request;
  return <ShowRedirect entryId={request.entryId} />;
}

export function TvDetail({ tmdbId }: { tmdbId: number }) {
  const trpc = useTRPC();
  const detail = useQuery(trpc.tv.details.queryOptions({ tmdbId }, { staleTime: 30_000 }));
  if (detail.isError) return <Failure error={detail.error} retry={() => void detail.refetch()} />;
  if (!detail.data) return <p role="status">Loading TV series...</p>;
  if (detail.data.showId) return <ShowRedirect entryId={detail.data.showId} />;
  const { series, unavailableReason } = detail.data;
  return <>
    <Link href="/library" className="back">Back to library</Link>
    {!series ? <p role="alert" className="notice">{unavailableReason ?? "TV metadata is unavailable."}</p> : <>
      <section className="movie-hero">
        <div className="jacket">{series.posterUrl ? <img src={series.posterUrl} alt="" /> : <span>{series.title}</span>}</div>
        <div><h1>{series.title}</h1><p>{series.year}</p><p className="synopsis">{series.overview}</p></div>
      </section>
      <TvSeasonPicker tmdbId={tmdbId} />
    </>}
  </>;
}

function TvSeasonPicker({ tmdbId, showId }: { tmdbId: number; showId?: number }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const result = useQuery(trpc.tv.details.queryOptions({ tmdbId }, { staleTime: 30_000 }));
  const [selected, setSelected] = useState<number[]>([]);
  const request = useMutation(trpc.tv.request.mutationOptions({ onSuccess: ({ request }) => {
    setSelected([]);
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequests.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.requestCards.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.details.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.catalog.tvEntry.pathKey() });
    if (request.entryId !== showId) navigate(`/tv/library/${request.entryId}`, { replace: true });
  } }));
  const series = result.data?.series;
  if (!series) return null;
  if (showId && result.data?.showId !== showId) return null;
  const states = new Map((result.data?.seasons ?? []).map(s => [s.number, s]));
  const options = series.seasons.map(s => {
    const state = states.get(s.seasonNumber);
    return { ...s, ready: state?.available === s.episodeCount && s.episodeCount > 0,
      requested: state?.requested ?? false, available: state?.available ?? 0 };
  });
  const selectable = options.filter(s => s.requestable && !s.ready && !s.requested);
  const selection = selected.filter(n => selectable.some(s => s.seasonNumber === n));
  const visible = showId ? selectable : options;
  if (!visible.length) return null;
  return <section className="media-section">
    <h2>{showId ? "Request more episodes" : "Choose seasons"}</h2>
    <fieldset className="season-options" disabled={request.isPending}>
      <legend>Seasons</legend>
      {visible.map(s => <label key={s.seasonNumber}>
        <input type="checkbox" checked={selection.includes(s.seasonNumber)} disabled={!s.requestable || s.ready || s.requested}
          onChange={e => setSelected(previous => e.target.checked ? [...previous, s.seasonNumber] : previous.filter(n => n !== s.seasonNumber))} />
        <span><strong>{s.title}</strong> · {s.ready ? "Ready in Plex" : s.requested ? "Already requested"
          : s.available ? `${s.episodeCount - s.available} missing` : `${s.episodeCount} ${s.episodeCount === 1 ? "episode" : "episodes"}`}
          {!s.requestable && <small>{s.unavailableReason ?? "Season information is unavailable."}</small>}</span>
      </label>)}
    </fieldset>
    {selectable.length > 0 && <button disabled={!selection.length || request.isPending}
      onClick={() => request.mutate({ tmdbId, seasonNumbers: selection })}>
      {request.isPending ? "Requesting..." : `Request ${selection.length === 1 ? "season" : "seasons"}`}
    </button>}
    {request.isError && <Failure error={request.error} />}
  </section>;
}

export function TvLibraryDetail({ entryId }: { entryId: number }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const result = useQuery(trpc.catalog.tvEntry.queryOptions({ entryId }));
  useAcquisitionEvents(true, () => {
    void cache.invalidateQueries({ queryKey: trpc.catalog.tvEntry.queryKey({ entryId }) });
  });
  const [expanded, setExpanded] = useState<number[]>([]);
  const changed = () => {
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequest.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequests.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.requestCards.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.details.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.catalog.tvEntry.pathKey() });
  };
  const retry = useMutation(trpc.tv.retryMyRequest.mutationOptions({ onSuccess: changed }));
  const withdraw = useMutation(trpc.tv.withdraw.mutationOptions({ onSuccess: changed }));
  if (result.isError) return <Failure error={result.error} retry={() => void result.refetch()} />;
  if (!result.data) return <p role="status">Loading show...</p>;
  const r = result.data;
  const total = r.progress.seasons.reduce((sum, s) => sum + s.total, 0);
  const future = r.progress.seasons.reduce((sum, s) => sum + s.future, 0);
  const active = r.progress.activeBatches > 0;
  const title = active ? "Adding episodes" : r.availableEpisodes > 0 ? "Available in Plex"
    : future === total && total > 0 ? "Waiting for upcoming episodes" : "Not in Plex yet";
  const seasons = r.progress.seasons;
  const requestedSeasons = new Set(r.requests.flatMap(request => request.seasons));
  const tracked = seasons.filter(s => !requestedSeasons.size || requestedSeasons.has(s.seasonNumber));
  const atStage = (...stages: typeof seasons[number]["stage"][]) => tracked.filter(s => stages.includes(s.stage)).length;
  const searches = atStage("searching"), downloads = atStage("downloading", "pack-pending");
  const verifying = atStage("verifying"), publishing = atStage("publishing");
  const ready = tracked.reduce((sum, s) => sum + s.available, 0);
  const allReady = tracked.length > 0 && tracked.every(s => s.stage === "ready");
  const waiting = atStage("waiting", "upcoming"), stopped = atStage("failed");
  const seasonCount = (n: number) => n ? `${n} season${n === 1 ? "" : "s"}` : undefined;
  const labels = { ready: "Ready in Plex", queued: "Queued", searching: "Finding a release", downloading: "Downloading", verifying: "Checking episodes", publishing: "Adding to Plex", "pack-pending": "Waiting for pack", waiting: "Episodes missing", failed: "Acquisition stopped", upcoming: "Not aired yet" };
  const episodeLabels = { ...labels, missing: "Not in Plex", upcoming: "Upcoming", verifying: "Checking release" };
  const stamp = (value: string) => new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  return <>
    <Link href="/library" className="back">Back to library</Link>
    <section className="tv-request-hero">
      {r.entry.posterUrl && <img className="tv-request-poster" src={r.entry.posterUrl} alt="" />}
      <div className="tv-request-summary">
        <p className="tv-request-meta">TV series{r.entry.year ? ` · ${r.entry.year}` : ""}</p>
        <h1>{r.entry.title}</h1><h2>{title}</h2>
        <p className="tv-request-count"><strong>{r.availableEpisodes.toLocaleString()}</strong> episodes ready</p>
        {total > 0 && <><progress value={r.availableEpisodes} max={Math.max(total, r.availableEpisodes)} aria-label="Available TV episodes" />
          <p>{r.availableEpisodes} of {total} episodes verified in Plex.</p></>}
        {r.progress.lastPublishedAt && <p className="tv-request-meta">Last added {stamp(r.progress.lastPublishedAt)}</p>}
      </div>
    </section>
    {(active || r.requests.length > 0) && <section className="panel progress-panel" aria-label="TV acquisition progress">
      <div className="progress-heading"><h2>{active
        ? `${r.progress.activeBatches} episode ${r.progress.activeBatches === 1 ? "batch" : "batches"} in progress`
        : "Request progress"}</h2></div>
      <StageTracker reached={[true, Boolean(searches || downloads || verifying || publishing || allReady),
        Boolean(downloads || verifying || publishing || allReady), Boolean(verifying || publishing || allReady),
        Boolean(publishing || allReady)]}
        details={[seasonCount(requestedSeasons.size), seasonCount(searches), seasonCount(downloads),
          seasonCount(verifying), ready ? `${ready} ready` : publishing ? "Adding episodes" : undefined]} />
      {waiting > 0 && <p>{seasonCount(waiting)} waiting for new releases or upcoming episodes.</p>}
      {stopped > 0 && <p>{seasonCount(stopped)} stopped. Check the season list below.</p>}
      {r.progress.download && <TvDownloadProgress download={r.progress.download} label="Active downloads" />}
    </section>}
    <section className="tv-request-seasons" aria-label="Season progress">
      <h2>Episodes by season</h2>
      {seasons.length ? <div className="tv-season-list">{seasons.map(s => {
        const name = s.seasonNumber === 0 ? "Specials" : `Season ${s.seasonNumber}`;
        const open = expanded.includes(s.seasonNumber);
        const panelId = `tv-show-${entryId}-season-${s.seasonNumber}`;
        return <article className={`tv-season-card tv-season-${s.stage}`} key={s.seasonNumber}>
          <h3><button className="tv-season-toggle" aria-label={name} aria-describedby={`${panelId}-summary`} aria-expanded={open} aria-controls={panelId}
            onClick={() => setExpanded(current => current.includes(s.seasonNumber) ? current.filter(n => n !== s.seasonNumber) : [...current, s.seasonNumber])}>
            <span className="tv-season-name">{name}</span>
            <span className="tv-season-facts" id={`${panelId}-summary`}>
              <span className="tv-season-count"><strong>{s.available}</strong> / {s.total} ready{s.future > 0 ? ` · ${s.future} upcoming` : ""}</span>
              <span className="tv-season-stage">{labels[s.stage]}</span>
            </span>
            <span className="tv-season-chevron" aria-hidden="true" />
          </button></h3>
          {s.download && <div className="tv-season-transfer">
            <TvDownloadProgress download={s.download} label={`${name} downloads`} />
          </div>}
          <div id={panelId} hidden={!open}>
            {open && <ol className="tv-episode-list" aria-label={`${name} episodes`}>
              {s.episodes.map(ep => {
                const date = ep.airDate?.slice(0, 10);
                return <li key={ep.id}>
                  <span className="tv-episode-number">{ep.number}</span>
                  <span className="tv-episode-title">{ep.title}</span>
                  {date && <time dateTime={date}>{new Date(`${date}T00:00:00Z`).toLocaleDateString(undefined, { dateStyle: "medium", timeZone: "UTC" })}</time>}
                  <span className={`tv-episode-status tv-episode-${ep.status}`}>{episodeLabels[ep.status]}</span>
                </li>;
              })}
            </ol>}
          </div>
        </article>;
      })}</div> : <p>No episode information available.</p>}
    </section>
    {r.entry.tmdbId && <TvSeasonPicker tmdbId={r.entry.tmdbId} showId={entryId} />}
    {r.requests.length > 0 && <section className="media-section" aria-label="Manage requests"><h2>Manage requests</h2>
      {r.requests.map(request => <div key={request.id} className="tv-request-actions">
        <strong>{request.seasons.map(s => s === 0 ? "Specials" : `Season ${s}`).join(", ")}</strong>
        {request.searchable && <button disabled={retry.isPending} onClick={() => retry.mutate({ requestId: request.id })}>
          {retry.isPending ? "Queuing search..." : "Search missing episodes"}
        </button>}
        <button className="secondary" disabled={withdraw.isPending} onClick={() => {
          if (window.confirm("Withdraw this request? Existing episodes will stay in Plex.")) withdraw.mutate({ requestId: request.id });
        }}>Withdraw request</button>
      </div>)}
      {retry.isError && <Failure error={retry.error} />}
      {withdraw.isError && <Failure error={withdraw.error} />}
    </section>}
    {r.crossSeeds.length > 0 && <section className="media-section" aria-label="Cross-seeding">
      <h2>Cross-seeding</h2>
      <ul className="tv-cross-seeds">{r.crossSeeds.map((seed, i) => <li key={i}>
        <strong>{seed.title}</strong>
        <span>{[seed.provider, seed.status === "registered" ? "Registered to seed" : seed.status === "preparing" ? "Preparing" : "Queued",
          seed.reusedBytes != null ? `${metricBytes(seed.reusedBytes)} reused` : null].filter(Boolean).join(" · ")}</span>
      </li>)}</ul>
    </section>}
    {r.entry.relations.length > 0 && <section className="media-section"><h2>Related works</h2><ul>{r.entry.relations.map(work => <li key={`${work.key}:${work.relation}`}>
      {work.title} · {work.relation}
    </li>)}</ul></section>}
  </>;
}

function downloadEta(seconds: number) {
  if (seconds < 60) return "<1 min";
  const minutes = Math.ceil(seconds / 60);
  if (minutes < 60) return `${minutes} min`;
  const hours = Math.floor(minutes / 60), remaining = minutes % 60;
  return `${hours}h${remaining ? ` ${remaining}m` : ""}`;
}

function TvDownloadProgress({ download, label }: { download: { bytesDone: number; sizeBytes: number; bytesPerSecond: number | null; etaSeconds: number | null; lastReportedAt?: string }; label: string }) {
  const { bytesDone, sizeBytes, bytesPerSecond, etaSeconds, lastReportedAt } = download;
  return <div className="tv-transfer">
    <div className="tv-transfer-caption"><strong>{label}</strong>
      <span>{metricBytes(bytesDone)} of {metricBytes(sizeBytes)}{bytesPerSecond !== null ? ` · ${metricBytes(bytesPerSecond)}/s` : ""}
        {etaSeconds != null && <> · <span title="Estimated time left to download, before Plex verification">ETA {downloadEta(etaSeconds)}</span></>}
        {lastReportedAt && <> · Last reported <time dateTime={lastReportedAt}>{new Date(lastReportedAt).toLocaleTimeString(undefined, { hour: "numeric", minute: "2-digit" })}</time></>}
      </span>
    </div>
    <progress aria-label={`${label} progress`} value={bytesDone} max={sizeBytes} />
  </div>;
}

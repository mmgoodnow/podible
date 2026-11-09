import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Failure } from "./books";
import { Link, navigate } from "./router";
import "./tv.css";

export function TvLibraryDetail({ entryId }: { entryId: number }) {
  const trpc = useTRPC();
  const detail = useQuery(trpc.catalog.tvEntry.queryOptions({ entryId }));
  if (detail.isError) return <Failure error={detail.error} retry={() => void detail.refetch()} />;
  if (!detail.data) return <p role="status">Loading published TV work...</p>;
  const { entry, availableEpisodes } = detail.data;
  return <><Link href="/library?media=tv" className="back">Back to library</Link>
    <h1>{entry.title}</h1><p>{availableEpisodes} episodes published in Plex.</p>
    {entry.tmdbId ? <Link className="button" href={`/tv/${entry.tmdbId}`}>Choose seasons</Link> : <p>A TMDB work mapping is needed before requesting additional seasons.</p>}
    <section className="media-section"><h2>Related works</h2>{entry.relations.length ? <ul>{entry.relations.map(r => <li key={`${r.key}:${r.relation}`}>{r.title} · {r.relation}</li>)}</ul> : <p>No related-work metadata is available.</p>}</section>
  </>;
}

export function TvDetail({ tmdbId }: { tmdbId: number }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const detail = useQuery(trpc.tv.details.queryOptions({ tmdbId }));
  const [selected, setSelected] = useState<number[]>([]);
  const request = useMutation(trpc.tv.request.mutationOptions({ onSuccess: ({ request }) => {
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequests.pathKey() });
    navigate(`/tv/requests/${request.requestId}`);
  } }));
  if (detail.isError) return <Failure error={detail.error} retry={() => void detail.refetch()} />;
  if (!detail.data) return <p role="status">Loading TV series...</p>;
  const { series, unavailableReason } = detail.data;
  return <>
    <Link href="/library?media=tv" className="back">Back to library</Link>
    {!series ? <p role="alert" className="notice">{unavailableReason ?? "TV metadata is unavailable."}</p> : <>
      <section className="movie-hero">
        <div className="jacket">{series.posterUrl ? <img src={series.posterUrl} alt="" /> : <span>{series.title}</span>}</div>
        <div><h1>{series.title}</h1><p>{series.year}</p><p className="synopsis">{series.overview}</p></div>
      </section>
      <section className="media-section"><h2>Choose seasons</h2>
        <p>Requests include every known episode in the selected seasons, including future episodes. Numbering follows TVDB aired order.</p>
        {unavailableReason && <p className="notice">{unavailableReason}</p>}
        <fieldset className="season-options" disabled={request.isPending}>
          <legend>Seasons</legend>
          {series.seasons.map(s => <label key={s.seasonNumber}>
            <input type="checkbox" checked={selected.includes(s.seasonNumber)} disabled={!s.requestable}
              onChange={e => setSelected(previous => e.target.checked ? [...previous, s.seasonNumber].sort((a, b) => a - b) : previous.filter(n => n !== s.seasonNumber))} />
            <span><strong>{s.title}</strong> · {s.episodeCount} episodes{s.airDate ? ` · ${s.airDate}` : ""}
              {!s.requestable && <small>{s.unavailableReason ?? "Episode identity mapping is unavailable."}</small>}</span>
          </label>)}
        </fieldset>
        <button disabled={!selected.length || request.isPending} onClick={() => request.mutate({ tmdbId, seasonNumbers: selected })}>
          {request.isPending ? "Requesting..." : `Request ${selected.length === 1 ? "season" : "seasons"}`}
        </button>
        {request.isError && <Failure error={request.error} />}
      </section>
    </>}
  </>;
}

export function TvRequestDetail({ requestId }: { requestId: number }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const result = useQuery(trpc.tv.myRequest.queryOptions({ requestId }, { refetchInterval: 5000 }));
  const [showReady, setShowReady] = useState(false);
  const retry = useMutation(trpc.tv.retryMyRequest.mutationOptions({ onSuccess: (data) => {
    cache.setQueryData(trpc.tv.myRequest.queryKey({ requestId }), data);
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequest.pathKey() });
  } }));
  const withdraw = useMutation(trpc.tv.withdraw.mutationOptions({ onSuccess: () => {
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequest.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequests.pathKey() });
  } }));
  if (result.isError) return <Failure error={result.error} retry={() => void result.refetch()} />;
  if (!result.data) return <p role="status">Loading TV request...</p>;
  const r = result.data.request;
  const complete = r.requestedEpisodes > 0 && r.availableEpisodes === r.requestedEpisodes;
  const active = r.progress.activeBatches > 0;
  const missingAired = Math.max(0, r.requestedEpisodes - r.availableEpisodes - r.futureEpisodes);
  const title = r.withdrawn ? "Request withdrawn" : r.intentionallyAbsent ? "Removed from the library" : complete ? "Ready in Plex" : active ? "Adding more episodes" : !missingAired ? "Waiting for upcoming episodes" : "More episodes needed";
  const readySeasons = r.progress.seasons.filter(s => s.stage === "ready");
  const seasons = r.progress.seasons.filter(s => showReady || s.stage !== "ready");
  const labels = { ready: "Ready in Plex", queued: "Queued", searching: "Finding a release", downloading: "Downloading", verifying: "Checking episodes", publishing: "Adding to Plex", "pack-pending": "Waiting for pack", waiting: "No match selected", failed: "Acquisition stopped", upcoming: "Not aired yet" };
  const stamp = (value: string) => new Date(value).toLocaleString(undefined, { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" });
  return <>
    <Link href="/library?media=tv" className="back">Back to library</Link>
    <section className="tv-request-hero">
      {r.entry.posterUrl && <img className="tv-request-poster" src={r.entry.posterUrl} alt="" />}
      <div className="tv-request-summary">
        <p className="tv-request-meta">TV series{r.entry.year ? ` · ${r.entry.year}` : ""} · {r.seasonNumbers.length} {r.seasonNumbers.length === 1 ? "season" : "seasons"} requested</p>
        <h1>{r.entry.title}</h1><h2>{title}</h2>
        <p className="tv-request-count"><strong>{r.availableEpisodes.toLocaleString()}</strong> / {r.requestedEpisodes.toLocaleString()} episodes ready</p>
        <progress value={r.availableEpisodes} max={Math.max(1, r.requestedEpisodes)} aria-label="Available TV episodes" />
        <p>{r.availableEpisodes} of {r.requestedEpisodes} episodes verified in Plex.</p>
        {r.progress.lastPublishedAt && <p className="tv-request-meta">Last added {stamp(r.progress.lastPublishedAt)}</p>}
        {!r.withdrawn && !complete && r.progress.searchableAiredEpisodes > 0 && <div className="tv-request-search">
          <button disabled={retry.isPending} onClick={() => retry.mutate({ requestId })}>
            {retry.isPending ? "Queuing search..." : r.intentionallyAbsent ? "Search and restore" : "Search missing episodes"}
          </button>
          {retry.isError && <Failure error={retry.error} />}
        </div>}
      </div>
    </section>
    {!r.withdrawn && !complete && <section className={`tv-request-update${!active && missingAired ? " tv-request-paused" : ""}`} aria-label="Current acquisition status">
      <h3>{r.intentionallyAbsent ? "Downloads are paused" : active ? `${r.progress.activeBatches} episode ${r.progress.activeBatches === 1 ? "batch" : "batches"} in progress` : missingAired ? "Waiting for matching releases" : "Caught up with aired episodes"}</h3>
      <p>{r.intentionallyAbsent ? "This title was intentionally removed." : active && r.progress.searchableAiredEpisodes > 0 ? `${r.progress.searchableAiredEpisodes.toLocaleString()} aired episodes aren't queued. You can search for them without interrupting current downloads.` : active ? "Podible is working on the missing episodes." : missingAired ? `${missingAired.toLocaleString()} aired episodes are still missing. Podible is watching for new releases. You can also search existing releases now.` : "New episodes will be acquired when suitable releases arrive."}</p>
      {r.progress.lastAttemptAt && !active && <p className="tv-request-meta">Last search started {stamp(r.progress.lastAttemptAt)}</p>}
    </section>}
    {r.futureEpisodes > 0 && <p className="tv-request-meta">{r.futureEpisodes} episodes have not aired or do not yet have an air date. Releases are monitored automatically.</p>}
    <section className="tv-request-seasons" aria-label="Season progress">
      <div className="tv-request-section-heading"><h2>Episodes by season</h2>
        {readySeasons.length > 0 && <button className="secondary" onClick={() => setShowReady(value => !value)}>{showReady ? "Hide completed seasons" : `Show ${readySeasons.length} completed ${readySeasons.length === 1 ? "season" : "seasons"}`}</button>}
      </div>
      {seasons.length ? <div className="tv-season-grid">{seasons.map(s => <article className={`tv-season-card tv-season-${s.stage}`} key={s.seasonNumber}>
        <div><h3>{s.seasonNumber === 0 ? "Specials" : `Season ${s.seasonNumber}`}</h3><span className="tv-season-stage">{labels[s.stage]}</span></div>
        <p><strong>{s.available}</strong> / {s.total} ready{s.future > 0 ? ` · ${s.future} upcoming` : ""}</p>
        <progress value={s.available} max={Math.max(1,s.total)} aria-label={`Season ${s.seasonNumber} available episodes`} />
      </article>)}</div> : <p>All requested seasons are ready in Plex.</p>}
    </section>
    <div className="tv-request-actions">
      {!r.withdrawn && <button className="secondary" disabled={withdraw.isPending} onClick={() => {
        if (window.confirm("Withdraw this request? Existing media and other users' requests will be kept.")) withdraw.mutate({ requestId });
      }}>{withdraw.isPending ? "Withdrawing..." : "Withdraw my request"}</button>}
      {withdraw.isError && <Failure error={withdraw.error} />}
    </div>
    {r.entry.relations.length > 0 && <section className="media-section"><h2>Related works</h2><ul>{r.entry.relations.map(work => <li key={`${work.key}:${work.relation}`}>
      {work.title} · {work.relation}
    </li>)}</ul></section>}
  </>;
}

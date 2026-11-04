import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Failure } from "./books";
import { Link, navigate } from "./router";

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
  const withdraw = useMutation(trpc.tv.withdraw.mutationOptions({ onSuccess: () => {
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequest.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.tv.myRequests.pathKey() });
  } }));
  if (result.isError) return <Failure error={result.error} retry={() => void result.refetch()} />;
  if (!result.data) return <p role="status">Loading TV request...</p>;
  const r = result.data.request;
  const complete = r.requestedEpisodes > 0 && r.availableEpisodes === r.requestedEpisodes;
  const active = r.acquisitions.some(a => a.status === "running" || a.status === "queued");
  const title = r.withdrawn ? "Request withdrawn" : r.intentionallyAbsent ? "Intentionally unavailable" : complete ? "Ready in Plex" : active ? "Acquiring episodes" : r.futureEpisodes ? "Waiting for upcoming episodes" : "Waiting for suitable releases";
  return <>
    <Link href="/library?media=tv" className="back">Back to library</Link>
    <h1>{r.entry.title}</h1>
    <section className="media-section"><h2>{title}</h2>
      <p>Seasons {r.seasonNumbers.join(", ")}</p>
      <progress value={r.availableEpisodes} max={Math.max(1, r.requestedEpisodes)} aria-label="Available TV episodes" />
      <p>{r.availableEpisodes} of {r.requestedEpisodes} episodes verified in Plex.</p>
      {r.futureEpisodes > 0 && <p>{r.futureEpisodes} episodes have not aired or do not yet have an air date. Suitable releases will be picked up from indexer feeds.</p>}
      {!active && !complete && !r.withdrawn && !r.intentionallyAbsent && <p>New releases are monitored automatically; no action is needed while a suitable release is unavailable.</p>}
      {!r.withdrawn && <button className="secondary" disabled={withdraw.isPending} onClick={() => {
        if (window.confirm("Withdraw this request? Existing media and other users' requests will be kept.")) withdraw.mutate({ requestId });
      }}>{withdraw.isPending ? "Withdrawing..." : "Withdraw my request"}</button>}
      {withdraw.isError && <Failure error={withdraw.error} />}
    </section>
    {r.entry.relations.length > 0 && <section className="media-section"><h2>Related works</h2><ul>{r.entry.relations.map(work => <li key={`${work.key}:${work.relation}`}>
      {work.title} · {work.relation}
    </li>)}</ul></section>}
    <section className="media-section"><h2>Activity</h2>
      {r.events.length ? <ol>{r.events.map((event, i) => <li key={`${event.createdAt}:${i}`}><time dateTime={event.createdAt}>{new Date(event.createdAt).toLocaleString()}</time> · {event.message}</li>)}</ol> : <p>No acquisition activity yet.</p>}
      {r.acquisitions.map(a => <p key={a.id}>{a.reason}: {a.status}{a.error ? ` · ${a.error}` : ""}</p>)}
    </section>
  </>;
}

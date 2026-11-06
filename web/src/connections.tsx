import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { Link } from "./router";
import "./connections.css";

export type ConnectionFeed = inferRouterOutputs<AppRouter>["adminWeb"]["ops"]["feeds"][number];
type State = ConnectionFeed["state"];
const labels: Record<State, string> = {
  unavailable: "Poll errors", cooldown: "Cooling down", unknown: "Unknown", healthy: "Success recorded",
};
const countLabel = (state: State, count: number) => state === "healthy" ? count === 1 ? "success" : "successes"
  : state === "cooldown" ? count === 1 ? "cooldown" : "cooldowns" : state === "unavailable" ? count === 1 ? "error" : "errors" : "unknown";
const order: Record<State, number> = { unavailable: 0, cooldown: 1, unknown: 2, healthy: 3 };
const media: Record<string, string> = { movie: "Movies", tv: "TV", audio: "Audiobooks", ebook: "Ebooks" };
const mediaLabel = (value: string) => media[value] ?? value;
const validTime = (value: string | null) => value !== null && Number.isFinite(Date.parse(value));

function latestTime(values: (string | null)[]): string | null {
  return values.filter((value): value is string => validTime(value)).sort((a, b) => Date.parse(b) - Date.parse(a))[0] ?? null;
}

/** This payload identifies sources by label, not by the gateway's private endpoint key. */
export function connectionProviders(feeds: readonly ConnectionFeed[]) {
  const groups = new Map<string, ConnectionFeed[]>();
  for (const feed of feeds) {
    const group = groups.get(feed.provider) ?? [];
    group.push(feed);
    groups.set(feed.provider, group);
  }
  return [...groups].map(([name, rows]) => {
    const states = [...new Set(rows.map(feed => feed.state))].sort((a, b) => order[a] - order[b]);
    const errors = new Map<string, { message: string; media: string[] }>();
    for (const feed of rows) if (feed.error) {
      const key = JSON.stringify([feed.error.code, feed.error.message]);
      const error = errors.get(key) ?? { message: feed.error.message, media: [] };
      if (!error.media.includes(mediaLabel(feed.mediaType))) error.media.push(mediaLabel(feed.mediaType));
      errors.set(key, error);
    }
    return {
      name, feeds: [...rows].sort((a, b) => a.mediaType.localeCompare(b.mediaType) || a.id.localeCompare(b.id)),
      states, mixed: states.length > 1, label: states.length > 1 ? "Mixed reports" : labels[states[0]!],
      reports: states.map(state => ({ state, count: rows.filter(feed => feed.state === state).length })),
      tone: states[0]!, errors: [...errors.values()],
      media: [...new Set(rows.map(feed => mediaLabel(feed.mediaType)))],
      lastPollAt: latestTime(rows.map(feed => feed.lastPolledAt)),
    };
  }).sort((a, b) => order[a.tone] - order[b.tone] || a.name.localeCompare(b.name));
}

function Timestamp({ value }: { value: string | null }) {
  if (!validTime(value)) return <span className="connections-time-unknown">Not recorded</span>;
  return <time dateTime={value!}>{new Date(value!).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })}</time>;
}

function StateIcon({ state }: { state: State }) {
  return <svg className="connections-icon" width="16" height="16" viewBox="0 0 16 16" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">
    <circle cx="8" cy="8" r="6.2" />
    {state === "healthy" ? <path d="m4.7 8 2.2 2.2 4.5-4.5" /> : state === "cooldown" ? <path d="M6 5v6M10 5v6" /> : state === "unavailable" ? <path d="M8 4.7v4M8 11.2v.1" /> : <path d="M6.2 5.5a2 2 0 0 1 3.8.9c0 1.5-2 1.5-2 3M8 11.5v.1" />}
  </svg>;
}

export function Connections({ feeds, observedAt, truncated = false }: {
  feeds: readonly ConnectionFeed[];
  observedAt: string;
  truncated?: boolean;
}) {
  const providers = connectionProviders(feeds);
  const pollErrors = providers.filter(provider => provider.states.includes("unavailable")).length;
  const cooldowns = providers.filter(provider => provider.states.includes("cooldown")).length;
  return <section className="connections" aria-label="Connections">
    <div className="connections-heading"><h2>Connections</h2><span>{providers.length} {providers.length === 1 ? "provider" : "providers"}{truncated ? " shown" : ""}</span></div>
    {(pollErrors > 0 || cooldowns > 0) && <ul className="connections-overview" aria-label="Provider observation summary">
      {pollErrors > 0 && <li data-state="unavailable"><StateIcon state="unavailable" />{pollErrors} with poll errors</li>}
      {cooldowns > 0 && <li data-state="cooldown"><StateIcon state="cooldown" />{cooldowns} cooling down</li>}
    </ul>}
    {truncated && <p className="connections-limit" role="status">This overview is bounded; some provider observations may not be shown. Counts refer only to the saved feeds shown here.</p>}
    {!providers.length ? <p className="connections-empty">No saved feed observations. Provider availability is unknown.</p> :
      <ul className="connections-grid">{providers.map(provider => <li key={provider.name}>
        <details className="connections-provider" data-tone={provider.tone}>
          <summary>
            <span className="connections-provider__heading"><strong>{provider.name}</strong><span className="connections-badge"><StateIcon state={provider.tone} />{provider.label}</span></span>
            <span className="connections-media">{provider.media.join(" / ")}</span>
            {provider.mixed && <span className="connections-counts" aria-label="Saved feed reports">{provider.reports.map(report => <span key={report.state} data-state={report.state}>
              <StateIcon state={report.state} /><span>{report.count} {countLabel(report.state, report.count)}</span>
            </span>)}</span>}
            <span className="connections-provider__foot"><span>Last poll <Timestamp value={provider.lastPollAt} /></span><span className="connections-disclosure">Feed details ({provider.feeds.length})</span></span>
          </summary>
          <div className="connections-provider__details">
            {provider.states.includes("cooldown") && <p className="connections-explanation">A cooldown was reported. It does not establish the upstream cause or prove rate limiting.</p>}
            {provider.errors.map((error, index) => <p className="connections-error" key={index}>{error.message} <span>Recorded for {error.media.join(", ")}.</span></p>)}
            {provider.feeds.map(feed => <article className="connections-feed" key={feed.id} aria-label={`${mediaLabel(feed.mediaType)} feed checkpoints`}>
              <h3>{mediaLabel(feed.mediaType)}</h3>
              <dl>
                <div><dt>Last poll attempt</dt><dd><Timestamp value={feed.lastPolledAt} /></dd></div>
                <div><dt>Last successful poll</dt><dd><Timestamp value={feed.lastSuccessAt} /></dd></div>
                <div><dt>Saved retry boundary</dt><dd><Timestamp value={feed.retryAt} /></dd></div>
                <div><dt>Shared feed sync wake</dt><dd><Timestamp value={feed.nextPollAt} /></dd></div>
              </dl>
            </article>)}
          </div>
        </details>
      </li>)}</ul>}
    <div className="connections-footer"><Link href="/settings">Connection settings</Link><span>Overview updated <Timestamp value={observedAt} /></span></div>
    <p className="connections-help">Saved poll results, not a live connection test.</p>
  </section>;
}

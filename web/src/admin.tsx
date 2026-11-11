import { lazy, Suspense, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { Link } from "./router";
import "./activity.css";
import { activityItems, activityPhase, attentionItems, downloadProgress, problemAction, type ActivityDownload } from "./activity-model";
import { Connections } from "./connections";
import { RecordedDownloads, SharingHistory, SharingOverview, useSharingMetrics } from "./transfer-metrics";
import { LibraryOverview } from "./library-inventory";
import { CrossSeedBackfill } from "./cross-seed-backfill";

const Settings = lazy(() => import("./settings"));

function AdminNavigation({ current }: { current: "settings" | "users" | "ops" }) {
  return <nav aria-label="Administrator pages" style={current === "settings" ? undefined : { marginBottom: "1rem" }}>
    {(["settings", "users", "ops"] as const).map((page) => <Link key={page} href={`/${page}`} aria-current={current === page ? "page" : undefined}>
      {page === "ops" ? "Activity" : page === "users" ? "Users" : "Settings"}
    </Link>)}
  </nav>;
}

export function SettingsPage() {
  return <Suspense fallback={<p role="status">Opening settings...</p>}>
    <Settings navigation={<AdminNavigation current="settings" />} />
  </Suspense>;
}

export function UsersPage() {
  const trpc = useTRPC();
  const users = useQuery(trpc.adminWeb.users.queryOptions({}));
  return <>
    <h1 style={{ fontSize: "2rem", margin: "0 0 0.5rem" }}>Users</h1>
    <AdminNavigation current="users" />
    {users.isPending ? <p role="status">Loading users...</p> : users.isError ? <p role="alert">{users.error.message}</p> : <section className="panel" aria-label="Application users">
      <p>{users.data.users.length} {users.data.users.length === 1 ? "user" : "users"}.</p>
      {!users.data.users.length ? <p>No users yet.</p> : <div style={{ overflowX: "auto" }}><table style={{ width: "100%", textAlign: "left" }}>
        <thead><tr><th>User</th><th>Provider</th><th>Role</th><th>Updated</th></tr></thead>
        <tbody>{users.data.users.map((user) => <tr key={user.id}>
          <td><strong>{user.displayName ?? user.username}</strong><br /><small>{user.username} / #{user.id}</small></td>
          <td>{user.provider}</td><td>{user.isAdmin ? "Administrator" : "Reader"}</td>
          <td><time dateTime={user.updatedAt}>{new Date(user.updatedAt).toLocaleString()}</time></td>
        </tr>)}</tbody>
      </table></div>}
    </section>}
  </>;
}

type Ops = inferRouterOutputs<AppRouter>["adminWeb"]["ops"];
type Job = Ops["diagnostics"]["recentJobs"][number];
const recoveryLabels: Record<Job["recovery"], string> = {
  running: "Working", "automatic-retry": "Will retry automatically", "scheduled-wake": "Waiting to continue", queued: "Queued",
  "manual-retry": "Manual retry available", cancelled: "Cancelled", complete: "Complete", unknown: "Recovery state unknown",
};
const date = (value: string | null) => value ? new Date(value).toLocaleString() : "Not reported";
function bytes(value: number | null): string {
  if (value === null) return "Unknown";
  if (value < 1024) return `${value} B`;
  const unit = Math.min(4, Math.floor(Math.log(value) / Math.log(1024)));
  return `${(value / 1024 ** unit).toFixed(1)} ${["B", "KiB", "MiB", "GiB", "TiB"][unit]}`;
}
function WorkTitle({ title, href }: { title: string; href: string | null }) {
  return href?.startsWith("/") && !href.startsWith("//") ? <Link href={href}><strong>{title}</strong></Link> : <strong>{title}</strong>;
}
function Recovery({ recovery, nextWakeAt }: Pick<Job, "recovery" | "nextWakeAt">) {
  return <span>{recoveryLabels[recovery]}{nextWakeAt ? `; next wake ${date(nextWakeAt)}` : recovery === "scheduled-wake" ? "; wake time not reported" : ""}</span>;
}
function ActivityProblem({ incident, onRetried }: { incident: Ops["incidents"][number]; onRetried: (title: string) => void }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const retry = useMutation(trpc.jobs.retry.mutationOptions({ onSuccess: async () => {
    onRetried(incident.title);
  }, onSettled: () => cache.invalidateQueries({ queryKey: trpc.adminWeb.ops.pathKey() }) }));
  const retryable = incident.jobId !== null && ["manual-retry", "cancelled"].includes(incident.recovery);
  const correction = problemAction(incident.code);
  const comparison = incident.code === "comparison-failed";
  const mapping = incident.code === "tv-season-mapping" && incident.jobId === null;
  return <article className="activity-item problem" aria-label={incident.title}>
    <div className="activity-item-heading"><h3><WorkTitle title={incident.title} href={incident.href} /></h3>
      <span className="activity-badge">{mapping ? "Episode mapping needed" : comparison ? "Comparison stopped" : retryable ? "Stopped" : "Needs attention"}</span></div>
    <p>{incident.detail}</p>
    <p>{mapping ? "Podible researches episode correspondences and retries automatically. There is no need to retry the download." : <Recovery recovery={incident.recovery} nextWakeAt={incident.nextWakeAt} />}</p>
    {correction && <div className="actions">{correction.href.startsWith("#") ? <a className="button" href={correction.href}>{correction.label}</a>
      : <Link className="button" href={correction.href}>{correction.label}</Link>}</div>}
    {retryable && !correction && <div className="actions"><button disabled={retry.isPending} onClick={() => retry.mutate({ jobId: incident.jobId! })}>
      {retry.isPending ? "Requesting retry..." : comparison ? "Retry comparison" : "Retry"}</button></div>}
    {retry.error && <p role="alert">Could not confirm the retry: {retry.error.message}. Check the refreshed status before trying again.</p>}
    {!mapping && <details><summary>Technical details</summary><p>{incident.code}{incident.jobId ? ` / job #${incident.jobId}` : ""}</p>
      {retryable && correction && <button disabled={retry.isPending} onClick={() => retry.mutate({ jobId: incident.jobId! })}>Retry after resolving the problem</button>}
    </details>}
  </article>;
}

function ActivityWork({ work, downloads }: { work: Ops["activeAcquisitions"][number]; downloads: ActivityDownload[] }) {
  const phase = activityPhase(work, downloads);
  const measured = downloadProgress(downloads);
  return <article className="activity-item" aria-label={work.title}>
    <div className="activity-item-heading"><h3><WorkTitle title={work.title} href={work.href} /></h3><span className="activity-badge">{phase.label}</span></div>
    {work.scope && <p>{work.scope}</p>}
    {work.waitingPack && <p>{work.waitingPack.title} is expected to cover {work.waitingPack.episodeCount} missing {work.waitingPack.episodeCount === 1 ? "episode" : "episodes"}.</p>}
    {work.media === "book" && work.stage && <p>{work.stage}</p>}
    {measured ? <>
      {phase.transferLabel !== "Download progress" && <p>{phase.transferLabel}</p>}
      <progress aria-label={`Download progress for ${work.title}`} value={measured.done} max={measured.size} />
      <p>{bytes(measured.done)} of {bytes(measured.size)}{measured.speed !== null && measured.live ? ` / ${bytes(measured.speed)}/s` : ""}{!measured.live ? " / last recorded progress" : ""}</p>
    </> : downloads.length > 0 && <p>Waiting for download status.</p>}
    {downloads.length > 1 && <p>{downloads.length} downloads attached to this batch.</p>}
    {downloads.filter(d => d.problem).map(download => <p key={download.key}>{download.problem!.message}</p>)}
    {phase.showWake && work.job.nextWakeAt && <p>Next check: {date(work.job.nextWakeAt)}.</p>}
    <details><summary>Technical details</summary>
      <p>{work.media} / job #{work.job.id} / {work.job.type}. Attempts {work.job.attempts}/{work.job.maxAttempts}.</p>
      {work.stage && <p>Workflow stage: {work.stage}</p>}
      {downloads.map(download => <p key={download.key}>Telemetry: {download.telemetry}. Observed {date(download.observedAt)}.</p>)}
    </details>
  </article>;
}

export function OpsPage() {
  const trpc = useTRPC(), cache = useQueryClient();
  const [retryNotice, setRetryNotice] = useState<string | null>(null);
  const ops = useQuery(trpc.adminWeb.ops.queryOptions({ includeJobs: false }, { refetchInterval: 5000 }));
  const sharing = useSharingMetrics();
  const refresh = useMutation(trpc.adminWeb.refresh.mutationOptions({ onSuccess: () => cache.invalidateQueries({ queryKey: trpc.adminWeb.ops.queryKey() }) }));
  const work = ops.data ? activityItems(ops.data) : [];
  const running = work.filter(item => activityPhase(item.work, item.downloads).active);
  const waiting = work.filter(item => !activityPhase(item.work, item.downloads).active);
  const attention = ops.data ? attentionItems(ops.data.incidents) : [];
  const incomplete = ops.isError || ops.data?.diagnostics.truncated || Boolean(ops.data?.diagnostics.warnings.length);
  const displayedTorrentIds = new Set(ops.data?.downloads.map(download => download.torrentId));
  const otherDownloads = ops.data ? sharing.query.data?.downloads.filter(download => !displayedTorrentIds.has(download.id)) ?? [] : [];
  const refreshOverview = () => {
    void ops.refetch();
    void sharing.query.refetch();
    void cache.invalidateQueries({ queryKey: trpc.transferMetrics.published.queryKey() });
    void cache.invalidateQueries({ queryKey: trpc.transferMetrics.library.queryKey() });
  };
  return <div className="activity-page">
    <div className="activity-heading"><h1>Activity</h1><button className="secondary" disabled={ops.isFetching} onClick={refreshOverview}>Refresh</button></div>
    <nav className="activity-jumps" aria-label="Activity sections">
      <a href="#activity-library">Library</a><a href="#activity-sharing">Sharing</a><a href="#activity-work">Current work</a><a href="#activity-system">System</a>
      {attention.length > 0 ? <a className="activity-issues" href="#activity-attention">{attention.length}{incomplete ? "+" : ""} to review</a>
        : incomplete && <a className="activity-issues" href="#activity-work">Status incomplete</a>}
    </nav>
    <LibraryOverview />
    <SharingOverview metrics={sharing} />
    <section className="activity-section activity-work" id="activity-work" aria-label="Current work">
      <div className="transfer-heading"><h2>Current work</h2>{ops.data && <span className="transfer-live">Checked {date(ops.data.observedAt)}</span>}</div>
      {retryNotice !== null && <p role="status">Retry requested for {retryNotice}. Its current progress appears below.</p>}
      {!ops.data ? ops.isError ? <p role="alert">Could not load current work. <button className="secondary" onClick={() => ops.refetch()}>Try again</button></p>
        : <p role="status">Loading current work...</p> : <>
      {ops.isRefetchError && <p className="muted">Could not refresh current work. Showing the last response.</p>}
      <section className="activity-section" aria-label="Active work">
        <h3>In progress <small>{running.length || ""}</small></h3>
        {!running.length ? <p className="muted">{incomplete ? "No active requests could be confirmed."
          : otherDownloads.length ? "No requests in progress." : "No downloads or imports in progress."}</p> : <div className="activity-items">
          {running.map(item => <ActivityWork key={item.work.job.id} work={item.work} downloads={item.downloads} />)}</div>}
      </section>
      <RecordedDownloads downloads={otherDownloads} />
      {(waiting.length > 0 || ops.data.summary.waiting > 0) && <section className="activity-section" aria-label="Waiting work">
        <h3>Waiting</h3>
        <div className="activity-items">{waiting.map(item => <ActivityWork key={item.work.job.id} work={item.work} downloads={item.downloads} />)}</div>
        {ops.data.summary.waiting > 0 && <p className="activity-waiting">{ops.data.summary.waiting} {ops.data.summary.waiting === 1 ? "request is" : "requests are"} waiting for a release or an unaired episode. <Link href="/library">View library</Link></p>}
      </section>}
      {attention.length > 0 && <section className="activity-section" id="activity-attention" aria-label="Needs attention">
        <h3>Needs attention <small>{attention.length}</small></h3>
        <div className="activity-items">{attention.map(incident => <ActivityProblem key={incident.key} incident={incident} onRetried={setRetryNotice} />)}</div>
      </section>}
      {ops.data.diagnostics.truncated && <p className="muted">Some work is not shown in this overview.</p>}
      {ops.data.diagnostics.warnings.length > 0 && <details className="activity-status"><summary>Some activity status is unavailable</summary>
        {ops.data.diagnostics.warnings.map((warning, index) => <p key={index}>{warning}</p>)}</details>}
      </>}
    </section>
    <SharingHistory metrics={sharing} />
    <section className="activity-section activity-system" id="activity-system" aria-label="System">
      <div className="transfer-heading"><h2>System</h2><Link href="/stewardship">Review torrent problems</Link></div>
      {ops.data && <>
      <section className="activity-section" id="activity-storage" aria-label="Storage and links">
        <h3>Storage</h3>
        <div className="activity-storage">{ops.data.storage.map(storage => <article key={storage.media}>
          <h3>{storage.media === "book" ? "Books" : storage.media === "movie" ? "Movies" : "TV"}</h3>
          <p>{storage.capacityState === "known" ? `${bytes(storage.availableBytes)} free` : storage.capacityState === "not-configured" ? "Not configured" : "Capacity unknown"}</p>
          {storage.totalBytes !== null && storage.totalBytes > 0 && storage.availableBytes !== null &&
            <meter aria-label={`${storage.media} storage used`} min={0} max={storage.totalBytes} value={storage.totalBytes - storage.availableBytes} />}
          {storage.identityState !== "verified" && <p>{storage.identityState === "not-configured" ? "Storage not configured" : "Storage could not be verified"}</p>}
          <details><summary>Storage details</summary><p><code>{storage.root ?? "Not configured"}</code></p><p>{storage.reason}</p>
            <p>Published links have not been checked by this overview.</p>
            {storage.media !== "book" && <p><Link href={storage.media === "tv" ? "/tv-retention" : "/retention"}>Review removable titles</Link></p>}
            {storage.media !== "book" && <p><Link href={`/recovery?media=${storage.media}`}>Inspect missing files and links</Link></p>}
          </details>
        </article>)}</div>
      </section>
      <Connections feeds={ops.data.feeds} observedAt={ops.data.observedAt} truncated={ops.data.feeds.length >= 100} />
      </>}
    <CrossSeedBackfill />
    <section className="activity-section" aria-label="Library maintenance">
      <h3>Library maintenance</h3>
      <button className="secondary" disabled={refresh.isPending} onClick={() => refresh.mutate({})}>{refresh.isPending ? "Requesting refresh..." : "Refresh library metadata"}</button>
      {refresh.error && <p role="alert">Library refresh was not queued: {refresh.error.message}</p>}
      {refresh.isSuccess && <p role="status">Library refresh {refresh.data.shared ? "already queued or running" : "queued"}.</p>}
    </section>
    </section>
  </div>;
}

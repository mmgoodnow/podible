import { useEffect, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import type { AppSettings } from "../../src/app-types";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";
import { Link } from "./router";
import "./activity.css";
import { activityItems, attentionItems, downloadProgress, problemAction, type ActivityDownload } from "./activity-model";
import { Connections } from "./connections";

type SettingsDraft = { text: string; base: string } | null;
const draftKey = ["admin-settings-draft"] as const;
const json = (value: unknown) => JSON.stringify(value, null, 2);
const record = (value: unknown): value is Record<string, unknown> => value !== null && typeof value === "object" && !Array.isArray(value);
function parsedSettings(text: string): { value: AppSettings | null; error: string | null } {
  try {
    const value: unknown = JSON.parse(text);
    return record(value) ? { value: value as AppSettings, error: null } : { value: null, error: "Settings must be a JSON object." };
  } catch (error) { return { value: null, error: `Invalid JSON: ${(error as Error).message}` }; }
}

function AdminNavigation({ current }: { current: "settings" | "users" | "ops" }) {
  return <nav aria-label="Administrator pages" style={{ marginBottom: "1rem" }}>
    {(["settings", "users", "ops"] as const).map((page) => <Link key={page} href={`/${page}`} aria-current={current === page ? "page" : undefined}>
      {page === "ops" ? "Activity" : page === "users" ? "Users" : "Settings"}
    </Link>)}
  </nav>;
}

function rebasePlex(text: string, machineId: string): string | null {
  const parsed = parsedSettings(text);
  if (!parsed.value || !record(parsed.value.auth) || !record(parsed.value.auth.plex)) return null;
  return json({ ...parsed.value, auth: { ...parsed.value.auth, plex: { ...parsed.value.auth.plex, machineId } } });
}

export function SettingsPage() {
  const trpc = useTRPC(), cache = useQueryClient();
  const settings = useQuery(trpc.settings.get.queryOptions({}, { refetchOnWindowFocus: true }));
  // Actor-scoped QueryClient memory survives SPA navigation, never browser storage.
  const draft = useQuery<SettingsDraft>({ queryKey: draftKey, queryFn: async () => null, enabled: false, initialData: null, gcTime: Infinity });
  const base = settings.data ? json(settings.data) : "";
  const text = draft.data?.text ?? base;
  const parsed = parsedSettings(text);
  const dirty = draft.data !== null && text !== base;
  const save = useMutation(trpc.settings.update.mutationOptions());
  const plex = useQuery(trpc.adminWeb.plexServers.queryOptions({}, { refetchOnWindowFocus: false }));
  const [pendingMachine, setPendingMachine] = useState<string | null>(null);
  const selectServer = useMutation(trpc.adminWeb.selectPlexServer.mutationOptions({ onSuccess: async ({ selectedMachineId }) => {
    cache.setQueryData<SettingsDraft>(draftKey, (current): SettingsDraft => {
      if (!current) return null;
      const rebased = rebasePlex(current.text, selectedMachineId), rebasedBase = rebasePlex(current.base, selectedMachineId);
      return rebased && rebasedBase ? { text: rebased, base: rebasedBase } : current;
    });
    await cache.invalidateQueries({ queryKey: trpc.settings.get.queryKey() });
    await cache.invalidateQueries({ queryKey: trpc.adminWeb.plexServers.queryKey() });
    setPendingMachine(null);
  } }));
  const selectedMachine = pendingMachine ?? plex.data?.selectedMachineId ?? settings.data?.auth.plex.machineId ?? "";
  const canSelect = !!plex.data?.servers.some((server) => server.machineId === selectedMachine) &&
    selectedMachine !== plex.data?.selectedMachineId && rebasePlex(text, selectedMachine) !== null;
  useEffect(() => {
    if (!dirty) return;
    const beforeUnload = (event: BeforeUnloadEvent) => { event.preventDefault(); event.returnValue = ""; };
    window.addEventListener("beforeunload", beforeUnload);
    return () => window.removeEventListener("beforeunload", beforeUnload);
  }, [dirty]);
  const submit = () => {
    if (!parsed.value || save.isPending || selectServer.isPending) return;
    const submitted = text;
    save.mutate({ settings: parsed.value }, { onSuccess: (saved) => {
      cache.setQueryData(trpc.settings.get.queryKey({}), saved);
      cache.setQueryData<SettingsDraft>(draftKey, (current) => current?.text === submitted ? null
        : current ? { ...current, base: json(saved) } : null);
      void cache.invalidateQueries({ queryKey: trpc.adminWeb.plexServers.queryKey() });
    } });
  };
  return <>
    <h1 style={{ fontSize: "2rem", margin: "0 0 0.5rem" }}>Settings</h1>
    <AdminNavigation current="settings" />
    <section className="panel" aria-label="Plex access control">
      <h2>Plex access control</h2>
      <p>Select which existing Plex server controls browser sign-ins.</p>
      {plex.isPending ? <p role="status">Loading accessible Plex servers...</p> : plex.isError ? <p role="alert">{plex.error.message}</p> : <>
        <p>Owner token: {plex.data.ownerTokenConfigured ? plex.data.ownerTokenExpired === true ? "Expired; sign in again to relink" : plex.data.ownerTokenExpired === false ? "Configured, not expired" : "Configured; expiry unknown" : "Not configured"}. Server list: {plex.data.source}.</p>
        <p>Selected server: <strong>{plex.data.servers.find((server) => server.machineId === plex.data.selectedMachineId)?.name ?? plex.data.selectedMachineId ?? "Not selected"}</strong>.</p>
        {plex.data.error && <p role="alert">{plex.data.error}</p>}
        <div className="actions">
          <div style={{ display: "grid", gap: "0.4rem", minWidth: 0, maxWidth: "100%" }}><label htmlFor="admin-plex-server">Plex access server</label>
            <select id="admin-plex-server" value={selectedMachine} disabled={selectServer.isPending || save.isPending || !plex.data.servers.length}
              onChange={(event) => { setPendingMachine(event.target.value); selectServer.reset(); }} style={{ maxWidth: "100%" }}>
              {!plex.data.servers.some((server) => server.machineId === selectedMachine) && <option value={selectedMachine}>{selectedMachine || "Select a server"}</option>}
              {plex.data.servers.map((server) => <option key={server.machineId} value={server.machineId}>
                {server.name}{server.owned ? " (owned)" : server.sourceTitle ? ` (shared by ${server.sourceTitle})` : " (shared)"}
              </option>)}
            </select>
          </div>
          <button disabled={!canSelect || selectServer.isPending || save.isPending} onClick={() => selectServer.mutate({ machineId: selectedMachine })}>
            {selectServer.isPending ? "Selecting..." : "Use selected Plex server"}
          </button>
          <button disabled={plex.isFetching || selectServer.isPending} onClick={() => plex.refetch()}>Reload Plex server list</button>
        </div>
        {!plex.data.servers.length && <p>No accessible servers reported. The existing selection is preserved.</p>}
        {draft.data && rebasePlex(text, selectedMachine) === null && <p>Fix the JSON draft's auth.plex object before selecting a server. Your draft is preserved.</p>}
      </>}
      {selectServer.error && <p role="alert">Plex selection was not changed: {selectServer.error.message}</p>}
      {selectServer.isSuccess && <p role="status">Plex access server selected. Other unsaved settings are preserved.</p>}
    </section>
    <section className="panel" aria-label="Application settings">
      {settings.isPending && <p role="status">Loading settings...</p>}
      {settings.error && <p role="alert">{settings.error.message}. Your unsaved draft has been kept.</p>}
      {dirty && <p role="status">Unsaved changes</p>}
      {dirty && draft.data?.base !== base && <p role="status">Server settings changed while you were editing. Your draft is preserved; review it before saving.</p>}
      <div style={{ display: "grid", gap: "0.5rem" }}>
        <label htmlFor="admin-settings-json">Settings JSON</label>
        <textarea id="admin-settings-json" value={text} spellCheck={false} autoComplete="off" rows={24}
          disabled={selectServer.isPending || (!settings.data && !draft.data)} aria-invalid={!!draft.data && !!parsed.error}
          aria-describedby="settings-json-note"
          style={{ width: "100%", maxWidth: "100%", minHeight: "20rem", fontFamily: "monospace", fontSize: "0.85rem", tabSize: 2 }}
          onChange={(event) => {
            const next: SettingsDraft = { text: event.target.value, base: draft.data?.base ?? base };
            cache.setQueryData<SettingsDraft>(draftKey, (): SettingsDraft => next);
            if (!save.isPending) save.reset();
          }} />
      </div>
      <p id="settings-json-note">This administrator-only editor includes server credentials. Changes replace the complete settings object; omitted or invalid fields are checked by the server.</p>
      {draft.data && parsed.error && <p role="alert">{parsed.error}</p>}
      {save.error && <p role="alert">Settings were not saved: {save.error.message}. Your draft has been kept.</p>}
      {save.isSuccess && <p role="status">Settings saved{dirty ? "; newer edits remain unsaved" : ""}.</p>}
      <div className="actions">
        <button disabled={!settings.data || !dirty || !parsed.value || save.isPending || selectServer.isPending} onClick={submit}>{save.isPending ? "Saving..." : "Save settings"}</button>
        <button disabled={settings.isFetching || save.isPending} onClick={() => settings.refetch()}>Reload server settings</button>
        {dirty && <button disabled={save.isPending} onClick={() => { cache.setQueryData(draftKey, null); save.reset(); }}>Discard draft</button>}
      </div>
    </section>
  </>;
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
  running: "Working", "automatic-retry": "Will retry automatically", "scheduled-wake": "Scheduled wake", queued: "Queued",
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
function JobDiagnostic({ job }: { job: Job }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const retry = useMutation(trpc.jobs.retry.mutationOptions({ onSettled: () => cache.invalidateQueries({ queryKey: trpc.adminWeb.ops.pathKey() }) }));
  return <details className="panel">
    <summary>Job #{job.id} / {job.type} / {job.status}</summary>
    <p><Recovery recovery={job.recovery} nextWakeAt={job.nextWakeAt} />. Attempts {job.attempts}/{job.maxAttempts}.</p>
    {job.error && <p>{job.error.code}: {job.error.message}</p>}
    {job.bookId && <p><Link href={`/book/${job.bookId}`}>Open book #{job.bookId}</Link></p>}
    {["failed", "cancelled"].includes(job.status) && <button disabled={retry.isPending} onClick={() => retry.mutate({ jobId: job.id })}>
      {retry.isPending ? "Queuing..." : `Retry job #${job.id}`}
    </button>}
    {retry.error && <p role="alert">Could not confirm the retry: {retry.error.message}. Check the refreshed status before trying again.</p>}
    {retry.isSuccess && <p role="status">Retry requested for job #{job.id}. Follow its actual execution state above.</p>}
  </details>;
}
function OpsDiagnostics() {
  const trpc = useTRPC();
  const diagnostics = useQuery(trpc.adminWeb.ops.queryOptions({ includeJobs: true }, { refetchInterval: 5000 }));
  return diagnostics.isPending ? <p role="status">Loading execution details...</p> : diagnostics.isError ? <p role="alert">{diagnostics.error.message}</p> : <>
    {diagnostics.data.diagnostics.warnings.map((warning, index) => <p key={index}>{warning}</p>)}
    {diagnostics.data.diagnostics.truncated && <p>Only the bounded recent execution history is shown.</p>}
    {!diagnostics.data.diagnostics.recentJobs.length && <p>No recent jobs.</p>}
    {diagnostics.data.diagnostics.recentJobs.map((job) => <JobDiagnostic key={job.id} job={job} />)}
  </>;
}

function ActivityProblem({ incident, onRetried }: { incident: Ops["incidents"][number]; onRetried: (id: number) => void }) {
  const trpc = useTRPC(), cache = useQueryClient();
  const retry = useMutation(trpc.jobs.retry.mutationOptions({ onSuccess: async () => {
    onRetried(incident.jobId!);
  }, onSettled: () => cache.invalidateQueries({ queryKey: trpc.adminWeb.ops.pathKey() }) }));
  const retryable = incident.jobId !== null && ["manual-retry", "cancelled"].includes(incident.recovery);
  const correction = problemAction(incident.code);
  return <article className="activity-item problem" aria-label={incident.title}>
    <div className="activity-item-heading"><h3><WorkTitle title={incident.title} href={incident.href} /></h3>
      <span className="activity-badge">{retryable ? "Stopped" : "Needs attention"}</span></div>
    <p>{incident.detail}</p>
    <p><Recovery recovery={incident.recovery} nextWakeAt={incident.nextWakeAt} /></p>
    {correction && <div className="actions">{correction.href.startsWith("#") ? <a className="button" href={correction.href}>{correction.label}</a>
      : <Link className="button" href={correction.href}>{correction.label}</Link>}</div>}
    {retryable && !correction && <div className="actions"><button disabled={retry.isPending} onClick={() => retry.mutate({ jobId: incident.jobId! })}>
      {retry.isPending ? "Requesting retry..." : "Retry"}</button></div>}
    {retry.error && <p role="alert">Could not confirm the retry: {retry.error.message}. Check the refreshed status before trying again.</p>}
    <details><summary>Technical details</summary><p>{incident.code}{incident.jobId ? ` / job #${incident.jobId}` : ""}</p>
      {retryable && correction && <button disabled={retry.isPending} onClick={() => retry.mutate({ jobId: incident.jobId! })}>Retry after resolving the problem</button>}
    </details>
  </article>;
}

function ActivityWork({ work, downloads }: { work: Ops["activeAcquisitions"][number]; downloads: ActivityDownload[] }) {
  const phase = work.job.recovery === "running" ? work.stage?.replaceAll("_", " ") || "Working" : recoveryLabels[work.job.recovery];
  const measured = downloadProgress(downloads);
  return <article className="activity-item" aria-label={work.title}>
    <div className="activity-item-heading"><h3><WorkTitle title={work.title} href={work.href} /></h3><span className="activity-badge">{phase}</span></div>
    {measured ? <>
      <progress aria-label={`Download progress for ${work.title}`} value={measured.done} max={measured.size} />
      <p>{bytes(measured.done)} of {bytes(measured.size)}{measured.speed !== null && measured.live ? ` / ${bytes(measured.speed)}/s` : ""}{!measured.live ? " / last recorded progress" : ""}</p>
    </> : downloads.length > 0 && <p>Waiting for download status.</p>}
    {downloads.length > 1 && <p>{downloads.length} downloads in this request.</p>}
    {downloads.filter(d => d.problem).map(download => <p key={download.key}>{download.problem!.message}</p>)}
    {work.job.nextWakeAt && <p>Next attempt: {date(work.job.nextWakeAt)}.</p>}
    <details><summary>Technical details</summary>
      <p>{work.media} / job #{work.job.id} / {work.job.type}. Attempts {work.job.attempts}/{work.job.maxAttempts}.</p>
      {downloads.map(download => <p key={download.key}>Telemetry: {download.telemetry}. Observed {date(download.observedAt)}.</p>)}
    </details>
  </article>;
}

export function OpsPage() {
  const trpc = useTRPC(), cache = useQueryClient();
  const [diagnosticsOpen, setDiagnosticsOpen] = useState(false);
  const [retryNotice, setRetryNotice] = useState<number | null>(null);
  const ops = useQuery(trpc.adminWeb.ops.queryOptions({ includeJobs: false }, { refetchInterval: 5000 }));
  const refresh = useMutation(trpc.adminWeb.refresh.mutationOptions({ onSuccess: () => cache.invalidateQueries({ queryKey: trpc.adminWeb.ops.queryKey() }) }));
  const work = ops.data ? activityItems(ops.data) : [];
  const attention = ops.data ? attentionItems(ops.data.incidents) : [];
  return <div className="activity-page">
    <div className="activity-heading"><h1>Activity</h1><button className="secondary" disabled={ops.isFetching} onClick={() => ops.refetch()}>Refresh</button></div>
    {retryNotice !== null && <p role="status">Retry requested for job #{retryNotice}. Its current progress appears below.</p>}
    {ops.isPending ? <p role="status">Loading operations...</p> : ops.isError ? <p role="alert">{ops.error.message}</p> : <>
      <section className="activity-section" aria-label="Needs attention">
        <h2>Needs attention <small>{attention.length}</small></h2>
        {!attention.length ? <p className="muted">Nothing needs your attention.</p> : <div className="activity-items">
          {attention.map(incident => <ActivityProblem key={incident.key} incident={incident} onRetried={setRetryNotice} />)}</div>}
      </section>
      <section className="activity-section" aria-label="Active work">
        <h2>In progress <small>{work.length}</small></h2>
        {!work.length ? <p className="muted">No downloads or imports in progress.</p> : <div className="activity-items">
          {work.map(item => <ActivityWork key={item.work.job.id} work={item.work} downloads={item.downloads} />)}</div>}
        {ops.data.summary.waiting > 0 && <p className="activity-waiting">{ops.data.summary.waiting} {ops.data.summary.waiting === 1 ? "request is" : "requests are"} waiting for a release or an unaired episode. <Link href="/library">View library</Link></p>}
      </section>
      {ops.data.diagnostics.warnings.length > 0 && <details className="activity-status"><summary>Some activity status is unavailable</summary>
        {ops.data.diagnostics.warnings.map((warning, index) => <p key={index}>{warning}</p>)}</details>}
      <section className="activity-section" id="activity-storage" aria-label="Storage and links">
        <h2>Storage</h2>
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
      <details className="activity-status"><summary>Torrent health</summary><div><Link href="/stewardship">Inspect private torrent registrations</Link></div></details>
      <p className="muted">Updated {date(ops.data.observedAt)}.</p>
    </>}
    <details className="activity-status" open={diagnosticsOpen} onToggle={(event) => setDiagnosticsOpen(event.currentTarget.open)}>
      <summary>Execution details</summary>
      <div className="actions"><button disabled={refresh.isPending} onClick={() => refresh.mutate({})}>{refresh.isPending ? "Queuing..." : "Queue library refresh"}</button></div>
      {refresh.error && <p role="alert">Library refresh was not queued: {refresh.error.message}</p>}
      {refresh.isSuccess && <p role="status">Library refresh {refresh.data.shared ? "already queued or running" : "queued"} as job #{refresh.data.jobId}.</p>}
      {diagnosticsOpen && <OpsDiagnostics />}
    </details>
  </div>;
}

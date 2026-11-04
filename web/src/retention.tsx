import { useState, type CSSProperties, type ReactNode } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
import { useTRPC } from "./rpc";

type Resource = inferRouterOutputs<AppRouter>["retention"]["audit"];
const factsStyle: CSSProperties = {
  display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))", gap: "1rem",
};
const displayState: Record<Resource["displayState"], string> = {
  present: "Present", missing: "Missing", unknown: "Inspection unknown",
  "intentional-absence": "Intentionally absent (verified missing)",
  "eviction-intent-only": "Eviction intent only; files still present",
};
const recoveryLabel: Record<Resource["desiredRecovery"]["action"], string> = {
  recover: "Recovery recommended", none: "No recovery recommended", inspect: "Inspect before recovery",
};
const known = (value: number | null) => value === null ? "Unknown" : value.toLocaleString();
function bytes(value: number | null): string {
  if (value === null) return "Unknown (not zero)";
  if (value < 1024) return `${value.toLocaleString()} B`;
  const units = ["KiB", "MiB", "GiB", "TiB", "PiB"];
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1024)) - 1, units.length - 1);
  return `${(value / 1024 ** (index + 1)).toLocaleString(undefined, { maximumFractionDigits: 1 })} ${units[index]} (${value.toLocaleString()} bytes)`;
}
function timestamp(value: string): string {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "Unknown" : date.toLocaleString();
}
function Fact({ label, children }: { label: string; children: ReactNode }) {
  return <div><dt className="muted">{label}</dt><dd style={{ margin: "0.35rem 0 0" }}>{children}</dd></div>;
}

function ResourceAudit({ resource, sourceUpdatedAt }: { resource: Resource; sourceUpdatedAt: number }) {
  const trpc = useTRPC();
  const audit = useQuery(trpc.retention.audit.queryOptions(resource.key, {
    enabled: false, retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false,
  }));
  const preview = useQuery(trpc.retention.preview.queryOptions({ targetId: resource.targetId }, {
    enabled: false, retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false,
  }));
  const evict = useMutation(trpc.retention.evict.mutationOptions({ onSuccess: () => { void audit.refetch(); void preview.refetch(); } }));
  const candidate = preview.data?.candidates.find((candidate) => candidate.targetId === resource.targetId);
  // A page refresh supersedes an older per-resource query still retained in the cache.
  const item = audit.data && audit.dataUpdatedAt >= sourceUpdatedAt ? audit.data : resource;
  return <details className="panel">
    <summary>
      <strong>{item.title}</strong> / {displayState[item.displayState]}
      <span className="muted" style={{ display: "block", marginTop: "0.35rem" }}>
        {item.key.namespace}:{item.key.id} / {item.audit.blockers.length} blockers / Reclaimable: {bytes(item.verifiedReclaimableBytes)}
      </span>
    </summary>
    <div className="actions">
      <button type="button" className="secondary" disabled={audit.isFetching} onClick={() => { void audit.refetch(); }}>
        {audit.isFetching ? "Inspecting resource..." : "Refresh resource audit"}
      </button>
      <span className="muted">Observed {timestamp(item.observedAt)}</span>
    </div>
    {audit.isError && <p role="alert">Resource refresh failed: {audit.error.message}. Showing the last successful observation.</p>}
    <dl style={factsStyle}>
      <Fact label="Desired presence">{item.storedIntent
        ? item.storedIntent.desiredPresence === "evicted" ? "Eviction intent recorded (not proof of removal)" : "Present"
        : "Untracked; no intent inferred"}</Fact>
      <Fact label="Actual presence (live)">{item.observation.presence}</Fact>
      <Fact label="Stored observation">{item.storedIntent?.actualPresence ?? "Not recorded"}</Fact>
      <Fact label="Intent revision">{item.storedIntent?.revision ?? "Not recorded"}</Fact>
      <Fact label="Active acquisitions">{known(item.activeAcquisitions)}{item.activeAcquisitions === null ? " (blocks deletion)" : ""}</Fact>
      <Fact label="Verified reclaimable storage">{bytes(item.verifiedReclaimableBytes)}</Fact>
      <Fact label="Audit-plan reclaimable bytes">{bytes(item.audit.reclaimableBytes)}</Fact>
    </dl>
    <p className="muted">Canonical movie objects only. Allocated blocks are not proof of reclaimable space; unknown bytes do not mean zero. Audit-plan bytes are zero whenever blocked.</p>
    {item.observation.reasons.length > 0 && <>
      <h3>Observation reasons</h3>
      <ul>{item.observation.reasons.map((reason, index) => <li key={index}>{reason}</li>)}</ul>
    </>}
    <h3>{recoveryLabel[item.desiredRecovery.action]}</h3>
    <p>{item.desiredRecovery.reason}</p>
    <p className="muted">Recommendation only; this page never starts downloads. Stored intent gate: {item.recoveryAllowedByIntent ? "desired present" : "blocked or untracked"}.</p>
    <h3>Watch history</h3>
    <p><strong>{item.watch.state === "unknown" ? "Unknown" : item.watch.state === "watched" ? "Watched" : "Unwatched"}</strong>. Coverage: {item.watch.coverage === "global-all-users" ? "Global, across users" : "Unknown; not all-user evidence"}.</p>
    <p>{item.watch.reason}</p>
    <h3>Seeding evidence</h3>
    <p><strong>{item.seeding.state === "verified-independent-seed-store" ? "Independent seed store verified" : "Last-copy safety unverified"}</strong>.</p>
    <p>{item.seeding.reason}</p>
    {!item.seeding.obligations.length && <p>Seed obligations and history are unknown, not zero.</p>}
    {item.seeding.obligations.map((obligation) => <details key={obligation.id}>
      <summary>Seed obligation <code>{obligation.id}</code></summary>
      <dl style={factsStyle}>
        <Fact label="Surviving seedable copies">{known(obligation.remainingSeedableCopies)}</Fact>
        <Fact label="Verified seconds seeded">{known(obligation.verifiedSecondsSeeded)}</Fact>
        <Fact label="Tracker-required seconds">{known(obligation.requiredSecondsSeeded)}</Fact>
        <Fact label="Other tracker requirements">Unknown</Fact>
      </dl>
    </details>)}
    <p className="muted">Last-seed-copy deletion requires at least 30 days of verified seeding and known tracker requirements. Time since added is not seed history. This audit does not remove seed-store files or registrations.</p>
    <h3>{item.audit.eligible ? "Evidence passes this scoped audit" : "Deletion audit blocked"}</h3>
    <p>{item.audit.summary}</p>
    {item.audit.blockers.length > 0 && <ul aria-label="Deletion blockers">
      {item.audit.blockers.map((blocker, index) => <li key={`${blocker.code}:${index}`}>
        <code>{blocker.code}</code>: {blocker.message}
      </li>)}
    </ul>}
    <p className="notice">Read-only result. Even passing evidence does not authorize deletion or change desired presence.</p>
    <h3>Eviction preview</h3>
    <p>Only canonical movie objects are removed. Torrent registrations and their independent seed-store files remain intact. The server rechecks all safety evidence before acting.</p>
    <button type="button" className="secondary" disabled={preview.isFetching || evict.isPending}
      onClick={() => { void preview.refetch(); }}>{preview.isFetching ? "Checking eligibility..." : "Preview eviction"}</button>
    {preview.isError && <p role="alert">Eviction preview failed: {preview.error.message}</p>}
    {preview.data && !candidate && <p>No selected canonical movie version can be evaluated for this target.</p>}
    {candidate && <>
      <p>{candidate.eligible ? "Eligible after current checks" : "Not eligible"} / Reclaimable: {bytes(candidate.reclaimableBytes)} / Plays: {known(candidate.playCount)}</p>
      {candidate.blockers.length > 0 && <ul aria-label="Eviction safety blockers">{candidate.blockers.map((blocker) => <li key={blocker}>{blocker}</li>)}</ul>}
      {candidate.eligible && candidate.expectedRevision !== null && <button type="button" disabled={evict.isPending} onClick={() => {
        if (window.confirm(`Remove Podible's canonical copy of ${item.title}? Seed-store data is retained. You can request it again later.`))
          evict.mutate({ targetId: item.targetId, expectedRevision: candidate.expectedRevision!, reason: "Administrator requested eviction after inspecting eligibility" });
      }}>{evict.isPending ? "Queueing eviction..." : "Evict canonical copy"}</button>}
    </>}
    {evict.isError && <p role="alert">Eviction was not queued: {evict.error.message}</p>}
    {evict.data && <p role="status">Eviction queued as job {evict.data.jobId}. Queueing is not confirmation that bytes were removed.</p>}
    <h3>Canonical file facts</h3>
    {!item.files.length && <p>No complete owned-file inventory is available. This is not proof of zero bytes or intentional absence.</p>}
    {item.files.map((file) => <details className="panel" key={file.id}>
      <summary><code>{file.relativePath || file.id}</code> / {file.presence}</summary>
      <dl style={factsStyle}>
        <Fact label="Ownership">{file.ownership}</Fact>
        <Fact label="References">{file.references}</Fact>
        <Fact label="Database references">{known(file.databaseReferences)}</Fact>
        <Fact label="Physical identity"><code>{file.physicalId ?? "Unknown"}</code></Fact>
        <Fact label="Inode links">{known(file.linkCount)}</Fact>
        <Fact label="Allocated blocks (bytes)">{bytes(file.allocatedBytes)}</Fact>
        <Fact label="Verified reclaimable bytes">{bytes(file.verifiedReclaimableBytes)}</Fact>
      </dl>
      {file.reasons.length > 0 && <ul>{file.reasons.map((reason, index) => <li key={index}>{reason}</li>)}</ul>}
    </details>)}
  </details>;
}

/** Route/auth navigation is parent-owned; both server queries independently require admin access. */
export function RetentionAudit() {
  const trpc = useTRPC();
  const [cursors, setCursors] = useState([0]);
  const afterTargetId = cursors.at(-1)!;
  const status = useQuery(trpc.retention.status.queryOptions({ afterTargetId, limit: 20 }, {
    retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false,
  }));
  const pressure = useQuery(trpc.retention.preview.queryOptions({}, {
    enabled: false, retry: false, refetchOnWindowFocus: false, refetchOnReconnect: false,
  }));
  return <>
    <h1>Retention audit</h1>
    <section className="panel" aria-label="Disk-pressure management">
      <h2>Disk-pressure management</h2>
      <p>The background worker checks every 5 minutes and reclaims eligible owned movie objects only when free space falls below the configured threshold. It protects recent requests, protected titles, active playback and acquisitions, shared files, and torrent seed data.</p>
      <p>Policy is in Settings JSON under <code>retention.minimumFreeBytes</code> and <code>retention.protectedMovieTargetIds</code>. The default free-space floor is 20 GiB. TV, books and foreign libraries are not automatically evicted.</p>
      <button type="button" disabled={pressure.isFetching} onClick={() => { void pressure.refetch(); }}>
        {pressure.isFetching ? "Checking disk..." : "Preview disk pressure"}
      </button>
      {pressure.isError && <p role="alert">Disk preview failed: {pressure.error.message}</p>}
      {pressure.data && <>
        <p>Available: {bytes(pressure.data.disk.availableBytes)} / {pressure.data.pressure === null ? "Pressure unknown" : pressure.data.pressure ? "Below the free-space floor" : "No disk pressure"}</p>
        <p className="muted">{pressure.data.disk.reason} Observed {timestamp(pressure.data.disk.observedAt)}. This preview does not delete anything.</p>
        {pressure.data.candidates.length > 0 && <ul>{pressure.data.candidates.map((candidate) => <li key={candidate.targetId}>{candidate.title}: {candidate.eligible ? "Eligible" : candidate.blockers.join(", ")} / {bytes(candidate.reclaimableBytes)}</li>)}</ul>}
      </>}
    </section>
    <section className="panel" aria-label="Read-only audit mode">
      <h2>Read-only resource observations</h2>
      <p>Loading or refreshing these audits does not delete files, start downloads, or change intent. Explicit eviction controls appear separately after an eligibility preview.</p>
      <p>Current scope: canonical movie objects. TV and book retention audits are not available yet. Watch history, seed history, and reclaimable storage stay unknown until verified.</p>
      <button type="button" disabled={status.isFetching} onClick={() => { void status.refetch(); }}>
        {status.isFetching ? "Inspecting page..." : "Refresh page audit"}
      </button>
      <p className="muted">Inspections run on page load or manual refresh, not on a polling timer. Observations are not persisted.</p>
    </section>
    {status.isPending && <p role="status">Loading retention audit...</p>}
    {status.isError && <p role="alert">Could not load the retention audit: {status.error.message}{status.data ? ". Showing the last successful page; refresh to verify current facts." : ""}</p>}
    {status.data && <section aria-label="Resource audits" aria-busy={status.isFetching}>
      <h2>Movie resources</h2>
      {!status.data.resources.length && <p>{afterTargetId ? "No further movie targets on this page." : "No movie targets to audit yet."}</p>}
      {status.data.resources.map((resource) => <ResourceAudit key={`${resource.key.namespace}:${resource.key.id}`}
        resource={resource} sourceUpdatedAt={status.dataUpdatedAt} />)}
      <nav className="actions" aria-label="Retention audit pages">
        <button type="button" className="secondary" disabled={cursors.length === 1 || status.isFetching}
          onClick={() => setCursors((previous) => previous.slice(0, -1))}>Previous page</button>
        <button type="button" className="secondary" disabled={status.data.nextAfterTargetId === null || status.isFetching}
          onClick={() => {
            const next = status.data?.nextAfterTargetId;
            if (next !== null && next !== undefined) setCursors((previous) => [...previous, next]);
          }}>Next page</button>
      </nav>
    </section>}
  </>;
}

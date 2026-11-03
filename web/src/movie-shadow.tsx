import { useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import type { inferRouterOutputs } from "@trpc/server";
import type { AppRouter } from "../../src/trpc";
type Case = inferRouterOutputs<AppRouter>["movieShadow"]["get"]["case"];
function Review({ item }: { item: Case }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const [verdict, setVerdict] = useState<
    NonNullable<Case["review"]>["verdict"]
  >(item.review?.verdict ?? "uncertain");
  const [notes, setNotes] = useState(item.review?.notes ?? "");
  const mutation = useMutation(
    trpc.movieShadow.review.mutationOptions({
      onSuccess: () =>
        cache.invalidateQueries({ queryKey: trpc.movieShadow.list.queryKey() }),
    }),
  );
  const retry = useMutation(
    trpc.movieShadow.retry.mutationOptions({
      onSuccess: () =>
        cache.invalidateQueries({ queryKey: trpc.movieShadow.list.queryKey() }),
    }),
  );
  return (
    <div className="panel">
      <h3>Independent evidence</h3>
      <p>{item.input.observation.releaseTitle}</p>
      <p>
        Source: {item.input.source}. Files: {item.input.files.length}. Model:{" "}
        {item.result?.model ?? "Not evaluated yet"}.
      </p>
      {item.input.warnings.map((warning, i) => (
        <p key={i}>{warning}</p>
      ))}
      {item.error && <p role="alert">{item.error}</p>}
      {item.result && (
        <>
          <p>
            <strong>Podible: {item.result.decision}</strong>.{" "}
            {item.result.reason}
          </p>
          {item.result.quality?.reasons.map((reason) => (
            <p key={reason}>{reason}</p>
          ))}
          <p>Selected file: {item.result.selectedFile ?? "None"}</p>
        </>
      )}
      <p>
        <strong>Radarr: {item.input.observation.radarr.outcome}</strong>
      </p>
      {item.input.observation.radarr.messages.map((message, i) => (
        <p key={i}>{message}</p>
      ))}
      <details>
        <summary>Snapshot and probe evidence</summary>
        <pre className="shadow-json">
          {JSON.stringify({ input: item.input, result: item.result }, null, 2)}
        </pre>
      </details>
      {["failed", "cancelled"].includes(item.status) && (
        <button
          disabled={retry.isPending}
          onClick={() => retry.mutate({ caseId: item.id })}
        >
          Retry evaluation
        </button>
      )}
      {retry.error && <p role="alert">{retry.error.message}</p>}
      <form
        onSubmit={(event) => {
          event.preventDefault();
          mutation.mutate({ caseId: item.id, verdict, notes });
        }}
      >
        <label>
          Human verdict
          <select
            value={verdict}
            onChange={(event) =>
              setVerdict(event.target.value as typeof verdict)
            }
          >
            {[
              "uncertain",
              "podible-correct",
              "radarr-correct",
              "both-correct",
              "both-wrong",
            ].map((value) => (
              <option key={value}>{value}</option>
            ))}
          </select>
        </label>
        <label>
          Review notes
          <textarea
            value={notes}
            maxLength={4000}
            onChange={(event) => setNotes(event.target.value)}
          />
        </label>
        <button disabled={mutation.isPending}>Save review</button>
        {mutation.error && <p role="alert">{mutation.error.message}</p>}
        {mutation.isSuccess && <p role="status">Review saved.</p>}
      </form>
    </div>
  );
}
export function MovieShadow() {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const [cursor, setCursor] = useState<number>();
  const status = useQuery(
    trpc.movieShadow.status.queryOptions({}, { refetchInterval: 5000 }),
  );
  const cases = useQuery(
    trpc.movieShadow.list.queryOptions(
      { limit: 25, cursor },
      { refetchInterval: 5000 },
    ),
  );
  const sync = useMutation(
    trpc.movieShadow.sync.mutationOptions({
      onSuccess: () => cache.invalidateQueries(),
    }),
  );
  return (
    <>
      <span className="eyebrow">ADMIN / READ-ONLY OBSERVER</span>
      <h1>Movie shadow audit</h1>
      <p>
        Radarr stays in control. Podible inspects existing files and records
        independent decisions. No downloads, imports, links, or Plex changes are
        performed.
      </p>
      {status.isError ? (
        <p role="alert">{status.error.message}</p>
      ) : (
        status.data && (
          <section className="panel">
            <h2>Configuration</h2>
            <p>
              Shadow: {status.data.enabled ? "Enabled" : "Disabled"}. Radarr
              key: {status.data.keyConfigured ? "Configured" : "Missing"}.
            </p>
            <p>
              Poll every {status.data.pollIntervalMinutes} minutes;{" "}
              {status.data.lookbackDays}-day history; at most{" "}
              {status.data.maxCasesPerSync} new cases per poll.
            </p>
            <p>
              Last sync: {status.data.lastSync?.status ?? "Not run"}.{" "}
              {status.data.lastSync?.error}
            </p>
            <details>
              <summary>Settings (secrets omitted)</summary>
              <pre className="shadow-json">
                {JSON.stringify(status.data, null, 2)}
              </pre>
            </details>
            <p>
              Edit <code>movies.shadow</code> in{" "}
              <a href="/admin/settings">admin settings JSON</a>.
            </p>
            <button
              disabled={!status.data.enabled || sync.isPending}
              onClick={() => sync.mutate({})}
            >
              {sync.isPending ? "Queuing..." : "Observe Radarr now"}
            </button>
            {sync.isSuccess && <p role="status">Observation queued.</p>}
            {sync.error && <p role="alert">{sync.error.message}</p>}
          </section>
        )
      )}
      {cases.isPending ? (
        <p role="status">Loading audit cases...</p>
      ) : cases.isError ? (
        <p role="alert">{cases.error.message}</p>
      ) : (
        <>
          <h2>Import cases</h2>
          {!cases.data.items.length && <p>No cases captured yet.</p>}
          {cases.data.items.map((item) => (
            <details className="panel" key={item.id}>
              <summary>
                #{item.id} {item.input.observation.movie.title} (
                {item.input.observation.movie.year}) ·{" "}
                {item.result?.decision ?? item.status} · Radarr{" "}
                {item.input.observation.radarr.outcome}
              </summary>
              <Review item={item} />
            </details>
          ))}
          {cases.data.nextCursor && (
            <button onClick={() => setCursor(cases.data.nextCursor!)}>
              Older cases
            </button>
          )}
          {cursor && (
            <button onClick={() => setCursor(undefined)}>Newest cases</button>
          )}
        </>
      )}
    </>
  );
}

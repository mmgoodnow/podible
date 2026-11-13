import { useEffect, useState } from "react";
import { rpc } from "./rpc";
import { metricBytes } from "./transfer-metrics-model";
import { transferIntervalMs, type AcquisitionTransferSample as Transfer, type AcquisitionTransferScope as Scope } from "../../src/runtime/acquisition-transfer-contracts";

const labels = ["Requested", "Search", "Download", "Verify", "Plex"];
function useTransfer({ media, id }: Scope, enabled: boolean) {
  const [state, setState] = useState<{ media: string; id: number; sample: Transfer }>();
  useEffect(() => {
    if (!enabled) return;
    let stop: (() => void) | undefined;
    const update = () => {
      stop?.(); stop = undefined;
      setState(undefined);
      if (document.hidden) return;
      const subscription = rpc.acquisition.transfer.subscribe({ media, id }, {
        onData: ({ sample }) => setState(sample ? { media, id, sample } : undefined),
        onError: () => setState(undefined),
      });
      stop = () => subscription.unsubscribe();
    };
    update();
    document.addEventListener("visibilitychange", update);
    return () => { stop?.(); document.removeEventListener("visibilitychange", update); };
  }, [enabled, media, id]);
  return enabled && state?.media === media && state.id === id ? state.sample : undefined;
}

function DownloadStage({ scope }: { scope: Scope }) {
  const sample = useTransfer(scope, true);
  if (!sample) return <><span className="stage-marker" aria-hidden="true" />Download</>;
  const percent = Math.min(99.9, 100 * sample.bytesDone / sample.sizeBytes);
  const end = Math.min(99.9, 100 * (sample.bytesDone + transferIntervalMs / 1000 * sample.bytesPerSecond) / sample.sizeBytes);
  const eta = sample.etaSeconds;
  const remaining = eta === null ? null : eta >= 3600
    ? `${Math.floor(eta / 3600)}h ${Math.floor((eta % 3600) / 60)}m`
    : eta >= 60 ? `${Math.ceil(eta / 60)} min` : `${eta}s`;
  const stats = [`${Math.floor(percent)}%`, metricBytes(sample.sizeBytes),
    ...(sample.bytesPerSecond > 0 ? [`${metricBytes(sample.bytesPerSecond)}/s`] : []),
    ...(remaining ? [`ETA ${remaining}`] : [])];
  return <>
    <div className="stage-download-meter" role="progressbar" aria-label="Download progress" aria-valuemin={0} aria-valuemax={100}
      aria-valuenow={Math.floor(percent)}>
      <span key={`${percent}:${end}`} style={{ "--transfer-start": `${percent}%`, "--transfer-end": `${end}%`,
        "--transfer-interval": `${transferIntervalMs}ms` } as React.CSSProperties} />
    </div>
    Download <small className="stage-download-stats">({stats.join(" · ")})</small>
  </>;
}

export function StageTracker({ reached, details = [], transfer }: {
  reached: readonly boolean[];
  details?: readonly (string | undefined)[];
  transfer?: Scope;
}) {
  return <ol className="stage-tracker" aria-label="Acquisition stages">
    {labels.map((label, i) => <li key={label} className={reached[i] ? "reached" : undefined}>
      {i === 2 && transfer ? <DownloadStage scope={transfer} /> : <><span className="stage-marker" aria-hidden="true" />{label}</>}
      {details[i] && <small>{details[i]}</small>}
    </li>)}
  </ol>;
}

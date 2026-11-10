const labels = ["Requested", "Search", "Download", "Verify", "Plex"];

export function StageTracker({ reached, details = [] }: {
  reached: readonly boolean[];
  details?: readonly (string | undefined)[];
}) {
  return <ol className="stage-tracker" aria-label="Acquisition stages">
    {labels.map((label, i) => <li key={label} className={reached[i] ? "reached" : undefined}>
      <span aria-hidden="true" />
      {label}
      {details[i] && <small>{details[i]}</small>}
    </li>)}
  </ol>;
}

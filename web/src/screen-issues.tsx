import { useId, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { MAX_SCREEN_ISSUE_DESCRIPTION, type ScreenIssueCategory, type ScreenIssueFinding,
  type ScreenIssueTitle } from "../../src/issues/contracts";
import { useTRPC } from "./rpc";
import "./screen-issues.css";

const categories: Record<ScreenIssueCategory, string> = {
  playback: "Playback", "audio-subtitles": "Audio or subtitles", "wrong-content": "Wrong content",
  missing: "Missing title or episode", other: "Something else",
};
const findings: Record<ScreenIssueFinding, string> = {
  "content-loss-recorded": "Podible previously recorded a file loss for matching content on your request.",
  "publication-recorded": "Podible has a publication record for this content.",
  "no-publication-recorded": "Podible has no active publication record for this content.",
};
type EpisodeGroup = { seasonNumber: number; episodes: { id: number; number: number; title: string }[] };

export function ScreenIssuePanel({ title, episodeGroups = [] }: { title: ScreenIssueTitle; episodeGroups?: EpisodeGroup[] }) {
  const trpc = useTRPC(), cache = useQueryClient(), panelId = useId();
  const [open, setOpen] = useState(false);
  const [description, setDescription] = useState("");
  const [category, setCategory] = useState<ScreenIssueCategory>("playback");
  const [episode, setEpisode] = useState("");
  const [beforeId, setBeforeId] = useState<number>();
  const history = useQuery(trpc.issues.listMine.queryOptions({ title, beforeId }, { enabled: open }));
  const submit = useMutation(trpc.issues.create.mutationOptions({ onSuccess: () => {
    setDescription("");
    setBeforeId(undefined);
    void cache.invalidateQueries({ queryKey: trpc.issues.listMine.pathKey() });
  } }));

  return <section className="media-section screen-issues" aria-label="Media issue reports">
    <button type="button" className="secondary" aria-expanded={open} aria-controls={panelId}
      onClick={() => setOpen(value => !value)}>{open ? "Hide issue reports" : "Report a problem"}</button>
    <div id={panelId} hidden={!open}>
      {open && <div className="panel">
        <h2>Report a problem</h2>
        <p>Administrators can read your report. Filing it records a local check but does not check Plex playback or apply a fix.</p>
        <form onSubmit={event => {
          event.preventDefault();
          if (!description.trim() || submit.isPending) return;
          const subject = title.media === "tv" && episode ? { ...title, episodeId: Number(episode) } : title;
          submit.mutate({ subject, category, description });
        }}>
          <fieldset disabled={submit.isPending}>
            <label>Issue type
              <select value={category} onChange={event => setCategory(event.target.value as ScreenIssueCategory)}>
                {Object.entries(categories).map(([value, label]) => <option key={value} value={value}>{label}</option>)}
              </select>
            </label>
            {title.media === "tv" && episodeGroups.some(group => group.episodes.length > 0) && <label>Episode (optional)
              <select value={episode} onChange={event => setEpisode(event.target.value)}>
                <option value="">Whole show</option>
                {episodeGroups.map(group => <optgroup key={group.seasonNumber}
                  label={group.seasonNumber === 0 ? "Specials" : `Season ${group.seasonNumber}`}>
                  {group.episodes.map(item => <option key={item.id} value={item.id}>
                    {`${item.number}. ${item.title}`}
                  </option>)}
                </optgroup>)}
              </select>
            </label>}
            <label>Description
              <textarea required rows={4} maxLength={MAX_SCREEN_ISSUE_DESCRIPTION} value={description}
                onChange={event => setDescription(event.target.value)} />
            </label>
            <small>{description.length} / {MAX_SCREEN_ISSUE_DESCRIPTION} characters. Do not include passwords or access tokens.</small>
            <button type="submit" disabled={!description.trim()}>{submit.isPending ? "Sending..." : "Send report"}</button>
          </fieldset>
        </form>
        {submit.isError && <p role="alert" className="notice error">{submit.error.data?.code === "NOT_FOUND"
          ? "Podible could not verify that title or episode for reporting. For TV, try reporting the whole show."
          : "Could not save the report. Your description is still here."}</p>}
        {submit.isSuccess && <p role="status">Report #{submit.data.id} saved. No repair was attempted.</p>}
        <h3>Your reports for this title</h3>
        {history.isPending ? <p role="status">Loading reports...</p> : history.isError
          ? <p>Could not load reports. <button type="button" className="secondary" onClick={() => void history.refetch()}>Try again</button></p>
          : history.data.reports.length ? <ul className="screen-issue-history">{history.data.reports.map(report => <li key={report.id}>
            <strong>#{report.id} {categories[report.category]}{report.episodeTitle ? `: ${report.episodeTitle}` : ""}</strong>
            <span>Open · <time dateTime={report.createdAt}>{new Date(report.createdAt).toLocaleString()}</time></span>
            <p className="screen-issue-description">{report.description}</p>
            <small>Initial check: {findings[report.triage.finding]} No repair was attempted.</small>
          </li>)}</ul> : <p>No reports filed.</p>}
        {beforeId !== undefined && <button type="button" className="secondary" onClick={() => setBeforeId(undefined)}>Latest reports</button>}
        {history.data?.nextBeforeId != null && <button type="button" className="secondary"
          onClick={() => setBeforeId(history.data?.nextBeforeId ?? undefined)}>Older reports</button>}
      </div>}
    </div>
  </section>;
}

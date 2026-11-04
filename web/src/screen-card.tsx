import { useState } from "react";
import { Link } from "./router";

export type ScreenBadge = { label: string; tone: "available" | "active" | "waiting" | "paused" | "catalog" };
export function ScreenCard({ title, year, posterUrl, href, media, badge, detail, source, progress }: {
  title: string; year?: number | null; posterUrl: string | null; href: string; media: "movie" | "tv";
  badge: ScreenBadge; detail?: string; source?: string;
  progress?: { value: number; max: number; label: string };
}) {
  const [failedPoster, setFailedPoster] = useState<string | null>(null);
  const poster = posterUrl?.trim() || null;
  const showPoster = poster !== null && failedPoster !== poster;
  const palette = [...title].reduce((sum, letter) => sum + letter.codePointAt(0)!, 0) % 3;
  const hasProgress = progress && Number.isFinite(progress.value) && Number.isFinite(progress.max) && progress.max > 0;
  const kind = media === "movie" ? "Movie" : "TV series";
  return <Link href={href} className={`screen-card screen-card--${media}`}>
    <div className="screen-poster">
      {showPoster ? <img src={poster} alt={`${title} ${kind.toLowerCase()} poster`} loading="lazy" decoding="async"
        width="300" height="450" onError={() => setFailedPoster(poster)} /> :
        <div className="screen-poster__fallback" data-palette={palette} role="img" aria-label={`No poster available for ${title}`}>
          <span className="screen-poster__format" aria-hidden="true">{media === "movie" ? "Film" : "Television"}</span>
          <span className="screen-poster__art" aria-hidden="true" />
          <span className="screen-poster__title" aria-hidden="true">{title}</span>
          <span className="screen-poster__year" aria-hidden="true">{year ?? kind}</span>
        </div>}
      <span className={`screen-badge screen-badge--${badge.tone}`}><span aria-hidden="true" />{badge.label}</span>
    </div>
    <div className="screen-card__caption">
      <h3 title={title}>{title}</h3>
      <p className="screen-card__meta">{year != null && <span>{year}</span>}{source && <span>{source}</span>}</p>
      {hasProgress && <progress className="screen-card__progress" max={progress.max} value={Math.min(progress.max, Math.max(0, progress.value))}
        aria-label={progress.label} />}
      {detail && <p className="screen-card__detail" title={detail}>{detail}</p>}
    </div>
  </Link>;
}

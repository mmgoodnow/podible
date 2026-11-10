import { useEffect, useId, useRef, useState, type ReactNode } from "react";
import { Link } from "./router";
import "./content-rail.css";

type RailLink = { href: string; label: string };
export function ContentRail({ title, children, more, variant, loading = false }: {
  title: string; children: ReactNode; more?: RailLink | RailLink[]; variant?: "requests"; loading?: boolean;
}) {
  const id = useId(), track = useRef<HTMLDivElement>(null);
  const [overflow, setOverflow] = useState(false);
  useEffect(() => {
    const el = track.current; if (!el) return;
    const update = () => setOverflow(el.scrollWidth > el.clientWidth + 1);
    update();
    const observer = new ResizeObserver(update); observer.observe(el);
    return () => observer.disconnect();
  }, [children]);
  function scroll(direction: number) {
    const el = track.current;
    if (el) el.scrollBy({ left: direction * Math.max(180, el.clientWidth * .8), behavior: "instant" });
  }
  return <section className={variant ? `content-rail content-rail--${variant}` : "content-rail"} aria-labelledby={id} aria-busy={loading || undefined}>
    <div className="content-rail__heading"><h2 id={id}>{title}</h2>
      <div className="content-rail__actions">{(more ? Array.isArray(more) ? more : [more] : []).map(link => <Link key={link.href} href={link.href}>{link.label}</Link>)}
        {!loading && overflow && <><button type="button" className="secondary" aria-label={`Scroll ${title} left`} onClick={() => scroll(-1)}><span aria-hidden="true">&#8592;</span></button>
        <button type="button" className="secondary" aria-label={`Scroll ${title} right`} onClick={() => scroll(1)}><span aria-hidden="true">&#8594;</span></button></>}
      </div>
    </div>
    <div className="content-rail__track" ref={track} tabIndex={loading ? undefined : 0} role="group" aria-label={`${title} cards`} onKeyDown={event => {
      if (event.target !== event.currentTarget) return;
      if (event.key === "ArrowLeft" || event.key === "ArrowRight") { event.preventDefault(); scroll(event.key === "ArrowLeft" ? -1 : 1); }
      if (event.key === "Home" || event.key === "End") { event.preventDefault(); event.currentTarget.scrollLeft = event.key === "Home" ? 0 : event.currentTarget.scrollWidth; }
    }}>{children}</div>
  </section>;
}

export function RailBook({ book }: { book: { id: number; title: string; author: string; posterUrl: string | null } }) {
  const [failed, setFailed] = useState(false);
  return <Link className="rail-book" href={`/book/${book.id}`}>
    <div className="rail-book__cover">{book.posterUrl && !failed ? <img src={book.posterUrl} width="300" height="450" loading="lazy" alt={`${book.title} book cover`} onError={() => setFailed(true)} />
      : <div role="img" aria-label={`No cover available for ${book.title}`}><span>Book</span><strong>{book.title}</strong><span>{book.author}</span></div>}</div>
    <h3 title={book.title}>{book.title}</h3><p>{book.author}</p>
  </Link>;
}

import { useEffect, useRef, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useTRPC } from "./rpc";
import { Link, navigate, useLocation } from "./router";
import { bytes, Failure, seriesHref } from "./books";
import { BookAdmin } from "./book-admin";

function clock(seconds: number) {
  return `${Math.floor(seconds / 3600)}:${String(Math.floor(seconds / 60) % 60).padStart(2, "0")}:${String(Math.floor(seconds) % 60).padStart(2, "0")}`;
}
const transcriptLabels = {
  current: "Available",
  stale: "Ready to generate or refresh",
  pending: "Queued",
  running: "Generating...",
  failed: "Generation failed",
  missing_audio: "No audio edition yet",
  missing_config: "API key not configured",
};

function Artwork({ bookId }: { bookId: number }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const [show, setShow] = useState(false);
  const covers = useQuery(
    trpc.openlibrary.covers.queryOptions(
      { bookId, limit: 24 },
      { enabled: show },
    ),
  );
  const apply = useMutation(
    trpc.openlibrary.setCover.mutationOptions({
      onSuccess: async (result) => {
        cache.setQueryData(trpc.library.get.queryKey({ bookId }), (current) => current ? {
          ...current, book: { ...current.book, coverUrl: result.book.coverUrl },
        } : current);
        await cache.invalidateQueries({ queryKey: trpc.library.pathKey() });
      },
    }),
  );
  return (
    <section className="panel artwork-panel">
      <div className="progress-heading">
        <div>
          <h2>Cover artwork</h2>
        </div>
        <button className="secondary" onClick={() => setShow(!show)}>
          {show ? "Hide covers" : "Choose artwork"}
        </button>
      </div>
      {show && (
        <>
          {covers.isPending ? (
            <p role="status">Finding alternate covers...</p>
          ) : covers.isError ? (
            <Failure error={covers.error} retry={() => void covers.refetch()} />
          ) : covers.data.results.length ? (
            <div className="cover-choices">
              {covers.data.results.map((cover) => (
                <button
                  className="cover-choice secondary"
                  key={cover.coverId}
                  disabled={apply.isPending}
                  onClick={() =>
                    apply.mutate({ bookId, coverId: cover.coverId })
                  }
                >
                  <img
                    src={cover.coverUrl}
                    alt={`Cover ${cover.coverId}`}
                    loading="lazy"
                  />
                  <span>Use cover {cover.coverId}</span>
                  <small>
                    {[cover.publishDate, cover.publisher, cover.language]
                      .filter(Boolean)
                      .join(" · ")}
                  </small>
                </button>
              ))}
            </div>
          ) : (
            <p>No alternate covers found.</p>
          )}
        </>
      )}
      {apply.isError && <Failure error={apply.error} />}
      {apply.isSuccess && <p role="status">Cover updated.</p>}
    </section>
  );
}

function AudioContents({
  bookId,
  audio,
  player,
}: {
  bookId: number;
  audio: NonNullable<import("./books").Book["playback"]["audio"]>;
  player: React.RefObject<HTMLAudioElement | null>;
}) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const params = { bookId, manifestationId: audio.manifestationId };
  const status = useQuery(
    trpc.library.transcriptStatus.queryOptions(params, {
      refetchInterval: 4_000,
    }),
  );
  const chapters = useQuery(
    trpc.book.chapters.queryOptions(params, {
      refetchInterval:
        status.data?.status === "pending" || status.data?.status === "running"
          ? 6_000
          : false,
    }),
  );
  useEffect(() => {
    if (status.data?.status !== "current") return;
    void cache.invalidateQueries({
      queryKey: trpc.book.chapters.queryKey({
        bookId,
        manifestationId: audio.manifestationId,
      }),
    });
    void cache.invalidateQueries({
      queryKey: trpc.library.get.queryKey({ bookId }),
    });
  }, [status.data?.status, bookId, audio.manifestationId, cache, trpc]);
  const generate = useMutation(
    trpc.library.requestTranscription.mutationOptions({
      onSuccess: (result) => {
        cache.setQueryData(
          trpc.library.transcriptStatus.queryKey(params),
          result,
        );
        void cache.invalidateQueries({
          queryKey: trpc.library.get.queryKey({ bookId }),
        });
        void cache.invalidateQueries({
          queryKey: trpc.book.chapters.queryKey(params),
        });
      },
    }),
  );
  const pending =
    status.data?.status === "pending" || status.data?.status === "running";
  return (
    <section className="panel contents-panel">
      <h2>Chapters & transcript</h2>
      <p className="muted">
        For the selected audio edition. Generation also curates chapter
        boundaries.
      </p>
      {status.isPending ? (
        <p role="status">Checking transcript...</p>
      ) : status.isError ? (
        <Failure error={status.error} retry={() => void status.refetch()} />
      ) : (
        <>
          <p role="status">
            <strong>{transcriptLabels[status.data.status]}</strong>
          </p>
          {status.data.status === "missing_config" && (
            <p>
              Transcription needs a configured API key. Ask an administrator to
              check Settings.
            </p>
          )}
          {status.data.status === "failed" && (
            <p>
              The previous generation failed. Retry here, or ask an
              administrator to inspect the job.
            </p>
          )}
          <div className="actions">
            <button
              disabled={
                generate.isPending ||
                pending ||
                status.data.status === "missing_config" ||
                status.data.status === "missing_audio"
              }
              onClick={() => generate.mutate({ ...params, force: false })}
            >
              {pending
                ? "Generation in progress"
                : "Generate chapters & transcript"}
            </button>
            <button
              className="secondary"
              disabled={
                generate.isPending ||
                pending ||
                status.data.status === "missing_config" ||
                status.data.status === "missing_audio"
              }
              onClick={() => generate.mutate({ ...params, force: true })}
            >
              Force regeneration
            </button>
          </div>
        </>
      )}
      {generate.isError && <Failure error={generate.error} />}
      <div className="actions resource-links">
        <a href={audio.chaptersUrl}>Chapters JSON</a>
        {audio.transcriptUrl && (
          <a href={audio.transcriptUrl}>Transcript JSON</a>
        )}
      </div>
      {chapters.isPending ? (
        <p role="status">Loading chapters...</p>
      ) : chapters.isError ? (
        <Failure error={chapters.error} retry={() => void chapters.refetch()} />
      ) : chapters.data.chapters.length ? (
        <ol className="chapter-list">
          {chapters.data.chapters.map((chapter, index) => (
            <li key={`${index}:${chapter.startTime}`}>
              <button
                className="chapter-seek"
                onClick={() => {
                  if (player.current) {
                    player.current.currentTime = chapter.startTime;
                    void player.current.play().catch(() => {});
                  }
                }}
              >
                <span>{chapter.title}</span>
                <time>{clock(chapter.startTime)}</time>
              </button>
            </li>
          ))}
        </ol>
      ) : (
        <p className="muted">
          No chapter data yet. Generate it above when transcription is
          configured.
        </p>
      )}
    </section>
  );
}

const requestLabels = { dramatized: "Dramatized / GraphicAudio", narrated: "Narrated audiobook" };
const requestStatus = {
  idle: null,
  queued: "Request queued.",
  searching: "Looking for this performance...",
  importing: "Downloading or importing this edition...",
  ready: "Ready to listen.",
  no_match: "No confidently matching complete edition was found.",
  failed: "This request could not finish. Try again or ask an administrator to check acquisition.",
};

function AudioEditionRequests({ bookId, refresh }: { bookId: number; refresh: () => void }) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const options = trpc.book.audioEditionRequests.queryOptions({ bookId }, { refetchInterval: 5_000 });
  const requests = useQuery(options);
  const readyIds = requests.data?.requests.flatMap((entry) => entry.manifestationId ?? []).join(",");
  useEffect(() => {
    if (!readyIds) return;
    void cache.invalidateQueries({ queryKey: trpc.library.get.queryKey({ bookId }) });
    void cache.invalidateQueries({ queryKey: trpc.book.editions.queryKey({ bookId }) });
  }, [readyIds, bookId, cache, trpc]);
  const request = useMutation(trpc.book.requestAudioEdition.mutationOptions({
    onSuccess: (result) => {
      cache.setQueryData(options.queryKey, (previous) => previous && ({
        ...previous, requests: previous.requests.map((entry) => entry.edition === result.edition ? result : entry),
      }));
      refresh();
    },
  }));
  return (
    <section className="panel">
      <h2>Request an audiobook edition</h2>
      <p className="muted">Find a specific performance. It won&apos;t replace the audio you already have.</p>
      {requests.isPending && <p role="status">Checking edition requests...</p>}
      {requests.isError && <Failure error={requests.error} retry={() => void requests.refetch()} />}
      {requests.data && !requests.data.enabled && <p className="muted">Targeted search is not configured.</p>}
      {requests.data?.requests.map(({ edition, status, manifestationId }) => (
        <div key={edition}>
          <p><strong>{requestLabels[edition]}</strong><span role="status">{requestStatus[status] && `: ${requestStatus[status]}`}</span></p>
          {manifestationId ? (
            <Link className="button secondary" href={`/book/${bookId}?edition=${manifestationId}`}>
              Open {requestLabels[edition]}
            </Link>
          ) : requests.data.enabled && (
            <button
              className="secondary"
              disabled={request.isPending || status === "queued" || status === "searching" || status === "importing"}
              onClick={() => request.mutate({ bookId, edition })}
            >
              {status === "failed" || status === "no_match" ? "Retry" : "Request"} {requestLabels[edition]}
            </button>
          )}
        </div>
      ))}
      {request.isError && <Failure error={request.error} />}
    </section>
  );
}

export function BookDetail({
  bookId,
  isAdmin,
}: {
  bookId: number;
  isAdmin: boolean;
}) {
  const trpc = useTRPC();
  const cache = useQueryClient();
  const route = useLocation();
  const params = new URLSearchParams(route.split("?")[1]);
  const requested = Number(params.get("edition"));
  const [ebookId, setEbookId] = useState<number | null>(null);
  const [confirmReport, setConfirmReport] = useState<{
    mediaType: "audio" | "ebook";
    releaseId: number;
    manifestationId?: number;
  } | null>(null);
  const [confirmDelete, setConfirmDelete] = useState(false);
  const player = useRef<HTMLAudioElement>(null);
  const detail = useQuery(
    trpc.library.get.queryOptions({ bookId }, { refetchInterval: 6_000 }),
  );
  const editions = useQuery(
    trpc.book.editions.queryOptions({ bookId }, { refetchInterval: 6_000 }),
  );
  const activity = useQuery(
    trpc.book.activity.queryOptions({ bookId }, { refetchInterval: 5_000 }),
  );
  const refresh = () => {
    void cache.invalidateQueries({ queryKey: trpc.library.pathKey() });
    void cache.invalidateQueries({ queryKey: trpc.book.pathKey() });
  };
  const acquire = useMutation(
    trpc.library.acquire.mutationOptions({ onSuccess: refresh }),
  );
  const report = useMutation(
    trpc.library.reportImportIssue.mutationOptions({
      onSuccess: () => {
        setConfirmReport(null);
        refresh();
      },
    }),
  );
  const remove = useMutation(
    trpc.library.delete.mutationOptions({
      onSuccess: () => {
        refresh();
        navigate("/library");
      },
    }),
  );
  if (detail.isPending) return <p role="status">Opening your book...</p>;
  if (detail.isError)
    return (
      <>
        <Link className="back" href="/library">
          Back to library
        </Link>
        <Failure error={detail.error} retry={() => void detail.refetch()} />
      </>
    );
  const { book, releases } = detail.data;
  const audio =
    book.playback.audioOptions.find(
      (option) => option.manifestationId === requested,
    ) ?? book.playback.audio;
  const ebook =
    editions.data?.ebooks.find((option) => option.assetId === ebookId) ??
    editions.data?.ebooks.find(
      (option) => option.assetId === book.playback.ebook?.assetId,
    ) ??
    editions.data?.ebooks[0];
  const audioReleaseIds =
    editions.data?.audio.find(
      (option) => option.manifestationId === audio?.manifestationId,
    )?.releaseIds ?? [];
  const latest = activity.data?.jobs[0];
  const acquisitionBusy =
    acquire.isPending ||
    activity.isPending ||
    activity.isError ||
    activity.data.activeAcquisition;
  return (
    <>
      <Link className="back" href="/library">
        Back to library
      </Link>
      <section className="book-hero">
        <div className="book-portrait">
          <div className="jacket">
            {book.coverUrl ? (
              <img src={book.coverUrl} alt={`Cover of ${book.title}`} />
            ) : (
              <span>{book.title}</span>
            )}
          </div>
        </div>
        <div className="book-story">
          <h1>{book.title}</h1>
          <Link
            className="book-author"
            href={`/author?${new URLSearchParams({ name: book.author })}`}
          >
            {book.author}
          </Link>
          <div className="series-links">
            {book.series.map((series) => (
              <Link
                className="series-link"
                key={`${series.key}:${series.name}`}
                href={seriesHref(series)}
              >
                {series.name}
                {series.position ? ` · Book ${series.position}` : ""}
              </Link>
            ))}
          </div>
          <div className="book-facts">
            <span>
              Audio ·{" "}
              {book.playback.audio
                ? "Available"
                : book.audioStatus === "wanted" && latest?.status === "failed"
                  ? "Acquisition stopped"
                  : book.audioStatus}
            </span>
            <span>
              Ebook ·{" "}
              {book.playback.ebook
                ? "Available"
                : book.ebookStatus === "wanted" && latest?.status === "failed"
                  ? "Acquisition stopped"
                  : book.ebookStatus}
            </span>
            {book.publishedAt && <span>{book.publishedAt}</span>}
            {book.language && <span>{book.language}</span>}
          </div>
          {book.description && <p className="book-description">{book.description}</p>}
          {book.addedByUser && (
            <p className="muted">
              Added by{" "}
              {book.addedByUser.displayName || book.addedByUser.username}
            </p>
          )}
        </div>
      </section>
      <div className="book-columns">
        <div>
          <section className="panel listening-panel">
            <h2>Listen or download</h2>
            {book.playback.audioOptions.length > 0 && (
              <label className="field">
                Audio edition
                <select
                  value={audio?.manifestationId}
                  onChange={(e) => {
                    setConfirmReport(null);
                    const next = new URLSearchParams(params);
                    next.set("edition", e.target.value);
                    navigate(`${location.pathname}?${next}`);
                  }}
                >
                  {book.playback.audioOptions.map((option, index) => (
                    <option
                      key={option.manifestationId}
                      value={option.manifestationId}
                    >
                      {option.label || `Audio edition ${index + 1}`}
                      {option.editionNote ? ` · ${option.editionNote}` : ""}
                    </option>
                  ))}
                </select>
              </label>
            )}
            {audio ? (
              <>
                <p className="muted">
                  {audio.durationMs
                    ? `${Math.round(audio.durationMs / 60_000)} minutes · `
                    : ""}
                  {bytes(audio.sizeBytes)}
                </p>
                <audio
                  key={audio.streamUrl}
                  ref={player}
                  controls
                  preload="none"
                  src={audio.streamUrl}
                  aria-label="Book audio player"
                />
                <div className="actions resource-links">
                  <a href={audio.streamUrl}>Open audio</a>
                  <a href={audio.streamUrl} download>
                    Download audio
                  </a>
                </div>
              </>
            ) : (
              <p>No playable audio edition yet.</p>
            )}
            {editions.isError && (
              <Failure
                error={editions.error}
                retry={() => void editions.refetch()}
              />
            )}
            {editions.isPending ? (
              <p role="status">Loading ebook editions...</p>
            ) : ebook ? (
              <>
                <label className="field">
                  Ebook edition
                  <select
                    value={ebook.assetId}
                    onChange={(e) => {
                      setConfirmReport(null);
                      setEbookId(Number(e.target.value));
                    }}
                  >
                    {editions.data?.ebooks.map((option) => (
                      <option value={option.assetId} key={option.assetId}>
                        {option.label}
                        {option.editionNote
                          ? ` · ${option.editionNote}`
                          : ""} ·{" "}
                        {option.mimeType === "application/pdf" ? "PDF" : "EPUB"}
                      </option>
                    ))}
                  </select>
                </label>
                <div className="actions">
                  <a className="button secondary" href={ebook.downloadUrl}>
                    Download{" "}
                    {ebook.mimeType === "application/pdf" ? "PDF" : "EPUB"}
                  </a>
                  <span className="muted">{bytes(ebook.sizeBytes)}</span>
                </div>
              </>
            ) : (
              <p>No downloadable ebook edition yet.</p>
            )}
          </section>
          {audio && (
            <AudioContents
              key={audio.manifestationId}
              bookId={bookId}
              audio={audio}
              player={player}
            />
          )}
          <Artwork bookId={bookId} />
        </div>
        <aside>
          <section className="panel acquisition-panel">
            <h2>
              {latest?.state === "waiting_provider"
                ? "Waiting for the provider."
                : latest?.state === "scheduled"
                  ? "Next attempt scheduled."
                  : latest?.state === "needs_attention"
                    ? "Provider needs attention."
                    : latest?.status === "failed"
                      ? "Acquisition failed."
                      : "Acquisition"}
            </h2>
            {activity.isPending && <p role="status">Checking acquisition...</p>}
            {activity.isError && (
              <Failure
                error={activity.error}
                retry={() => void activity.refetch()}
              />
            )}
            {latest ? (
              <div
                className={`acquisition-status ${latest.status === "failed" ? "attention" : ""}`}
              >
                <p>
                  <strong>
                    {latest.type} · {latest.status.replaceAll("_", " ")}
                  </strong>
                </p>
                <p>{latest.message}</p>
                {latest.nextRunAt && latest.status === "queued" && (
                  <p>
                    Next attempt: {new Date(latest.nextRunAt).toLocaleString()}
                  </p>
                )}
              </div>
            ) : (
              !activity.isPending &&
              !activity.isError && (
                <p>
                  Find audio, an ebook, or both. Existing editions stay
                  available.
                </p>
              )
            )}
            {latest && latest.status === "succeeded" && !audio && !ebook && (
              <p>
                No playable editions were found. Try again, or choose a specific
                release below.
              </p>
            )}
            <div className="actions">
              <button
                disabled={acquisitionBusy}
                onClick={() =>
                  acquire.mutate({ bookId, media: ["audio", "ebook"] })
                }
              >
                {activity.data?.activeAcquisition
                  ? "Acquisition in progress"
                  : latest?.status === "failed"
                    ? "Retry acquisition"
                    : "Find both"}
              </button>
              <button
                className="secondary"
                disabled={acquisitionBusy}
                onClick={() => acquire.mutate({ bookId, media: ["audio"] })}
              >
                Find audio
              </button>
              <button
                className="secondary"
                disabled={acquisitionBusy}
                onClick={() => acquire.mutate({ bookId, media: ["ebook"] })}
              >
                Find ebook
              </button>
            </div>
            {acquire.isError && <Failure error={acquire.error} />}
            {acquire.isSuccess && (
              <p role="status">Acquisition queued.</p>
            )}
          </section>
          <AudioEditionRequests key={bookId} bookId={bookId} refresh={refresh} />
          {(audioReleaseIds.length > 0 || ebook?.releaseId) && (
            <section className="panel">
              <h2>Report a problem</h2>
              <p className="muted">
                Report a wrong import. We keep this edition while reviewing
                alternatives and, if needed, reacquiring.
              </p>
              <div className="actions">
                {audioReleaseIds.map((releaseId, index) => (
                  <button
                    className="secondary"
                    key={releaseId}
                    disabled={report.isPending}
                    onClick={() =>
                      setConfirmReport({
                        mediaType: "audio",
                        releaseId,
                        manifestationId: audio!.manifestationId,
                      })
                    }
                  >
                    Report wrong audio
                    {audioReleaseIds.length > 1 ? ` part ${index + 1}` : ""}
                  </button>
                ))}
                {ebook?.releaseId && (
                  <button
                    className="secondary"
                    disabled={report.isPending}
                    onClick={() =>
                      setConfirmReport({
                        mediaType: "ebook",
                        releaseId: ebook.releaseId!,
                        manifestationId: ebook.manifestationId ?? undefined,
                      })
                    }
                  >
                    Report wrong ebook
                  </button>
                )}
              </div>
              {confirmReport && (
                <div className="notice">
                  <p>
                    Review the selected {confirmReport.mediaType} edition? Its
                    current files will be preserved.
                  </p>
                  <div className="actions">
                    <button
                      disabled={report.isPending}
                      onClick={() =>
                        report.mutate({ bookId, ...confirmReport })
                      }
                    >
                      Confirm report
                    </button>
                    <button
                      className="secondary"
                      disabled={report.isPending}
                      onClick={() => setConfirmReport(null)}
                    >
                      Cancel
                    </button>
                  </div>
                </div>
              )}
              {report.isError && <Failure error={report.error} />}
              {report.isSuccess && (
                <p role="status">
                  Review queued. The current edition
                  is preserved.
                </p>
              )}
            </section>
          )}
          <details className="panel">
            <summary>Release history</summary>
            {releases.length ? (
              releases.map((release) => (
                <div className="history-row" key={release.id}>
                  <strong>{release.title}</strong>
                  <p>
                    {release.media_type} · {release.provider} · {release.status}
                  </p>
                </div>
              ))
            ) : (
              <p>No releases yet.</p>
            )}
          </details>
          <details className="panel">
            <summary>Manage this book</summary>
            <p>
              This removes the book from the library. Downloaded files are
              kept so existing torrents can continue seeding.
            </p>
            {confirmDelete ? (
              <div className="actions">
                <button
                  disabled={remove.isPending}
                  onClick={() => remove.mutate({ bookId })}
                >
                  Remove book
                </button>
                <button
                  className="secondary"
                  onClick={() => setConfirmDelete(false)}
                >
                  Keep book
                </button>
              </div>
            ) : (
              <button
                className="secondary"
                onClick={() => setConfirmDelete(true)}
              >
                Remove from library
              </button>
            )}
            {remove.isError && <Failure error={remove.error} />}
          </details>
        </aside>
      </div>
      {isAdmin && (
        <BookAdmin
          bookId={bookId}
          title={book.title}
          author={book.author}
          selectedManifestationId={audio?.manifestationId}
          refresh={refresh}
        />
      )}
    </>
  );
}

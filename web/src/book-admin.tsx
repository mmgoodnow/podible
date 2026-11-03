import { useState } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import { rpc, useTRPC } from "./rpc";
import { bytes, Failure } from "./books";
import { BookDiagnostics } from "./book-diagnostics";

type Media = "audio" | "ebook";

export function BookAdmin({
  bookId,
  title,
  author,
  selectedManifestationId,
  refresh,
}: {
  bookId: number;
  title: string;
  author: string;
  selectedManifestationId?: number;
  refresh: () => void;
}) {
  const trpc = useTRPC();
  const [media, setMedia] = useState<Media>("audio");
  const [query, setQuery] = useState(`${title} ${author}`);
  const [selected, setSelected] = useState<number[]>([]);
  const [label, setLabel] = useState("");
  const [note, setNote] = useState("");
  const search = useMutation({
    mutationFn: (input: { bookId: number; mediaType: Media; query: string }) =>
      rpc.library.searchReleases.query(input),
    onSuccess: () => setSelected([]),
  });
  const snatch = useMutation(
    trpc.library.createManifestationFromSearch.mutationOptions({
      onSuccess: () => {
        setSelected([]);
        refresh();
      },
    }),
  );
  const hydrate = useMutation(
    trpc.library.rehydrate.mutationOptions({ onSuccess: refresh }),
  );
  return (
    <section className="book-admin">
      <div className="section-heading">
        <div>
          <span className="eyebrow">LIBRARIAN'S DESK</span>
          <h2 className="editorial-heading">A little more control.</h2>
        </div>
        <div className="actions">
          <a href="/admin/ops">Operations</a>
          <a href="/admin/settings">Settings</a>
        </div>
      </div>
      <details className="panel">
        <summary>Find a specific edition</summary>
        <p>
          Select releases in playback order. They become a new edition without
          replacing the current one.
        </p>
        <form
          className="admin-search-form"
          onSubmit={(e) => {
            e.preventDefault();
            setSelected([]);
            search.mutate({ bookId, mediaType: media, query });
          }}
        >
          <label className="field">
            Media
            <select
              value={media}
              onChange={(e) => {
                setMedia(e.target.value as Media);
                setSelected([]);
                search.reset();
              }}
            >
              <option value="audio">Audio</option>
              <option value="ebook">Ebook</option>
            </select>
          </label>
          <label className="field">
            Release search
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              required
            />
          </label>
          <button disabled={search.isPending || snatch.isPending}>
            Search releases
          </button>
        </form>
        {search.isPending && <p role="status">Searching providers...</p>}
        {search.isError && <Failure error={search.error} />}
        {search.data && (
          <>
            <p className="muted">
              {search.data.results.length} releases · search expires{" "}
              {new Date(search.data.expiresAt).toLocaleTimeString()}
            </p>
            {search.data.results.map((result) => (
              <label className="book-release-choice" key={result.index}>
                <input
                  type="checkbox"
                  disabled={snatch.isPending || search.isPending}
                  checked={selected.includes(result.index)}
                  onChange={(e) =>
                    setSelected(
                      e.target.checked
                        ? [...selected, result.index]
                        : selected.filter((index) => index !== result.index),
                    )
                  }
                />
                <span>
                  <strong>{result.title}</strong>
                  <small>
                    {result.provider} · {result.seeders ?? "?"} seeders
                    {result.sizeBytes ? ` · ${bytes(result.sizeBytes)}` : ""}
                    {selected.includes(result.index)
                      ? ` · Part ${selected.indexOf(result.index) + 1}`
                      : ""}
                  </small>
                </span>
              </label>
            ))}
            {search.data.results.length > 0 && (
              <>
                <div className="form-grid">
                  <label className="field">
                    Edition label
                    <input
                      value={label}
                      onChange={(e) => setLabel(e.target.value)}
                      placeholder="e.g. Unabridged"
                    />
                  </label>
                  <label className="field">
                    Edition note
                    <input
                      value={note}
                      onChange={(e) => setNote(e.target.value)}
                      placeholder="Narrator, publisher, or language"
                    />
                  </label>
                </div>
                <button
                  disabled={
                    !selected.length || snatch.isPending || search.isPending
                  }
                  onClick={() =>
                    snatch.mutate({
                      bookId,
                      mediaType: search.data.mediaType,
                      searchId: search.data.searchId,
                      indexes: selected,
                      manifestation: {
                        label: label || null,
                        editionNote: note || null,
                      },
                    })
                  }
                >
                  {snatch.isPending
                    ? "Queuing..."
                    : "Acquire selected as new edition"}
                </button>
              </>
            )}
          </>
        )}
        {snatch.isError && <Failure error={snatch.error} />}
        {snatch.isSuccess && (
          <p role="status">
            New edition queued · {snatch.data.results.length} release(s).
          </p>
        )}
      </details>
      <ManualImport bookId={bookId} refresh={refresh} />
      <BookDiagnostics bookId={bookId} selectedManifestationId={selectedManifestationId} />
      <details className="panel">
        <summary>Metadata & diagnostics</summary>
        <p>
          Refresh this book from Open Library. Inspect editions and chapter curation
          above, or open Operations for execution history.
        </p>
        <div className="actions">
          <button
            className="secondary"
            disabled={hydrate.isPending}
            onClick={() => hydrate.mutate({ bookId })}
          >
            Refresh metadata
          </button>
          <a href="/admin/db">Database browser</a>
        </div>
        {hydrate.isError && <Failure error={hydrate.error} />}
        {hydrate.isSuccess && (
          <p role="status">
            Metadata refresh complete · {hydrate.data.updatedBookIds.length}{" "}
            book updated.
          </p>
        )}
      </details>
    </section>
  );
}

function ManualImport({
  bookId,
  refresh,
}: {
  bookId: number;
  refresh: () => void;
}) {
  const trpc = useTRPC();
  const [sourcePath, setSourcePath] = useState("");
  const [media, setMedia] = useState<Media>("audio");
  const [paths, setPaths] = useState<string[]>([]);
  const inspect = useMutation({
    mutationFn: (path: string) => rpc.import.inspect.query({ path }),
    onSuccess: (result) =>
      setPaths(
        result.files
          .filter((file) =>
            media === "audio" ? file.supportedAudio : file.supportedEbook,
          )
          .map((file) => file.sourcePath),
      ),
  });
  const importFiles = useMutation(
    trpc.import.manual.mutationOptions({ onSuccess: refresh }),
  );
  const validInspection = inspect.data?.path === sourcePath.trim();
  return (
    <details className="panel">
      <summary>Import local files</summary>
      <p>Inspect a path on the server, then choose the files to import.</p>
      <form
        className="admin-search-form"
        onSubmit={(e) => {
          e.preventDefault();
          inspect.mutate(sourcePath.trim());
        }}
      >
        <label className="field">
          Media
          <select
            value={media}
            onChange={(e) => {
              setMedia(e.target.value as Media);
              inspect.reset();
              setPaths([]);
            }}
          >
            <option value="audio">Audio</option>
            <option value="ebook">Ebook</option>
          </select>
        </label>
        <label className="field">
          Server path
          <input
            value={sourcePath}
            onChange={(e) => {
              setSourcePath(e.target.value);
              setPaths([]);
            }}
            placeholder="/absolute/path/to/files"
            required
          />
        </label>
        <button disabled={inspect.isPending || importFiles.isPending}>
          Inspect files
        </button>
      </form>
      {inspect.isError && <Failure error={inspect.error} />}
      {validInspection &&
        inspect.data?.files.map((file) => (
          <label className="book-release-choice" key={file.sourcePath}>
            <input
              type="checkbox"
              disabled={importFiles.isPending}
              checked={paths.includes(file.sourcePath)}
              onChange={(e) =>
                setPaths(
                  e.target.checked
                    ? [...paths, file.sourcePath]
                    : paths.filter((path) => path !== file.sourcePath),
                )
              }
            />
            <span>
              {file.relativePath}
              <small>{bytes(file.size)}</small>
            </span>
          </label>
        ))}
      {validInspection && (
        <div className="actions">
          <button
            disabled={!paths.length || importFiles.isPending}
            onClick={() =>
              importFiles.mutate({
                bookId,
                mediaType: media,
                path: sourcePath.trim(),
                selectedPaths: paths,
              })
            }
          >
            {importFiles.isPending ? "Importing..." : "Import selected files"}
          </button>
        </div>
      )}
      {importFiles.isError && <Failure error={importFiles.error} />}
      {importFiles.isSuccess && (
        <p role="status">
          Imported {importFiles.data.linkedFiles.length} file(s). Your edition
          is ready.
        </p>
      )}
    </details>
  );
}

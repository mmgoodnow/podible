# Book Web Application

The Bun-built React SPA is served at `/`, `/library`, `/add`, `/book/:bookId`,
`/series` and `/author`. Movies, requests, shadow audit and movie sync also use
root-level URLs. Existing `/app` page URLs redirect permanently to those routes,
preserving query parameters. Built frontend files are served from `/web-assets`.
The old book UI,
its `/classic` routes and HTML form mutation endpoints have been removed.
Authentication, feeds, administrator pages, JSON-RPC, typed tRPC and opaque media
URLs retain their existing routes.

The book screens use tRPC adapters over existing authenticated domain methods.
Browser-only read models expose acquisition activity, downloadable ebook editions,
and chapter previews without exposing provider credentials or raw execution logs.
The administrator-only `book.diagnostics` read model supplies the manifestation,
container, release and file tree plus chapter-curation summaries inside the SPA.
Mutations reuse the public domain handlers rather than duplicating business rules.

Behind a TLS-terminating reverse proxy, preserve the public `Host` and overwrite
`X-Forwarded-Proto` with a single `http` or `https` value. Typed mutation origin
checks use that external scheme and the request host, never `X-Forwarded-Host`.
Foreign origins and cross-site requests remain forbidden; rejection responses use
the tRPC envelope so clients can display the actual error. The admin browser
journey runs through an HTTPS-to-HTTP proxy fixture to cover this deployment shape.

Book detail supports audio/ebook edition selection, playback and downloads,
manifestation-scoped chapter/transcript generation and forced regeneration,
non-destructive issue reporting, artwork selection, metadata refresh and deletion.
Acquisition failures and scheduled waits remain visible rather than looking like
an indefinitely loading book. Administrators additionally get manual release
selection, local import and diagnostics; user-level release selection remains
available in the book's management panel.

Server-returned media links are opaque. Clients select returned editions and use
their stream, chapters, transcript and download URLs; they do not derive asset
paths from numeric IDs. Artwork URLs carry a revision so cover changes are visible
without a hard browser refresh.

Chrome acceptance uses the real HTTP/tRPC routes and built assets, generated audio
with byte-range playback, PDF downloads, realistic catalog/indexer fixtures,
session/admin boundaries, and desktop/mobile layouts:

```sh
bun run build
bun run test/browser/books.ts
```

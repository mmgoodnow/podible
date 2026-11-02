# Web application

The new UI is a Bun-built React SPA at `/app`. Existing book pages and JSON-RPC
remain available while screens migrate. tRPC at `/api/trpc` calls the same
authenticated, validated method handlers as JSON-RPC; browser types are inferred.

- `bun run dev`: backend plus Bun frontend HMR (use an isolated `CONFIG_DIR`).
- `bun run build`: ahead-of-time browser bundles into ignored `web/dist`.
- `bun run typecheck`: Bun native checking of server/tests and browser projects.
- `bun run test/browser/spa.ts`: real Chrome acceptance against a synthetic,
  in-memory fixture server; requires Google Chrome locally. No production access.

Docker builds assets in a separate stage. Production has no frontend development
server, no source bundling on first request, and no dev-only dependencies.
Authentication uses the existing HttpOnly cookie. Do not put credentials in the
frontend, build environment substitutions, or localStorage. Server router imports
in browser code must be type-only.

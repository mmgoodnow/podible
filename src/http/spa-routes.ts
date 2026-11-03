import path from "node:path";
import { Hono } from "hono";

export const SPA_BUILD_DIRECTORY = path.resolve(
  import.meta.dir,
  "../../web/dist",
);

export function createSpaRoutes(directory = SPA_BUILD_DIRECTORY) {
  const app = new Hono();
  app.get("*", async (c) => {
    const pathname = new URL(c.req.url).pathname;
    const relative = pathname.replace(/^\/app\/?/, "");
    if (relative.startsWith("assets/")) {
      if (
        relative
          .split("/")
          .some(
            (part) =>
              !part || part === "." || part === ".." || part.includes("\\"),
          )
      )
        return c.notFound();
      const file = Bun.file(path.join(directory, relative));
      if (!(await file.exists())) return c.notFound();
      return new Response(file, {
        headers: { "Cache-Control": "public, max-age=31536000, immutable" },
      });
    }
    if (
      relative.includes(".") ||
      !/^$|^(library|movies|requests|movie-shadow)(\/\d+)?\/?$/.test(relative)
    )
      return c.notFound();
    const entry = Bun.file(path.join(directory, "index.html"));
    if (!(await entry.exists()))
      return c.text("SPA build missing. Run bun run build.", 503);
    return new Response(entry, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": "no-cache",
      },
    });
  });
  return app;
}

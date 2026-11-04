import path from "node:path";
import { Hono } from "hono";

export const SPA_BUILD_DIRECTORY = path.resolve(
  import.meta.dir,
  "../../web/dist",
);

export function createSpaRoutes(directory = SPA_BUILD_DIRECTORY) {
  const app = new Hono();
  app.get("*", async (c) => {
    const url = new URL(c.req.url);
    const pathname = url.pathname;
    const relative = pathname.replace(/^\/app(?=\/|$)/, "").replace(/^\//, "");
    const legacyAsset = pathname.startsWith("/app/assets/");
    const assetPath = legacyAsset ? relative.replace(/^assets\//, "web-assets/") : relative;
    if (assetPath.startsWith("web-assets/")) {
      if (
        assetPath
          .split("/")
          .some(
            (part) =>
              !part || part === "." || part === ".." || part.includes("\\"),
          )
      )
        return c.notFound();
      const file = Bun.file(path.join(directory, assetPath));
      if (!(await file.exists())) return c.notFound();
      if (/^\/app(?:\/|$)/.test(pathname)) return c.redirect(`/${assetPath}${url.search}`, 308);
      return new Response(file, {
        headers: { "Cache-Control": "public, max-age=31536000, immutable" },
      });
    }
    if (
      relative.includes(".") ||
      !/^(?:|library|add|series|author|movies(?:\/\d+)?|requests(?:\/\d+)?|tv\/(?:(?:requests|library)\/)?\d+|book\/\d+|movie-shadow(?:\/\d+)?|movie-dual-write|tv-sync|retention|tv-retention|recovery|stewardship|settings|users|ops|login(?:\/plex\/(?:loading|result))?)\/?$/.test(
        relative,
      )
    )
      return c.notFound();
    if (/^movie-shadow(?:\/\d+)?\/?$/.test(relative))
      return c.redirect(`/movie-dual-write${url.search}#radarr-audit`, 308);
    if (/^\/app(?:\/|$)/.test(pathname))
      return c.redirect(`/${relative}${url.search}`, 308);
    const entry = Bun.file(path.join(directory, "index.html"));
    if (!(await entry.exists()))
      return c.text("SPA build missing. Run bun run build.", 503);
    return new Response(entry, {
      headers: {
        "Content-Type": "text/html; charset=utf-8",
        "Cache-Control": relative.startsWith("login") ? "no-store" : "no-cache",
      },
    });
  });
  return app;
}

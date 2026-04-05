import { createHash } from "node:crypto";

import { Hono } from "hono";

import { fetchPlexServerDevices } from "../plex";
import { BooksRepo } from "../repo";
import { renderAdminPage } from "./admin-page";
import { getCurrentSession, requireAdminSession, type HttpEnv } from "./middleware";
import { redirect } from "./route-helpers";
import type { AppSettings } from "../types";

type CachedPlexServers = {
  ownerTokenHash: string;
  fetchedAtMs: number;
  servers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }>;
};

const PLEX_SERVERS_CACHE_KEY = "plex_server_devices";
const PLEX_SERVERS_CACHE_TTL_MS = 5 * 60 * 1000;

function hashOwnerToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

function getCachedPlexServers(
  repo: BooksRepo,
  settings: AppSettings
): CachedPlexServers | null {
  const ownerToken = settings.auth.plex.ownerToken;
  if (!ownerToken) return null;
  const cached = repo.getJsonState<CachedPlexServers>(PLEX_SERVERS_CACHE_KEY);
  if (!cached) return null;
  if (cached.ownerTokenHash !== hashOwnerToken(ownerToken)) return null;
  return cached;
}

function cachePlexServers(
  repo: BooksRepo,
  settings: AppSettings,
  servers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }>
): void {
  const ownerToken = settings.auth.plex.ownerToken;
  if (!ownerToken) return;
  repo.setJsonState(PLEX_SERVERS_CACHE_KEY, {
    ownerTokenHash: hashOwnerToken(ownerToken),
    fetchedAtMs: Date.now(),
    servers,
  } satisfies CachedPlexServers);
}

export function createAdminRoutes(repo: BooksRepo): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();

  app.use("*", requireAdminSession);

  app.get("/plex", (c) => c.redirect("/admin", 303));

  app.post("/plex/select", async (c) => {
    const settings = repo.getSettings();
    if (settings.auth.mode !== "plex") {
      return new Response("Forbidden", { status: 403 });
    }
    const form = new URLSearchParams(await c.req.raw.text());
    const machineId = (form.get("machineId") ?? "").trim();
    if (!machineId) {
      return redirect("/admin?plex_error=Missing%20machine%20id");
    }
    repo.updateSettings({
      ...settings,
      auth: {
        ...settings.auth,
        plex: {
          ...settings.auth.plex,
          machineId,
        },
      },
    });
    return redirect("/admin?plex_notice=Selected%20server.");
  });

  app.post("/refresh", () => {
    const job = repo.createJob({ type: "full_library_refresh" });
    return redirect(`/admin?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`);
  });

  app.get("/", async (c) => {
    const settings = repo.getSettings();
    let plexServers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }> = [];
    const notice = c.req.query("notice");
    const error = c.req.query("error");
    let plexError = c.req.query("plex_error");
    if (settings.auth.mode === "plex" && settings.auth.plex.ownerToken) {
      const cached = getCachedPlexServers(repo, settings);
      try {
        if (cached && Date.now() - cached.fetchedAtMs < PLEX_SERVERS_CACHE_TTL_MS) {
          plexServers = cached.servers;
        } else {
          plexServers = await fetchPlexServerDevices(settings);
          cachePlexServers(repo, settings, plexServers);
        }
      } catch (error) {
        if (cached?.servers?.length) {
          plexServers = cached.servers;
        } else {
          plexError = plexError || (error as Error).message || "Unable to load Plex servers.";
        }
      }
    }
    return renderAdminPage(repo, settings, getCurrentSession(c), {
      plexServers,
      apiKey: null,
      notice,
      error,
      plexNotice: c.req.query("plex_notice"),
      plexError,
    });
  });

  return app;
}

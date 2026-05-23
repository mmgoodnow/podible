import { createHash } from "node:crypto";

import { Hono } from "hono";

import { fetchPlexServerDevices } from "../plex";
import { BooksRepo } from "../repo";
import {
  renderAdminContentPage,
  renderAdminCurationPage,
  renderAdminDbPage,
  renderAdminDownloadsPage,
  renderAdminJobsPage,
  renderAdminPage,
  renderAdminSettingsPage,
  renderAdminUsersPage,
} from "./admin-page";
import { curationRunsResponse, curationTraceResponse } from "./curation-dashboard";
import { getCurrentSession, requireAdminSession, type HttpEnv } from "./middleware";
import { formString } from "./route-helpers";
import type { AppSettings } from "../app-types";
import type { BuildInfo } from "../build-info";

type PlexServerView = {
  machineId: string;
  name: string;
  product: string;
  owned: boolean;
  sourceTitle: string | null;
};

type CachedPlexServers = {
  ownerTokenHash: string;
  fetchedAtMs: number;
  servers: PlexServerView[];
};

const PLEX_SERVERS_CACHE_KEY = "plex_server_devices";
const PLEX_SERVERS_CACHE_TTL_MS = 5 * 60 * 1000;

function hashOwnerToken(token: string): string {
  return createHash("sha256").update(token).digest("hex");
}

function getCachedPlexServers(repo: BooksRepo, settings: AppSettings): CachedPlexServers | null {
  const ownerToken = settings.auth.plex.ownerToken;
  if (!ownerToken) return null;
  const cached = repo.getJsonState<CachedPlexServers>(PLEX_SERVERS_CACHE_KEY);
  if (!cached) return null;
  if (cached.ownerTokenHash !== hashOwnerToken(ownerToken)) return null;
  return cached;
}

function cachePlexServers(repo: BooksRepo, settings: AppSettings, servers: PlexServerView[]): void {
  const ownerToken = settings.auth.plex.ownerToken;
  if (!ownerToken) return;
  repo.setJsonState(PLEX_SERVERS_CACHE_KEY, {
    ownerTokenHash: hashOwnerToken(ownerToken),
    fetchedAtMs: Date.now(),
    servers,
  } satisfies CachedPlexServers);
}

function jsonResponse(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      "Cache-Control": "no-store",
    },
  });
}

async function loadPlexServersForSettings(
  repo: BooksRepo,
  settings: AppSettings,
  explicitError: string | undefined
): Promise<{ plexServers: PlexServerView[]; plexError: string | undefined }> {
  let plexServers: PlexServerView[] = [];
  let plexError = explicitError;
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
  return { plexServers, plexError };
}

export function createAdminRoutes(repo: BooksRepo, startTime: number, buildInfo: BuildInfo | null): Hono<HttpEnv> {
  const app = new Hono<HttpEnv>();
  const adminRuntime = { startTime, buildInfo };

  app.use("*", requireAdminSession);

  app.get("/plex", (c) => c.redirect("/admin/settings", 303));

  app.post("/plex/select", async (c) => {
    const settings = repo.getSettings();
    if (settings.auth.mode !== "plex") {
      return c.text("Forbidden", 403);
    }
    const body = await c.req.parseBody();
    const machineId = formString(body, "machineId").trim();
    if (!machineId) {
      return c.redirect("/admin/settings?plex_error=Missing%20machine%20id", 303);
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
    return c.redirect("/admin/settings?plex_notice=Selected%20server.", 303);
  });

  app.post("/refresh", (c) => {
    const job = repo.createJob({ type: "full_library_refresh" });
    return c.redirect(`/admin/settings?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`, 303);
  });

  app.get("/", (c) =>
    renderAdminPage(repo, repo.getSettings(), getCurrentSession(c), {
      notice: c.req.query("notice"),
      error: c.req.query("error"),
      apiKey: null,
      activePath: "/admin",
      ...adminRuntime,
    })
  );

  app.get("/settings", async (c) => {
    const settings = repo.getSettings();
    const { plexServers, plexError } = await loadPlexServersForSettings(repo, settings, c.req.query("plex_error"));
    return renderAdminSettingsPage(settings, getCurrentSession(c), {
      plexServers,
      apiKey: null,
      notice: c.req.query("notice"),
      error: c.req.query("error"),
      plexNotice: c.req.query("plex_notice"),
      plexError,
      activePath: "/admin/settings",
      ...adminRuntime,
    });
  });

  app.get("/users", (c) => renderAdminUsersPage(repo, repo.getSettings(), getCurrentSession(c), { apiKey: null, activePath: "/admin/users", ...adminRuntime }));

  app.get("/jobs", (c) => renderAdminJobsPage(repo.getSettings(), getCurrentSession(c), { apiKey: null, activePath: "/admin/jobs", ...adminRuntime }));

  app.get("/downloads", (c) => renderAdminDownloadsPage(repo.getSettings(), getCurrentSession(c), { apiKey: null, activePath: "/admin/downloads", ...adminRuntime }));

  app.get("/content", (c) => renderAdminContentPage(repo, repo.getSettings(), getCurrentSession(c), { apiKey: null, activePath: "/admin/content", ...adminRuntime }));

  app.get("/curation", (c) => renderAdminCurationPage(repo.getSettings(), getCurrentSession(c), { apiKey: null, activePath: "/admin/curation", ...adminRuntime }));

  app.get("/curation/api/runs", (c) => jsonResponse(curationRunsResponse(c.req.query("selectedRunId") ?? null)));

  app.get("/curation/api/trace", (c) => jsonResponse(curationTraceResponse(c.req.query("runId") ?? null, c.req.query("file") ?? null)));

  app.get("/db", (c) => {
    const table = c.req.query("table");
    if (table && !repo.adminDbTableNames().includes(table)) {
      return c.text("Unknown table", 400);
    }
    return renderAdminDbPage(repo, repo.getSettings(), getCurrentSession(c), {
      table,
      limit: Number(c.req.query("limit") ?? 25),
      offset: Number(c.req.query("offset") ?? 0),
      apiKey: null,
      activePath: "/admin/db",
      ...adminRuntime,
    });
  });

  return app;
}

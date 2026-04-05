import { fetchPlexServerDevices } from "../plex";
import { BooksRepo } from "../repo";
import { renderAdminPage } from "./support";
import { redirect } from "./route-helpers";
import type { AppSettings, SessionWithUserRow } from "../types";

export function isAdminRoute(pathname: string): boolean {
  return pathname === "/admin" || pathname === "/admin/plex" || pathname === "/admin/plex/select" || pathname === "/admin/refresh";
}

export async function handleAdminRoute(input: {
  repo: BooksRepo;
  request: Request;
  settings: AppSettings;
  currentSession: SessionWithUserRow | null;
  pathname: string;
  method: string;
}): Promise<{ response: Response; settings: AppSettings } | null> {
  const { repo, request, currentSession, pathname, method } = input;
  let { settings } = input;
  const url = new URL(request.url);

  if (pathname === "/admin/plex" && method === "GET") {
    return { response: redirect("/admin"), settings };
  }

  if (pathname === "/admin/plex/select" && method === "POST") {
    if (settings.auth.mode !== "plex") {
      return { response: new Response("Forbidden", { status: 403 }), settings };
    }
    const form = new URLSearchParams(await request.text());
    const machineId = (form.get("machineId") ?? "").trim();
    if (!machineId) {
      return { response: redirect("/admin?plex_error=Missing%20machine%20id"), settings };
    }
    settings = repo.updateSettings({
      ...settings,
      auth: {
        ...settings.auth,
        plex: {
          ...settings.auth.plex,
          machineId,
        },
      },
    });
    return { response: redirect("/admin?plex_notice=Selected%20server."), settings };
  }

  if (pathname === "/admin/refresh" && method === "POST") {
    const job = repo.createJob({ type: "full_library_refresh" });
    return { response: redirect(`/admin?notice=${encodeURIComponent(`Queued library refresh job ${job.id}.`)}`), settings };
  }

  if (pathname === "/admin" && method === "GET") {
    let plexServers: Array<{ machineId: string; name: string; product: string; owned: boolean; sourceTitle: string | null }> = [];
    const notice = url.searchParams.get("notice");
    const error = url.searchParams.get("error");
    let plexError = url.searchParams.get("plex_error");
    if (settings.auth.mode === "plex" && settings.auth.plex.ownerToken) {
      try {
        plexServers = await fetchPlexServerDevices(settings);
      } catch (error) {
        plexError = plexError || (error as Error).message || "Unable to load Plex servers.";
      }
    }
    return {
      response: renderAdminPage(repo, settings, currentSession, {
        plexServers,
        apiKey: null,
        notice,
        error,
        plexNotice: url.searchParams.get("plex_notice"),
        plexError,
      }),
      settings,
    };
  }

  return null;
}

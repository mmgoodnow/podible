import { setTimeout as sleep } from "node:timers/promises";

/** Plex's SSE transport can disconnect without replaying events. On open,
 * verify pending publications once against Plex itself. */
export function plexScanEnded(message: unknown): string | null {
  const notification = (message as { ActivityNotification?: {
    event?: string; Activity?: { type?: string; Context?: { librarySectionID?: number | string; key?: string } };
  } } | null)?.ActivityNotification;
  const activity = notification?.Activity;
  if (notification?.event !== "ended" || !activity?.type ||
      !["library.update.section", "library.refresh.items"].includes(activity.type)) return null;
  const section = String(activity.Context?.librarySectionID ??
    /^\/library\/sections\/(\d+)(?:\/|$)/.exec(activity.Context?.key ?? "")?.[1] ?? "");
  return /^\d+$/.test(section) ? section : "";
}

export function watchPlexCompletions(
  signal: (source: string, prefix?: boolean) => Promise<void>,
  onDisconnect: () => void, fetcher: typeof fetch = fetch,
) {
  let config: { url: string; token: string } | undefined;
  let current: AbortController | undefined;
  let stopped = false;
  let pending: Promise<void> | undefined;
  async function read(settings: { url: string; token: string }, lifetime: AbortSignal) {
    let delay = 1000;
    while (!lifetime.aborted) {
      let reader: { read(): Promise<{ done: boolean; value?: string }>; cancel(): Promise<void> } | undefined;
      try {
        const url = new URL("/:/eventsource/notifications", settings.url);
        if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return;
        url.searchParams.set("filters", "activity");
        const response = await fetcher(url, {
          redirect: "error", signal: lifetime,
          headers: { Accept: "text/event-stream", "X-Plex-Token": settings.token },
        });
        if (!response.ok || !response.body || !response.headers.get("content-type")?.startsWith("text/event-stream")) {
          await response.body?.cancel();
          throw new Error("Plex event stream unavailable");
        }
        reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
        await signal("plex:", true);
        delay = 1000;
        let buffer = "";
        let fields: string[] = [];
        let length = 0;
        while (!lifetime.aborted) {
          const { done, value } = await reader.read();
          if (done) break;
          buffer += value;
          let boundary: number;
          while ((boundary = buffer.indexOf("\n")) !== -1) {
            const line = buffer.slice(0, boundary).replace(/\r$/, "");
            buffer = buffer.slice(boundary + 1);
            if (line.startsWith("data:")) { fields.push(line.slice(5).trimStart()); length += line.length; }
            if (!line && fields.length) {
              let section: string | null = null;
              try { section = plexScanEnded(JSON.parse(fields.join("\n"))); } catch { /* Not a JSON notification. */ }
              fields = [];
              length = 0;
              if (section !== null) await signal("plex:" + section, section === "");
            }
            if (length > 128_000) throw new Error("Plex event exceeds size limit");
          }
          if (buffer.length > 128_000) throw new Error("Plex event exceeds size limit");
        }
      } catch { /* No payloads or credentials in logs. */ }
      finally { await reader?.cancel().catch(() => {}); }
      if (lifetime.aborted) return;
      onDisconnect();
      await sleep(delay, undefined, { signal: lifetime, ref: false }).catch(() => {});
      delay = Math.min(delay * 2, 60_000);
    }
  }
  return {
    configure(settings: { url: string; token: string }) {
      if (stopped || (config?.url === settings.url && config?.token === settings.token)) return;
      current?.abort();
      config = settings;
      if (!settings.url || !settings.token) return;
      current = new AbortController();
      pending = read(settings, current.signal);
    },
    async stop() {
      stopped = true;
      current?.abort();
      await pending;
    },
  };
}

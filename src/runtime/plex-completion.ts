import { setTimeout as sleep } from "node:timers/promises";

function sectionId(value: unknown): string | null {
  if (typeof value !== "string" && typeof value !== "number") return null;
  const section = String(value);
  return /^\d+$/.test(section) ? section : null;
}

export function plexPublicationChanged(message: unknown): { section: string; settle?: true } | null {
  const event = message as {
    ActivityNotification?: { event?: string; Activity?: {
      type?: string; Context?: { librarySectionID?: number | string; key?: string };
    } };
    TimelineEntry?: { sectionID?: unknown; type?: unknown; state?: unknown; metadataState?: unknown };
  } | null;
  const timeline = event?.TimelineEntry;
  if (timeline && /^[1-4]$/.test(String(timeline.type))) {
    const section = sectionId(timeline.sectionID);
    if (section === null) return null;
    // Plex can finish metadata while media analysis is still running. Its catalog can be readable even earlier.
    if (String(timeline.state) === "5") return { section };
    if (String(timeline.state) === "4" && timeline.metadataState === "loading") return { section, settle: true };
  }
  const notification = event?.ActivityNotification;
  const activity = notification?.Activity;
  if (notification?.event !== "ended" || !activity?.type ||
      !["library.update.section", "library.refresh.items"].includes(activity.type)) return null;
  return { section: sectionId(activity.Context?.librarySectionID) ??
    /^\/library\/sections\/(\d+)(?:\/|$)/.exec(activity.Context?.key ?? "")?.[1] ?? "" };
}

/** Plex does not replay missed events, so verify pending publications once on connect. */
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
      const loading = new Map<string, ReturnType<typeof setTimeout>>();
      const earlyWakes = new Set<Promise<unknown>>();
      try {
        const url = new URL("/:/eventsource/notifications", settings.url);
        if (!["http:", "https:"].includes(url.protocol) || url.username || url.password) return;
        url.searchParams.set("filters", "activity,timeline");
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
        const wake = async ({ section, settle }: NonNullable<ReturnType<typeof plexPublicationChanged>>) => {
          if (settle) {
            // One early check per section burst, not a repeating probe. Let metadata commit and batch episode events.
            if (loading.has(section) || loading.size >= 64) return;
            const timer = setTimeout(() => {
              loading.delete(section);
              if (lifetime.aborted) return;
              const delivery = signal("plex:" + section).catch(() => reader?.cancel().catch(() => {}));
              earlyWakes.add(delivery);
              void delivery.finally(() => earlyWakes.delete(delivery));
            }, 250);
            timer.unref();
            loading.set(section, timer);
            return;
          }
          // A terminal update supersedes any scheduled early check for this section.
          if (section) { clearTimeout(loading.get(section)); loading.delete(section); }
          await signal("plex:" + section, section === "");
        };
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
              let notification: ReturnType<typeof plexPublicationChanged> = null;
              try { notification = plexPublicationChanged(JSON.parse(fields.join("\n"))); } catch { /* Not a JSON notification. */ }
              fields = [];
              length = 0;
              if (notification && !lifetime.aborted) await wake(notification);
            }
            if (length > 128_000) throw new Error("Plex event exceeds size limit");
          }
          if (buffer.length > 128_000) throw new Error("Plex event exceeds size limit");
        }
      } catch { /* No payloads or credentials in logs. */ }
      finally {
        for (const timer of loading.values()) clearTimeout(timer);
        await reader?.cancel().catch(() => {});
        await Promise.allSettled(earlyWakes);
      }
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

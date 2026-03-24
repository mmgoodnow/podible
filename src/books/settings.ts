import { randomBytes } from "node:crypto";

import type { AppSettings } from "./types";

export function defaultSettings(overrides?: Partial<AppSettings>): AppSettings {
  return {
    torznab: [],
    rtorrent: {
      transport: "http-xmlrpc",
      url: "http://127.0.0.1/RPC2",
      username: "",
      password: "",
      downloadPath: "",
    },
    libraryRoot: "/media/library",
    polling: {
      rtorrentMs: 5000,
      scanMs: 30000,
    },
    recovery: {
      stalledTorrentMinutes: 10,
    },
    feed: {
      title: "Kindling",
      author: "Unknown",
    },
    auth: {
      mode: "apikey",
      key: randomBytes(24).toString("hex"),
    },
    agents: {
      enabled: false,
      provider: "openai-responses",
      model: "gpt-5-mini",
      apiKey: "",
      lowConfidenceThreshold: 0.45,
      timeoutMs: 30000,
    },
    notifications: {
      pushover: {
        enabled: false,
        apiToken: "",
        userKey: "",
      },
    },
    ...overrides,
  };
}

export function parseSettings(value: string): AppSettings {
  const parsed = JSON.parse(value) as Partial<AppSettings>;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Invalid settings payload");
  }

  const defaults = defaultSettings();
  const parsedAgents = (parsed.agents && typeof parsed.agents === "object" ? parsed.agents : {}) as Partial<AppSettings["agents"]>;
  const parsedRecovery =
    parsed.recovery && typeof parsed.recovery === "object" ? (parsed.recovery as Partial<AppSettings["recovery"]>) : {};
  const parsedNotifications =
    parsed.notifications && typeof parsed.notifications === "object"
      ? (parsed.notifications as Partial<AppSettings["notifications"]>)
      : {};
  const parsedPushover =
    parsedNotifications.pushover && typeof parsedNotifications.pushover === "object"
      ? (parsedNotifications.pushover as Partial<AppSettings["notifications"]["pushover"]>)
      : {};
  const stalledTorrentMinutes =
    typeof parsedRecovery.stalledTorrentMinutes === "number" && Number.isFinite(parsedRecovery.stalledTorrentMinutes)
      ? Math.max(0, Math.trunc(parsedRecovery.stalledTorrentMinutes))
      : defaults.recovery.stalledTorrentMinutes;
  return {
    ...defaults,
    ...parsed,
    torznab: Array.isArray(parsed.torznab) ? parsed.torznab : defaults.torznab,
    rtorrent: {
      ...defaults.rtorrent,
      ...(parsed.rtorrent && typeof parsed.rtorrent === "object" ? parsed.rtorrent : {}),
      downloadPath:
        parsed.rtorrent &&
        typeof parsed.rtorrent === "object" &&
        typeof (parsed.rtorrent as Partial<AppSettings["rtorrent"]>).downloadPath === "string"
          ? (parsed.rtorrent as Partial<AppSettings["rtorrent"]>).downloadPath ?? defaults.rtorrent.downloadPath
          : defaults.rtorrent.downloadPath,
    },
    polling: {
      ...defaults.polling,
      ...(parsed.polling && typeof parsed.polling === "object" ? parsed.polling : {}),
    },
    recovery: {
      stalledTorrentMinutes,
    },
    feed: {
      ...defaults.feed,
      ...(parsed.feed && typeof parsed.feed === "object" ? parsed.feed : {}),
    },
    auth: {
      ...defaults.auth,
      ...(parsed.auth && typeof parsed.auth === "object" ? parsed.auth : {}),
    },
    agents: {
      enabled: typeof parsedAgents.enabled === "boolean" ? parsedAgents.enabled : defaults.agents.enabled,
      provider: parsedAgents.provider === "openai-responses" ? parsedAgents.provider : defaults.agents.provider,
      model: typeof parsedAgents.model === "string" && parsedAgents.model.trim() ? parsedAgents.model : defaults.agents.model,
      apiKey: typeof parsedAgents.apiKey === "string" ? parsedAgents.apiKey : defaults.agents.apiKey,
      lowConfidenceThreshold:
        typeof parsedAgents.lowConfidenceThreshold === "number" && Number.isFinite(parsedAgents.lowConfidenceThreshold)
          ? parsedAgents.lowConfidenceThreshold
          : defaults.agents.lowConfidenceThreshold,
      timeoutMs:
        typeof parsedAgents.timeoutMs === "number" && Number.isFinite(parsedAgents.timeoutMs)
          ? Math.max(1000, Math.trunc(parsedAgents.timeoutMs))
          : defaults.agents.timeoutMs,
    },
    notifications: {
      pushover: {
        enabled: typeof parsedPushover.enabled === "boolean" ? parsedPushover.enabled : defaults.notifications.pushover.enabled,
        apiToken: typeof parsedPushover.apiToken === "string" ? parsedPushover.apiToken : defaults.notifications.pushover.apiToken,
        userKey: typeof parsedPushover.userKey === "string" ? parsedPushover.userKey : defaults.notifications.pushover.userKey,
      },
    },
  };
}

import type { AppSettings } from "./types";

type SettingsOverrides = Partial<Omit<AppSettings, "rtorrent" | "polling" | "recovery" | "feed" | "auth" | "agents" | "notifications">> & {
  rtorrent?: Partial<AppSettings["rtorrent"]>;
  polling?: Partial<AppSettings["polling"]>;
  recovery?: Partial<AppSettings["recovery"]>;
  feed?: Partial<AppSettings["feed"]>;
    auth?: Partial<Omit<AppSettings["auth"], "plex">> & {
      plex?: Partial<AppSettings["auth"]["plex"]>;
    };
  agents?: Partial<AppSettings["agents"]>;
  notifications?: {
    pushover?: Partial<AppSettings["notifications"]["pushover"]>;
  };
};

export function defaultSettings(overrides?: SettingsOverrides): AppSettings {
  const defaults: AppSettings = {
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
    },
    recovery: {
      stalledTorrentMinutes: 10,
    },
    feed: {
      title: "Kindling",
      author: "Unknown",
    },
    auth: {
      mode: "plex",
      appRedirectURIs: [],
      plex: {
        productName: "Podible",
        ownerToken: "",
        machineId: "",
      },
    },
    agents: {
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
  };

  return {
    ...defaults,
    ...overrides,
    rtorrent: {
      ...defaults.rtorrent,
      ...(overrides?.rtorrent ?? {}),
    },
    polling: {
      ...defaults.polling,
      rtorrentMs: overrides?.polling?.rtorrentMs ?? defaults.polling.rtorrentMs,
    },
    recovery: {
      ...defaults.recovery,
      ...(overrides?.recovery ?? {}),
    },
    feed: {
      ...defaults.feed,
      ...(overrides?.feed ?? {}),
    },
    auth: {
      ...defaults.auth,
      ...(overrides?.auth ?? {}),
      plex: {
        ...defaults.auth.plex,
        ...(overrides?.auth?.plex ?? {}),
      },
    },
    agents: {
      ...defaults.agents,
      ...(overrides?.agents ?? {}),
    },
    notifications: {
      pushover: {
        ...defaults.notifications.pushover,
        ...(overrides?.notifications?.pushover ?? {}),
      },
    },
  };
}

export function parseSettings(value: string): AppSettings {
  const parsed = JSON.parse(value) as Partial<AppSettings>;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Invalid settings payload");
  }

  const defaults = defaultSettings();
  const parsedAuth = (parsed.auth && typeof parsed.auth === "object" ? parsed.auth : {}) as Partial<AppSettings["auth"]>;
  const parsedAuthPlex =
    parsedAuth.plex && typeof parsedAuth.plex === "object" ? (parsedAuth.plex as Partial<AppSettings["auth"]["plex"]>) : {};
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
      mode: parsedAuth.mode === "plex" ? parsedAuth.mode : defaults.auth.mode,
      appRedirectURIs: Array.isArray(parsedAuth.appRedirectURIs)
        ? parsedAuth.appRedirectURIs.filter((value): value is string => typeof value === "string" && value.trim().length > 0)
        : defaults.auth.appRedirectURIs,
      plex: {
        productName:
          typeof parsedAuthPlex.productName === "string" && parsedAuthPlex.productName.trim()
            ? parsedAuthPlex.productName
            : defaults.auth.plex.productName,
        ownerToken:
          typeof parsedAuthPlex.ownerToken === "string"
            ? parsedAuthPlex.ownerToken
            : defaults.auth.plex.ownerToken,
        machineId:
          typeof parsedAuthPlex.machineId === "string"
            ? parsedAuthPlex.machineId
            : defaults.auth.plex.machineId,
      },
    },
    agents: {
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

import type { AppSettings } from "./app-types";
import path from "node:path";
import { defaultMovieSettings, movieSettingsSchema } from "./movies/settings";
import { prowlarrSettingsSchema } from "./media/prowlarr";
import { tvSettingsSchema } from "./tv/settings";
import { retentionPolicySchema } from "./media/retention-policy-types";
import { parseNotificationSettings } from "./notifications/settings";

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
    delivery?: AppSettings["notifications"]["delivery"];
    email?: Partial<AppSettings["notifications"]["email"]>;
    pushover?: Partial<AppSettings["notifications"]["pushover"]>;
  };
};

export function defaultSettings(overrides?: SettingsOverrides): AppSettings {
  const defaults: AppSettings = {
    prowlarr: prowlarrSettingsSchema.parse({}),
    movies: defaultMovieSettings(),
    tv: tvSettingsSchema.parse({}),
    retention: retentionPolicySchema.parse({}),
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
        ownerClientIdentifier: "",
        machineId: "",
      },
    },
    agents: {
      apiKey: "",
      editionPreference: "",
      timeoutMs: 30000,
    },
    notifications: parseNotificationSettings(undefined),
  };

  return resolveLibraryStorage({
    ...defaults,
    ...overrides,
    movies: movieSettingsSchema.parse(overrides?.movies ?? defaults.movies),
    tv: tvSettingsSchema.parse(overrides?.tv ?? defaults.tv),
    retention: retentionPolicySchema.parse(overrides?.retention ?? {}),
    prowlarr: prowlarrSettingsSchema.parse(overrides?.prowlarr ?? {}),
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
      delivery: overrides?.notifications?.delivery ?? defaults.notifications.delivery,
      email: { ...defaults.notifications.email, ...overrides?.notifications?.email },
      pushover: {
        ...defaults.notifications.pushover,
        ...(overrides?.notifications?.pushover ?? {}),
      },
    },
  });
}

export function resolveLibraryStorage(settings: AppSettings): AppSettings {
  if (!settings.storageRoot) return settings;
  const root = path.resolve(settings.storageRoot);
  const movies = path.join(root, "movies"), tv = path.join(root, "tv");
  return {
    ...settings,
    storageRoot: root,
    libraryRoot: path.join(root, "books"),
    movies: { ...settings.movies, managedRoot: movies, plexViewPath: path.join(movies, "views", "plex", "movies") },
    tv: { ...settings.tv, managedRoot: tv, plexViewPath: path.join(tv, "views", "plex", "tv") },
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
  const stalledTorrentMinutes =
    typeof parsedRecovery.stalledTorrentMinutes === "number" && Number.isFinite(parsedRecovery.stalledTorrentMinutes)
      ? Math.max(0, Math.trunc(parsedRecovery.stalledTorrentMinutes))
      : defaults.recovery.stalledTorrentMinutes;
  const movies = parsed.movies && typeof parsed.movies === "object" && !Array.isArray(parsed.movies)
    ? Object.fromEntries(Object.entries(parsed.movies).filter(([key]) => key !== "downloadGroupId" && key !== "automaticUpgrades"))
    : parsed.movies;
  return resolveLibraryStorage({
    ...defaults,
    ...parsed,
    movies: movieSettingsSchema.parse(movies ?? {}),
    tv: tvSettingsSchema.parse(parsed.tv ?? {}),
    retention: retentionPolicySchema.parse(parsed.retention ?? {}),
    prowlarr: prowlarrSettingsSchema.parse(parsed.prowlarr ?? {}),
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
        ownerClientIdentifier:
          typeof parsedAuthPlex.ownerClientIdentifier === "string"
            ? parsedAuthPlex.ownerClientIdentifier
            : defaults.auth.plex.ownerClientIdentifier,
        machineId:
          typeof parsedAuthPlex.machineId === "string"
            ? parsedAuthPlex.machineId
            : defaults.auth.plex.machineId,
      },
    },
    agents: {
      apiKey: typeof parsedAgents.apiKey === "string" ? parsedAgents.apiKey : defaults.agents.apiKey,
      editionPreference:
        typeof parsedAgents.editionPreference === "string" ? parsedAgents.editionPreference : defaults.agents.editionPreference,
      timeoutMs:
        typeof parsedAgents.timeoutMs === "number" && Number.isFinite(parsedAgents.timeoutMs)
          ? Math.max(1000, Math.trunc(parsedAgents.timeoutMs))
          : defaults.agents.timeoutMs,
    },
    notifications: parseNotificationSettings(parsed.notifications),
  });
}

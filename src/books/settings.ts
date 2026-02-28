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
  };
}

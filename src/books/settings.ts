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
    },
    libraryRoot: "/media/library",
    polling: {
      rtorrentMs: 5000,
      scanMs: 30000,
    },
    transcode: {
      enabled: true,
      format: "mp3",
      bitrateKbps: 64,
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
      lowConfidenceThreshold: 0.45,
      timeoutMs: 30000,
      search: {
        enableOnFailure: true,
        enableOnLowConfidence: true,
      },
      manualImport: {
        enableOnFailure: true,
        enableOnLowConfidence: true,
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
  const parsedSearch =
    parsedAgents.search && typeof parsedAgents.search === "object"
      ? (parsedAgents.search as Partial<AppSettings["agents"]["search"]>)
      : {};
  const parsedManualImport =
    parsedAgents.manualImport && typeof parsedAgents.manualImport === "object"
      ? (parsedAgents.manualImport as Partial<AppSettings["agents"]["manualImport"]>)
      : {};

  return {
    ...defaults,
    ...parsed,
    torznab: Array.isArray(parsed.torznab) ? parsed.torznab : defaults.torznab,
    rtorrent: {
      ...defaults.rtorrent,
      ...(parsed.rtorrent && typeof parsed.rtorrent === "object" ? parsed.rtorrent : {}),
    },
    polling: {
      ...defaults.polling,
      ...(parsed.polling && typeof parsed.polling === "object" ? parsed.polling : {}),
    },
    transcode: {
      ...defaults.transcode,
      ...(parsed.transcode && typeof parsed.transcode === "object" ? parsed.transcode : {}),
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
      ...defaults.agents,
      ...parsedAgents,
      search: {
        ...defaults.agents.search,
        ...parsedSearch,
      },
      manualImport: {
        ...defaults.agents.manualImport,
        ...parsedManualImport,
      },
    },
  };
}

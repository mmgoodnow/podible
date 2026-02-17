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
      timeoutMs: 8000,
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
  const parsed = JSON.parse(value) as AppSettings;
  if (!parsed || typeof parsed !== "object") {
    throw new Error("Invalid settings payload");
  }
  return parsed;
}

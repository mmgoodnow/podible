import { createPrivateKey, generateKeyPairSync, randomBytes, sign as cryptoSign } from "node:crypto";
import { XMLParser } from "fast-xml-parser";

import type { AppSettings, PlexJwk } from "./app-types";

type PlexPinResponse = {
  id: number;
  code: string;
  authToken?: string | null;
};

type PlexUserIdentity = {
  id: string;
  username: string;
  displayName: string;
  thumbUrl: string | null;
};

export type PlexServerDevice = {
  machineId: string;
  name: string;
  product: string;
  owned: boolean;
  provides: string[];
  accessToken: string | null;
  sourceTitle: string | null;
};

export type PlexClientIdentity = {
  productName: string;
  clientIdentifier: string;
  publicJwk: PlexJwk;
  privateKeyPkcs8: string;
};

const xmlParser = new XMLParser({
  ignoreAttributes: false,
  attributeNamePrefix: "",
  parseTagValue: false,
  parseAttributeValue: false,
});

function asArray<T>(value: T | T[] | null | undefined): T[] {
  if (Array.isArray(value)) return value;
  return value == null ? [] : [value];
}

function base64UrlJson(value: unknown): string {
  return Buffer.from(JSON.stringify(value)).toString("base64url");
}

function plexHeaders(productName: string, clientIdentifier: string, extra?: Record<string, string>): Headers {
  const headers = new Headers(extra);
  headers.set("Accept", "application/json");
  headers.set("X-Plex-Product", productName);
  headers.set("X-Plex-Client-Identifier", clientIdentifier);
  return headers;
}

function settingsClientIdentifier(_settings: AppSettings): string {
  return "podible-server";
}

function coercePlexUser(payload: unknown): PlexUserIdentity {
  const raw = (payload && typeof payload === "object" ? payload : {}) as Record<string, unknown>;
  const source =
    (raw.user && typeof raw.user === "object" ? (raw.user as Record<string, unknown>) : null) ??
    (raw.MediaContainer && typeof raw.MediaContainer === "object" ? (raw.MediaContainer as Record<string, unknown>) : null) ??
    raw;
  const idValue = source.id ?? source.uuid ?? source.userID ?? source.userId;
  const usernameValue = source.username ?? source.email ?? source.title ?? source.name;
  const displayNameValue = source.title ?? source.username ?? source.email ?? source.name;
  const thumbValue = source.thumb ?? source.avatar ?? null;
  const id = typeof idValue === "string" || typeof idValue === "number" ? String(idValue) : "";
  const username =
    (typeof usernameValue === "string" && usernameValue.trim()) ||
    (typeof displayNameValue === "string" && displayNameValue.trim()) ||
    (id ? `plex-${id}` : "");
  if (!id || !username) {
    throw new Error("Unable to determine Plex user identity");
  }
  return {
    id,
    username,
    displayName:
      (typeof displayNameValue === "string" && displayNameValue.trim()) ||
      (typeof usernameValue === "string" && usernameValue.trim()) ||
      username,
    thumbUrl: typeof thumbValue === "string" && thumbValue.trim() ? thumbValue : null,
  };
}

export function createEphemeralPlexIdentity(productName: string): PlexClientIdentity {
  const { publicKey, privateKey } = generateKeyPairSync("ed25519");
  const exported = publicKey.export({ format: "jwk" }) as Record<string, unknown>;
  const x = typeof exported.x === "string" ? exported.x : "";
  if (!x) {
    throw new Error("Unable to export Plex public JWK");
  }

  const kid = `podible-plex-${randomBytes(8).toString("hex")}`;
  const publicJwk: PlexJwk = {
    kty: "OKP",
    crv: "Ed25519",
    x,
    kid,
    alg: "EdDSA",
    use: "sig",
  };
  return {
    productName,
    clientIdentifier: `podible-${randomBytes(12).toString("hex")}`,
    publicJwk,
    privateKeyPkcs8: privateKey.export({ format: "pem", type: "pkcs8" }).toString(),
  };
}

export function buildPlexAuthUrl(identity: Pick<PlexClientIdentity, "clientIdentifier" | "productName">, code: string, forwardUrl: string): string {
  const params = new URLSearchParams({
    clientID: identity.clientIdentifier,
    code,
    forwardUrl,
    "context[device][product]": identity.productName,
  });
  return `https://app.plex.tv/auth#?${params.toString()}`;
}

export async function createPlexPin(identity: PlexClientIdentity): Promise<PlexPinResponse> {
  const response = await fetch("https://plex.tv/api/v2/pins?strong=true", {
    method: "POST",
    headers: plexHeaders(identity.productName, identity.clientIdentifier, {
      "Content-Type": "application/json",
    }),
    body: JSON.stringify({
      jwk: identity.publicJwk,
      strong: true,
    }),
  });
  if (!response.ok) {
    throw new Error(`Plex PIN creation failed with ${response.status}`);
  }
  return (await response.json()) as PlexPinResponse;
}

export function signPlexDeviceJwt(identity: PlexClientIdentity): string {
  const kid = identity.publicJwk?.kid;
  if (!kid || !identity.privateKeyPkcs8 || !identity.clientIdentifier) {
    throw new Error("Plex auth is not configured");
  }
  const now = Math.floor(Date.now() / 1000);
  const header = {
    alg: "EdDSA",
    typ: "JWT",
    kid,
  };
  const payload = {
    aud: "plex.tv",
    iss: identity.clientIdentifier,
    iat: now,
    exp: now + 300,
  };
  const unsigned = `${base64UrlJson(header)}.${base64UrlJson(payload)}`;
  const privateKey = createPrivateKey(identity.privateKeyPkcs8);
  const signature = cryptoSign(null, Buffer.from(unsigned), privateKey);
  return `${unsigned}.${signature.toString("base64url")}`;
}

export async function exchangePlexPinForToken(identity: PlexClientIdentity, pinId: number): Promise<string> {
  const deviceJwt = signPlexDeviceJwt(identity);
  const url = new URL(`https://plex.tv/api/v2/pins/${pinId}`);
  url.searchParams.set("deviceJWT", deviceJwt);
  const response = await fetch(url, {
    headers: plexHeaders(identity.productName, identity.clientIdentifier),
  });
  if (!response.ok) {
    if (response.status === 429) {
      throw new Error("Plex PIN exchange is being rate limited");
    }
    throw new Error(`Plex PIN exchange failed with ${response.status}`);
  }
  const payload = (await response.json()) as PlexPinResponse;
  if (!payload.authToken) {
    throw new Error("Plex PIN has not been claimed yet");
  }
  return payload.authToken;
}

export async function fetchPlexUser(
  settings: AppSettings,
  plexToken: string,
  clientIdentifier = settingsClientIdentifier(settings)
): Promise<PlexUserIdentity> {
  const response = await fetch("https://plex.tv/api/v2/user", {
    headers: plexHeaders(settings.auth.plex.productName, clientIdentifier, {
      "X-Plex-Token": plexToken,
    }),
  });
  if (!response.ok) {
    throw new Error(`Plex user lookup failed with ${response.status}`);
  }
  return coercePlexUser(await response.json());
}

export async function fetchPlexServerDevices(settings: AppSettings, plexToken = settings.auth.plex.ownerToken): Promise<PlexServerDevice[]> {
  if (!plexToken) {
    return [];
  }
  const response = await fetch("https://plex.tv/api/resources?includeHttps=1", {
    headers: plexHeaders(settings.auth.plex.productName, settingsClientIdentifier(settings), {
      "X-Plex-Token": plexToken,
    }),
  });
  if (!response.ok) {
    throw new Error(`Plex device lookup failed with ${response.status}`);
  }
  const payload = xmlParser.parse(await response.text()) as Record<string, unknown>;
  const rawDevices = asArray((payload.MediaContainer as Record<string, unknown> | undefined)?.Device as Record<string, unknown> | Array<Record<string, unknown>> | undefined);
  return rawDevices
    .map((device) => (device && typeof device === "object" ? (device as Record<string, unknown>) : null))
    .filter((device): device is Record<string, unknown> => Boolean(device))
    .map((device) => ({
      machineId: typeof device.clientIdentifier === "string" ? device.clientIdentifier : "",
      name: typeof device.name === "string" ? device.name : typeof device.sourceTitle === "string" ? device.sourceTitle : "Unknown server",
      product: typeof device.product === "string" ? device.product : "",
      owned: device.owned === "1" || device.owned === 1 || device.owned === true,
      provides:
        typeof device.provides === "string"
          ? device.provides.split(",").map((value) => value.trim()).filter(Boolean)
          : [],
      accessToken: typeof device.accessToken === "string" ? device.accessToken : null,
      sourceTitle: typeof device.sourceTitle === "string" ? device.sourceTitle : null,
    }))
    .filter((device) => device.machineId && device.provides.includes("server"));
}

// Plex.tv supports `GET /api/v2/ping` as a "keep this token alive" endpoint.
// Hitting it with a still-valid token before it expires extends the token's
// server-side validity (similar to a session-touch). Overseerr does this once
// daily for every user — that's how it avoids the 7-day JWT expiry trap that
// bit us. Returns true on a successful pong; false on any failure (including
// 401, which means the token is already past saving and an admin must re-link).
export async function pingPlexOwnerToken(settings: AppSettings): Promise<boolean> {
  const ownerToken = settings.auth.plex.ownerToken;
  if (!ownerToken) return false;
  try {
    const response = await fetch("https://plex.tv/api/v2/ping", {
      headers: plexHeaders(settings.auth.plex.productName, settingsClientIdentifier(settings), {
        "X-Plex-Token": ownerToken,
      }),
    });
    if (!response.ok) return false;
    const payload = (await response.json().catch(() => null)) as { pong?: unknown } | null;
    return Boolean(payload && payload.pong);
  } catch {
    return false;
  }
}

export type PlexTokenExpiry = {
  expSeconds: number | null;
  expired: boolean;
  expiresInMs: number | null;
};

// Plex.tv issues short-lived (~7 day) JWT tokens via the PIN flow. The
// payload's `exp` claim tells us when it stops working. This helper is
// best-effort — if the token isn't a JWT or the payload doesn't parse, we
// treat expiry as unknown (callers should treat unknown as "still try it,"
// since older non-JWT Plex tokens never expire).
export function decodePlexTokenExpiry(token: string | null | undefined, nowMs = Date.now()): PlexTokenExpiry {
  if (!token) return { expSeconds: null, expired: false, expiresInMs: null };
  const parts = token.split(".");
  if (parts.length !== 3) return { expSeconds: null, expired: false, expiresInMs: null };
  try {
    const payload = JSON.parse(Buffer.from(parts[1]!, "base64url").toString("utf8")) as { exp?: number };
    const exp = typeof payload.exp === "number" ? payload.exp : null;
    if (exp === null) return { expSeconds: null, expired: false, expiresInMs: null };
    const expiresInMs = exp * 1000 - nowMs;
    return { expSeconds: exp, expired: expiresInMs <= 0, expiresInMs };
  } catch {
    return { expSeconds: null, expired: false, expiresInMs: null };
  }
}

export async function checkPlexUserAccess(settings: AppSettings, userId: string): Promise<boolean> {
  const ownerToken = settings.auth.plex.ownerToken;
  const machineId = settings.auth.plex.machineId;
  if (!ownerToken || !machineId) {
    return false;
  }
  // Short-circuit on a known-expired JWT so we don't waste an HTTP round trip
  // and we surface a clearer error than "401". The admin page reads the same
  // expiry helper to display a re-link prompt.
  if (decodePlexTokenExpiry(ownerToken).expired) {
    throw new Error("Plex owner token has expired — an admin needs to re-link Plex");
  }
  const response = await fetch("https://plex.tv/api/users", {
    headers: plexHeaders(settings.auth.plex.productName, settingsClientIdentifier(settings), {
      "X-Plex-Token": ownerToken,
    }),
  });
  if (!response.ok) {
    if (response.status === 401) {
      throw new Error("Plex owner token was rejected (401) — an admin needs to re-link Plex");
    }
    throw new Error(`Plex shared-user lookup failed with ${response.status}`);
  }
  const payload = xmlParser.parse(await response.text()) as Record<string, unknown>;
  const rawUsers = asArray((payload.MediaContainer as Record<string, unknown> | undefined)?.User as Record<string, unknown> | Array<Record<string, unknown>> | undefined);
  const matched = rawUsers
    .map((user) => (user && typeof user === "object" ? (user as Record<string, unknown>) : null))
    .filter((user): user is Record<string, unknown> => Boolean(user))
    .find((user) => String(user.id ?? "") === userId);
  if (!matched) {
    return false;
  }
  const rawServers = asArray(matched.Server as Record<string, unknown> | Array<Record<string, unknown>> | undefined);
  return rawServers
    .map((server) => (server && typeof server === "object" ? (server as Record<string, unknown>) : null))
    .filter((server): server is Record<string, unknown> => Boolean(server))
    .some((server) => String(server.machineIdentifier ?? "") === machineId);
}

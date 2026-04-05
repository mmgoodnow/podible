import { randomBytes } from "node:crypto";

import { createSessionToken, hashSessionToken, sessionExpiresAt } from "../auth";
import type { AppSettings } from "../types";

import { RpcError, asString, type RpcMethodDefinition } from "./shared";

export function createPublicRpcMethods(
  getMethodsByName: () => Record<string, RpcMethodDefinition>
): Record<string, RpcMethodDefinition> {
  return {
    help: {
      auth: "public",
      readOnly: true,
      summary: "List available RPC methods with read-only flags and auth levels.",
      async handler() {
        const methodsByName = getMethodsByName();
        const methods = Object.keys(methodsByName)
          .sort()
          .map((name) => ({
            name,
            readOnly: Boolean(methodsByName[name]?.readOnly),
            auth: methodsByName[name]?.auth ?? null,
            description: methodsByName[name]?.summary ?? null,
          }));
        return {
          name: "podible-rpc",
          version: "v1",
          methodCount: methods.length,
          methods,
        };
      },
    },

    "system.health": {
      auth: "public",
      readOnly: true,
      summary: "Service health summary (job/release counts and queue size).",
      async handler(ctx) {
        return {
          ok: true,
          ...ctx.repo.getHealthSummary(),
        };
      },
    },

    "system.server": {
      auth: "public",
      readOnly: true,
      summary: "Server runtime metadata (name, runtime, uptime, time).",
      async handler(ctx) {
        return {
          name: "podible-backend",
          runtime: "bun",
          uptimeMs: Date.now() - ctx.startTime,
          now: new Date().toISOString(),
        };
      },
    },

    "auth.beginAppLogin": {
      auth: "public",
      summary: "Create a short-lived app login attempt and return a browser authorize URL.",
      async handler(ctx, params) {
        const redirectUri = asString(params.redirectUri, "redirectUri").trim();
        const settings = ctx.repo.getSettings();
        if (!settings.auth.appRedirectURIs.includes(redirectUri)) {
          throw new RpcError(-32602, "redirectUri is not allowed");
        }
        const now = Date.now();
        ctx.repo.deleteExpiredAppLoginAttempts(new Date(now).toISOString());
        ctx.repo.deleteExpiredAuthCodes(new Date(now).toISOString());
        const attemptId = randomBytes(24).toString("base64url");
        const state = randomBytes(24).toString("base64url");
        const expiresAt = new Date(now + 10 * 60_000).toISOString();
        ctx.repo.createAppLoginAttempt({
          id: attemptId,
          redirectUri,
          state,
          expiresAt,
        });
        const authorizeUrl = new URL(`/auth/app/${encodeURIComponent(attemptId)}`, ctx.request.url).toString();
        return {
          attemptId,
          state,
          authorizeUrl,
          expiresAt,
        };
      },
    },

    "auth.exchange": {
      auth: "public",
      summary: "Exchange a one-time app auth code for a Podible bearer token.",
      async handler(ctx, params) {
        const code = asString(params.code, "code").trim();
        const consumed = ctx.repo.consumeAuthCode(hashSessionToken(code));
        if (!consumed) {
          throw new RpcError(-32000, "Auth code is invalid or expired", { error: "not_found" });
        }
        ctx.repo.deleteAppLoginAttempt(consumed.attempt_id);
        ctx.repo.deleteExpiredAuthCodes(new Date().toISOString());
        const accessToken = createSessionToken();
        const session = ctx.repo.createSession(consumed.user.id, hashSessionToken(accessToken), sessionExpiresAt(), "app");
        return {
          accessToken,
          expiresAt: session.expires_at,
          user: {
            id: consumed.user.id,
            provider: consumed.user.provider,
            username: consumed.user.username,
            displayName: consumed.user.display_name,
            thumbUrl: consumed.user.thumb_url,
            isAdmin: consumed.user.is_admin === 1,
          },
        };
      },
    },
  };
}

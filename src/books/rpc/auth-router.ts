import { randomBytes } from "node:crypto";

import { createSessionToken, hashSessionToken, sessionExpiresAt } from "../auth";

import { defineMethod, defineRouter, type RpcMethodDefinition } from "./framework";
import { emptyParamsSchema, nonEmptyStringSchema } from "./schemas";
import { RpcError } from "./shared";

export function createAuthRouter(
  getMethodsByName: () => Record<string, RpcMethodDefinition>
) {
  return defineRouter({
    help: defineMethod({
      auth: "public",
      readOnly: true,
      summary: "List available RPC methods with read-only flags and auth levels.",
      paramsSchema: emptyParamsSchema,
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
    }),

    auth: defineRouter({
      beginAppLogin: defineMethod({
        auth: "public",
        summary: "Create a short-lived app login attempt and return a browser authorize URL.",
        paramsSchema: emptyParamsSchema.extend({
          redirectUri: nonEmptyStringSchema,
        }),
        async handler(ctx, params) {
          const redirectUri = params.redirectUri.trim();
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
      }),

      exchange: defineMethod({
        auth: "public",
        summary: "Exchange a one-time app auth code for a Podible bearer token.",
        paramsSchema: emptyParamsSchema.extend({
          code: nonEmptyStringSchema,
        }),
        async handler(ctx, params) {
          const consumed = ctx.repo.consumeAuthCode(hashSessionToken(params.code.trim()));
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
      }),

      me: defineMethod({
        auth: "user",
        readOnly: true,
        summary: "Return the current authenticated user and session metadata.",
        paramsSchema: emptyParamsSchema,
        async handler(ctx) {
          const session = ctx.session;
          if (!session) {
            throw new RpcError(-32001, "Unauthorized");
          }
          return {
            user: {
              id: session.user_id,
              provider: session.provider,
              username: session.username,
              displayName: session.display_name,
              thumbUrl: session.thumb_url,
              isAdmin: session.is_admin === 1,
            },
            session: {
              kind: session.kind,
              expiresAt: session.expires_at,
            },
          };
        },
      }),

      logout: defineMethod({
        auth: "user",
        summary: "Invalidate the current authenticated session.",
        paramsSchema: emptyParamsSchema,
        async handler(ctx) {
          const session = ctx.session;
          if (!session) {
            throw new RpcError(-32001, "Unauthorized");
          }
          ctx.repo.deleteSession(session.id);
          return { ok: true };
        },
      }),
    }),
  });
}

import { createTRPCClient, httpBatchLink, httpLink, httpSubscriptionLink, retryLink, splitLink } from "@trpc/client";
import { createTRPCContext } from "@trpc/tanstack-react-query";
import type { AppRouter } from "../../src/trpc";

export const rpc = createTRPCClient<AppRouter>({
  links: [splitLink({
    condition: operation => operation.type === "subscription",
    true: [
      // A proxy can reject the connection outright during a deploy. Reopen it,
      // but don't retry typed errors such as an expired login.
      retryLink({ retry: ({ error }) => !error.data, retryDelayMs: attempt => Math.min(500 * 2 ** (attempt - 1), 15_000) }),
      httpSubscriptionLink({ url: "/api/trpc" }),
    ],
    false: splitLink({
      // Keep independent provider reads and Activity panels off the shared response barrier.
      condition: operation => ["openlibrary.search", "movies.search", "tv.search", "catalog.discovery",
        "catalog.recent", "requests.list", "tv.requestCards",
        "adminWeb.ops", "transferMetrics.published", "transferMetrics.library", "transferMetrics.status"].includes(operation.path),
      true: httpLink({ url: "/api/trpc" }),
      false: httpBatchLink({ url: "/api/trpc" }),
    }),
  })],
});
export const { TRPCProvider, useTRPC } = createTRPCContext<AppRouter>();

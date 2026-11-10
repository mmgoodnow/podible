import { createTRPCClient, httpBatchLink, httpLink, splitLink } from "@trpc/client";
import { createTRPCContext } from "@trpc/tanstack-react-query";
import type { AppRouter } from "../../src/trpc";

export const rpc = createTRPCClient<AppRouter>({
  links: [splitLink({
    // Do not put independent provider reads or Activity panels behind a shared response barrier.
    condition: operation => ["openlibrary.search", "movies.search", "tv.search", "catalog.discovery",
      "catalog.recent", "requests.list", "tv.requestCards",
      "adminWeb.ops", "transferMetrics.published", "transferMetrics.library", "transferMetrics.status"].includes(operation.path),
    true: httpLink({ url: "/api/trpc" }),
    false: httpBatchLink({ url: "/api/trpc" }),
  })],
});
export const { TRPCProvider, useTRPC } = createTRPCContext<AppRouter>();

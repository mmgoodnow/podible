import { createTRPCClient, httpBatchLink, httpLink, splitLink } from "@trpc/client";
import { createTRPCContext } from "@trpc/tanstack-react-query";
import type { AppRouter } from "../../src/trpc";

export const rpc = createTRPCClient<AppRouter>({
  links: [splitLink({
    // Independent providers must not share a response barrier with local reads.
    condition: operation => ["openlibrary.search", "movies.search", "tv.search"].includes(operation.path),
    true: httpLink({ url: "/api/trpc" }),
    false: httpBatchLink({ url: "/api/trpc" }),
  })],
});
export const { TRPCProvider, useTRPC } = createTRPCContext<AppRouter>();

// Shared by the matcher and metrics so historical Decisions counters cannot imply the current default.
export const defaultTvFeedRoutingPolicy = { responsesOnly: true, tableResponses: true } as const;

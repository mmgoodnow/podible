/** Shared execution/API inventory. The dispatcher must exhaust this list. */
export const JOB_TYPES = [
  "tv_acquire", "tv_sync", "tv_reconcile", "tv_shadow_sync", "tv_shadow_evaluate", "tv_feed_match",
  "indexer_feed_sync", "movie_feed_match", "book_feed_match",
  "movie_acquire", "movie_reconcile", "movie_shadow_sync", "movie_shadow_evaluate", "movie_dual_write_sync",
  "acquire", "download", "import", "reconcile", "full_library_refresh",
  "chapter_analysis", "cover_generation", "metadata_hydration",
] as const;

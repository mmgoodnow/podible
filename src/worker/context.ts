import type { ModelProvider } from "@openai/agents-core";

import type { AppSettings } from "../app-types";
import type { BooksRepo } from "../repo";

export type WorkerContext = {
  repo: BooksRepo;
  getSettings: () => AppSettings;
  onLog?: (message: string) => void;
  shouldStop?: () => boolean;
  /** Override the agent model provider (e.g. a fake for tests). */
  modelProvider?: ModelProvider;
};

export function workerLog(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

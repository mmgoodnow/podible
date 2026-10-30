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
  /** In-process resource lanes; sibling steps share execution ownership. */
  activeLocks?: Set<string>;
  execution?: {
    key: string;
    run<T>(name: string, handler: () => T | Promise<T>): Promise<T>;
    sleep(name: string, milliseconds: number): Promise<void>;
    assertActive(): void;
  };
};

export function durableStep<T>(ctx: WorkerContext, name: string, handler: () => T | Promise<T>): Promise<T> {
  return ctx.execution ? ctx.execution.run(name, handler) : Promise.resolve().then(handler);
}

export function workerLog(ctx: WorkerContext, message: string): void {
  if (ctx.onLog) {
    ctx.onLog(message);
    return;
  }
  console.log(message);
}

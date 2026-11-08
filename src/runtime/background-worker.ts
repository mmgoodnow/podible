import { Worker } from "node:worker_threads";
import path from "node:path";
import type { BackgroundWorkerInput, BackgroundWorkerMessage, BackgroundWorkerReady } from "./background-protocol";

export type BackgroundWorkerState = "idle" | "starting" | "ready" | "stopping" | "stopped" | "failed";
export interface BackgroundWorkerOptions {
  databasePath: string;
  /** Trusted local bootstrap injection for acceptance tests, never a request/settings value. */
  entrypoint?: URL;
  startupTimeoutMs?: number;
  shutdownTimeoutMs?: number;
  terminateTimeoutMs?: number;
  heartbeatTimeoutMs?: number;
  onFailure?: (error: Error) => void;
}
const owners = new Map<string, BackgroundWorker>();

/** One thread/engine per app DB. Failed termination never permits a replacement engine. */
export class BackgroundWorker {
  private worker?: Worker;
  private exited?: Promise<number>;
  private starting?: Promise<BackgroundWorkerReady>;
  private stopping?: Promise<{ forced: boolean }>;
  private restarting?: Promise<BackgroundWorkerReady>;
  private plannedStop = false;
  private currentState: BackgroundWorkerState = "idle";
  private watchdog?: ReturnType<typeof setInterval>;
  private progress = { activeEffects: 0, stalledEffects: 0, reservations: 0, observedAt: 0 };
  private readonly databasePath: string;
  constructor(private readonly options: BackgroundWorkerOptions) {
    if (!options.databasePath || options.databasePath === ":memory:")
      throw new Error("Background execution requires a persistent app database");
    this.databasePath = path.resolve(options.databasePath);
    for (const value of [options.startupTimeoutMs, options.shutdownTimeoutMs, options.terminateTimeoutMs, options.heartbeatTimeoutMs])
      if (value !== undefined && (!Number.isSafeInteger(value) || value <= 0 || value > 120_000))
        throw new Error("Invalid background worker deadline");
  }
  get state(): BackgroundWorkerState { return this.currentState; }
  get diagnostics() { return { ...this.progress }; }

  start(): Promise<BackgroundWorkerReady> {
    if (this.starting && (this.currentState === "starting" || this.currentState === "ready")) return this.starting;
    if (this.worker || this.stopping) return Promise.reject(new Error("Stop the existing background worker before starting another"));
    const owner = owners.get(this.databasePath);
    if (owner && owner !== this) return Promise.reject(new Error("A background worker already owns this app database"));
    owners.set(this.databasePath, this);
    this.plannedStop = false;
    this.currentState = "starting";
    let ready = false;
    let failureReported = false;
    let resolveReady!: (message: BackgroundWorkerReady) => void;
    let rejectReady!: (error: Error) => void;
    const initialized = new Promise<BackgroundWorkerReady>((resolve, reject) => { resolveReady = resolve; rejectReady = reject; });
    const failed = (message: string) => {
      this.currentState = "failed";
      const error = new Error(message);
      rejectReady(error);
      if (ready && !this.plannedStop && !failureReported) {
        failureReported = true;
        this.options.onFailure?.(error);
      }
    };
    const timer = setTimeout(() => rejectReady(new Error("Background worker startup timed out")), this.options.startupTimeoutMs ?? 60_000);
    try {
      const data: BackgroundWorkerInput = { databasePath: this.databasePath };
      const worker = new Worker(this.options.entrypoint ?? new URL("./worker-entry.ts", import.meta.url), { workerData: data });
      this.worker = worker;
      this.exited = new Promise(resolve => worker.once("exit", (code: number) => {
        clearTimeout(timer);
        clearInterval(this.watchdog);
        this.worker = undefined;
        if (owners.get(this.databasePath) === this) owners.delete(this.databasePath);
        this.currentState = this.plannedStop ? "stopped" : "failed";
        rejectReady(new Error("Background worker exited before readiness"));
        resolve(code);
        if (!this.plannedStop) failed("Background worker exited unexpectedly");
      }));
      worker.on("message", (message: BackgroundWorkerMessage) => {
        if (message?.type === "ready" && !ready && !this.plannedStop) {
          if (message.pid !== process.pid || message.busyTimeoutMs !== 2000 || !Number.isInteger(message.threadId) || message.threadId <= 0) {
            rejectReady(new Error("Invalid background worker readiness"));
            return;
          }
          ready = true;
          clearTimeout(timer);
          this.currentState = "ready";
          this.progress = { activeEffects: 0, stalledEffects: 0, reservations: 0, observedAt: Date.now() };
          const heartbeatTimeout = this.options.heartbeatTimeoutMs ?? 60_000;
          this.watchdog = setInterval(() => {
            if (!this.plannedStop && Date.now() - this.progress.observedAt > heartbeatTimeout)
              failed("Background worker stopped responding; refusing a second engine until confirmed exit");
          }, Math.min(1000, heartbeatTimeout));
          this.watchdog.unref();
          resolveReady(message);
        } else if (message?.type === "heartbeat" && ready && !this.plannedStop) {
          if ([message.activeEffects, message.stalledEffects, message.reservations].every(value => Number.isSafeInteger(value) && value >= 0))
            this.progress = { activeEffects: message.activeEffects, stalledEffects: message.stalledEffects,
              reservations: message.reservations, observedAt: Date.now() };
        } else if (message?.type === "failed") {
          failed("Background worker initialization or execution failed");
        }
      });
      worker.on("error", () => {
        failed("Background worker failed");
      });
    } catch (error) {
      clearTimeout(timer);
      owners.delete(this.databasePath);
      this.currentState = "failed";
      rejectReady(error instanceof Error ? error : new Error("Could not create background worker"));
    }
    this.starting = initialized.catch(async error => { await this.stop(); throw error; });
    return this.starting;
  }

  stop(): Promise<{ forced: boolean }> {
    if (this.stopping) return this.stopping;
    if (!this.worker) { this.starting = undefined; return Promise.resolve({ forced: false }); }
    this.plannedStop = true;
    clearInterval(this.watchdog);
    this.currentState = "stopping";
    const worker = this.worker, exited = this.exited!;
    try { worker.postMessage({ type: "stop" }); } catch { /* Exit may already be queued. */ }
    this.stopping = (async () => {
      const graceful = await this.waitForExit(exited, this.options.shutdownTimeoutMs ?? 5_000);
      if (graceful) return { forced: false };
      void worker.terminate().catch(() => {});
      if (!(await this.waitForExit(exited, this.options.terminateTimeoutMs ?? 2_000))) {
        this.currentState = "failed";
        throw new Error("Background worker termination was not confirmed; refusing replacement");
      }
      return { forced: true };
    })().finally(() => {
      this.starting = undefined;
      this.stopping = undefined;
    });
    return this.stopping;
  }

  restart(): Promise<BackgroundWorkerReady> {
    if (!this.restarting) this.restarting = (async () => {
      await this.stop();
      return this.start();
    })().finally(() => { this.restarting = undefined; });
    return this.restarting;
  }
  private async waitForExit(exited: Promise<number>, timeout: number): Promise<boolean> {
    let timer: ReturnType<typeof setTimeout> | undefined;
    try { return await Promise.race([exited.then(() => true), new Promise<false>(resolve => { timer = setTimeout(() => resolve(false), timeout); })]); }
    finally { if (timer) clearTimeout(timer); }
  }
}

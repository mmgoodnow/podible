import { TRPCError } from "@trpc/server";

export const acquisitionEventsPath = "/api/trpc/acquisition.changes";

/** Coalesce invalidations; a fresh snapshot on connect covers missed changes. */
export class AcquisitionEvents {
  private listeners = new Set<{ notify(): void; close(): void }>();
  private closed = false;

  notify() {
    for (const listener of this.listeners) listener.notify();
  }

  close() {
    this.closed = true;
    for (const listener of this.listeners) listener.close();
  }

  async *changes(signal: AbortSignal) {
    if (signal.aborted) return;
    if (this.listeners.size >= 100) throw new TRPCError({ code: "TOO_MANY_REQUESTS" });
    let pending = true;
    let wake: (() => void) | undefined;
    const stop = () => wake?.();
    const listener = { notify: () => { pending = true; wake?.(); }, close: stop };
    this.listeners.add(listener);
    signal.addEventListener("abort", stop, { once: true });
    try {
      while (!signal.aborted) {
        // An error (rather than a completed subscription) tells tRPC to reconnect.
        if (this.closed) throw new TRPCError({ code: "SERVICE_UNAVAILABLE" });
        if (pending) {
          pending = false;
          yield { type: "change" } as const;
        } else {
          await new Promise<void>(resolve => { wake = resolve; });
          wake = undefined;
        }
      }
    } finally {
      this.listeners.delete(listener);
      signal.removeEventListener("abort", stop);
    }
  }
}

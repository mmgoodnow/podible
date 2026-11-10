export const acquisitionEventsPath = "/api/events/acquisition";
const frame = new TextEncoder().encode("event: change\ndata: {}\n\n");
const heartbeat = new TextEncoder().encode(": keepalive\n\n");

/** Invalidation only: a fresh authorized snapshot on connect covers missed events. */
export class AcquisitionEvents {
  private listeners = new Set<{ notify(): void; close(): void }>();

  notify() {
    for (const listener of this.listeners) listener.notify();
  }

  close() {
    for (const listener of this.listeners) listener.close();
  }

  response(signal: AbortSignal) {
    if (this.listeners.size >= 100) return new Response("Too many event connections", { status: 503 });
    let cleanup: () => void;
    let pendingChange = false;
    const stream = new ReadableStream<Uint8Array>({
      start: controller => {
        const send = (data: Uint8Array) => {
          if ((controller.desiredSize ?? 0) > 0) controller.enqueue(data);
        };
        const notify = () => {
          if ((controller.desiredSize ?? 0) > 0) send(frame);
          else pendingChange = true;
        };
        const keepalive = setInterval(() => send(heartbeat), 20_000);
        keepalive.unref();
        const abort = () => { cleanup(); controller.close(); };
        const listener = { notify, close: abort };
        cleanup = () => {
          this.listeners.delete(listener);
          clearInterval(keepalive);
          signal.removeEventListener("abort", abort);
        };
        this.listeners.add(listener);
        signal.addEventListener("abort", abort, { once: true });
        if (signal.aborted) abort();
        else notify();
      },
      pull: controller => {
        if (pendingChange) {
          pendingChange = false;
          controller.enqueue(frame);
        }
      },
      cancel: () => cleanup(),
    });
    return new Response(stream, { headers: {
      "Content-Type": "text/event-stream; charset=utf-8",
      "Cache-Control": "no-store, no-transform",
      "X-Accel-Buffering": "no",
    } });
  }
}

import os from "node:os";
import { Worker } from "node:worker_threads";

import type { AnalysisResult, TranscriptAnalysisInput } from "./chapter-analysis";

type PendingTask = {
  id: number;
  input: TranscriptAnalysisInput;
  resolve: (value: AnalysisResult) => void;
  reject: (reason?: unknown) => void;
};

type WorkerSlot = {
  worker: Worker;
  current: PendingTask | null;
};

type AnalyzeResponse =
  | {
      id: number;
      result: AnalysisResult;
    }
  | {
      id: number;
      error: {
        name: string;
        message: string;
        stack?: string;
      };
    };

function transcriptAnalysisWorkerCount(): number {
  const available = os.availableParallelism?.() ?? os.cpus().length;
  return Math.max(1, Math.min(4, available > 1 ? available - 1 : 1));
}

class TranscriptAnalysisPool {
  private readonly workerUrl = new URL("./transcript-analysis-worker.ts", import.meta.url);
  private readonly size = transcriptAnalysisWorkerCount();
  private readonly slots: WorkerSlot[] = [];
  private readonly queue: PendingTask[] = [];
  private nextId = 1;

  enqueue(input: TranscriptAnalysisInput): Promise<AnalysisResult> {
    return new Promise((resolve, reject) => {
      this.queue.push({
        id: this.nextId++,
        input,
        resolve,
        reject,
      });
      this.ensureWorkers();
      this.pump();
    });
  }

  private ensureWorkers(): void {
    while (this.slots.length < this.size) {
      this.slots.push(this.createWorkerSlot());
    }
  }

  private createWorkerSlot(): WorkerSlot {
    const slot: WorkerSlot = {
      worker: new Worker(this.workerUrl),
      current: null,
    };
    slot.worker.unref();
    slot.worker.on("message", (message: AnalyzeResponse) => this.onMessage(slot, message));
    slot.worker.on("error", (error) => this.onWorkerFailure(slot, error));
    slot.worker.on("exit", (code) => {
      if (code !== 0) {
        this.onWorkerFailure(slot, new Error(`transcript analysis worker exited with code ${code}`));
      }
    });
    return slot;
  }

  private replaceWorker(slot: WorkerSlot): void {
    const index = this.slots.indexOf(slot);
    if (index === -1) return;
    this.slots[index] = this.createWorkerSlot();
  }

  private onMessage(slot: WorkerSlot, message: AnalyzeResponse): void {
    if (!slot.current || slot.current.id !== message.id) return;
    const task = slot.current;
    slot.current = null;
    if ("result" in message) {
      task.resolve(message.result);
    } else {
      const error = new Error(message.error.message);
      error.name = message.error.name;
      if (message.error.stack) error.stack = message.error.stack;
      task.reject(error);
    }
    this.pump();
  }

  private onWorkerFailure(slot: WorkerSlot, error: unknown): void {
    const task = slot.current;
    slot.current = null;
    if (task) {
      task.reject(error);
    }
    try {
      slot.worker.terminate();
    } catch {
      // ignore termination errors while replacing a crashed worker
    }
    this.replaceWorker(slot);
    this.pump();
  }

  private pump(): void {
    for (const slot of this.slots) {
      if (slot.current || this.queue.length === 0) continue;
      const task = this.queue.shift();
      if (!task) break;
      slot.current = task;
      slot.worker.postMessage({
        id: task.id,
        input: task.input,
      });
    }
  }
}

const transcriptAnalysisPool = new TranscriptAnalysisPool();

export function analyzeTranscriptInWorkerPool(input: TranscriptAnalysisInput): Promise<AnalysisResult> {
  return transcriptAnalysisPool.enqueue(input);
}

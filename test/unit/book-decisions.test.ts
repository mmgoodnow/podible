import { expect, spyOn, test } from "bun:test";
import { Database } from "bun:sqlite";
import { Usage, type ModelProvider, type ModelResponse } from "@openai/agents-core";
import { agentUsagePrometheus, withAgentUsageAccounting } from "../../src/agent-usage";
import { runMigrations } from "../../src/db";
import { selectSearchCandidates } from "../../src/library/agents";
import { decideBookRelease } from "../../src/library/decisions";
import type { TorznabResult } from "../../src/library/torznab";
import { BooksRepo } from "../../src/repo";
import { defaultSettings } from "../../src/settings";
import { processAcquireJob } from "../../src/worker/acquire";
import { selectWithIndexerRecovery } from "../../src/worker/indexer-search";

const evidence = {
  target: { title: "The Left Hand of Darkness", author: "Ursula K. Le Guin", media: "ebook" as const },
  candidates: [{ title: "The Left Hand of Darkness - EPUB" }, { title: "The Left Hand of Darkness - audiobook" }],
};
const options = { apiKey: "mock-key-not-a-secret", timeoutMs: 500, minConfidence: 0.85, minMargin: 0.2 };
const probabilities = [
  { value: "c1", probability: 0.92 }, { value: "c2", probability: 0.03 }, { value: "abstain", probability: 0.05 },
];
const answer = { type: "choice", name: "book_release", choice: "c1", confidence: 0.9, probabilities };
const usage = { input_tokens: 200, output_tokens: 0, input_tokens_details: { cached_tokens: 50, cache_write_tokens: 20 } };
const wire = (answers: unknown[] = [answer]) => ({ model: "gpt-6-luna", answers, usage });
const mock = (body: unknown = wire()) => ({ ...options, fetch: async () => Response.json(body) });

test("sends only allowlisted evidence with request-local choice IDs and uses the injected key", async () => {
  const target = { ...evidence.target, title: ` ${evidence.target.title} `, userId: "private-user-id", token: "private-token" };
  const candidate = { ...evidence.candidates[0]!, id: 7812, provider: "private-tracker", guid: "private-guid",
    url: "https://private.invalid/torrent?passkey=private-passkey", infoHash: "private-hash" };
  let calls = 0;
  const result = await decideBookRelease({ target, candidates: [candidate, evidence.candidates[1]!] }, {
    ...options,
    async fetch(url, init) {
      calls++;
      expect(url).toBe("https://api.openai.com/v1/decisions");
      expect(init.method).toBe("POST");
      expect(init.redirect).toBe("error");
      expect(new Headers(init.headers).get("authorization")).toBe(`Bearer ${options.apiKey}`);
      expect(new Headers(init.headers).get("content-type")).toBe("application/json");
      const body = JSON.parse(String(init.body));
      expect(body.model).toBe("gpt-6-luna");
      expect(body.input).toEqual([{ role: "user", content: [{ type: "input_json", data: {
        target: evidence.target,
        candidates: [{ id: "c1", title: candidate.title }, { id: "c2", title: evidence.candidates[1]!.title }],
      } }] }]);
      expect(body.questions).toEqual([expect.objectContaining({
        name: "book_release", type: "choice", choices: [{ value: "c1" }, { value: "c2" }, expect.objectContaining({ value: "abstain" })],
      })]);
      expect(String(init.body)).not.toContain("private-");
      expect(String(init.body)).not.toContain(options.apiKey);
      return Response.json({ ...wire(), model: "gpt-6-luna-2026-10-06", private: "provider metadata" });
    },
  });
  expect(calls).toBe(1);
  expect(result).toMatchObject({ kind: "selected", candidateIndex: 0, confidence: 0.9,
    usage: { input: 200, output: 0, cached: 50, cacheWrite: 20 } });
  expect(JSON.stringify(result)).not.toContain("provider metadata");
});

test("maps the selected alias to the original candidate index regardless of probability order", async () => {
  const result = await decideBookRelease(evidence, mock(wire([{ ...answer, choice: "c2", probabilities: [
    { value: "abstain", probability: 0.05 }, { value: "c2", probability: 0.9 }, { value: "c1", probability: 0.05 },
  ] }])));
  expect(result).toMatchObject({ kind: "selected", candidateIndex: 1 });
});

test("a single candidate still competes with abstain", async () => {
  const result = await decideBookRelease({ ...evidence, candidates: evidence.candidates.slice(0, 1) }, mock(wire([{ ...answer, probabilities: [
    { value: "c1", probability: 0.5 }, { value: "abstain", probability: 0.5 },
  ] }])));
  expect(result).toMatchObject({ kind: "ambiguous", reason: "insufficient_margin", margin: 0 });
});

test("requires both confidence and margin, counting abstain as a runner-up", async () => {
  expect(await decideBookRelease(evidence, mock(wire([{ ...answer, confidence: 0.84 }]))))
    .toMatchObject({ kind: "ambiguous", reason: "low_confidence" });
  expect(await decideBookRelease(evidence, mock(wire([{ ...answer, probabilities: [
    { value: "c1", probability: 0.55 }, { value: "c2", probability: 0 }, { value: "abstain", probability: 0.45 },
  ] }])))).toMatchObject({ kind: "ambiguous", reason: "insufficient_margin" });
});

test("distinguishes an explicit model abstention from a refusal", async () => {
  expect(await decideBookRelease(evidence, mock(wire([{ ...answer, choice: "abstain", probabilities: [
    { value: "c1", probability: 0.04 }, { value: "c2", probability: 0.03 }, { value: "abstain", probability: 0.93 },
  ] }])))).toMatchObject({ kind: "abstain", reason: "model_abstained" });
  const result = await decideBookRelease(evidence, mock(wire([{ type: "refusal", name: "book_release", detail: "private provider text" }])));
  expect(result).toMatchObject({ kind: "error", reason: "refusal" });
  expect(JSON.stringify(result)).not.toContain("private provider text");
});

test.each([
  ["invented choice", wire([{ ...answer, choice: "c42" }])],
  ["boolean choice", wire([{ ...answer, choice: true }])],
  ["wrong question", wire([{ ...answer, name: "other_question" }])],
  ["extra answer", wire([answer, answer])],
  ["no answers", wire([])],
  ["wrong model", { ...wire(), model: "another-model" }],
  ["out of range confidence", wire([{ ...answer, confidence: 1.01 }])],
  ["out of range probability", wire([{ ...answer, probabilities: [{ ...probabilities[0], probability: 1.01 }, ...probabilities.slice(1)] }])],
  ["duplicate probability IDs", wire([{ ...answer, probabilities: [probabilities[0], probabilities[0], probabilities[2]] }])],
  ["missing probability", wire([{ ...answer, probabilities: probabilities.slice(0, 2) }])],
  ["unknown probability ID", wire([{ ...answer, probabilities: [probabilities[0], probabilities[1], { value: "other", probability: 0.05 }] }])],
  ["probabilities do not sum to one", wire([{ ...answer, probabilities: probabilities.map(entry => ({ ...entry, probability: 0.1 })) }])],
  ["choice below runner-up", wire([{ ...answer, choice: "c2" }])],
] as const)("rejects a malformed answer: %s", async (_, payload) => {
  expect(await decideBookRelease(evidence, mock(payload))).toMatchObject({ kind: "error", reason: "invalid_response" });
});

test("omits invalid usage rather than returning raw data or treating it as zero", async () => {
  const result = await decideBookRelease(evidence, mock({ ...wire(), usage: { ...usage, input_tokens: 5 } }));
  expect(result).toMatchObject({ kind: "selected", candidateIndex: 0 });
  expect(result.usage).toBeUndefined();
});

test("does not call transport for empty, unsafe, oversized, or misconfigured requests", async () => {
  let calls = 0;
  const deps = { ...options, fetch: async () => { calls++; return Response.json(wire()); } };
  expect(await decideBookRelease({ ...evidence, candidates: [] }, deps)).toEqual({ kind: "abstain", reason: "no_candidates" });
  for (const candidates of [
    [{ title: "https://private.invalid/ebook?passkey=secret" }], [{ title: "Novel sk-test-key-abcdef" }],
    [{ title: "x".repeat(513) }], Array.from({ length: 13 }, () => evidence.candidates[0]!),
  ]) {
    expect(await decideBookRelease({ ...evidence, candidates }, deps)).toEqual({ kind: "error", reason: "invalid_input" });
  }
  for (const invalid of [{ apiKey: " " }, { apiKey: "bad\nheader" }, { timeoutMs: 0 }, { minConfidence: NaN }, { minMargin: 0 }]) {
    expect(await decideBookRelease(evidence, { ...deps, ...invalid })).toEqual({ kind: "error", reason: "invalid_configuration" });
  }
  expect(calls).toBe(0);
});

test("returns bounded error categories without reading HTTP error JSON or echoing exceptions", async () => {
  const providerError = new Response("private provider text", { status: 429 });
  const read = spyOn(providerError, "json");
  expect(await decideBookRelease(evidence, { ...options, fetch: async () => providerError }))
    .toEqual({ kind: "error", reason: "http", status: 429 });
  expect(read).not.toHaveBeenCalled();
  expect(await decideBookRelease(evidence, { ...options, fetch: async () => new Response("private non-JSON body") }))
    .toEqual({ kind: "error", reason: "invalid_response" });
  expect(await decideBookRelease(evidence, { ...options, fetch: async () => { throw new Error(options.apiKey); } }))
    .toEqual({ kind: "error", reason: "transport" });
});

test("aborts on timeout even when the injected transport never settles", async () => {
  let signal: AbortSignal | null | undefined;
  expect(await decideBookRelease(evidence, { ...options, timeoutMs: 10, fetch: async (_, init) => {
    signal = init.signal;
    return new Promise<Response>(() => {});
  } })).toEqual({ kind: "error", reason: "timeout" });
  expect(signal?.aborted).toBe(true);
});

test("the timeout also covers reading a successful response body", async () => {
  let cancelled = false;
  const response = new Response(new ReadableStream({ cancel() { cancelled = true; } }));
  expect(await decideBookRelease(evidence, { ...options, timeoutMs: 10, fetch: async () => response }))
    .toEqual({ kind: "error", reason: "timeout" });
  expect(cancelled).toBe(true);
});

test("bounds even successful Decisions response bodies", async () => {
  expect(await decideBookRelease(evidence, mock({ ...wire(), extra: "x".repeat(33 * 1024) })))
    .toEqual({ kind: "error", reason: "invalid_response" });
});

const offer: TorznabResult = {
  title: "Dune Frank Herbert [EPUB]", mediaType: "ebook", provider: "fixture", guid: "private-guid",
  url: "https://fixture.invalid/download?key=private-indexer-key", sizeBytes: 100, seeders: 5, leechers: 0, infoHash: null, raw: {},
};
const settings = defaultSettings({ agents: { ...defaultSettings().agents, apiKey: options.apiKey, editionPreference: "Narrated only" } });
const singletonAnswer = { ...answer, confidence: 0.98, probabilities: [{ value: "c1", probability: 0.98 }, { value: "abstain", probability: 0.02 }] };

function searchFixture(db: Database) {
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({ title: "Dune", author: "Frank Herbert" });
  const input = { query: "Dune Frank Herbert", media: "ebook" as const, book, results: [offer] };
  return { repo, book, input };
}

function responsesProvider(output: unknown, inspect = false) {
  let turns = 0;
  const provider: ModelProvider = { getModel: () => ({
    async getResponse(request): Promise<ModelResponse> {
      turns++;
      expect(request.tools.some(tool => tool.name === "inspect")).toBe(true);
      if (inspect && turns === 1) return { usage: new Usage(), output: [{
        type: "function_call", callId: "mock-inspection", name: "inspect", arguments: '{"index":99}', status: "completed",
      }] };
      if (inspect) expect(JSON.stringify(request.input)).toContain("Invalid index 99");
      return { usage: new Usage(), output: [{ type: "message", role: "assistant", status: "completed",
        content: [{ type: "output_text", text: JSON.stringify(output) }] }] };
    },
    async *getStreamedResponse() { throw new Error("Unexpected stream"); },
  }) };
  return { provider, turns: () => turns };
}

test("eligible singleton uses Decisions first and meters estimated usage without changing the selection shape", async () => {
  using db = new Database(":memory:");
  const { repo, input } = searchFixture(db);
  const responses = responsesProvider({});
  const selected = await withAgentUsageAccounting(repo, () => selectSearchCandidates(settings, input, {
    repo, modelProvider: responses.provider, preferSearchAgent: true, freshAcquisitionDecisions: mock(wire([singletonAnswer])),
  }));
  expect(selected.mode).toBe("agent");
  expect(selected.selections).toEqual([{ parts: [offer], manifestation: { label: null, editionNote: null, language: null } }]);
  expect(responses.turns()).toBe(0);
  expect(agentUsagePrometheus(repo)).toContain('podible_agent_model_calls_total{workflow="book-selection",api="decisions",model="gpt-6-luna"} 1');
});

test.each([
  ["ambiguous", () => Response.json(wire([{ ...singletonAnswer, confidence: 0.4 }]))],
  ["abstain", () => Response.json(wire([{ ...singletonAnswer, choice: "abstain", probabilities: [
    { value: "c1", probability: 0.02 }, { value: "abstain", probability: 0.98 },
  ] }]))],
  ["HTTP error", () => new Response("private provider error", { status: 503 })],
] as const)("%s falls through to the same tool-using Responses selector", async (_, response) => {
  using db = new Database(":memory:");
  const { repo, input } = searchFixture(db);
  const responses = responsesProvider({ selections: [{ parts: [0], manifestation: { label: "Tool result" } }], confidence: 0.8 }, true);
  const selected = await selectSearchCandidates(settings, input, {
    repo, modelProvider: responses.provider, freshAcquisitionDecisions: { ...options, fetch: async () => response() },
  });
  expect(selected.selections[0]?.manifestation.label).toBe("Tool result");
  expect(responses.turns()).toBe(2);
});

test("opt-in leaves deterministic and Responses behavior intact outside the cohort", async () => {
  using db = new Database(":memory:");
  const { repo, book, input } = searchFixture(db);
  let calls = 0;
  const responses = responsesProvider({ selections: [{ parts: [0], matchingIndexes: [[1]] }], confidence: 0.9 });
  const runtime = { repo, modelProvider: responses.provider, freshAcquisitionDecisions: { ...options, fetch: async () => {
    calls++;
    return Response.json(wire([singletonAnswer]));
  } } };
  const multipart = { ...offer, title: "Dune Frank Herbert [EPUB] Part 1 of 2" };
  expect((await selectSearchCandidates(settings, { ...input, results: [multipart] }, runtime)).mode).toBe("deterministic");
  const copy = { ...offer, url: "https://fixture.invalid/copy", seeders: 1 };
  const multiple = { ...input, results: [offer, copy] };
  expect((await selectSearchCandidates(settings, multiple, runtime)).mode).toBe("deterministic");
  const equivalent = await selectSearchCandidates(settings, multiple, { ...runtime, preferSearchAgent: true });
  expect(equivalent.selections[0]?.matchingCopies).toEqual([[copy]]);
  const singletonResponses = responsesProvider({ selections: [{ parts: [0] }], confidence: 0.9 });
  for (const bypass of [{ forceAgent: true }, { priorFailure: true }, { rejectedGuids: ["old-guid"] }]) {
    const result = await selectSearchCandidates(settings, { ...input, ...bypass }, {
      ...runtime, modelProvider: singletonResponses.provider, preferSearchAgent: true,
    });
    expect(result.selections[0]?.parts).toEqual([offer]);
  }
  repo.addManifestation({ bookId: book.id, kind: "ebook" });
  expect((await selectSearchCandidates(settings, input, runtime)).mode).toBe("deterministic");
  expect(responses.turns()).toBe(1);
  expect(singletonResponses.turns()).toBe(3);
  expect(calls).toBe(0);
});

test("the worker reaches Decisions for a complete fresh ebook search even with an audio preference", async () => {
  using db = new Database(":memory:");
  const { repo, book } = searchFixture(db);
  const job = repo.createJob({ type: "acquire", bookId: book.id, payload: { bookId: book.id, media: ["ebook"] } });
  let decisionCalls = 0;
  let selection: unknown;
  const mockFetch = async (request: string | URL | Request, init?: RequestInit) => {
    const url = new URL(request instanceof Request ? request.url : String(request));
    if (url.hostname === "api.openai.com" && url.pathname === "/v1/responses") return new Response(null, { status: 400 });
    if (url.hostname === "api.openai.com" && url.pathname === "/v1/decisions") {
      decisionCalls++;
      expect(new Headers(init?.headers).get("authorization")).toBe(`Bearer ${options.apiKey}`);
      expect(String(init?.body)).not.toMatch(/https?:|private-indexer-key|private-guid|Narrated only/);
      return Response.json(wire([singletonAnswer]));
    }
    if (url.hostname === "fixture.invalid" && url.pathname === "/api") return new Response(
      `<rss><channel><item><title>${offer.title}</title><enclosure url="${offer.url}" length="100" /></item></channel></rss>`);
    throw new Error("Unexpected mock request");
  };
  const transport = spyOn(globalThis, "fetch").mockImplementation(Object.assign(mockFetch, { preconnect: fetch.preconnect }));
  try {
    const stop = new Error("stop before snatch");
    await expect(processAcquireJob({
      repo, getSettings: () => ({ ...settings, torznab: [{ name: "fixture", baseUrl: "https://fixture.invalid", apiKey: "indexer-mock" }] }),
      onLog: () => {}, execution: {
        key: "mock-execution", assertActive() {}, async sleep() { throw new Error("Unexpected sleep"); },
        async run(name, handler) {
          const result = await handler();
          if (name === "ebook:selection") { selection = result; throw stop; }
          return result;
        },
      },
    }, job)).rejects.toBe(stop);
    expect(selection).toMatchObject({ reason: "Decisions matched the only eligible ebook offer" });
    expect(decisionCalls).toBe(1);
  } finally { transport.mockRestore(); }
});

test("checkpoint replay and partial search results cannot opt into Decisions", async () => {
  using db = new Database(":memory:");
  const { repo, book } = searchFixture(db);
  for (const checkpoint of [[offer], { kind: "indexer-search-deferred", results: [offer], failures: [{ status: 429 }] }]) {
    const result = await selectWithIndexerRecovery({
      repo, getSettings: () => settings, onLog: () => {}, execution: {
        key: "mock", assertActive() {}, async sleep() { throw new Error("Unexpected sleep"); },
        async run<T>(name: string) { expect(name).toBe("ebook:search"); return checkpoint as T; },
      },
    }, { query: "Dune", media: "ebook", jobId: 1, bookId: book.id }, async (results, step, completeFreshBatch) => {
      expect(step).toBe("ebook:selection");
      expect(completeFreshBatch).toBe(false);
      return { selections: results };
    });
    expect(result.selections).toEqual([offer]);
  }
});

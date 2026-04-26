import OpenAI from "openai";

import type { ImportInspectionFile } from "./importer";
import type { BooksRepo } from "../repo";
import { rankSearchResults } from "./service";
import type { TorznabResult } from "./torznab";
import { getOrFetchCachedTorrentBytes, inspectTorrentFiles } from "./torrent-cache";
import { normalizeInfoHash } from "./torrent";
import type { AppSettings, MediaType } from "../app-types";

/**
 * Optional agent decision layer.
 *
 * Deterministic ranking/selection stays the default path. Responses API is
 * consulted only when configured and triggered (forced, prior failure, or low
 * confidence). Agent invocation failures surface as errors to the caller.
 */
type DecisionMode = "deterministic" | "agent";
type DecisionTrigger = "none" | "forced" | "prior_failure" | "low_confidence";

type SearchSelectionResult = {
  selections: SearchSelection[];
  confidence: number;
  mode: DecisionMode;
  trigger: DecisionTrigger;
  reason: string;
  error: string | null;
};

type SearchSelection = {
  manifestation: {
    label: string | null;
    editionNote: string | null;
  };
  parts: TorznabResult[];
};

type ManualImportSelectionResult = {
  selectedPaths: string[];
  confidence: number;
  mode: DecisionMode;
  trigger: DecisionTrigger;
  reason: string;
  error: string | null;
};

type SearchSelectionInput = {
  query: string;
  media: MediaType;
  results: TorznabResult[];
  rejectedUrls?: string[];
  rejectedGuids?: string[];
  rejectedInfoHashes?: string[];
  forceAgent?: boolean;
  priorFailure?: boolean;
  editionPreference?: string;
  book?: {
    id: number;
    title: string;
    author: string;
  } | null;
};

type ManualImportSelectionInput = {
  mediaType: MediaType;
  files: ImportInspectionFile[];
  rejectedSourcePaths?: string[];
  forceAgent?: boolean;
  priorFailure?: boolean;
  book?: {
    id: number;
    title: string;
    author: string;
  } | null;
};

type SearchAgentOutput = {
  selections?: Array<{
    manifestation?: {
      label?: string | null;
      editionNote?: string | null;
      edition_note?: string | null;
    } | null;
    parts?: number[];
  }>;
  confidence: number;
  reason: string;
};

type SearchSelectionRuntime = {
  repo?: BooksRepo;
};

type ManualImportAgentOutput = {
  selectedIndices: number[];
  confidence: number;
  reason: string;
};

function samePathSet(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const aSet = new Set(a);
  if (aSet.size !== b.length) return false;
  for (const item of b) {
    if (!aSet.has(item)) return false;
  }
  return true;
}

function clamp01(value: number): number {
  if (!Number.isFinite(value)) return 0;
  if (value < 0) return 0;
  if (value > 1) return 1;
  return value;
}

function humanSize(value: number | null | undefined): string | null {
  if (typeof value !== "number" || !Number.isFinite(value) || value < 0) return null;
  const units = ["B", "KB", "MB", "GB", "TB"];
  let n = value;
  let i = 0;
  while (n >= 1024 && i < units.length - 1) {
    n /= 1024;
    i += 1;
  }
  return `${i === 0 ? Math.round(n) : n.toFixed(1)} ${units[i]}`;
}

function mdInline(value: unknown): string {
  const text = String(value ?? "");
  return text.replace(/`/g, "\\`");
}

function mdTableCell(value: unknown): string {
  const text = String(value ?? "");
  return text.replace(/\|/g, "\\|").replace(/\n/g, " ");
}

function formatTargetMedia(media: MediaType): string {
  return media === "ebook" ? "ebook (not audio)" : "audio (not ebook)";
}

function configuredAgent(settings: AppSettings) {
  const apiKey = settings.agents.apiKey?.trim();
  if (!apiKey) return null;
  return {
    apiKey,
    model: settings.agents.model || "gpt-5-mini",
    timeoutMs: Math.max(1000, Math.trunc(settings.agents.timeoutMs || 30000)),
  };
}

function determineTrigger(
  settings: AppSettings,
  _domain: "search" | "manualImport",
  confidence: number,
  options: { forceAgent?: boolean; priorFailure?: boolean }
): DecisionTrigger {
  if (options.forceAgent) return "forced";
  const lowConfidenceThreshold =
    typeof settings.agents?.lowConfidenceThreshold === "number" ? settings.agents.lowConfidenceThreshold : 0.45;
  if (options.priorFailure) return "prior_failure";
  if (confidence < lowConfidenceThreshold) return "low_confidence";
  return "none";
}

function openAiClient(apiKey: string, timeoutMs: number): OpenAI {
  return new OpenAI({
    apiKey,
    timeout: timeoutMs,
  });
}

async function callResponsesJson<T>(settings: AppSettings, system: string, user: string): Promise<T> {
  const agent = configuredAgent(settings);
  if (!agent) {
    throw new Error("OpenAI agent not configured");
  }
  const client = openAiClient(agent.apiKey, agent.timeoutMs);
  const response = await client.responses.create(
    {
      model: agent.model,
      input: [
        {
          role: "system",
          content: [{ type: "input_text", text: system }],
        },
        {
          role: "user",
          content: [{ type: "input_text", text: user }],
        },
      ],
    },
    {
      timeout: agent.timeoutMs,
    }
  );
  const text = (response.output_text ?? "").trim();
  if (!text) {
    throw new Error("OpenAI response text was empty");
  }
  return JSON.parse(text) as T;
}

function parseJsonObject(text: string): Record<string, unknown> {
  const parsed = JSON.parse(text) as unknown;
  if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
    throw new Error("Expected JSON object");
  }
  return parsed as Record<string, unknown>;
}

async function callSearchResponsesWithInspectTool(
  settings: AppSettings,
  system: string,
  user: string,
  ranked: ReturnType<typeof rankSearchResults>,
  repo: BooksRepo
): Promise<SearchAgentOutput> {
  const agent = configuredAgent(settings);
  if (!agent) {
    throw new Error("OpenAI agent not configured");
  }
  const client = openAiClient(agent.apiKey, agent.timeoutMs);
  const tool = {
    type: "function" as const,
    name: "inspect",
    description:
      "Download and inspect the .torrent file for a candidate by index. Returns the torrent file list so you can confirm whether it contains the target book.",
    strict: true,
    parameters: {
      type: "object",
      additionalProperties: false,
      properties: {
        index: { type: "integer", minimum: 0 },
      },
      required: ["index"],
    },
  };

  let response = await client.responses.create(
    {
      model: agent.model,
      tools: [tool],
      input: [
        { role: "system", content: [{ type: "input_text", text: system }] },
        { role: "user", content: [{ type: "input_text", text: user }] },
      ],
    },
    { timeout: agent.timeoutMs }
  );

  for (let step = 0; step < 4; step += 1) {
    const outputItems = Array.isArray((response as any).output) ? ((response as any).output as any[]) : [];
    const functionCalls = outputItems.filter((item) => item?.type === "function_call" && item?.name === "inspect");
    if (functionCalls.length === 0) {
      const text = String((response as any).output_text ?? "").trim();
      if (!text) throw new Error("OpenAI response text was empty");
      return JSON.parse(text) as SearchAgentOutput;
    }

    const toolOutputs: Array<{ type: "function_call_output"; call_id: string; output: string }> = [];
    for (const call of functionCalls) {
      const callId = String(call.call_id ?? "");
      const args = parseJsonObject(String(call.arguments ?? "{}"));
      const index = Number(args.index);
      if (!Number.isInteger(index) || index < 0 || index >= ranked.length) {
        toolOutputs.push({
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify({ ok: false, error: `Invalid index ${args.index}` }),
        });
        continue;
      }
      const candidate = ranked[index]?.result;
      if (!candidate) {
        toolOutputs.push({
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify({ ok: false, error: `Candidate ${index} not found` }),
        });
        continue;
      }

      try {
        const { bytes, cacheHit } = await getOrFetchCachedTorrentBytes(repo, {
          provider: candidate.provider,
          providerGuid: candidate.guid ?? null,
          url: candidate.url,
          infoHash: candidate.infoHash ?? null,
        });
        const files = inspectTorrentFiles(bytes).map((file) => ({
          path: file.path,
          size: humanSize(file.size) ?? file.size,
        }));
        toolOutputs.push({
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify({
            ok: true,
            index,
            cacheHit,
            fileCount: files.length,
            files,
          }),
        });
      } catch (error) {
        toolOutputs.push({
          type: "function_call_output",
          call_id: callId,
          output: JSON.stringify({
            ok: false,
            index,
            error: (error as Error).message || "inspect failed",
          }),
        });
      }
    }

    response = await client.responses.create(
      {
        model: agent.model,
        tools: [tool],
        previous_response_id: (response as any).id ?? null,
        input: toolOutputs,
      },
      { timeout: agent.timeoutMs }
    );
  }

  throw new Error("Agent exceeded inspect tool call limit");
}

function normalizeRejectedUrl(value: string): string {
  const trimmed = value.trim();
  if (!trimmed) return "";
  try {
    const parsed = new URL(trimmed);
    parsed.hash = "";
    return parsed.toString();
  } catch {
    return trimmed;
  }
}

function isRejectedCandidate(
  candidate: TorznabResult,
  rejectedUrls: Set<string>,
  rejectedGuids: Set<string>,
  rejectedInfoHashes: Set<string>
): boolean {
  if (rejectedUrls.size > 0) {
    const normalizedUrl = normalizeRejectedUrl(candidate.url);
    if (normalizedUrl && rejectedUrls.has(normalizedUrl)) {
      return true;
    }
  }
  if (rejectedGuids.size > 0 && candidate.guid && rejectedGuids.has(candidate.guid.trim())) {
    return true;
  }
  if (rejectedInfoHashes.size > 0 && candidate.infoHash) {
    const normalized = normalizeInfoHash(candidate.infoHash);
    if (normalized && rejectedInfoHashes.has(normalized)) {
      return true;
    }
  }
  return false;
}

function searchSelectionFromIndex(result: TorznabResult): SearchSelection {
  return { manifestation: { label: null, editionNote: null }, parts: [result] };
}

function parseSearchAgentSelections(output: SearchAgentOutput, ranked: ReturnType<typeof rankSearchResults>): SearchSelection[] {
  if (!Array.isArray(output.selections)) {
    throw new Error("Agent returned invalid selections");
  }

  const selections: SearchSelection[] = [];
  for (const selection of output.selections) {
    const parts = selection?.parts;
    if (!Array.isArray(parts)) {
      throw new Error("Agent returned selection without parts");
    }
    const selectedParts: TorznabResult[] = [];
    const seen = new Set<number>();
    for (const index of parts) {
      if (!Number.isInteger(index) || index < 0 || index >= ranked.length) {
        throw new Error("Agent selected invalid candidate index");
      }
      if (seen.has(index)) {
        throw new Error("Agent selected the same candidate more than once");
      }
      seen.add(index);
      selectedParts.push(ranked[index]!.result);
    }
    if (selectedParts.length === 0) continue;
    const manifestation = selection?.manifestation ?? null;
    const label = typeof manifestation?.label === "string" && manifestation.label.trim() ? manifestation.label.trim() : null;
    const rawEditionNote = manifestation?.editionNote ?? manifestation?.edition_note;
    const editionNote = typeof rawEditionNote === "string" && rawEditionNote.trim() ? rawEditionNote.trim() : null;
    selections.push({
      manifestation: { label, editionNote },
      parts: selectedParts,
    });
  }

  return selections;
}

function deterministicSearchSelection(input: SearchSelectionInput): SearchSelectionResult {
  const rejectedUrls = new Set((input.rejectedUrls ?? []).map((item) => normalizeRejectedUrl(item)).filter(Boolean));
  const rejectedGuids = new Set((input.rejectedGuids ?? []).map((item) => item.trim()).filter(Boolean));
  const rejectedInfoHashes = new Set(
    (input.rejectedInfoHashes ?? [])
      .map((item) => normalizeInfoHash(item))
      .filter((item): item is string => Boolean(item))
  );
  const ranked = rankSearchResults(input.query, input.media, input.results).filter(
    (item) => !isRejectedCandidate(item.result, rejectedUrls, rejectedGuids, rejectedInfoHashes)
  );
  if (ranked.length === 0) {
    return {
      selections: [],
      confidence: 0,
      mode: "deterministic",
      trigger: "none",
      reason: "No matching search candidate",
      error: null,
    };
  }
  const top = ranked[0];
  const second = ranked[1];
  const words = input.query
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean);
  const title = top.result.title.toLowerCase();
  const matched = words.length === 0 ? 1 : words.filter((word) => title.includes(word)).length / words.length;
  const gap = second ? top.score - second.score : 40;
  const gapNormalized = clamp01((gap + 20) / 120);
  const confidence = clamp01(0.25 + matched * 0.45 + gapNormalized * 0.3);
  return {
    selections: [searchSelectionFromIndex(top.result)],
    confidence,
    mode: "deterministic",
    trigger: "none",
    reason: `Top deterministic score=${top.score} gap=${second ? top.score - second.score : "n/a"}`,
    error: null,
  };
}

function deterministicManualImportSelection(input: ManualImportSelectionInput): ManualImportSelectionResult {
  const supported = input.files.filter((file) => (input.mediaType === "audio" ? file.supportedAudio : file.supportedEbook));
  if (supported.length === 0) {
    return {
      selectedPaths: [],
      confidence: 0,
      mode: "deterministic",
      trigger: "none",
      reason: "No supported files for media type",
      error: null,
    };
  }

  if (input.mediaType === "ebook") {
    const epub = supported.find((file) => file.ext === ".epub");
    if (epub) {
      return {
        selectedPaths: [epub.sourcePath],
        confidence: supported.length === 1 ? 0.95 : 0.45,
        mode: "deterministic",
        trigger: "none",
        reason: "Prefer EPUB for ebook import",
        error: null,
      };
    }
    const pdf = supported.find((file) => file.ext === ".pdf");
    if (pdf) {
      return {
        selectedPaths: [pdf.sourcePath],
        confidence: supported.length === 1 ? 0.9 : 0.45,
        mode: "deterministic",
        trigger: "none",
        reason: "Selected first PDF candidate",
        error: null,
      };
    }
  }

  const m4 = supported.filter((file) => file.ext === ".m4b" || file.ext === ".m4a" || file.ext === ".mp4");
  if (m4.length > 0) {
    const largest = [...m4].sort((a, b) => b.size - a.size)[0];
    return {
      selectedPaths: [largest.sourcePath],
      confidence: m4.length === 1 ? 0.9 : 0.5,
      mode: "deterministic",
      trigger: "none",
      reason: "Prefer largest single-file m4* audio",
      error: null,
    };
  }

  const mp3 = supported.filter((file) => file.ext === ".mp3").sort((a, b) => a.sourcePath.localeCompare(b.sourcePath));
  if (mp3.length > 0) {
    return {
      selectedPaths: mp3.map((file) => file.sourcePath),
      confidence: mp3.length === 1 ? 0.85 : 0.45,
      mode: "deterministic",
      trigger: "none",
      reason: mp3.length === 1 ? "Single MP3 file" : "Selected all MP3 parts",
      error: null,
    };
  }

  return {
    selectedPaths: [supported[0].sourcePath],
    confidence: 0.35,
    mode: "deterministic",
    trigger: "none",
    reason: "Fallback to first supported file",
    error: null,
  };
}

function hasImportableFilesForMedia(input: ManualImportSelectionInput): boolean {
  return input.files.some((file) => (input.mediaType === "audio" ? file.supportedAudio : file.supportedEbook));
}

function buildSearchAgentPrompt(trigger: DecisionTrigger, input: SearchSelectionInput, ranked: ReturnType<typeof rankSearchResults>): string {
  const lines: string[] = [];
  lines.push("# Task");
  lines.push("Pick the best torrent candidate for a single-book acquisition.");
  lines.push("");
  lines.push("# Context");
  lines.push(`- Query: \`${mdInline(input.query)}\``);
  lines.push(`- Target media: **${formatTargetMedia(input.media)}**`);
  if (input.book) {
    lines.push(`- Book: **${mdTableCell(input.book.title)}** by **${mdTableCell(input.book.author)}**`);
  } else {
    lines.push("- Book: unknown");
  }
  const editionPreference = input.media === "audio" ? input.editionPreference?.trim() : "";
  if (editionPreference) {
    lines.push(`- Global audio edition preference: ${mdTableCell(editionPreference)}`);
  }
  if (trigger !== "none" && trigger !== "forced") {
    lines.push(`- Context: ${trigger === "prior_failure" ? "this follows a prior failure" : "deterministic confidence was low"}`);
  }
  lines.push("");
  lines.push("# Candidates");
  lines.push("| index | title | size | seeders |");
  lines.push("| ---: | --- | --- | ---: |");
  for (let index = 0; index < ranked.length; index += 1) {
    const item = ranked[index];
    lines.push(
      `| ${index} | ${mdTableCell(item.result.title)} | ${mdTableCell(humanSize(item.result.sizeBytes) ?? "?")} | ${item.result.seeders ?? 0} |`
    );
  }
  lines.push("");
  lines.push("# Output");
  lines.push("Return strict JSON only with keys `selections`, `confidence`, `reason`.");
  lines.push("`selections` is an array. Use an empty array when no safe candidate exists.");
  lines.push("For normal single-release acquisitions, return one selection with one candidate index in `parts`.");
  lines.push("If the preferred edition is split across multiple releases, return one selection whose ordered `parts` array contains each release index.");
  lines.push("Each selection may include `manifestation.label` and `manifestation.editionNote`; use null when there is no meaningful edition label.");
  return lines.join("\n");
}

function buildManualImportAgentPrompt(trigger: DecisionTrigger, input: ManualImportSelectionInput): string {
  const lines: string[] = [];
  const importableFiles = input.files
    .map((file, index) => ({ file, index }))
    .filter(({ file }) => (input.mediaType === "audio" ? file.supportedAudio : file.supportedEbook));
  lines.push("# Task");
  lines.push("Pick the exact subset of files to import for one book.");
  lines.push("");
  lines.push("# Context");
  lines.push(`- Target media: **${formatTargetMedia(input.mediaType)}**`);
  if (input.book) {
    lines.push(`- Book: **${mdTableCell(input.book.title)}** by **${mdTableCell(input.book.author)}**`);
  } else {
    lines.push("- Book: unknown");
  }
  if (trigger !== "none" && trigger !== "forced") {
    lines.push(`- Context: ${trigger === "prior_failure" ? "this follows a prior import failure" : "deterministic confidence was low"}`);
  }
  const rejected = Array.isArray(input.rejectedSourcePaths) ? input.rejectedSourcePaths.filter(Boolean) : [];
  if (rejected.length > 0) {
    lines.push("- Previously imported source paths from the rejected import:");
    lines.push("  - These paths are not individually banned.");
    lines.push("  - Do not return the exact same full set again.");
    lines.push("  - A strict subset is allowed and may be the correct answer when the prior import included extra books/files.");
    for (const p of rejected) {
      lines.push(`  - \`${mdInline(p)}\``);
    }
  }
  if (importableFiles.length !== input.files.length) {
    lines.push(`- Only importable ${input.mediaType} files are listed below (${importableFiles.length} of ${input.files.length} total files).`);
  }
  lines.push("");
  lines.push("# Importable Files (JSON Array)");
  lines.push("```json");
  lines.push(
    JSON.stringify(
      importableFiles.map(({ file, index }) => ({
        index,
        path: file.relativePath,
        size: humanSize(file.size) ?? file.size,
      })),
      null,
      2
    )
  );
  lines.push("```");
  lines.push("");
  lines.push("# Output");
  lines.push("Return strict JSON only with keys `selectedIndices`, `confidence`, `reason`.");
  return lines.join("\n");
}

export async function selectSearchCandidates(
  settings: AppSettings,
  input: SearchSelectionInput,
  runtime: SearchSelectionRuntime = {}
): Promise<SearchSelectionResult> {
  const deterministic = deterministicSearchSelection(input);
  const trigger = determineTrigger(settings, "search", deterministic.confidence, {
    forceAgent: input.forceAgent,
    priorFailure: input.priorFailure,
  });
  if (trigger === "none" || deterministic.selections.length === 0) {
    return deterministic;
  }

  try {
    const rejectedUrls = new Set((input.rejectedUrls ?? []).map((item) => normalizeRejectedUrl(item)).filter(Boolean));
    const rejectedGuids = new Set((input.rejectedGuids ?? []).map((item) => item.trim()).filter(Boolean));
    const rejectedInfoHashes = new Set(
      (input.rejectedInfoHashes ?? [])
        .map((item) => normalizeInfoHash(item))
        .filter((item): item is string => Boolean(item))
    );
    const ranked = rankSearchResults(input.query, input.media, input.results)
      .filter((item) => !isRejectedCandidate(item.result, rejectedUrls, rejectedGuids, rejectedInfoHashes))
      .slice(0, 12);
    const system = [
      "You select the best torrent candidate for a single book acquisition.",
      "You may call inspect(index) to inspect a candidate's torrent file list before deciding.",
      "Use inspect(index) when titles are ambiguous, when a collection/box set may be the best fallback, or when you need to confirm the torrent contains the target work.",
      "Prefer the requested prose book itself, not related works, guides, excerpts, or adaptations.",
      "For ebook selection, this app can import EPUB and PDF only. Prefer EPUB/PDF candidates over unsupported ebook formats (such as AZW3/MOBI/LIT), even if the unsupported candidate looks like a better title match.",
      "For ebook selection, avoid comic/graphic-novel formats and comic releases (for example CBR/CBZ or titles mentioning graphic novel) unless the user explicitly asked for that.",
      "If no good single-book candidate exists, it is better to choose a plausible box set/collection containing the target book than to choose the wrong work.",
      "If the list is too poor or ambiguous to choose safely, return no candidate.",
      "Return strict JSON only with keys: selections, confidence, reason.",
      "selections must be an array. Use an empty array when no candidate is safe.",
      "Each selection must have ordered candidate indices in parts.",
      "confidence must be a number from 0 to 1.",
    ].join(" ");
    const user = buildSearchAgentPrompt(trigger, input, ranked);
    const output = runtime.repo
      ? await callSearchResponsesWithInspectTool(settings, system, user, ranked, runtime.repo)
      : await callResponsesJson<SearchAgentOutput>(settings, system, user);
    const selections = parseSearchAgentSelections(output, ranked);
    if (selections.length === 0) {
      return {
        selections: [],
        confidence: clamp01(output.confidence),
        mode: "agent",
        trigger,
        reason: output.reason || "Agent chose no candidate",
        error: null,
      };
    }
    return {
      selections,
      confidence: clamp01(output.confidence),
      mode: "agent",
      trigger,
      reason: output.reason || "Agent selected candidates",
      error: null,
    };
  } catch (error) {
    const message = (error as Error).message || "unknown agent error";
    throw new Error(`Agent search selection failed (trigger=${trigger}): ${message}`);
  }
}

export async function selectManualImportPaths(
  settings: AppSettings,
  input: ManualImportSelectionInput
): Promise<ManualImportSelectionResult> {
  const deterministic = deterministicManualImportSelection(input);
  const trigger = determineTrigger(settings, "manualImport", deterministic.confidence, {
    forceAgent: input.forceAgent,
    priorFailure: input.priorFailure,
  });
  if (trigger === "none" || input.files.length === 0 || !hasImportableFilesForMedia(input)) {
    return deterministic;
  }

  try {
    const system = [
      "You choose the exact subset of files to import for one book.",
      "Return strict JSON only with keys: selectedIndices, confidence, reason.",
      "selectedIndices must be an array of integer indices into the files array.",
      "If there is no valid alternative importable file set, return an empty selectedIndices array.",
      "Never include unsupported media files.",
      "If previously imported paths are provided, do NOT treat each listed path as individually forbidden.",
      "Those paths represent a previously rejected import set that may include valid files mixed with extra files.",
      "Avoid returning the exact same full set again, but selecting a strict subset is allowed and often desirable.",
      "confidence must be a number from 0 to 1.",
    ].join(" ");
    const user = buildManualImportAgentPrompt(trigger, input);
    const output = await callResponsesJson<ManualImportAgentOutput>(settings, system, user);
    if (!Array.isArray(output.selectedIndices)) {
      throw new Error("Agent returned invalid selectedIndices");
    }
    if (output.selectedIndices.length === 0) {
      return {
        selectedPaths: [],
        confidence: clamp01(output.confidence),
        mode: "agent",
        trigger,
        reason: output.reason || "Agent found no alternative importable files",
        error: null,
      };
    }
    const selectedPaths: string[] = [];
    for (const index of output.selectedIndices) {
      if (!Number.isInteger(index) || index < 0 || index >= input.files.length) {
        throw new Error("Agent selected out-of-range file index");
      }
      const selected = input.files[index];
      if (input.mediaType === "audio" && !selected.supportedAudio) {
        throw new Error("Agent selected unsupported audio file");
      }
      if (input.mediaType === "ebook" && !selected.supportedEbook) {
        throw new Error("Agent selected unsupported ebook file");
      }
      selectedPaths.push(selected.sourcePath);
    }
    const uniquePaths = Array.from(new Set(selectedPaths));
    const rejectedPaths = Array.isArray(input.rejectedSourcePaths)
      ? Array.from(new Set(input.rejectedSourcePaths.filter(Boolean)))
      : [];
    if (rejectedPaths.length > 0 && samePathSet(uniquePaths, rejectedPaths)) {
      return {
        selectedPaths: [],
        confidence: clamp01(output.confidence),
        mode: "agent",
        trigger,
        reason:
          output.reason || "Agent selected the previously reported wrong file set; treating as no alternative importable files",
        error: null,
      };
    }
    return {
      selectedPaths: uniquePaths,
      confidence: clamp01(output.confidence),
      mode: "agent",
      trigger,
      reason: output.reason || "Agent selected import files",
      error: null,
    };
  } catch (error) {
    const message = (error as Error).message || "unknown agent error";
    throw new Error(`Agent manual-import selection failed (trigger=${trigger}): ${message}`);
  }
}

export type { ManualImportSelectionInput, ManualImportSelectionResult, SearchSelection, SearchSelectionInput, SearchSelectionResult };

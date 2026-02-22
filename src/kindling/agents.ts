import OpenAI from "openai";

import type { ImportInspectionFile } from "./importer";
import { rankSearchResults } from "./service";
import type { TorznabResult } from "./torznab";
import { normalizeInfoHash } from "./torrent";
import type { AppSettings, MediaType } from "./types";

/**
 * Optional agent decision layer.
 *
 * Deterministic ranking/selection stays the default path. Responses API is
 * consulted only when configured and triggered (forced, prior failure, or low
 * confidence), and all agent failures fall back to deterministic output.
 */
type DecisionMode = "deterministic" | "agent";
type DecisionTrigger = "none" | "forced" | "prior_failure" | "low_confidence";

type SearchSelectionResult = {
  candidate: TorznabResult | null;
  confidence: number;
  mode: DecisionMode;
  trigger: DecisionTrigger;
  reason: string;
  error: string | null;
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
  book?: {
    id: number;
    title: string;
    author: string;
  } | null;
};

type ManualImportSelectionInput = {
  mediaType: MediaType;
  files: ImportInspectionFile[];
  forceAgent?: boolean;
  priorFailure?: boolean;
  book?: {
    id: number;
    title: string;
    author: string;
  } | null;
};

type SearchAgentOutput = {
  selectedIndex: number | null;
  confidence: number;
  reason: string;
};

type ManualImportAgentOutput = {
  selectedIndices: number[];
  confidence: number;
  reason: string;
};

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

function configuredAgent(settings: AppSettings) {
  if (!settings.agents?.enabled) return null;
  const apiKey = process.env.OPENAI_API_KEY?.trim();
  if (!apiKey) return null;
  return {
    apiKey,
    model: settings.agents.model || "gpt-5-mini",
    timeoutMs: Math.max(1000, Math.trunc(settings.agents.timeoutMs || 8000)),
  };
}

function determineTrigger(
  settings: AppSettings,
  domain: "search" | "manualImport",
  confidence: number,
  options: { forceAgent?: boolean; priorFailure?: boolean }
): DecisionTrigger {
  if (options.forceAgent) return "forced";
  const cfg = domain === "search" ? settings.agents.search : settings.agents.manualImport;
  if (options.priorFailure && cfg.enableOnFailure) return "prior_failure";
  if (cfg.enableOnLowConfidence && confidence < settings.agents.lowConfidenceThreshold) return "low_confidence";
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
      candidate: null,
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
    candidate: top.result,
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

export async function selectSearchCandidate(settings: AppSettings, input: SearchSelectionInput): Promise<SearchSelectionResult> {
  const deterministic = deterministicSearchSelection(input);
  const trigger = determineTrigger(settings, "search", deterministic.confidence, {
    forceAgent: input.forceAgent,
    priorFailure: input.priorFailure,
  });
  if (trigger === "none" || !deterministic.candidate) {
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
      "Be conservative and avoid box sets/collections unless clearly exact.",
      "Return strict JSON only with keys: selectedIndex, confidence, reason.",
      "selectedIndex must be an integer index into the candidate list or null.",
      "confidence must be a number from 0 to 1.",
    ].join(" ");
    const user = JSON.stringify(
      {
        task: "pick_best_candidate",
        trigger,
        query: input.query,
        media: input.media,
        book: input.book ?? null,
        candidates: ranked.map((item, index) => ({
          index,
          title: item.result.title,
          size: humanSize(item.result.sizeBytes),
          seeders: item.result.seeders,
        })),
      },
      null,
      2
    );
    const output = await callResponsesJson<SearchAgentOutput>(settings, system, user);
    if (output.selectedIndex === null) {
      return {
        candidate: null,
        confidence: clamp01(output.confidence),
        mode: "agent",
        trigger,
        reason: output.reason || "Agent chose no candidate",
        error: null,
      };
    }
    if (!Number.isInteger(output.selectedIndex) || output.selectedIndex < 0 || output.selectedIndex >= ranked.length) {
      throw new Error("Agent selected invalid candidate index");
    }
    return {
      candidate: ranked[output.selectedIndex].result,
      confidence: clamp01(output.confidence),
      mode: "agent",
      trigger,
      reason: output.reason || "Agent selected candidate",
      error: null,
    };
  } catch (error) {
    return {
      ...deterministic,
      trigger,
      reason: `${deterministic.reason}; agent fallback to deterministic`,
      error: (error as Error).message,
    };
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
  if (trigger === "none" || input.files.length === 0) {
    return deterministic;
  }

  try {
    const system = [
      "You choose the exact subset of files to import for one book.",
      "Return strict JSON only with keys: selectedIndices, confidence, reason.",
      "selectedIndices must be an array of integer indices into the files array.",
      "Never include unsupported media files.",
      "confidence must be a number from 0 to 1.",
    ].join(" ");
    const user = JSON.stringify(
      {
        task: "pick_import_files",
        trigger,
        mediaType: input.mediaType,
        book: input.book ?? null,
        files: input.files.map((file, index) => ({
          index,
          relativePath: file.relativePath,
          sourcePath: file.sourcePath,
          ext: file.ext,
          size: file.size,
          supportedAudio: file.supportedAudio,
          supportedEbook: file.supportedEbook,
        })),
      },
      null,
      2
    );
    const output = await callResponsesJson<ManualImportAgentOutput>(settings, system, user);
    if (!Array.isArray(output.selectedIndices) || output.selectedIndices.length === 0) {
      throw new Error("Agent returned empty selectedIndices");
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
    return {
      selectedPaths: uniquePaths,
      confidence: clamp01(output.confidence),
      mode: "agent",
      trigger,
      reason: output.reason || "Agent selected import files",
      error: null,
    };
  } catch (error) {
    return {
      ...deterministic,
      trigger,
      reason: `${deterministic.reason}; agent fallback to deterministic`,
      error: (error as Error).message,
    };
  }
}

export type { ManualImportSelectionInput, ManualImportSelectionResult, SearchSelectionInput, SearchSelectionResult };

# Real audiobook transcription evaluation

This is an isolated local experiment, not a production transcription change.

```sh
bun scripts/transcription-eval/prepare-excerpts.ts
bun scripts/transcription-eval/run.ts tmp/transcription-eval/excerpts/manifest.json

# Repeat with the same production EPUB glossary extractor and prompt builder.
bun scripts/transcription-eval/prepare-glossary.ts
bun scripts/transcription-eval/run.ts tmp/transcription-eval/glossary/manifest.json

# Same source intervals at production's atempo=2, with and without glossary.
bun scripts/transcription-eval/prepare-speed.ts
bun scripts/transcription-eval/run.ts tmp/transcription-eval/speed2x/no-glossary/manifest.json
bun scripts/transcription-eval/run.ts tmp/transcription-eval/speed2x/glossary/manifest.json
```

The preparation script extracts three twenty-minute, normal-speed mono AAC
excerpts from existing local corpus audio. Both models receive identical bytes,
English language hints, and no EPUB text or vocabulary prompt. The comparison is
`whisper-1` versus `gpt-transcribe`. Whisper requests word and segment timestamps;
GPT requests JSON text. This measures model quality, not production's audio
speed-up configuration. Neither script contacts or modifies production.

Supply `OPENAI_API_KEY` through the environment. Results are cached by audio
bytes and request configuration and written immediately after each response.
Rerunning resumes completed requests. The readable `results/report.md`, plain
text transcripts, and raw responses remain under gitignored `tmp/`.
Up to three samples run concurrently, with each sample's model requests in order.

## Reference quality

A manifest sample can include `referencePath` and `referenceStatus`:
`audio-verified` or `epub-provisional`. Accurate WER requires the exact spoken
words for the exact excerpt, including cut-off sentences. An EPUB passage can
help construct that reference, but differences in narration, omitted material,
edition, or excerpt boundaries are not ASR errors. Never treat an unreviewed
EPUB alignment as authoritative WER or use one model's output as ground truth.

WER uses word-level Levenshtein edit distance: substitutions plus deletions plus
insertions divided by reference word count. Normalization ignores punctuation,
case, and apostrophes; accents and numeric spelling remain significant. It does
not find the reference passage within a whole book. Score bounded, checked
excerpts instead. Request elapsed times include upload and provider latency,
are not controlled benchmarks, and cached results retain original timing.

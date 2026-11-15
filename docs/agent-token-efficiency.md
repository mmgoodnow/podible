# Agent token efficiency

Podible now removes repeated instructions and encodes repeated evidence as rows with a column legend. This is a serialization change, not a tighter candidate filter: preserve the existing sanitized facts, index order, scope, tool access, and validation. Use the object form for a small batch if its JSON is smaller than the compact JSON plus its legend. That guard measures UTF-8 bytes; the live eval measures actual model tokens.

## Live checks

Both variants used the same model, fixtures, shortened prompts, and validation. The comparison isolates the evidence format, not the prompt rewrite. No production titles, download URLs, or audiobook/transcript data were sent.

| Workflow | Object input / total tokens | Compact input / total tokens | Expected results |
| --- | ---: | ---: | --- |
| TV import, 5 fixtures × 2 | 51,800 / 56,815 | 43,249 / 48,843 (**14.0% fewer total**) | Both: 34 exact mappings, no misses, extras, wrong assignments, or API failures |
| TV feed, 5 holdouts | 17,810 / 19,156 | 11,745 / 13,368 (**30.2% fewer total**) | Both: 30 correct matches across 215 offers, no misses, extras, or API failures |

TV import covers shuffled files, anime numbering aliases, combined episodes (including unrequested episodes), partial packs and misleading samples, and a wrong show. Compact import made 21 model calls versus 20, and was not faster in this run (median 6.63 s versus 5.90 s). The feed replay made eight calls per variant. Its usage-based estimated cost fell about 22%, not 30%, because output tokens and cache usage price differently. These are small fixture runs, not production-wide cost or quality estimates.

```sh
bun run test/lab/eval-tv-import-evidence.ts 2
bun run test/lab/eval-tv-feed-routing.ts --split=holdout --policies=responses,tableResponses --repeats=1
```

The runners use the existing `OPENAI_API_KEY` and report model usage without dumping prompt bodies. TV import also reports missing/extra episode IDs and wrong file assignments; TV feed reports missing/extra target-release pairs. Check those outcomes before comparing cost.

## Coverage

| Calls | Change |
| --- | --- |
| TV import | Share identical ordered probe stream profiles and numbering sources. Keep per-file durations, IDs, paging and existing large-pack tools. |
| TV, movie, and book feed routing | Pack repeated rows after the existing evidence projection and batch splitting. Keep TV's Responses-only default and the existing book/movie Decisions routing. |
| Movie/TV acquisition, movie import, TV pack coverage and episode mapping | Shorter prompts with the same identity, coverage, cross-seed tie-break, and quality requirements. Keep the actual tool responses and eligibility checks. |
| Book selection, manual import and upgrade | Keep selection arrays and edition rules; compact the manual import file list and remove repeated instructions. |
| Indexer query planning | Numbering tuples preserve schemes, coordinates and order; capability-aware ID search is unchanged. |
| Chapter curation | Consolidate repeated curator, classifier and judge instructions; remove JSON indentation while keeping transcript and EPUB evidence, required tools and judge acceptance. Not replayed live against the book corpus. |
| Decisions and issue triage | Shorten question/system text. Keep request/response schemas, abstention thresholds, escalation, and validation unchanged. |

No changes to the Whisper glossary or image-generation prompt. Other than the two TV evals above, checks are local contracts and integration tests, not matched model-quality evaluations. Use the existing per-workflow token and call telemetry after rollout to see whether the changes reduce actual spend; cheaper prompts are not a win if they cause more retries or tool turns.

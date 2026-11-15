# TV feed routing replay

Run from a checkout with the approved OpenAI key available as `OPENAI_API_KEY`:

```sh
bun run test/lab/eval-tv-feed-routing.ts --split=dev --policies=baseline,compact95,compact90,compact85,binary90,responses
bun run test/lab/eval-tv-feed-routing.ts --split=holdout --policies=baseline,responses --repeats=2
```

The replay calls the actual TV acquisition and reuse matchers, runs the same
metering code as the worker, and stores only local in-memory state. It cannot
query indexers, grab files, or create production jobs. Fixtures use public show
names and synthetic offers and episode scopes. Gold labels identify plausible
release-to-work routes; they are not proof that torrent contents are acceptable.
An extra route can add downstream work; a missed route can strand a request or
miss a cross-seed. Invalid IDs or a failed replay are not counted as negatives.

Each JSON line reports a case, variant, missed/extra index pairs, duration,
fallback reasons, and model usage by API. Compare identical case/repeat pairs.
Check `missing` and `unpriced` before comparing cost. `usd` uses observed token
usage and cache discounts; `uncachedUsd` reprices short-context calls as though
none of their input tokens were cached. Neither is an invoice total.

## November 14 comparison

TV feeds previously generated 382 Decisions escalations from 413 checks (92%),
368 of them uncertain rejections. The live replay tested lower rejection floors,
a binary reject-or-review gate, compact episode evidence and direct Responses.
With both API legs counted, compact Decisions at 90% was no cheaper than the
95% baseline on the four ordinary holdout cases and still called Responses for
every case. Lower floors and a binary gate also lost to direct Responses on dev.

On the first held-out replay (14 batch runs per policy, including the large
multi-page fixture), the baseline and direct Responses both kept all 86 labeled
routes. Direct Responses produced zero extra routes vs. four, and used 20 model
calls vs. 36. Estimated usage cost was $0.00299 vs. $0.02462 (88% lower). The
multi-page fixture took about 7-9 seconds on direct Responses vs. 3-5 seconds on
the baseline. Batches remain bounded to 12 targets and 50 offers per call.

A follow-up replay with alternating policy order (10 batch runs per policy)
again kept all 60 labeled routes. With identical uncached pricing, the cost was
$0.00502 vs. $0.02266 (78% lower), so cache discounts do not explain the win.
The baseline made 16 Decisions calls with 183,494 input tokens and escalated
532 of 588 offer checks, adding 32,848 Responses input tokens. Direct Responses
made 16 calls with 36,612 input tokens and 2,724 output tokens. One HTTP request
to Decisions contains a choice question per offer, with repeated instructions
and choices. This does not compare the APIs on a single question per batch.

## Single-question Decisions follow-up

```sh
bun run test/lab/eval-tv-feed-routing.ts --split=sparse --repeats=3 --policies=responses,gate95,gate90,gate85
bun run test/lab/eval-tv-feed-routing.ts --split=sparse --repeats=4 --policies=responses,workGate75,workGate60
```

The gate asks one question per page: can any release match any target? Only a
confident no-match skips routing. Review, bad output and low scores all use the
unchanged Responses matcher. The work-only version sends titles, aliases,
relationships and IDs, but no episode tables; a fallback gets the full evidence.
The five sparse cases include two empty pages (one with confusable remakes)
and three pages with exactly one positive (including a translated title and
absolute anime numbering). Repeats test stability, not additional distinct gold.

| Policy | Runs | Skipped Responses | Gold misses | Estimated uncached cost |
| --- | ---: | ---: | ---: | ---: |
| Direct Responses (first comparison) | 15 | 0 | 0 | $0.00516 |
| Full gate at 0.85-0.95 (per variant) | 15 | 0 | 0 | $0.00970-$0.00974 |
| Direct Responses (work-only comparison) | 20 | 0 | 0 | $0.00689 |
| Work-only gate at 0.60-0.75 (per variant) | 20 | 0 | 0 | $0.00850-$0.00853 |

The cheaper gate still cost 23-24% more end to end because it never skipped
Responses. It chose `no_match` on the easy empty page but repeated a reported
confidence of 0.12 and choice probability of 0.56. On the confusable empty
page it chose `review`. The gate retained recall by deferring, not by proving
it could safely reject. We did not replace confidence with probability or lower
the production floor based on this small sample. Direct Responses remains the
TV default; the gate is available only through the replay policy.

Next, test durable work-identity reuse across RSS/autobrr and acquisition/reuse
consumers. That would avoid a call without depending on a model negative.
Reuse positives only with matching evidence and catalog revision; re-evaluate
when identity evidence changes, and never use a stale negative to rule out new
demand. The sparse gate replay does not test that separate architecture.

These are synthetic holdouts with repeated examples, not production recall or
an SLA. Track both APIs' estimates under `workflow="tv-feed"` after rollout
and account for feed volume, catalog mix and backfill activity. The production
change is limited to TV candidate routing; downstream identity, quality and
file verification, Decisions for search/import, and other media are unchanged.

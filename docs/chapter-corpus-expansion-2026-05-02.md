# Chapter Corpus Expansion - 2026-05-02

This run added five chapter-analysis corpus cases under `/private/tmp/podible-chapter-cases`.

## Selected Cases

| Case | Prod IDs | Rationale | Source |
| --- | --- | --- | --- |
| `piranesi` | book `34`, manifestation `65`, audio asset `82`, EPUB asset `76`, job `297` | Short fiction with epigraph/front matter and part-level audiobook markers. | https://www.bloomsbury.com/us/piranesi-9781635575644/ |
| `say-nothing` | book `35`, manifestation `74`, audio asset `85`, EPUB asset `77`, job `298` | Dense nonfiction with titled EPUB chapters but generic embedded audio chapter numbers. | https://www.penguinrandomhouse.com/books/90837/say-nothing/audio |
| `world-war-z` | book `36`, manifestation `68`, audio asset `81`, EPUB asset `78`, job `299` | Oral-history/interview fiction with location cards and an acquired publisher/audio variant starting at interview 30. | https://books.apple.com/us/audiobook/world-war-z-the-complete-edition-an-oral-history/id1417748473 |
| `the-anthropocene-reviewed` | book `38`, manifestation `72`, audio asset `83`, EPUB asset `80`, job `301` | Multi-file MP3 release with essay-style titled chapters and file-boundary raw input. | https://www.penguinrandomhouse.com/books/672554/the-anthropocene-reviewed/audio |
| `the-handmaids-tale` | book `41`, manifestation `82`, audio asset `91`, EPUB asset `90`, job `324` | Publisher-boilerplate embedded chapter labels in a special-edition/full-cast style audiobook. | https://www.audioeditions.com/the-handmaid-s-tale-special-edition |

Follow-up prod work:

- `the-fifth-season`: after report-issue cleanup, book `39` had a cleaner EPUB/audio pairing. Transcription job `334` completed successfully, and a follow-up corpus case was built at `/private/tmp/podible-chapter-cases/the-fifth-season`.
- `the-way-of-kings`: report-issue cleanup produced another import, but available manifestations still looked like partial dramatized segments, so it was not used for this corpus set.
- `daisy-jones-and-the-six`: still lacked an EPUB asset suitable for the current harness.

## Answer-Key Notes

- `piranesi`: expected chapters use the acquired M4B embedded chapter table after checking the transcript/report windows.
- `say-nothing`: expected chapters use embedded timing boundaries with EPUB chapter titles assigned by sequence, so generic `Chapter N` audio labels are not treated as sufficient.
- `world-war-z`: expected chapters use the acquired M4B embedded chapter table for the actual audio manifestation, which starts at numbered interview 30.
- `the-anthropocene-reviewed`: expected chapters use transcript-confirmed EPUB essay headings; multi-file MP3 boundaries are retained only as raw input.
- `the-handmaids-tale`: expected chapters record the actual embedded audiobook track surface, making this a publisher-boilerplate variant rather than a semantic retitling case.
- `the-fifth-season`: expected chapters use EPUB semantic headings aligned to embedded chapter boundaries and transcript windows. The EPUB prologue starts at the first story utterance after opening credits, and the final raw marker starts chapter 23 rather than outro material.

## Scoring

Command:

```sh
dirs=(); for d in /private/tmp/podible-chapter-cases/*; do [ -f "$d/expected-chapters.json" ] && [ -f "$d/proposed-chapters.json" ] && dirs+=("$d"); done; bun tmp/score-chapter-proposal.ts "${dirs[@]}"
```

Results for new cases:

| Case | Score |
| --- | --- |
| `piranesi` | `exact=9/9 near=0 proposed=9 missing=0 extra=0` |
| `say-nothing` | `exact=0/33 near=1 proposed=2 missing=32 extra=1` |
| `world-war-z` | `exact=31/31 near=0 proposed=31 missing=0 extra=0` |
| `the-anthropocene-reviewed` | `exact=43/43 near=0 proposed=44 missing=0 extra=1` |
| `the-handmaids-tale` | `exact=53/53 near=0 proposed=53 missing=0 extra=0` |
| `the-fifth-season` | `exact=2/26 near=23 proposed=25 missing=1 extra=0` |

Full existing-corpus score output was saved to `/private/tmp/podible-chapter-cases/score-all.txt`.

## Weaknesses Found

- Generic embedded chapter numbers are not being retitled from EPUB headings. `say-nothing` is the clearest failure: the proposal collapsed the book to two part headings instead of the 33 expected chapter boundaries.
- Front/back matter filtering is still noisy. `the-anthropocene-reviewed` emitted `Also by John Green` as an extra opening marker.
- Prologue/interlude structures can shift semantic titles by one chapter. `the-fifth-season` identifies the right raw timing boundaries for most chapters, but labels the prologue as chapter 1, shifts subsequent titles, and misses chapter 23 at the last embedded boundary.
- A publisher-boilerplate track table can still dominate when it looks user-facing. `the-handmaids-tale` scored perfectly against the actual embedded track surface, but it remains a weak user-facing chapter experience if semantic titles are desired.

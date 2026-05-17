# Chapter Curation Corpus

The durable working directory for local chapter-curation cases is:

```text
tmp/chapter-cases/
```

That directory is intentionally gitignored because it can contain copyrighted EPUB,
audio, and transcript artifacts. Keep the heavy materials there, not in `/private/tmp`,
so they survive shell restarts and are easy to reuse between replay runs.

Committed corpus metadata should stay lightweight and safe to publish:

- book title and author
- manifestation shape, such as single M4B, multi-MP3, no embedded chapters, or bad embedded chapters
- expected chapter titles and timestamps
- notes explaining why the answer key is correct
- pointers to local artifact filenames under `tmp/chapter-cases/`, but not the artifact contents

The agentic chapter-curation path should be evaluated against that metadata plus
local ignored artifacts. If a case requires copyrighted text/audio to reproduce,
commit only the answer-key metadata and keep the source files ignored.

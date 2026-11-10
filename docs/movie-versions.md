# Independent movie versions

On a movie page, Request a specific version creates demand for an independent
target: resolution, cut, audio language, or SDR-only. Matching requests share a
target; different policies have separate acquisition, upgrades, availability and
withdrawal. Existing default requests retain their original policy.

The typed `movies.request` method accepts an optional `version` object:

```typescript
{ tmdbId: 101, version: { resolution: 2160, audioLanguage: "ja", sdrOnly: true } }
```

Constraints are validated and applied to the configured movie policy, retaining
its codec, source and upgrade preferences. Policy checks and probe/identity
verification remain mandatory. A 4K request cannot become ready because an HD
target for the same movie is ready. The page shows each active request's own
progress, labeled with its version requirements.

An existing published canonical version can satisfy multiple compatible targets.
The acquisition checkpoints that selection and uses the normal pinned-storage,
desired-view and exact Plex verification path. No extra search, download, copy or
probe is needed. The existing version is not moved or taken away from its original
target. Otherwise, the new target follows normal acquisition. Publication names
include target and version identity, avoiding conflicting Plex view paths.

This first user-facing slice is for movies. TV and book version selection and
translation identity are not implied by this API. No nightly title searches or
new background acquisition schedules are added.

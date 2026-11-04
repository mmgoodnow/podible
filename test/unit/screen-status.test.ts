import { expect, test } from "bun:test";
import { movieBadge, movieDetail, tvBadge, tvDetail, type MovieStatusInput, type TvStatusInput } from "../../web/src/screen-status";

const movie: MovieStatusInput = { available: false, upgrading: false, state: "queued", stage: null };
const tv: TvStatusInput = { intentionallyAbsent: false, requestedEpisodes: 3, availableEpisodes: 0, futureEpisodes: 2, acquisitions: [] };

test("blocked movies are stopped, even when their last recorded stage was downloading", () => {
  const blocked: MovieStatusInput = { ...movie, state: "blocked", stage: "downloading" };
  expect(movieBadge(blocked)).toEqual({ label: "Request stopped", tone: "paused" });
  expect(movieDetail(blocked)).toBe("Open request to resolve");
});

test("only the explicit movie waiting-for-release state is labelled as waiting", () => {
  for (const state of ["queued", "running", "blocked", "available", "withdrawn"] as const) {
    expect(movieBadge({ ...movie, state }).label).not.toBe("Waiting for release");
  }
  expect(movieBadge({ ...movie, state: "waiting-for-release", stage: "downloading" })).toEqual({ label: "Waiting for release", tone: "waiting" });
});

test("movie availability and upgrades retain priority over a failed upgrade", () => {
  const available: MovieStatusInput = { ...movie, available: true, state: "blocked", stage: "downloading" };
  expect(movieBadge(available)).toEqual({ label: "Available", tone: "available" });
  expect(movieDetail(available)).toBe("Ready in Plex");
  expect(movieBadge({ ...available, upgrading: true })).toEqual({ label: "Available · Upgrading", tone: "available" });
});

test("queued and running movie states do not inherit stale download progress labels", () => {
  expect(movieBadge({ ...movie, stage: "downloading" })).toEqual({ label: "Requested", tone: "active" });
  expect(movieBadge({ ...movie, state: "running", stage: "downloading" })).toEqual({ label: "Downloading", tone: "active" });
  expect(movieBadge({ ...movie, state: "running" })).toEqual({ label: "In progress", tone: "active" });
  expect(movieDetail({ ...movie, stage: "downloading" })).toBe("Track your request");
});

for (const status of ["failed", "cancelled"] as const) {
  test(`TV latest ${status} acquisition is stopped, not waiting or upcoming`, () => {
    for (const futureEpisodes of [0, 2]) {
      const stopped: TvStatusInput = { ...tv, futureEpisodes, acquisitions: [{ status }, { status: "running" }] };
      expect(tvBadge(stopped)).toEqual({ label: "Request stopped", tone: "paused" });
      expect(tvDetail(stopped)).toBe("0/3 episodes ready · Open request to resolve");
    }
  });

  test(`TV availability remains visible despite a latest ${status} acquisition`, () => {
    expect(tvBadge({ ...tv, availableEpisodes: 1, acquisitions: [{ status }] })).toEqual({ label: "Partly available", tone: "available" });
    expect(tvBadge({ ...tv, availableEpisodes: 3, acquisitions: [{ status }] })).toEqual({ label: "Available", tone: "available" });
  });
}

test("a newer TV retry or successful attempt supersedes historical failures", () => {
  for (const status of ["queued", "running"]) {
    expect(tvBadge({ ...tv, acquisitions: [{ status }, { status: "failed" }] })).toEqual({ label: "In progress", tone: "active" });
  }
  expect(tvBadge({ ...tv, acquisitions: [{ status: "succeeded" }, { status: "failed" }] })).toEqual({ label: "Upcoming episodes", tone: "waiting" });
});

test("TV without a failed attempt distinguishes future episodes from release waiting", () => {
  expect(tvBadge(tv)).toEqual({ label: "Upcoming episodes", tone: "waiting" });
  expect(tvBadge({ ...tv, futureEpisodes: 0 })).toEqual({ label: "Waiting for release", tone: "waiting" });
  expect(tvDetail(tv)).toBe("0/3 episodes ready · 2 upcoming");
});

test("intentional TV absence is not misrepresented as failed recovery", () => {
  expect(tvBadge({ ...tv, intentionallyAbsent: true, acquisitions: [{ status: "failed" }] })).toEqual({ label: "Intentionally unavailable", tone: "paused" });
});

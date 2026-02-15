import { describe, expect, test } from "bun:test";

import {
  computeDownloadFraction,
  pseudoProgressForBook,
  pseudoProgressForMediaStatus,
  pseudoProgressForRelease,
} from "../../src/kindling/progress";

describe("progress helpers", () => {
  test("maps media states to pseudo progress", () => {
    expect(pseudoProgressForMediaStatus("wanted")).toBe(0);
    expect(pseudoProgressForMediaStatus("snatched")).toBe(10);
    expect(pseudoProgressForMediaStatus("downloading")).toBe(20);
    expect(pseudoProgressForMediaStatus("downloaded")).toBe(90);
    expect(pseudoProgressForMediaStatus("imported")).toBe(100);
    expect(pseudoProgressForMediaStatus("error")).toBe(0);
  });

  test("computes book pseudo progress as media average", () => {
    expect(pseudoProgressForBook("wanted", "wanted")).toBe(0);
    expect(pseudoProgressForBook("imported", "wanted")).toBe(50);
    expect(pseudoProgressForBook("imported", "imported")).toBe(100);
  });

  test("maps release states and downloading fraction", () => {
    expect(pseudoProgressForRelease("snatched")).toBe(10);
    expect(pseudoProgressForRelease("downloaded")).toBe(90);
    expect(pseudoProgressForRelease("imported")).toBe(100);
    expect(pseudoProgressForRelease("failed")).toBe(0);
    expect(pseudoProgressForRelease("downloading", 0)).toBe(20);
    expect(pseudoProgressForRelease("downloading", 0.5)).toBe(55);
    expect(pseudoProgressForRelease("downloading", 1)).toBe(90);
  });

  test("derives download fraction from available telemetry", () => {
    expect(computeDownloadFraction({ bytesDone: 25, sizeBytes: 100, leftBytes: null })).toBe(0.25);
    expect(computeDownloadFraction({ bytesDone: 25, sizeBytes: null, leftBytes: 75 })).toBe(0.25);
    expect(computeDownloadFraction({ bytesDone: null, sizeBytes: 100, leftBytes: 75 })).toBeNull();
  });
});

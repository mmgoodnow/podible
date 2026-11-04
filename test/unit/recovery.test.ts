import { expect, test } from "bun:test";
import { randomUUID } from "node:crypto";
import {
  recoveryInventoryParamsSchema, recoveryKeySchema, recoveryPreviewParamsSchema, recoveryRecoverParamsSchema,
} from "../../src/media/recovery-types";
import { recoveryContained } from "../../src/media/recovery-filesystem";

test("recovery commands namespace target IDs and bound strict caller-authored scope", () => {
  const movie = { namespace: "movie-target", id: "1" } as const, tv = { namespace: "tv-target", id: "1" } as const;
  expect(recoveryPreviewParamsSchema.parse({ targets: [movie, tv] }).targets).toHaveLength(2);
  for (const key of [{ namespace: "movie-request", id: "1" }, { ...movie, id: "01" }, { ...movie, id: "9007199254740992" },
    { ...movie, path: "/tmp/foreign" }]) expect(() => recoveryKeySchema.parse(key)).toThrow();
  expect(() => recoveryPreviewParamsSchema.parse({ targets: [movie, movie] })).toThrow();
  expect(() => recoveryPreviewParamsSchema.parse({ targets: Array.from({ length: 21 }, (_, i) => ({ ...movie, id: String(i + 1) })) })).toThrow();
  const command = { operationId: randomUUID(), targets: [{ key: movie, expectedRevision: "a".repeat(64), action: "repair-view" as const }] };
  expect(recoveryRecoverParamsSchema.parse(command)).toEqual(command);
  for (const extra of [{ force: true }, { delete: true }, { torrentControl: true }, { observedMount: "verified" }])
    expect(() => recoveryRecoverParamsSchema.parse({ ...command, ...extra })).toThrow();
  expect(() => recoveryRecoverParamsSchema.parse({ ...command, targets: [{ ...command.targets[0], action: "download" }] })).toThrow();
  expect(() => recoveryRecoverParamsSchema.parse({ ...command, targets: [{ ...command.targets[0], expectedRevision: "stale" }] })).toThrow();
  expect(() => recoveryRecoverParamsSchema.parse({ targets: command.targets })).toThrow();
  expect(() => recoveryInventoryParamsSchema.parse({ namespace: "tv-target", limit: 21 })).toThrow();
  expect(recoveryInventoryParamsSchema.parse({ namespace: "tv-target" })).toEqual({ namespace: "tv-target", limit: 20, afterTargetId: 0 });
});

test("recovery paths never authorize escaped, hidden ownership, absolute, or symlink-style components", () => {
  expect(recoveryContained("/owned/objects", "files/ab/a.mkv")).toBe("/owned/objects/files/ab/a.mkv");
  for (const relative of ["../foreign.mkv", "/foreign.mkv", "a/../foreign", "a//b", "a\\b", "a\0b", ".podible-content.json", "a/./b", "a/.podible-link"]) {
    expect(() => recoveryContained("/owned/objects", relative)).toThrow();
  }
});

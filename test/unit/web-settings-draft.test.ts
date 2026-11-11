import { describe, expect, test } from "bun:test";
import { appSettingsSchema } from "../../src/rpc/schemas";
import { defaultSettings } from "../../src/settings";
import { applySettingsDraft, changeSetting, conflictingSettings, numericSetting, rebaseSettingsDraft } from "../../web/src/settings-draft";

describe("web settings draft", () => {
  test("edits only the intended field and preserves newly loaded credentials", () => {
    const original = defaultSettings({ agents: { apiKey: "old-key" } });
    const draft = changeSetting({}, original, "feed.title", "Feed title", "Reader feed");
    const refreshed = { ...original, agents: { ...original.agents, apiKey: "new-key" } };
    expect(conflictingSettings(refreshed, draft)).toEqual([]);
    expect(appSettingsSchema.parse(applySettingsDraft(refreshed, draft))).toEqual({
      ...refreshed, feed: { ...refreshed.feed, title: "Reader feed" },
    });
    expect(original.feed.title).toBe("Kindling");
  });

  test("explicit clear is a change and a newer value on the same field is a conflict", () => {
    const original = defaultSettings({ agents: { apiKey: "old-key" } });
    const draft = changeSetting({}, original, "agents.apiKey", "OpenAI API key", "");
    const refreshed = { ...original, agents: { ...original.agents, apiKey: "new-key" } };
    expect(conflictingSettings(refreshed, draft).map(([path]) => path)).toEqual(["agents.apiKey"]);
    const confirmed = rebaseSettingsDraft(refreshed, draft);
    expect(conflictingSettings(refreshed, confirmed)).toEqual([]);
    expect(applySettingsDraft(refreshed, confirmed).agents.apiKey).toBe("");
  });

  test("a save does not swallow an edit that returns to the prior saved value", () => {
    const original = defaultSettings();
    const submitted = changeSetting({}, original, "feed.title", "Feed title", "Submitted");
    const saved = applySettingsDraft(original, submitted);
    const current = changeSetting(submitted, original, "feed.title", "Feed title", original.feed.title, undefined, true);
    expect(rebaseSettingsDraft(saved, submitted, submitted)).toEqual({});
    const remaining = rebaseSettingsDraft(saved, current, submitted);
    expect(conflictingSettings(saved, remaining)).toEqual([]);
    expect(applySettingsDraft(saved, remaining).feed.title).toBe(original.feed.title);
  });

  test("distinguishes a cleared optional number from zero and from invalid input", () => {
    const saved = defaultSettings();
    expect(numericSetting("", true)).toBeNull();
    expect(numericSetting("0", true)).toBe(0);
    expect(numericSetting(".45")).toBe(0.45);
    expect(numericSetting("0x10")).toBeNaN();
    const draft = changeSetting({}, saved, "movies.maximumReleaseBytes", "Movie release limit", numericSetting("garbage", true), "garbage");
    expect(draft["movies.maximumReleaseBytes"]?.input).toBe("garbage");
    expect(appSettingsSchema.safeParse(applySettingsDraft(saved, draft)).success).toBe(false);
  });
});

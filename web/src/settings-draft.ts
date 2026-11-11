import type { AppSettings } from "../../src/app-types";

export type SettingsEdit = { original: unknown; value: unknown; label: string; input?: string };
export type SettingsDraft = Readonly<Record<string, SettingsEdit>>;

const equal = (left: unknown, right: unknown): boolean => Object.is(left, right) ||
  (left != null && right != null && typeof left === "object" && typeof right === "object" &&
    JSON.stringify(left) === JSON.stringify(right));

export function settingAt(value: unknown, path: string): unknown {
  for (const key of path.split(".")) value = (value as Record<string, unknown> | undefined)?.[key];
  return value;
}

function withSetting(record: object, path: string[], value: unknown): object {
  const [key, ...rest] = path;
  const next = { ...record } as Record<string, unknown>;
  if (rest.length) next[key] = withSetting(next[key] as object, rest, value);
  else if (value === undefined) delete next[key];
  else next[key] = value;
  return next;
}

export function applySettingsDraft(saved: AppSettings, draft: SettingsDraft): AppSettings {
  return Object.entries(draft).reduce((settings, [path, edit]) =>
    withSetting(settings, path.split("."), edit.value) as AppSettings, saved);
}

export function changeSetting(draft: SettingsDraft, saved: AppSettings, path: string, label: string,
  value: unknown, input?: string, submitting = false): SettingsDraft {
  const next = { ...draft }, current = settingAt(saved, path);
  if (equal(value, current) && !submitting) delete next[path];
  else next[path] = { original: draft[path] ? draft[path].original : current, value, label, input };
  return next;
}

export function conflictingSettings(saved: AppSettings, draft: SettingsDraft) {
  return Object.entries(draft).filter(([path, edit]) => {
    const current = settingAt(saved, path);
    return !equal(edit.original, current) && !equal(edit.value, current);
  });
}

export function rebaseSettingsDraft(saved: AppSettings, draft: SettingsDraft, submitted: SettingsDraft = {}): SettingsDraft {
  return Object.fromEntries(Object.entries(draft).flatMap(([path, edit]) => {
    const current = settingAt(saved, path);
    return submitted[path] === edit || equal(current, edit.value) ? [] : [[path, { ...edit, original: current }]];
  }));
}

export function numericSetting(input: string, nullable = false): number | null {
  const text = input.trim();
  if (!text && nullable) return null;
  return /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:e[+-]?\d+)?$/i.test(text) ? Number(text) : NaN;
}

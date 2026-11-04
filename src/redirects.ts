/** Only same-origin application paths, never scheme-relative URLs or browser backslash aliases. */
export function safeReturnPath(value: string | null | undefined): string | null {
  if (!value) return null;
  const path = value.trim();
  if (!path.startsWith("/") || path.startsWith("//") || /[\\\x00-\x1f\x7f]/.test(path)) return null;
  try {
    const url = new URL(path, "https://podible.invalid");
    return url.origin === "https://podible.invalid" ? url.pathname + url.search + url.hash : null;
  } catch { return null; }
}

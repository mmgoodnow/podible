function slugify(value: string): string {
  return value
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .replace(/--+/g, "-");
}

function decodeXmlEntities(value: string): string {
  return value
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&amp;/g, "&")
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'");
}

function cleanMetaValue(value: string | undefined): string | undefined {
  if (!value) return undefined;
  const trimmed = value.trim();
  if (!trimmed) return undefined;
  const lowered = trimmed.toLowerCase();
  if (lowered === "unknown" || lowered === "no description") return undefined;
  return trimmed;
}

function normalizeDescriptionHtml(raw: string | undefined): string | undefined {
  const decoded = raw ? decodeXmlEntities(raw) : undefined;
  const cleaned = cleanMetaValue(decoded);
  if (!cleaned) return undefined;
  return cleaned;
}

function htmlToPlainText(html: string | undefined): string | undefined {
  if (!html) return undefined;
  const withBreaks = html
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>/gi, "\n\n")
    .replace(/<\/li>/gi, "\n")
    .replace(/<li>/gi, "- ");
  const withoutTags = withBreaks.replace(/<[^>]+>/g, "");
  const normalized = withoutTags
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .replace(/[ \t]+/g, " ")
    .trim();
  return normalized || undefined;
}

function toArray<T>(value: T | T[] | undefined | null): T[] {
  if (value === undefined || value === null) return [];
  return Array.isArray(value) ? value : [value];
}

function nodeText(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined;
  if (typeof value === "string" || typeof value === "number") return String(value);
  if (typeof value === "object" && value && "#text" in (value as Record<string, unknown>)) {
    const text = (value as Record<string, unknown>)["#text"];
    if (typeof text === "string" || typeof text === "number") return String(text);
  }
  return undefined;
}

function escapeXml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function truncate(value: string, max: number): string {
  if (value.length <= max) return value;
  return `${value.slice(0, max - 3).trimEnd()}...`;
}

function firstLine(value: string): string {
  return value.split(/\r?\n/).map((part) => part.trim()).find(Boolean) ?? "";
}

export {
  slugify,
  decodeXmlEntities,
  cleanMetaValue,
  normalizeDescriptionHtml,
  htmlToPlainText,
  toArray,
  nodeText,
  escapeXml,
  truncate,
  firstLine,
};

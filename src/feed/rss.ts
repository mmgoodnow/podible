import { Book } from "../types";
import {
  FEED_AUTHOR,
  FEED_CATEGORY,
  FEED_COPYRIGHT,
  FEED_DESCRIPTION,
  FEED_EXPLICIT,
  FEED_IMAGE_URL,
  FEED_LANGUAGE,
  FEED_OWNER_EMAIL,
  FEED_OWNER_NAME,
  FEED_TITLE,
  FEED_TYPE,
  brandImageExists,
} from "../config";
import { buildId3ChaptersTag, estimateId3TagLength } from "../streaming/id3";
import { bookExtension, bookIsbn, bookMime, cleanLanguage, formatDateIso } from "../media/metadata";
import { escapeXml, firstLine, htmlToPlainText, truncate } from "../utils/strings";
import { formatDuration } from "../utils/time";

function buildItemNotes(book: Book): { description: string; subtitle: string; descriptionHtml?: string } {
  const baseDescription = book.description?.trim() ?? htmlToPlainText(book.descriptionHtml)?.trim();
  const summaryParts: string[] = [];
  if (baseDescription) {
    summaryParts.push(baseDescription);
  } else {
    summaryParts.push(`${book.title} by ${book.author}`);
  }

  const detailBits: string[] = [];
  const language = cleanLanguage(book.language);
  const isbn = bookIsbn(book);
  const published = formatDateIso(book.publishedAt);
  if (language) detailBits.push(`Language: ${language}`);
  if (isbn) detailBits.push(`ISBN: ${isbn}`);
  if (published) detailBits.push(`Published: ${published}`);
  if (book.kind === "multi" && book.files?.length) detailBits.push(`Parts: ${book.files.length}`);
  if (book.durationSeconds) {
    const mins = Math.round((book.durationSeconds / 60) * 10) / 10;
    detailBits.push(`Length: ${mins} min`);
  }

  if (detailBits.length > 0) {
    summaryParts.push(detailBits.join(" • "));
  }

  const description = summaryParts.join("\n\n");
  const subtitleSource = baseDescription || book.author || book.title;
  const subtitle = truncate(firstLine(subtitleSource), 200) || book.author;
  return { description, subtitle, descriptionHtml: book.descriptionHtml };
}

function rssFeed(books: Book[], origin: string, keySuffix = ""): { body: string; lastModified: Date } {
  const firstCover = books.find((b) => b.coverPath);
  const channelImage =
    FEED_IMAGE_URL ||
    (brandImageExists
      ? `${origin}/podible.png${keySuffix}`
      : firstCover
        ? `${origin}/covers/${firstCover.id}.jpg${keySuffix}`
        : "");
  const latestMtime = books
    .map((b) => b.publishedAt?.getTime() ?? 0)
    .filter((t) => t > 0);
  const lastModifiedMs = latestMtime.length > 0 ? Math.max(...latestMtime) : Date.now();
  const lastModified = new Date(lastModifiedMs);
  const pubDate = lastModified.toUTCString();
  const items = books
    .map((book) => {
      const ext = bookExtension(book);
      const mime = bookMime(book);
      const enclosureUrl = `${origin}/stream/${book.id}.${ext}${keySuffix}`;
      const streamable =
        (book.kind === "single" && Boolean(book.primaryFile)) ||
        (book.kind === "multi" && Boolean(book.files && book.files.length > 0));
      const cover = book.coverPath ? `<itunes:image href="${origin}/covers/${book.id}.jpg${keySuffix}" />` : "";
      const epubTag = book.epubPath
        ? `<podible:epub url="${origin}/epubs/${book.id}.epub${keySuffix}" type="application/epub+zip" />`
        : "";
      const tagLength = estimateId3TagLength(book);
      const enclosureLength = book.totalSize + tagLength;
      const durationSeconds = book.durationSeconds ?? 0;
      const duration = formatDuration(durationSeconds);
      const itemPubDate = (book.publishedAt ?? lastModified).toUTCString();
      const fallbackDescription = `${book.title} by ${book.author}`;
      const hasChapters =
        streamable && (book.kind === "multi" || (book.chapters && book.chapters.length > 0));
      const chaptersTag = hasChapters
        ? `<podcast:chapters url="${origin}/chapters/${book.id}.json${keySuffix}" type="application/json+chapters" />`
        : "";
      const chaptersDebugTag = hasChapters
        ? `<podcast:chaptersDebug url="${origin}/chapters-debug/${book.id}.json${keySuffix}" type="application/json" />`
        : "";
      const { description, subtitle, descriptionHtml } = buildItemNotes(book);
      const descriptionForXml = descriptionHtml
        ? `<![CDATA[${descriptionHtml.replace(/]]>/g, "]]]]><![CDATA[>")}]]>`
        : escapeXml(description || fallbackDescription);
      return [
        "<item>",
        `<guid isPermaLink="false">${escapeXml(book.id)}</guid>`,
        `<title>${escapeXml(book.title)}</title>`,
        `<itunes:author>${escapeXml(book.author)}</itunes:author>`,
        `<itunes:subtitle>${escapeXml(subtitle)}</itunes:subtitle>`,
        streamable ? `<enclosure url="${enclosureUrl}" length="${enclosureLength}" type="${mime}" />` : "",
        streamable ? `<link>${enclosureUrl}</link>` : "",
        `<pubDate>${itemPubDate}</pubDate>`,
        `<description>${descriptionForXml}</description>`,
        `<itunes:summary>${escapeXml(description || fallbackDescription)}</itunes:summary>`,
        `<itunes:explicit>${FEED_EXPLICIT}</itunes:explicit>`,
        streamable && duration ? `<itunes:duration>${duration}</itunes:duration>` : "",
        `<itunes:episodeType>full</itunes:episodeType>`,
        cover,
        chaptersTag,
        chaptersDebugTag,
        epubTag,
        "</item>",
      ]
        .filter(Boolean)
        .join("");
    })
    .join("");

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0" xmlns:atom="http://www.w3.org/2005/Atom" xmlns:podible="https://podible.invalid/xmlns">
<channel>
<title>${escapeXml(FEED_TITLE)}</title>
<link>${origin}/feed.xml${keySuffix}</link>
<atom:link href="${origin}/feed.xml${keySuffix}" rel="self" type="application/rss+xml" />
<description>${escapeXml(FEED_DESCRIPTION)}</description>
<language>${FEED_LANGUAGE}</language>
<copyright>${escapeXml(FEED_COPYRIGHT)}</copyright>
<lastBuildDate>${pubDate}</lastBuildDate>
<itunes:subtitle>${escapeXml(FEED_DESCRIPTION)}</itunes:subtitle>
<itunes:author>${escapeXml(FEED_AUTHOR)}</itunes:author>
<itunes:summary>${escapeXml(FEED_DESCRIPTION)}</itunes:summary>
<itunes:explicit>${FEED_EXPLICIT}</itunes:explicit>
<itunes:owner><itunes:name>${escapeXml(FEED_OWNER_NAME)}</itunes:name><itunes:email>${escapeXml(FEED_OWNER_EMAIL)}</itunes:email></itunes:owner>
${channelImage ? `<itunes:image href="${channelImage}" />` : ""}
<itunes:category text="${escapeXml(FEED_CATEGORY)}" />
<itunes:type>${FEED_TYPE}</itunes:type>
${items}
</channel>
</rss>`;

  return { body, lastModified };
}

export { buildItemNotes, rssFeed };

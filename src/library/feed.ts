import { escapeXml, firstLine, htmlToPlainText, truncate } from "../utils/strings";
import { formatDuration } from "../utils/time";

import { preferredAudioManifestationsForBooks, streamExtensionForManifestation } from "./media";
import type { BooksRepo } from "../repo";

function requestOrigin(request: Request): string {
  const url = new URL(request.url);
  const forwardedProto = request.headers.get("x-forwarded-proto");
  const proto = forwardedProto ? forwardedProto.split(",")[0]?.trim() : url.protocol.replace(":", "");
  return `${proto}://${url.host}`;
}

function itemDescription(description: string | null, descriptionHtml: string | null, title: string, author: string): {
  plain: string;
  html?: string;
  subtitle: string;
} {
  const plain = description?.trim() || htmlToPlainText(descriptionHtml || undefined)?.trim() || `${title} by ${author}`;
  const subtitle = truncate(firstLine(plain), 200) || author;
  return {
    plain,
    html: descriptionHtml ?? undefined,
    subtitle,
  };
}

export function buildRssFeed(request: Request, repo: BooksRepo, feedTitle: string, feedAuthor: string): Response {
  const origin = requestOrigin(request);
  const items = preferredAudioManifestationsForBooks(repo);
  const lastModified = items[0]?.book.addedAt ?? new Date().toISOString();

  const body = `<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:itunes="http://www.itunes.com/dtds/podcast-1.0.dtd" xmlns:podcast="https://podcastindex.org/namespace/1.0">
<channel>
<title>${escapeXml(feedTitle)}</title>
<link>${origin}/feed.xml</link>
<description>${escapeXml(feedTitle)}</description>
<itunes:author>${escapeXml(feedAuthor)}</itunes:author>
<lastBuildDate>${new Date(lastModified).toUTCString()}</lastBuildDate>
${items
  .map(({ book, manifestation, containers }) => {
    const primary = containers[0]!;
    const asset = primary.asset;
    const description = itemDescription(book.description, book.descriptionHtml, book.title, book.author);
    const ext = streamExtensionForManifestation(containers);
    const enclosure = `${origin}/stream/m/${manifestation.id}.${ext}`;
    const chapters = `${origin}/chapters/m/${manifestation.id}.json`;
    const pubDate = new Date(book.addedAt).toUTCString();
    const coverTag = book.coverUrl ? `<itunes:image href="${origin}${book.coverUrl}" />` : "";
    const durationMs = manifestation.duration_ms ?? asset.duration_ms ?? 0;
    const totalSize = manifestation.total_size || asset.total_size;
    return `<item>
<guid isPermaLink="false">book-${book.id}-asset-${asset.id}</guid>
<title>${escapeXml(book.title)}</title>
<itunes:author>${escapeXml(book.author)}</itunes:author>
<itunes:subtitle>${escapeXml(description.subtitle)}</itunes:subtitle>
<enclosure url="${enclosure}" length="${totalSize}" type="${asset.mime}" />
<link>${enclosure}</link>
<pubDate>${pubDate}</pubDate>
<description>${description.html ? `<![CDATA[${description.html.replace(/]]>/g, "]]]]><![CDATA[>")}]]>` : escapeXml(description.plain)}</description>
<itunes:summary>${escapeXml(description.plain)}</itunes:summary>
<itunes:duration>${formatDuration(durationMs / 1000)}</itunes:duration>
<podcast:chapters url="${chapters}" type="application/json+chapters" />
${coverTag}
</item>`;
  })
  .join("\n")}
</channel>
</rss>`;

  return new Response(body, {
    headers: {
      "Content-Type": "application/rss+xml",
      "Last-Modified": new Date(lastModified).toUTCString(),
      ETag: `W/"${Date.parse(lastModified)}"`,
    },
  });
}

export function buildJsonFeed(request: Request, repo: BooksRepo, feedTitle: string, feedAuthor: string): Response {
  const origin = requestOrigin(request);
  const items = preferredAudioManifestationsForBooks(repo).map(({ book, manifestation, containers }) => {
    const primary = containers[0]!;
    const asset = primary.asset;
    const description = itemDescription(book.description, book.descriptionHtml, book.title, book.author);
    const ext = streamExtensionForManifestation(containers);
    const streamUrl = `${origin}/stream/m/${manifestation.id}.${ext}`;
    const durationMs = manifestation.duration_ms ?? asset.duration_ms ?? 0;
    const totalSize = manifestation.total_size || asset.total_size;
    return {
      id: `book-${book.id}-asset-${asset.id}`,
      title: book.title,
      content_text: description.plain,
      ...(description.html ? { content_html: description.html } : {}),
      date_published: new Date(book.addedAt).toISOString(),
      date_modified: new Date(book.updatedAt).toISOString(),
      authors: [{ name: book.author }],
      ...(book.coverUrl ? { image: `${origin}${book.coverUrl}` } : {}),
      attachments: [
        {
          url: streamUrl,
          mime_type: asset.mime,
          title: `${book.title}.${ext}`,
          size_in_bytes: totalSize,
          duration_in_seconds: Math.round(durationMs / 1000),
        },
      ],
    };
  });

  const payload = {
    version: "https://jsonfeed.org/version/1.1",
    title: feedTitle,
    feed_url: `${origin}/feed.json`,
    home_page_url: origin,
    authors: [{ name: feedAuthor }],
    items,
  };

  return new Response(JSON.stringify(payload), {
    headers: {
      "Content-Type": "application/feed+json; charset=utf-8",
    },
  });
}

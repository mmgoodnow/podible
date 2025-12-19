import { Book } from "../types";
import {
  FEED_AUTHOR,
  FEED_DESCRIPTION,
  FEED_IMAGE_URL,
  FEED_LANGUAGE,
  FEED_TITLE,
  brandImageExists,
} from "../config";
import { bookExtension, bookMime, cleanLanguage } from "../media/metadata";
import { buildItemNotes } from "./rss.ts";

type JsonFeedAuthor = {
  name?: string;
  url?: string;
  avatar?: string;
};

type JsonFeedAttachment = {
  url: string;
  mime_type: string;
  title?: string;
  size_in_bytes?: number;
  duration_in_seconds?: number;
};

type JsonFeedItem = {
  id: string;
  url?: string;
  title?: string;
  content_text?: string;
  content_html?: string;
  summary?: string;
  date_published?: string;
  date_modified?: string;
  authors?: JsonFeedAuthor[];
  image?: string;
  attachments?: JsonFeedAttachment[];
};

type JsonFeed = {
  version: "https://jsonfeed.org/version/1.1";
  title: string;
  home_page_url?: string;
  feed_url?: string;
  description?: string;
  icon?: string;
  authors?: JsonFeedAuthor[];
  language?: string;
  items: JsonFeedItem[];
};

function jsonFeed(books: Book[], origin: string, keySuffix = ""): { body: string; lastModified: Date } {
  const latestMtime = books
    .map((b) => b.publishedAt?.getTime() ?? 0)
    .filter((t) => t > 0);
  const lastModifiedMs = latestMtime.length > 0 ? Math.max(...latestMtime) : Date.now();
  const lastModified = new Date(lastModifiedMs);

  const firstCover = books.find((b) => b.coverPath);
  const icon =
    FEED_IMAGE_URL ||
    (brandImageExists
      ? `${origin}/podible.png${keySuffix}`
      : firstCover
        ? `${origin}/covers/${firstCover.id}.jpg${keySuffix}`
        : undefined);

  const language = cleanLanguage(FEED_LANGUAGE) || undefined;

  const items: JsonFeedItem[] = books.map((book) => {
    const ext = bookExtension(book);
    const mime = bookMime(book);
    const streamUrl = `${origin}/stream/${book.id}.${ext}${keySuffix}`;
    const streamable =
      (book.kind === "single" && Boolean(book.primaryFile)) ||
      (book.kind === "multi" && Boolean(book.files && book.files.length > 0));
    const coverUrl = book.coverPath ? `${origin}/covers/${book.id}.jpg${keySuffix}` : undefined;
    const epubUrl = book.epubPath ? `${origin}/epubs/${book.id}.epub${keySuffix}` : undefined;
    const { description, descriptionHtml } = buildItemNotes(book);
    const authors: JsonFeedAuthor[] = book.author ? [{ name: book.author }] : [];
    const attachments: JsonFeedAttachment[] = [
      ...(streamable
        ? [
            {
              url: streamUrl,
              mime_type: mime,
              title: `${book.title}${ext ? `.${ext}` : ""}`,
              size_in_bytes: book.totalSize || undefined,
              duration_in_seconds: book.durationSeconds || undefined,
            },
          ]
        : []),
      ...(epubUrl ? [{ url: epubUrl, mime_type: "application/epub+zip", title: `${book.title}.epub` }] : []),
    ];

    return {
      id: book.id,
      ...(streamable ? { url: streamUrl } : {}),
      title: book.title,
      content_text: description || `${book.title} by ${book.author}`,
      ...(descriptionHtml ? { content_html: descriptionHtml } : {}),
      ...(book.publishedAt ? { date_published: book.publishedAt.toISOString() } : {}),
      ...(book.publishedAt ? { date_modified: book.publishedAt.toISOString() } : {}),
      ...(authors.length ? { authors } : {}),
      ...(coverUrl ? { image: coverUrl } : {}),
      ...(attachments.length ? { attachments } : {}),
    };
  });

  const feed: JsonFeed = {
    version: "https://jsonfeed.org/version/1.1",
    title: FEED_TITLE,
    home_page_url: `${origin}/${keySuffix}`,
    feed_url: `${origin}/feed.json${keySuffix}`,
    description: FEED_DESCRIPTION,
    ...(icon ? { icon } : {}),
    ...(FEED_AUTHOR ? { authors: [{ name: FEED_AUTHOR }] } : {}),
    ...(language ? { language } : {}),
    items,
  };

  return { body: JSON.stringify(feed), lastModified };
}

export { jsonFeed };

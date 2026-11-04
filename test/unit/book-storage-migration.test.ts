import { Database } from "bun:sqlite";
import { expect, test } from "bun:test";
import { runMigrations } from "../../src/db";
import { BooksRepo } from "../../src/repo";

test("mutable provider GUID migration preserves legacy releases and keeps infohash identity unique", () => {
  using db = new Database(":memory:");
  runMigrations(db);
  const repo = new BooksRepo(db);
  const book = repo.createBook({
    title: "A library work",
    author: "Synthetic Author",
  });
  const input = {
    bookId: book.id,
    provider: "Fixture",
    providerGuid: "reused-guid",
    title: "Original edition",
    mediaType: "audio" as const,
    infoHash: "a".repeat(40),
    url: "https://fixture.invalid/original",
  };
  const original = repo.createRelease(input);
  db.exec("DELETE FROM schema_migrations WHERE id=44");
  db.exec(
    "CREATE UNIQUE INDEX ux_releases_provider_guid ON releases(provider,provider_guid) WHERE provider_guid IS NOT NULL",
  );
  expect(() =>
    repo.createRelease({ ...input, infoHash: "b".repeat(40) }),
  ).toThrow();
  runMigrations(db);
  expect(repo.getRelease(original.id)).toEqual(original);
  expect(repo.bookTorrents.get(original.info_hash)).toBeNull();
  const replacement = repo.createRelease({
    ...input,
    infoHash: "b".repeat(40),
    title: "Corrected edition",
    url: "https://fixture.invalid/corrected",
  });
  expect(repo.findReleaseByProviderGuid("Fixture", "reused-guid")?.id).toBe(
    replacement.id,
  );
  expect(repo.findReleaseByInfoHash(original.info_hash)?.id).toBe(original.id);
  expect(() =>
    repo.createRelease({ ...input, providerGuid: "different-guid" }),
  ).toThrow();
  runMigrations(db);
  expect(repo.listReleasesByBook(book.id)).toHaveLength(2);
  expect(repo.getBookRow(book.id)).toEqual(book);
});

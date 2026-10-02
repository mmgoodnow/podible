import { expect, test } from "bun:test";
import { promptForChunk } from "../../src/library/chapter-analysis";

test("evaluation can reuse the exact production glossary prompt", () => {
  expect(promptForChunk({ title: "Example", author: "Author", language: "spa" }, ["Name", "Place"], { language: "en" }))
    .toBe("Example by Author. Language: en. Important names and terms may include: Name, Place. Preserve these spellings when spoken.");
});

test("empty glossary adds no vocabulary hints", () => {
  expect(promptForChunk({ title: "Example", author: "Author", language: null }, []))
    .toBe("Example by Author. Language: en.");
});

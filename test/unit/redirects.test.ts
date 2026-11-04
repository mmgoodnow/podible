import { expect, test } from "bun:test";
import { safeReturnPath } from "../../src/redirects";

test("authentication returns only to same-origin application paths", () => {
  for (const value of [null, "https://wrong.example", "//wrong.example", "/\\wrong.example", "/\nwrong", "javascript:alert(1)"])
    expect(safeReturnPath(value)).toBeNull();
  expect(safeReturnPath("/book/12?edition=3#chapters")).toBe("/book/12?edition=3#chapters");
  expect(safeReturnPath("/auth/app/opaque/complete")).toBe("/auth/app/opaque/complete");
});

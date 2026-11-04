/// <reference path="../../web/src/css.d.ts" />
import { expect, test } from "bun:test";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { ContentRail, RailBook } from "../../web/src/content-rail";

test("rail has named keyboard focus target, labelled controls and real book navigation/fallback", () => {
  const html = renderToStaticMarkup(createElement(ContentRail, { title: "Newly added books",
    children: createElement(RailBook, { book: { id: 4, title: "The House", author: "Author", posterUrl: null } }) }));
  expect(html).toContain('aria-label="Newly added books cards"');
  expect(html).toContain('tabindex="0"');
  expect(html).toContain('href="/book/4"');
  expect(html).toContain('aria-label="No cover available for The House"');
});

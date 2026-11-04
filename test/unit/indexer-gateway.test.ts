import { expect, test } from "bun:test";
import { IndexerGateway } from "../../src/media/indexer-gateway";
import { IndexerError } from "../../src/media/indexer-errors";
import { ProwlarrDiscovery } from "../../src/media/prowlarr";

const source = { name: "Shared", baseUrl: "https://indexer.invalid/1/api", apiKey: "private-key", categories: { audio: "3030", ebook: "7020" } };
test("movie, book and RSS requests share a provider cooldown and sanitize upstream failures", async () => {
  let now = 1_000_000, calls = 0;
  const client = new IndexerGateway(async () => { calls++; return new Response("private-key", {status:429,headers:{"retry-after":"300"}}); }, () => now);
  expect((await client.search([source], "Film", "movie")).failures[0]?.retryAt).toBe(now+300_000);
  expect((await client.search([source], "Book", "ebook")).failures[0]?.status).toBe(429);
  await expect(client.page(source, "movie")).rejects.toBeInstanceOf(IndexerError);
  expect(calls).toBe(1);
  now+=300_000;
  await client.search([source], "Book", "audio");
  expect(calls).toBe(2);
});
test("feed pages omit per-title queries, retain pagination and use normal normalized releases", async () => {
  const client = new IndexerGateway(async (url) => {
    expect(url.searchParams.has("q")).toBe(false);
    expect(url.searchParams.get("t")).toBe("search");
    expect(url.searchParams.get("cat")).toBe("2000");
    expect(url.searchParams.get("offset")).toBe("100");
    expect(url.searchParams.get("limit")).toBe("100");
    return new Response('<rss xmlns:newznab="urn:newznab"><channel><newznab:response offset="100" total="250"/><item><title>A Film</title><guid>item-1</guid><link>https://download.invalid/torrent</link></item></channel></rss>');
  });
  const page = await client.page(source,"movie",100,100);
  expect(page).toMatchObject({offset:100,total:250,count:1});
  expect(page.results[0]).toMatchObject({title:"A Film",guid:"item-1",mediaType:"movie"});
});
test("feed pagination mismatch and oversized chunked responses are not healthy empty feeds", async () => {
  const wrong = new IndexerGateway(async () => new Response('<rss><channel><response offset="0" total="200"/></channel></rss>'));
  await expect(wrong.page(source,"movie",100)).rejects.toBeInstanceOf(IndexerError);
  const large = new IndexerGateway(async () => new Response(new ReadableStream({
    start(controller) { for(let i=0;i<9;i++)controller.enqueue(new Uint8Array(1024**2));controller.close(); },
  })));
  await expect(large.page(source,"movie")).rejects.toBeInstanceOf(IndexerError);
});
test("RSS discovery respects RSS capability independently from search capability", async () => {
  const discovery = new ProwlarrDiscovery({url:"https://indexer.invalid",apiKey:"private-key"},async input=>Response.json(
    String(input).endsWith("/indexerstatus")?[]:[
      {id:1,name:"RSS only",enable:true,protocol:"torrent",supportsRss:true,supportsSearch:false,supportsPagination:false,capabilities:{categories:[{id:2000}]}},
      {id:2,name:"Search only",enable:true,protocol:"torrent",supportsRss:false,supportsSearch:true,capabilities:{categories:[{id:2000}]}},
    ],
  ));
  expect((await discovery.sources("movie","rss")).map(x=>x.name)).toEqual(["RSS only"]);
  expect((await discovery.sources("movie","rss"))[0]?.supportsPagination).toBe(false);
  expect((await discovery.sources("movie")).map(x=>x.name)).toEqual(["Search only"]);
});

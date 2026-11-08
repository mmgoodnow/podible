import type { Database } from "bun:sqlite";
import type { BooksRepo } from "../../src/repo";
import type { MovieRepo } from "../../src/movies/repo";
import type { TvRepo } from "../../src/tv/repo";
import { immediateTransaction } from "../../src/db-transaction";

export function synchronousTransactionTypes(db: Database, books: BooksRepo, movies: MovieRepo, tv: TvRepo) {
  const result: number = immediateTransaction(db, () => {}, () => 1);
  void result;
  // @ts-expect-error Async callbacks cannot run inside a synchronous SQLite transaction.
  immediateTransaction(db, () => {}, async () => 1);
  // @ts-expect-error A mixed sync/async return cannot escape validation through a union.
  immediateTransaction(db, () => {}, () => Math.random() ? 1 : Promise.resolve(1));
  // @ts-expect-error Custom thenable results are asynchronous too.
  immediateTransaction(db, () => {}, () => ({ then(resolve: (value: number) => void) { resolve(1); } }));
  // @ts-expect-error Repository wrappers preserve the synchronous callback contract.
  books.transaction(async () => 1);
  // @ts-expect-error Repository wrappers preserve the synchronous callback contract.
  movies.transaction(async () => 1);
  // @ts-expect-error Repository wrappers preserve the synchronous callback contract.
  tv.transaction(async () => 1);
}

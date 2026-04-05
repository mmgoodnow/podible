import { booksDbPath } from "../config";

import { openDatabase } from "./db";
import { BooksRepo } from "./repo";

let repo: BooksRepo | null = null;

function stateRepo(): BooksRepo {
  if (repo) return repo;
  repo = new BooksRepo(openDatabase(booksDbPath));
  return repo;
}

export function loadJsonState<T>(key: string): T | null {
  return stateRepo().getJsonState<T>(key);
}

export function saveJsonState(key: string, value: unknown): void {
  stateRepo().setJsonState(key, value);
}

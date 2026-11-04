import type { Database } from "bun:sqlite";
import type { Insertable, Selectable } from "kysely";
import type { DB } from "../../src/db/schema";
import { dbSql, typedAll, typedGet } from "../../src/db/typed";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends (<T>() => T extends B ? 1 : 2) ? true : false;
type Assert<T extends true> = T;

// Included by the normal typecheck, intentionally never called by runtime tests.
export function typedSQLiteCompileTimeContract(db: Database) {
  // @ts-expect-error Unknown tables must be rejected.
  dbSql.selectFrom("not_a_table");
  // @ts-expect-error Unknown columns must be rejected.
  dbSql.selectFrom("books").select("not_a_column");
  // @ts-expect-error Predicates cannot reference nonexistent columns.
  dbSql.selectFrom("books").selectAll().where("not_a_column", "=", "x");
  // @ts-expect-error Storage integers are not runtime-coerced booleans.
  dbSql.updateTable("users").set({ is_admin: true });
  // @ts-expect-error Required NOT NULL columns without defaults cannot be omitted.
  const invalidBook: Insertable<DB["books"]> = { title: "missing fields" };
  // @ts-expect-error Migration 34 drops lifecycle status; Reflow is its owner.
  dbSql.selectFrom("jobs").select("status");

  const query = dbSql.selectFrom("books").select(["id", "cover_path as cover"]);
  const row = typedGet(db, query);
  const rows = typedAll(db, query);
  const compiledRow = typedGet(db, query.compile());
  const inserted = typedGet(db, dbSql.insertInto("books").values({
    title: "Book", author: "Author", added_at: "now", updated_at: "now",
  }).returning(["id", "cover_path"]));
  type RowInference = Assert<Equal<typeof row, { id: number; cover: string | null } | null>>;
  type AllInference = Assert<Equal<typeof rows, { id: number; cover: string | null }[]>>;
  type CompiledInference = Assert<Equal<typeof compiledRow, { id: number; cover: string | null } | null>>;
  type ReturningInference = Assert<Equal<typeof inserted, { id: number; cover_path: string | null } | null>>;
  type NullableTextPrimaryKey = Assert<Equal<Selectable<DB["app_state"]>["key"], string | null>>;
  type DefaultIsNotNull = Assert<Equal<Selectable<DB["users"]>["is_admin"], number>>;
  type DefaultIsOptionalOnInsert = Assert<Equal<Insertable<DB["users"]>["is_admin"], number | undefined>>;
  type NullableHasNoUndefinedOnRead = Assert<Equal<Selectable<DB["books"]>["cover_path"], string | null>>;
  return { invalidBook, row, rows, compiledRow, inserted };
}

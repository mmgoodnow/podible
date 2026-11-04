import type { Changes, Database } from "bun:sqlite";
import {
  type Compilable,
  type CompiledQuery,
  type DatabaseConnection,
  DummyDriver,
  Kysely,
  SqliteAdapter,
  SqliteIntrospector,
  SqliteQueryCompiler,
} from "kysely";
import type { DB } from "./schema";

class CompileOnlyDriver extends DummyDriver {
  override async acquireConnection(): Promise<DatabaseConnection> {
    throw new Error("dbSql only compiles SQL; use typedGet, typedAll or typedRun with the existing Database");
  }
}

/** SQL compiler only: never opens a connection or owns transactions/migrations. */
export const dbSql = new Kysely<DB>({
  dialect: {
    createAdapter: () => new SqliteAdapter(),
    createDriver: () => new CompileOnlyDriver(),
    createIntrospector: (db) => new SqliteIntrospector(db),
    createQueryCompiler: () => new SqliteQueryCompiler(),
  },
});

export type TypedQuery<Row> = Compilable<Row> | CompiledQuery<Row>;
type Parameter = null | string | number | bigint | Uint8Array;

function assertParameter(value: unknown, index: number): asserts value is Parameter {
  if (value === null || typeof value === "string" || value instanceof Uint8Array) return;
  if (typeof value === "number" && Number.isFinite(value) && (!Number.isInteger(value) || Number.isSafeInteger(value))) return;
  if (typeof value === "bigint" && value >= -(2n ** 63n) && value < 2n ** 63n) return;
  // Booleans, dates and JSON must be deliberately encoded by the repository.
  throw new TypeError(`Invalid SQLite parameter at index ${index}: expected null, string, finite number, signed 64-bit bigint or Uint8Array`);
}

function prepare<Row>(db: Database, query: TypedQuery<Row>) {
  const compiled = "compile" in query ? query.compile() : query;
  const parameters: Parameter[] = [];
  for (let index = 0; index < compiled.parameters.length; index += 1) {
    const value = compiled.parameters[index];
    assertParameter(value, index);
    parameters.push(value);
  }
  const statement = db.query<Row, Parameter[]>(compiled.sql);
  // Bun caches statements and otherwise permits missing bindings to reuse old values.
  if (statement.paramsCount !== parameters.length) {
    throw new TypeError(`SQLite parameter count mismatch: expected ${statement.paramsCount}, received ${parameters.length}`);
  }
  return { statement, parameters };
}

/** Execute on the caller's connection, including inside existing transactions. */
export function typedGet<Row>(db: Database, query: TypedQuery<Row>): Row | null {
  const { statement, parameters } = prepare(db, query);
  return statement.get(...parameters);
}

export function typedAll<Row>(db: Database, query: TypedQuery<Row>): Row[] {
  const { statement, parameters } = prepare(db, query);
  return statement.all(...parameters);
}

export function typedRun(db: Database, query: TypedQuery<unknown>): Changes {
  const { statement, parameters } = prepare(db, query);
  return statement.run(...parameters);
}

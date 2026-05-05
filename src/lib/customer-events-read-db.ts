import { createClient } from "@libsql/client";
import type { InArgs } from "@libsql/core/api";
import type { DbClient, DbQueryResult } from "@/lib/db";

let readDb: DbClient | null = null;
let writeDb: DbClient | null = null;

export function getCustomerEventsReadDb(): DbClient {
  if (readDb) return readDb;

  const url = process.env.TURSO_DATABASE_URL;
  if (!url) {
    throw new Error("TURSO_DATABASE_URL is required for customer pages");
  }

  const client = createClient({
    url,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  readDb = {
    async query(sql: string, args: unknown[] = []): Promise<DbQueryResult> {
      const result = await client.execute({ sql, args: args as InArgs });
      return {
        rows: result.rows as unknown as Record<string, unknown>[],
        lastInsertRowid: result.lastInsertRowid ?? 0,
        changes: result.rowsAffected,
      };
    },
    async run(): Promise<DbQueryResult> {
      throw new Error("Customer event read DB does not allow writes");
    },
    async exec(): Promise<void> {
      throw new Error("Customer event read DB does not allow schema changes");
    },
  };

  return readDb;
}

export function getCustomerEventsWriteDb(): DbClient {
  if (writeDb) return writeDb;

  const url = process.env.TURSO_DATABASE_URL;
  if (!url) {
    throw new Error("TURSO_DATABASE_URL is required for customer pages");
  }

  const client = createClient({
    url,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  writeDb = {
    async query(sql: string, args: unknown[] = []): Promise<DbQueryResult> {
      const result = await client.execute({ sql, args: args as InArgs });
      return {
        rows: result.rows as unknown as Record<string, unknown>[],
        lastInsertRowid: result.lastInsertRowid ?? 0,
        changes: result.rowsAffected,
      };
    },
    async run(sql: string, args: unknown[] = []): Promise<DbQueryResult> {
      const result = await client.execute({ sql, args: args as InArgs });
      return {
        rows: result.rows as unknown as Record<string, unknown>[],
        lastInsertRowid: result.lastInsertRowid ?? 0,
        changes: result.rowsAffected,
      };
    },
    async exec(): Promise<void> {
      throw new Error("Customer event write DB does not allow schema changes");
    },
  };

  return writeDb;
}

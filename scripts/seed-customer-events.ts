#!/usr/bin/env npx tsx

import fs from "fs";
import path from "path";
import { createClient } from "@libsql/client";
import type { InArgs } from "@libsql/core/api";
import { CUSTOMER_EVENT_SEEDS } from "../src/lib/customer-event-seed";
import {
  ensureCustomerEventsTables,
  refreshCustomerEventSnapshots,
  replaceCustomerSeed,
  type CustomerOption,
} from "../src/lib/customer-events-store";
import type { DbClient, DbQueryResult } from "../src/lib/db";

function loadEnv() {
  const envPath = path.resolve(__dirname, "../.env.local");
  if (!fs.existsSync(envPath)) return;

  for (const line of fs.readFileSync(envPath, "utf8").split("\n")) {
    const match = line.match(/^([A-Z0-9_]+)=(.*)$/);
    if (match && !process.env[match[1]]) {
      process.env[match[1]] = match[2];
    }
  }
}

function createTursoDbClient(): DbClient {
  const url = process.env.TURSO_DATABASE_URL;
  if (!url) {
    throw new Error("TURSO_DATABASE_URL is required");
  }

  const client = createClient({
    url,
    authToken: process.env.TURSO_AUTH_TOKEN,
  });

  return {
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
    async exec(sql: string): Promise<void> {
      const statements = sql
        .split(";")
        .map((statement) => statement.trim())
        .filter(Boolean);
      for (const statement of statements) {
        await client.execute(statement);
      }
    },
  };
}

async function readCustomers(db: DbClient): Promise<CustomerOption[]> {
  const result = await db.query(`
    SELECT c.slug, c.name, COUNT(ce.event_id) AS event_count
    FROM customers c
    LEFT JOIN customer_events ce ON ce.customer_slug = c.slug
    GROUP BY c.slug, c.name
    ORDER BY c.name COLLATE NOCASE
  `);

  return result.rows.map((row) => ({
    slug: String(row.slug ?? ""),
    name: String(row.name ?? ""),
    eventCount: Number(row.event_count ?? 0),
  }));
}

async function main() {
  loadEnv();
  const db = createTursoDbClient();
  await ensureCustomerEventsTables(db);

  for (const seed of CUSTOMER_EVENT_SEEDS) {
    await replaceCustomerSeed(db, seed);
    const refreshed = await refreshCustomerEventSnapshots(db, seed.slug);
    console.log(
      `Seeded ${seed.name}: ${seed.eventIds.length} events, refreshed ${refreshed} rows`
    );
  }

  for (const customer of await readCustomers(db)) {
    console.log(`${customer.slug}: ${customer.eventCount} events`);
  }
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});

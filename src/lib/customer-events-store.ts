import type { DbClient } from "@/lib/db";
import type { CustomerEventSeed } from "@/lib/customer-event-seed";

export type CustomerSortKey =
  | "position"
  | "date"
  | "bitrate"
  | "fps"
  | "vru"
  | "production";
export type CustomerSortDir = "asc" | "desc";

export interface CustomerOption {
  slug: string;
  name: string;
  eventCount: number;
}

export interface CustomerEventRow {
  position: number;
  eventId: string;
  eventType: string | null;
  eventTimestamp: string | null;
  bitrateBps: number | null;
  fpsQc: string | null;
  lateFramePct: number | null;
  vruStatus: string;
  vruLabel: string | null;
  vruConfidence: number | null;
  productionStatus: string;
  productionPriority: number | null;
}

export interface CustomerEventList {
  customer: CustomerOption;
  customers: CustomerOption[];
  rows: CustomerEventRow[];
  sort: CustomerSortKey;
  dir: CustomerSortDir;
}

export const CUSTOMER_EVENTS_SCHEMA_SQL = `
  CREATE TABLE IF NOT EXISTS customers (
    slug TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS customer_events (
    customer_slug TEXT NOT NULL,
    event_id TEXT NOT NULL,
    list_position INTEGER NOT NULL,
    event_type TEXT,
    event_timestamp TEXT,
    lat REAL,
    lon REAL,
    bitrate_bps REAL,
    fps_qc TEXT,
    late_frame_pct REAL,
    max_delta_ms REAL,
    vru_status TEXT NOT NULL DEFAULT 'not_run',
    vru_label TEXT,
    vru_confidence REAL,
    production_status TEXT NOT NULL DEFAULT 'not_queued',
    production_priority INTEGER,
    refreshed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (customer_slug, event_id),
    UNIQUE (customer_slug, list_position)
  );

  CREATE INDEX IF NOT EXISTS idx_customer_events_slug_position
    ON customer_events (customer_slug, list_position);

  CREATE INDEX IF NOT EXISTS idx_customer_events_event_id
    ON customer_events (event_id);
`;

const CUSTOMER_OPTIONS_SQL = `
  SELECT
    c.slug,
    c.name,
    COUNT(ce.event_id) AS event_count
  FROM customers c
  LEFT JOIN customer_events ce ON ce.customer_slug = c.slug
  GROUP BY c.slug, c.name
  ORDER BY c.name COLLATE NOCASE
`;

const SORT_SQL: Record<CustomerSortKey, string> = {
  position: "ce.list_position",
  date: "ce.event_timestamp",
  bitrate: "ce.bitrate_bps",
  fps: "ce.fps_qc",
  vru: "COALESCE(ce.vru_status, 'not_run')",
  production: "COALESCE(pr.status, ce.production_status, 'not_queued')",
};

export function normalizeCustomerSort(
  sort?: string,
  dir?: string
): { sort: CustomerSortKey; dir: CustomerSortDir } {
  const sortKey = sort && sort in SORT_SQL ? (sort as CustomerSortKey) : "position";
  const sortDir: CustomerSortDir = dir === "desc" ? "desc" : "asc";
  return { sort: sortKey, dir: sortDir };
}

export async function ensureCustomerEventsTables(db: DbClient): Promise<void> {
  await db.exec(CUSTOMER_EVENTS_SCHEMA_SQL);
  await ensureCustomerEventSnapshotColumns(db);
}

export async function replaceCustomerSeed(
  db: DbClient,
  seed: CustomerEventSeed
): Promise<void> {
  await db.run(
    `INSERT INTO customers (slug, name, updated_at)
     VALUES (?, ?, datetime('now'))
     ON CONFLICT(slug) DO UPDATE SET
       name = excluded.name,
       updated_at = excluded.updated_at`,
    [seed.slug, seed.name]
  );
  await db.run("DELETE FROM customer_events WHERE customer_slug = ?", [seed.slug]);

  for (let index = 0; index < seed.eventIds.length; index++) {
    await db.run(
      `INSERT INTO customer_events (customer_slug, event_id, list_position)
       VALUES (?, ?, ?)`,
      [seed.slug, seed.eventIds[index], index + 1]
    );
  }
}

export async function refreshCustomerEventSnapshots(
  db: DbClient,
  slug: string
): Promise<number> {
  const result = await db.query(
    `
    WITH listed AS (
      SELECT customer_slug, event_id
      FROM customer_events
      WHERE customer_slug = ?
    ),
    vru_status AS (
      SELECT
        dr.video_id,
        CASE
          WHEN SUM(CASE WHEN dr.status = 'completed' THEN 1 ELSE 0 END) > 0 THEN 'completed'
          WHEN SUM(CASE WHEN dr.status = 'running' THEN 1 ELSE 0 END) > 0 THEN 'running'
          WHEN SUM(CASE WHEN dr.status = 'queued' THEN 1 ELSE 0 END) > 0 THEN 'queued'
          WHEN SUM(CASE WHEN dr.status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'failed'
          WHEN SUM(CASE WHEN dr.status = 'cancelled' THEN 1 ELSE 0 END) > 0 THEN 'cancelled'
          ELSE NULL
        END AS vru_status
      FROM detection_runs dr
      INNER JOIN listed l ON l.event_id = dr.video_id
      GROUP BY dr.video_id
    ),
    best_vru AS (
      SELECT video_id, label, max_confidence
      FROM (
        SELECT
          s.video_id,
          s.label,
          s.max_confidence,
          ROW_NUMBER() OVER (
            PARTITION BY s.video_id
            ORDER BY s.max_confidence DESC, s.created_at DESC
          ) AS rn
        FROM video_detection_segments s
        INNER JOIN listed l ON l.event_id = s.video_id
      )
      WHERE rn = 1
    )
    SELECT
      ce.event_id,
      tr.event_type,
      tr.event_timestamp,
      tr.lat,
      tr.lon,
      tr.bitrate_bps,
      q.bucket AS fps_qc,
      q.gap_pct AS late_frame_pct,
      q.max_delta_ms,
      vs.vru_status,
      bv.label AS vru_label,
      bv.max_confidence AS vru_confidence,
      pr.status AS production_status,
      pr.priority AS production_priority
    FROM listed ce
    LEFT JOIN triage_results tr ON tr.id = ce.event_id
    LEFT JOIN video_frame_timing_qc q ON q.video_id = ce.event_id
    LEFT JOIN vru_status vs ON vs.video_id = ce.event_id
    LEFT JOIN best_vru bv ON bv.video_id = ce.event_id
    LEFT JOIN production_runs pr ON pr.video_id = ce.event_id
    `,
    [slug]
  );

  for (const row of result.rows) {
    await db.run(
      `UPDATE customer_events
       SET
         event_type = ?,
         event_timestamp = ?,
         lat = ?,
         lon = ?,
         bitrate_bps = ?,
         fps_qc = ?,
         late_frame_pct = ?,
         max_delta_ms = ?,
         vru_status = ?,
         vru_label = ?,
         vru_confidence = ?,
         production_status = ?,
         production_priority = ?,
         refreshed_at = datetime('now'),
         updated_at = datetime('now')
       WHERE customer_slug = ? AND event_id = ?`,
      [
        stringValue(row.event_type),
        stringValue(row.event_timestamp),
        numberValue(row.lat),
        numberValue(row.lon),
        numberValue(row.bitrate_bps),
        stringValue(row.fps_qc),
        numberValue(row.late_frame_pct),
        numberValue(row.max_delta_ms),
        stringValue(row.vru_status) ?? "not_run",
        stringValue(row.vru_label),
        numberValue(row.vru_confidence),
        stringValue(row.production_status) ?? "not_queued",
        numberValue(row.production_priority),
        slug,
        stringValue(row.event_id),
      ]
    );
  }

  return result.rows.length;
}

export async function removeCustomerEvent(
  db: DbClient,
  slug: string,
  eventId: string
): Promise<void> {
  if (!slug || !eventId) return;

  await db.run(
    "DELETE FROM customer_events WHERE customer_slug = ? AND event_id = ?",
    [slug, eventId]
  );

  const remaining = await db.query(
    `SELECT event_id, list_position
     FROM customer_events
     WHERE customer_slug = ?
     ORDER BY list_position ASC`,
    [slug]
  );

  for (let index = 0; index < remaining.rows.length; index++) {
    const row = remaining.rows[index];
    const id = stringValue(row.event_id);
    const currentPosition = numberValue(row.list_position);
    const nextPosition = index + 1;
    if (!id || currentPosition === nextPosition) continue;

    await db.run(
      `UPDATE customer_events
       SET list_position = ?, updated_at = datetime('now')
       WHERE customer_slug = ? AND event_id = ?`,
      [nextPosition, slug, id]
    );
  }
}

export async function loadCustomerOptions(db: DbClient): Promise<CustomerOption[]> {
  const result = await db.query(CUSTOMER_OPTIONS_SQL);
  return result.rows.map((row) => ({
    slug: stringValue(row.slug) ?? "",
    name: stringValue(row.name) ?? "",
    eventCount: numberValue(row.event_count) ?? 0,
  })).filter((customer) => customer.slug && customer.name);
}

export async function loadCustomerEventList(
  db: DbClient,
  slug: string,
  sortParam?: string,
  dirParam?: string
): Promise<CustomerEventList | null> {
  const { sort, dir } = normalizeCustomerSort(sortParam, dirParam);
  const customers = await loadCustomerOptions(db);
  const customer = customers.find((item) => item.slug === slug);
  if (!customer) return null;

  const orderBy = SORT_SQL[sort];
  const nullOrder = sort === "position" ? "" : `${orderBy} IS NULL, `;
  const result = await db.query(
    `
    SELECT
      ce.list_position,
      ce.event_id,
      ce.event_type,
      ce.event_timestamp,
      ce.bitrate_bps,
      ce.fps_qc,
      ce.late_frame_pct,
      ce.vru_status,
      ce.vru_label,
      ce.vru_confidence,
      COALESCE(pr.status, ce.production_status, 'not_queued') AS production_status,
      COALESCE(pr.priority, ce.production_priority) AS production_priority
    FROM customer_events ce
    LEFT JOIN production_runs pr ON pr.video_id = ce.event_id
    WHERE ce.customer_slug = ?
    ORDER BY ${nullOrder}${orderBy} ${dir.toUpperCase()}, ce.list_position ASC
    `,
    [slug]
  );

  return {
    customer,
    customers,
    rows: result.rows.map(toCustomerEventRow),
    sort,
    dir,
  };
}

async function ensureCustomerEventSnapshotColumns(db: DbClient): Promise<void> {
  const result = await db.query("PRAGMA table_info(customer_events)");
  const columns = new Set(result.rows.map((row) => String(row.name)));
  const requiredColumns: Array<[string, string]> = [
    ["event_type", "TEXT"],
    ["event_timestamp", "TEXT"],
    ["lat", "REAL"],
    ["lon", "REAL"],
    ["bitrate_bps", "REAL"],
    ["fps_qc", "TEXT"],
    ["late_frame_pct", "REAL"],
    ["max_delta_ms", "REAL"],
    ["vru_status", "TEXT NOT NULL DEFAULT 'not_run'"],
    ["vru_label", "TEXT"],
    ["vru_confidence", "REAL"],
    ["production_status", "TEXT NOT NULL DEFAULT 'not_queued'"],
    ["production_priority", "INTEGER"],
    ["refreshed_at", "TEXT"],
  ];

  for (const [name, definition] of requiredColumns) {
    if (!columns.has(name)) {
      await db.run(`ALTER TABLE customer_events ADD COLUMN ${name} ${definition}`);
    }
  }
}

function toCustomerEventRow(row: Record<string, unknown>): CustomerEventRow {
  return {
    position: numberValue(row.list_position) ?? 0,
    eventId: stringValue(row.event_id) ?? "",
    eventType: stringValue(row.event_type),
    eventTimestamp: stringValue(row.event_timestamp),
    bitrateBps: numberValue(row.bitrate_bps),
    fpsQc: stringValue(row.fps_qc),
    lateFramePct: numberValue(row.late_frame_pct),
    vruStatus: stringValue(row.vru_status) ?? "not_run",
    vruLabel: stringValue(row.vru_label),
    vruConfidence: numberValue(row.vru_confidence),
    productionStatus: stringValue(row.production_status) ?? "not_queued",
    productionPriority: numberValue(row.production_priority),
  };
}

function stringValue(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function numberValue(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

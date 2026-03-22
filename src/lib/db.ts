import path from "path";
import fs from "fs";
import {
  CURRENT_PIPELINE_VERSION,
  SYSTEM_VRU_LABELS,
} from "@/lib/pipeline-config";

// ---------------------------------------------------------------------------
// DbClient abstraction – works with both better-sqlite3 (sync, local) and
// @libsql/client (async, Turso).  Callers always `await` the results; for the
// sync backend the promises resolve immediately.
// ---------------------------------------------------------------------------

export interface DbQueryResult {
  rows: Record<string, unknown>[];
  lastInsertRowid: number | bigint;
  changes: number;
}

export interface DbClient {
  /** SELECT-style query – returns rows. */
  query(sql: string, args?: unknown[]): Promise<DbQueryResult>;
  /** INSERT / UPDATE / DELETE – returns lastInsertRowid & changes. */
  run(sql: string, args?: unknown[]): Promise<DbQueryResult>;
  /** Execute raw SQL (DDL, multi-statement). */
  exec(sql: string): Promise<void>;
}

// ---------------------------------------------------------------------------
// Schema SQL shared by both backends
// ---------------------------------------------------------------------------

const SCHEMA_SQL = `
  CREATE TABLE IF NOT EXISTS labels (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS video_labels (
    video_id TEXT NOT NULL,
    label_id INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (video_id, label_id),
    FOREIGN KEY (label_id) REFERENCES labels(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS video_pipeline_state (
    video_id TEXT PRIMARY KEY,
    day TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'unprocessed',
    pipeline_version TEXT NOT NULL,
    model_name TEXT,
    labels_applied TEXT NOT NULL DEFAULT '[]',
    queued_at TEXT,
    started_at TEXT,
    completed_at TEXT,
    last_heartbeat_at TEXT,
    last_error TEXT
  );

  CREATE TABLE IF NOT EXISTS video_detection_segments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    label TEXT NOT NULL,
    start_ms INTEGER NOT NULL,
    end_ms INTEGER NOT NULL,
    max_confidence REAL NOT NULL,
    support_level TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'yolo',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS pipeline_runs (
    id TEXT PRIMARY KEY,
    day TEXT NOT NULL,
    batch_size INTEGER NOT NULL,
    status TEXT NOT NULL,
    cursor_offset INTEGER NOT NULL DEFAULT 0,
    totals_json TEXT NOT NULL DEFAULT '{}',
    pipeline_version TEXT NOT NULL,
    model_name TEXT,
    worker_pid INTEGER,
    bee_maps_key TEXT,
    started_at TEXT,
    completed_at TEXT,
    last_heartbeat_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS pipeline_run_seen_videos (
    run_id TEXT NOT NULL,
    video_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (run_id, video_id),
    FOREIGN KEY (run_id) REFERENCES pipeline_runs(id) ON DELETE CASCADE
  );

  CREATE TABLE IF NOT EXISTS video_detection_boxes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    label TEXT NOT NULL,
    confidence REAL NOT NULL,
    x1 REAL NOT NULL,
    y1 REAL NOT NULL,
    x2 REAL NOT NULL,
    y2 REAL NOT NULL,
    pipeline_version TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS clip_summaries (
    video_id TEXT PRIMARY KEY,
    summary TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE TABLE IF NOT EXISTS frame_detections (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    video_id TEXT NOT NULL,
    frame_ms INTEGER NOT NULL,
    label TEXT NOT NULL,
    x_min REAL NOT NULL,
    y_min REAL NOT NULL,
    x_max REAL NOT NULL,
    y_max REAL NOT NULL,
    confidence REAL NOT NULL,
    frame_width INTEGER NOT NULL,
    frame_height INTEGER NOT NULL,
    pipeline_version TEXT NOT NULL,
    model_name TEXT NOT NULL DEFAULT 'yolo11x',
    run_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_frame_detections_video_frame
    ON frame_detections (video_id, frame_ms);

  CREATE INDEX IF NOT EXISTS idx_frame_detections_run_id
    ON frame_detections (run_id);

  CREATE TABLE IF NOT EXISTS detection_runs (
    id TEXT PRIMARY KEY,
    video_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'queued',
    config_json TEXT NOT NULL DEFAULT '{}',
    detection_count INTEGER,
    worker_pid INTEGER,
    started_at TEXT,
    completed_at TEXT,
    last_heartbeat_at TEXT,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
  );

  CREATE INDEX IF NOT EXISTS idx_detection_runs_video
    ON detection_runs (video_id, created_at DESC);

  CREATE INDEX IF NOT EXISTS idx_video_pipeline_state_day
    ON video_pipeline_state (day);
`;

// ---------------------------------------------------------------------------
// better-sqlite3 backend (sync, local file)
// ---------------------------------------------------------------------------

async function createSqliteClient(): Promise<DbClient> {
  // Dynamic import so the module is only loaded when actually needed
  const { default: Database } = await import("better-sqlite3");

  const DB_DIR = path.join(process.cwd(), "data");
  const DB_PATH = path.join(DB_DIR, "labels.db");

  fs.mkdirSync(DB_DIR, { recursive: true });

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");

  const client: DbClient = {
    async query(sql: string, args?: unknown[]): Promise<DbQueryResult> {
      const stmt = db.prepare(sql);
      const rows = (args ? stmt.all(...args) : stmt.all()) as Record<
        string,
        unknown
      >[];
      return { rows, lastInsertRowid: 0, changes: 0 };
    },
    async run(sql: string, args?: unknown[]): Promise<DbQueryResult> {
      const stmt = db.prepare(sql);
      const result = args ? stmt.run(...args) : stmt.run();
      return {
        rows: [],
        lastInsertRowid: result.lastInsertRowid,
        changes: result.changes,
      };
    },
    async exec(sql: string): Promise<void> {
      db.exec(sql);
    },
  };

  return client;
}

// ---------------------------------------------------------------------------
// @libsql/client backend (async HTTP, Turso)
// ---------------------------------------------------------------------------

async function createTursoClient(): Promise<DbClient> {
  const { createClient } = await import("@libsql/client");
  type InArgs = import("@libsql/core/api").InArgs;

  const url = process.env.TURSO_DATABASE_URL!;
  const authToken = process.env.TURSO_AUTH_TOKEN;

  const libsql = createClient({ url, authToken });

  const client: DbClient = {
    async query(sql: string, args?: unknown[]): Promise<DbQueryResult> {
      const rs = await libsql.execute({
        sql,
        args: (args ?? []) as InArgs,
      });
      // libsql returns rows as objects when using .execute()
      const rows = rs.rows as unknown as Record<string, unknown>[];
      return {
        rows,
        lastInsertRowid: rs.lastInsertRowid
          ? BigInt(rs.lastInsertRowid)
          : BigInt(0),
        changes: rs.rowsAffected,
      };
    },
    async run(sql: string, args?: unknown[]): Promise<DbQueryResult> {
      const rs = await libsql.execute({
        sql,
        args: (args ?? []) as InArgs,
      });
      return {
        rows: rs.rows as unknown as Record<string, unknown>[],
        lastInsertRowid: rs.lastInsertRowid
          ? BigInt(rs.lastInsertRowid)
          : BigInt(0),
        changes: rs.rowsAffected,
      };
    },
    async exec(sql: string): Promise<void> {
      // libsql .executeMultiple() runs multiple semicolon-separated stmts
      await libsql.executeMultiple(sql);
    },
  };

  return client;
}

// ---------------------------------------------------------------------------
// Migrations & seed helpers (work with DbClient)
// ---------------------------------------------------------------------------

async function tableHasColumn(
  client: DbClient,
  table: string,
  column: string
): Promise<boolean> {
  const result = await client.query(`PRAGMA table_info(${table})`);
  return result.rows.some(
    (row) => (row as Record<string, unknown>).name === column
  );
}

async function ensureColumn(
  client: DbClient,
  table: string,
  column: string,
  definition: string
): Promise<void> {
  if (!(await tableHasColumn(client, table, column))) {
    await client.exec(
      `ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`
    );
  }
}

async function seedSystemLabels(client: DbClient): Promise<void> {
  for (const label of SYSTEM_VRU_LABELS) {
    await client.run(
      `INSERT INTO labels (name, is_system, support_level, enabled, detector_aliases)
       VALUES (?, 1, ?, 1, ?)
       ON CONFLICT(name) DO UPDATE SET
         is_system = excluded.is_system,
         support_level = excluded.support_level,
         enabled = excluded.enabled,
         detector_aliases = excluded.detector_aliases`,
      [label.key, label.supportLevel, JSON.stringify(label.detectorAliases)]
    );
  }
}

async function markOutdatedPipelineStates(client: DbClient): Promise<void> {
  await client.run(
    `UPDATE video_pipeline_state
     SET status = 'stale'
     WHERE status = 'processed' AND pipeline_version <> ?`,
    [CURRENT_PIPELINE_VERSION]
  );
}

async function seedDefaults(client: DbClient): Promise<void> {
  const defaults = ["pedestrian", "motorcycle", "bicycle", "wheelchair", "kids"];
  for (const name of defaults) {
    await client.run("INSERT OR IGNORE INTO labels (name) VALUES (?)", [name]);
  }
}

// ---------------------------------------------------------------------------
// Singleton + initialization
// ---------------------------------------------------------------------------

let clientPromise: Promise<DbClient> | null = null;

export function getDb(): Promise<DbClient> {
  if (clientPromise) return clientPromise;

  clientPromise = (async () => {
    const useTurso = !!process.env.TURSO_DATABASE_URL;
    const client = useTurso
      ? await createTursoClient()
      : await createSqliteClient();

    // Schema creation
    await client.exec(SCHEMA_SQL);

    // Migrations
    await ensureColumn(client, "labels", "is_system", "INTEGER NOT NULL DEFAULT 0");
    await ensureColumn(
      client,
      "labels",
      "support_level",
      "TEXT NOT NULL DEFAULT 'custom'"
    );
    await ensureColumn(client, "labels", "enabled", "INTEGER NOT NULL DEFAULT 1");
    await ensureColumn(client, "labels", "detector_aliases", "TEXT");

    // Seed defaults
    await seedDefaults(client);
    await seedSystemLabels(client);
    await markOutdatedPipelineStates(client);

    return client;
  })();

  return clientPromise;
}

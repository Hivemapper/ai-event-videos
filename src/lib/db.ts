import { createClient, type Client } from "@libsql/client";
import path from "path";
import fs from "fs";
import {
  CURRENT_PIPELINE_VERSION,
  SYSTEM_VRU_LABELS,
} from "@/lib/pipeline-config";

const DB_DIR = path.join(process.cwd(), "data");
const LOCAL_DB_PATH = path.join(DB_DIR, "labels.db");

let client: Client | null = null;
let initialized = false;

function createDbClient(): Client {
  const tursoUrl = process.env.TURSO_DATABASE_URL;
  const tursoToken = process.env.TURSO_AUTH_TOKEN;

  if (tursoUrl && tursoToken) {
    return createClient({ url: tursoUrl, authToken: tursoToken });
  }

  // Local file-based SQLite
  fs.mkdirSync(DB_DIR, { recursive: true });
  return createClient({ url: `file:${LOCAL_DB_PATH}` });
}

async function initSchema(db: Client): Promise<void> {
  if (initialized) return;

  await db.execute("PRAGMA journal_mode = WAL");
  await db.execute("PRAGMA foreign_keys = ON");

  await db.executeMultiple(`
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
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_frame_detections_video_frame
      ON frame_detections (video_id, frame_ms);

    CREATE INDEX IF NOT EXISTS idx_video_pipeline_state_day
      ON video_pipeline_state (day);
  `);

  // Ensure columns exist (idempotent migrations)
  const ensureColumn = async (table: string, column: string, definition: string) => {
    const info = await db.execute(`PRAGMA table_info(${table})`);
    const exists = info.rows.some((row) => row.name === column);
    if (!exists) {
      await db.execute(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  };

  await ensureColumn("labels", "is_system", "INTEGER NOT NULL DEFAULT 0");
  await ensureColumn("labels", "support_level", "TEXT NOT NULL DEFAULT 'custom'");
  await ensureColumn("labels", "enabled", "INTEGER NOT NULL DEFAULT 1");
  await ensureColumn("labels", "detector_aliases", "TEXT");
  await ensureColumn("frame_detections", "model_name", "TEXT NOT NULL DEFAULT 'yolo11x'");

  // Seed default labels
  for (const name of ["pedestrian", "motorcycle", "bicycle", "wheelchair", "kids"]) {
    await db.execute({
      sql: "INSERT OR IGNORE INTO labels (name) VALUES (?)",
      args: [name],
    });
  }

  // Seed system labels
  for (const label of SYSTEM_VRU_LABELS) {
    await db.execute({
      sql: `INSERT INTO labels (name, is_system, support_level, enabled, detector_aliases)
            VALUES (?, 1, ?, 1, ?)
            ON CONFLICT(name) DO UPDATE SET
              is_system = excluded.is_system,
              support_level = excluded.support_level,
              enabled = excluded.enabled,
              detector_aliases = excluded.detector_aliases`,
      args: [label.key, label.supportLevel, JSON.stringify(label.detectorAliases)],
    });
  }

  // Mark outdated pipeline states
  await db.execute({
    sql: `UPDATE video_pipeline_state
          SET status = 'stale'
          WHERE status = 'processed' AND pipeline_version <> ?`,
    args: [CURRENT_PIPELINE_VERSION],
  });

  initialized = true;
}

export async function getDb(): Promise<Client> {
  if (!client) {
    client = createDbClient();
    await initSchema(client);
  }
  return client;
}

import type { DbClient } from "@/lib/db";
import { TOP_HITS_SEED } from "@/lib/top-hits-seed";

export interface TopHitFrameTimingQc {
  fpsQc: string | null;
  lateFramePct: number | null;
}

export type TopHitVruStatus =
  | "not_run"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export type TopHitProductionStatus =
  | "not_queued"
  | "queued"
  | "processing"
  | "completed"
  | "failed";

export interface TopHitPipelineStatus {
  vruStatus: TopHitVruStatus;
  productionStatus: TopHitProductionStatus;
  productionPriority: number | null;
}

export interface TopHitEventSummary {
  eventId: string;
  eventType: string | null;
  eventTimestamp: string | null;
  lat: number | null;
  lon: number | null;
  bitrateBps: number | null;
  fpsQc: string | null;
  lateFramePct: number | null;
  vruLabel: string | null;
  vruConfidence: number | null;
  pipelineStatus: TopHitPipelineStatus;
}

export interface TopHitsResponse {
  ids: string[];
  rows: TopHitEventSummary[];
  frameTimingQcById: Record<string, TopHitFrameTimingQc>;
  pipelineStatusById: Record<string, TopHitPipelineStatus>;
}

const TOP_HITS_CACHE_TTL_MS = 15_000;

const CREATE_TOP_HITS_TABLE_SQL = `CREATE TABLE IF NOT EXISTS top_hits (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
)`;

const TOP_HITS_PERFORMANCE_INDEXES_SQL = `
  CREATE INDEX IF NOT EXISTS idx_video_detection_segments_video_confidence
    ON video_detection_segments (video_id, max_confidence DESC, created_at DESC);

  CREATE INDEX IF NOT EXISTS idx_production_runs_video_status
    ON production_runs (video_id, status);
`;

const SELECT_TOP_HITS_WITH_QC_SQL = `
  WITH listed AS (
    SELECT row_id, event_id
    FROM top_hits
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
    INNER JOIN listed l
      ON l.event_id = dr.video_id
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
      INNER JOIN listed l
        ON l.event_id = s.video_id
    )
    WHERE rn = 1
  ),
  production_status AS (
    SELECT
      pr.video_id,
      CASE
        WHEN SUM(CASE WHEN pr.status = 'completed' THEN 1 ELSE 0 END) > 0 THEN 'completed'
        WHEN SUM(CASE WHEN pr.status = 'processing' THEN 1 ELSE 0 END) > 0 THEN 'processing'
        WHEN SUM(CASE WHEN pr.status = 'queued' THEN 1 ELSE 0 END) > 0 THEN 'queued'
        WHEN SUM(CASE WHEN pr.status = 'failed' THEN 1 ELSE 0 END) > 0 THEN 'failed'
        ELSE NULL
      END AS production_status,
      MIN(pr.priority) AS production_priority
    FROM production_runs pr
    INNER JOIN listed l
      ON l.event_id = pr.video_id
    GROUP BY pr.video_id
  )
  SELECT
    h.event_id,
    t.event_type,
    t.event_timestamp,
    t.lat,
    t.lon,
    t.bitrate_bps,
    q.bucket AS fps_qc,
    q.gap_pct AS late_frame_pct,
    v.vru_status,
    bv.label AS vru_label,
    bv.max_confidence AS vru_confidence,
    p.production_status,
    p.production_priority
  FROM listed h
  LEFT JOIN triage_results t
    ON t.id = h.event_id
  LEFT JOIN video_frame_timing_qc q
    ON q.video_id = h.event_id
  LEFT JOIN vru_status v
    ON v.video_id = h.event_id
  LEFT JOIN best_vru bv
    ON bv.video_id = h.event_id
  LEFT JOIN production_status p
    ON p.video_id = h.event_id
  ORDER BY h.row_id DESC
`;

let readyPromise: Promise<void> | null = null;
let responseCache:
  | { response: TopHitsResponse; expiresAt: number }
  | null = null;

/**
 * Seed the table from TOP_HITS_SEED if it's empty. Inserts in reverse order
 * so that `ORDER BY row_id DESC` yields the original seed order (seed[0] first).
 * INSERT OR IGNORE keeps the operation idempotent under concurrent seeding.
 */
async function seedIfEmpty(db: DbClient): Promise<void> {
  const result = await db.query("SELECT COUNT(*) AS count FROM top_hits");
  const count = Number((result.rows[0] as { count: number | bigint }).count);
  if (count > 0) return;

  for (let i = TOP_HITS_SEED.length - 1; i >= 0; i--) {
    await db.run(
      "INSERT OR IGNORE INTO top_hits (event_id) VALUES (?)",
      [TOP_HITS_SEED[i]]
    );
  }
}

export async function ensureTopHitsTable(db: DbClient): Promise<void> {
  await db.exec(CREATE_TOP_HITS_TABLE_SQL);
  await db.exec(TOP_HITS_PERFORMANCE_INDEXES_SQL);
}

export async function ensureTopHitsReady(db: DbClient): Promise<void> {
  readyPromise ??= (async () => {
    await ensureTopHitsTable(db);
    await seedIfEmpty(db);
  })().catch((error) => {
    readyPromise = null;
    throw error;
  });

  await readyPromise;
}

export function invalidateTopHitsCache(): void {
  responseCache = null;
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export async function loadTopHitsResponse(db: DbClient): Promise<TopHitsResponse> {
  const result = await db.query(SELECT_TOP_HITS_WITH_QC_SQL);
  const ids: string[] = [];
  const rows: TopHitEventSummary[] = [];
  const frameTimingQcById: Record<string, TopHitFrameTimingQc> = {};
  const pipelineStatusById: Record<string, TopHitPipelineStatus> = {};

  for (const row of result.rows) {
    const id = typeof row.event_id === "string" ? row.event_id : String(row.event_id ?? "");
    if (!id) continue;
    ids.push(id);

    const fpsQc = typeof row.fps_qc === "string" ? row.fps_qc : null;
    const lateFramePct = nullableNumber(row.late_frame_pct);
    if (fpsQc || lateFramePct !== null) {
      frameTimingQcById[id] = { fpsQc, lateFramePct };
    }

    const vruStatus =
      typeof row.vru_status === "string"
        ? (row.vru_status as TopHitVruStatus)
        : "not_run";
    const productionStatus =
      typeof row.production_status === "string"
        ? (row.production_status as TopHitProductionStatus)
        : "not_queued";
    pipelineStatusById[id] = {
      vruStatus,
      productionStatus,
      productionPriority: nullableNumber(row.production_priority),
    };
    rows.push({
      eventId: id,
      eventType: typeof row.event_type === "string" ? row.event_type : null,
      eventTimestamp: typeof row.event_timestamp === "string" ? row.event_timestamp : null,
      lat: nullableNumber(row.lat),
      lon: nullableNumber(row.lon),
      bitrateBps: nullableNumber(row.bitrate_bps),
      fpsQc,
      lateFramePct,
      vruLabel: typeof row.vru_label === "string" ? row.vru_label : null,
      vruConfidence: nullableNumber(row.vru_confidence),
      pipelineStatus: pipelineStatusById[id],
    });
  }

  return { ids, rows, frameTimingQcById, pipelineStatusById };
}

export async function loadCachedTopHitsResponse(
  db: DbClient,
  options: { force?: boolean } = {}
): Promise<TopHitsResponse> {
  await ensureTopHitsReady(db);

  const now = Date.now();
  if (!options.force && responseCache && responseCache.expiresAt > now) {
    return responseCache.response;
  }

  const response = await loadTopHitsResponse(db);
  responseCache = {
    response,
    expiresAt: now + TOP_HITS_CACHE_TTL_MS,
  };
  return response;
}

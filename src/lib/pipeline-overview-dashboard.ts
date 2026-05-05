import type { DbClient } from "@/lib/db";
import {
  fetchTriageSourceEventTotal,
  getAwaitingTriageTotal,
  normalizeTriagePeriod,
  TRIAGE_PERIODS,
} from "@/lib/triage-source-total";

export type PipelineOverviewStage = "triage" | "vru" | "production";
export type PipelineOverviewSort =
  | "date"
  | "event_type"
  | "triage"
  | "vru"
  | "production"
  | "fps_qc"
  | "late_pct"
  | "bitrate"
  | "detections";
export type PipelineOverviewSortDir = "asc" | "desc";

export interface PipelineOverviewFilters {
  stage: PipelineOverviewStage;
  status: string;
  period: string | null;
  fpsQc: string[];
  eventTypes: string[];
  vruLabels: string[];
  sort: PipelineOverviewSort;
  dir: PipelineOverviewSortDir;
}

export interface PipelineOverviewRowsParams extends PipelineOverviewFilters {
  limit: number;
  offset: number;
}

export interface PipelineOverviewCounts {
  triage: Record<string, number | null>;
  vru: Record<string, number>;
  production: Record<string, number>;
}

const MIN_STANDARD_VIDEO_DURATION_SEC = 28;
const MIN_NON_LINEAR_BITRATE_BPS = 3_300_000;
const PERIOD_6_START = "2026-04-17T00:00:00.000Z";

const PERIODS = TRIAGE_PERIODS;

const TRIAGE_STATUSES = [
  "all",
  "filtered",
  "signal",
  "missing_video",
  "missing_metadata",
  "ghost",
  "open_road",
  "duplicate",
  "non_linear",
  "privacy",
  "skipped_firmware",
] as const;

const VRU_STATUSES = ["queued", "running", "completed", "failed"] as const;
const PRODUCTION_STATUSES = ["queued", "processing", "completed", "failed"] as const;

const EFFECTIVE_TRIAGE_EXPR = `
  CASE
    WHEN t.rules_triggered LIKE '%manual%' THEN t.triage_result
    WHEN t.triage_result IN ('duplicate', 'privacy', 'non_linear') THEN t.triage_result
    WHEN julianday(t.event_timestamp) >= julianday('${PERIOD_6_START}') AND (
      (t.video_length_sec IS NOT NULL AND t.video_length_sec < ${MIN_STANDARD_VIDEO_DURATION_SEC}) OR
      (t.bitrate_bps IS NOT NULL AND t.bitrate_bps < ${MIN_NON_LINEAR_BITRATE_BPS})
    ) THEN 'non_linear'
    ELSE t.triage_result
  END
`;

const VRU_STATUS_EXPR = `
  CASE
    WHEN EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id AND dr.status = 'completed') THEN 'completed'
    WHEN EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id AND dr.status = 'running') THEN 'running'
    WHEN EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id AND dr.status = 'queued') THEN 'queued'
    WHEN EXISTS (
      SELECT 1 FROM detection_runs dr
      WHERE dr.video_id = t.id AND dr.status = 'failed'
    ) AND NOT EXISTS (
      SELECT 1 FROM detection_runs active
      WHERE active.video_id = t.id AND active.status IN ('queued', 'running', 'completed')
    ) THEN 'failed'
    WHEN t.triage_result = 'signal'
      AND (t.road_class IS NULL OR t.road_class != 'motorway')
      AND (t.speed_min IS NULL OR t.speed_min < 45)
      AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
    THEN 'queued'
    ELSE 'not_queued'
  END
`;

const PRODUCTION_IMPLICIT_ELIGIBLE_EXPR = `
  (
    EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id AND dr.status = 'completed')
    OR (
      t.triage_result = 'signal'
      AND (t.road_class = 'motorway' OR (t.speed_min IS NOT NULL AND t.speed_min >= 45))
      AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
    )
  )
`;

const FRESH_PRODUCTION_PROCESSING_SQL = `
  COALESCE(pr.last_heartbeat_at, pr.started_at) >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
`;

const PRODUCTION_STATUS_EXPR = `
  CASE
    WHEN EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id AND pr.status = 'completed') THEN 'completed'
    WHEN EXISTS (
      SELECT 1 FROM production_runs pr
      WHERE pr.video_id = t.id
        AND pr.status = 'processing'
        AND ${FRESH_PRODUCTION_PROCESSING_SQL}
    ) THEN 'processing'
    WHEN EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id AND pr.status = 'queued') THEN 'queued'
    WHEN EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id AND pr.status = 'failed') THEN 'failed'
    WHEN NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
      AND ${PRODUCTION_IMPLICIT_ELIGIBLE_EXPR}
    THEN 'queued'
    ELSE 'not_queued'
  END
`;

function placeholders(values: unknown[]): string {
  return values.map(() => "?").join(", ");
}

function lowerValues(values: string[]): string[] {
  return Array.from(new Set(values.map((value) => value.trim().toLowerCase()).filter(Boolean)));
}

function getPeriodRange(period: string | null): readonly [string, string] | null {
  if (!period || !(period in PERIODS)) return null;
  return PERIODS[period as keyof typeof PERIODS];
}

function buildBaseFilter(filters: PipelineOverviewFilters): { clause: string; params: unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];

  const periodRange = getPeriodRange(filters.period);
  if (periodRange) {
    const [start, end] = periodRange;
    clauses.push("t.event_timestamp >= ? AND t.event_timestamp < ?");
    params.push(start, end);
  }

  if (filters.eventTypes.length > 0) {
    clauses.push(`t.event_type IN (${placeholders(filters.eventTypes)})`);
    params.push(...filters.eventTypes);
  }

  if (filters.fpsQc.length > 0) {
    const fpsBuckets = filters.fpsQc.filter((value) => value !== "missing");
    const includeMissing = filters.fpsQc.includes("missing");
    const fpsClauses: string[] = [];
    if (fpsBuckets.length > 0) {
      fpsClauses.push(`q.bucket IN (${placeholders(fpsBuckets)})`);
      params.push(...fpsBuckets);
    }
    if (includeMissing) {
      fpsClauses.push("q.bucket IS NULL");
    }
    if (fpsClauses.length > 0) {
      clauses.push(`(${fpsClauses.join(" OR ")})`);
    }
  }

  const vruLabels = lowerValues(filters.vruLabels);
  if (vruLabels.length > 0) {
    clauses.push(`
      EXISTS (
        SELECT 1 FROM frame_detections fd_filter
        WHERE fd_filter.video_id = t.id
          AND lower(fd_filter.label) IN (${placeholders(vruLabels)})
      )
    `);
    params.push(...vruLabels);
  }

  return {
    clause: clauses.length > 0 ? clauses.join(" AND ") : "1 = 1",
    params,
  };
}

function buildTriageStatusClause(status: string): { clause: string; params: unknown[] } {
  if (status === "filtered") {
    return { clause: `${EFFECTIVE_TRIAGE_EXPR} != ?`, params: ["signal"] };
  }
  if (status && status !== "all" && TRIAGE_STATUSES.includes(status as (typeof TRIAGE_STATUSES)[number])) {
    return { clause: `${EFFECTIVE_TRIAGE_EXPR} = ?`, params: [status] };
  }
  return { clause: "1 = 1", params: [] };
}

function buildVruStatusClause(status: string): string {
  if (status === "running") {
    return `
      t.triage_result = 'signal'
      AND NOT EXISTS (
        SELECT 1 FROM detection_runs completed
        WHERE completed.video_id = t.id AND completed.status = 'completed'
      )
      AND EXISTS (
        SELECT 1 FROM detection_runs dr
        WHERE dr.video_id = t.id AND dr.status = 'running'
      )
    `;
  }

  if (status === "completed") {
    return `
      t.triage_result = 'signal'
      AND EXISTS (
        SELECT 1 FROM detection_runs dr
        WHERE dr.video_id = t.id AND dr.status = 'completed'
      )
    `;
  }

  if (status === "failed") {
    return `
      t.triage_result = 'signal'
      AND EXISTS (
        SELECT 1 FROM detection_runs dr
        WHERE dr.video_id = t.id AND dr.status = 'failed'
      )
      AND NOT EXISTS (
        SELECT 1 FROM detection_runs active
        WHERE active.video_id = t.id AND active.status IN ('queued', 'running', 'completed')
      )
    `;
  }

  return `
    t.triage_result = 'signal'
    AND NOT EXISTS (
      SELECT 1 FROM detection_runs completed
      WHERE completed.video_id = t.id AND completed.status = 'completed'
    )
    AND (
      EXISTS (
        SELECT 1 FROM detection_runs queued
        WHERE queued.video_id = t.id AND queued.status = 'queued'
      )
      OR (
        (t.road_class IS NULL OR t.road_class != 'motorway')
        AND (t.speed_min IS NULL OR t.speed_min < 45)
        AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
      )
    )
  `;
}

function buildProductionStatusClause(status: string): string {
  if (status === "processing") {
    return `
      EXISTS (
        SELECT 1 FROM production_runs pr
        WHERE pr.video_id = t.id
          AND pr.status = 'processing'
          AND ${FRESH_PRODUCTION_PROCESSING_SQL}
      )
    `;
  }

  if (status === "completed" || status === "failed") {
    return `
      EXISTS (
        SELECT 1 FROM production_runs pr
        WHERE pr.video_id = t.id AND pr.status = '${status}'
      )
    `;
  }

  return `
    EXISTS (
      SELECT 1 FROM production_runs pr
      WHERE pr.video_id = t.id AND pr.status = 'queued'
    )
    OR (
      NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
      AND ${PRODUCTION_IMPLICIT_ELIGIBLE_EXPR}
    )
  `;
}

function buildStageStatusFilter(filters: PipelineOverviewFilters): { clause: string; params: unknown[] } {
  if (filters.stage === "triage") {
    return buildTriageStatusClause(filters.status);
  }
  if (filters.stage === "production") {
    const status = PRODUCTION_STATUSES.includes(filters.status as (typeof PRODUCTION_STATUSES)[number])
      ? filters.status
      : "queued";
    return { clause: buildProductionStatusClause(status), params: [] };
  }

  const status = VRU_STATUSES.includes(filters.status as (typeof VRU_STATUSES)[number])
    ? filters.status
    : "queued";
  return { clause: buildVruStatusClause(status), params: [] };
}

function buildWhere(filters: PipelineOverviewFilters, options: { includeStatus: boolean }) {
  const base = buildBaseFilter(filters);
  const clauses = [base.clause];
  const params = [...base.params];

  if (options.includeStatus) {
    const status = buildStageStatusFilter(filters);
    clauses.push(`(${status.clause})`);
    params.push(...status.params);
  }

  return {
    clause: clauses.join(" AND "),
    params,
  };
}

function getOrderBy(filters: PipelineOverviewFilters): string {
  const dir = filters.dir === "asc" ? "ASC" : "DESC";
  const dateDir = filters.sort === "date" ? dir : "DESC";
  const tieBreak = "t.event_timestamp DESC, t.created_at DESC, t.id ASC";

  if (filters.sort === "event_type") {
    return `t.event_type COLLATE NOCASE ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "triage") {
    return `${EFFECTIVE_TRIAGE_EXPR} COLLATE NOCASE ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "vru") {
    return `${VRU_STATUS_EXPR} COLLATE NOCASE ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "production") {
    return `${PRODUCTION_STATUS_EXPR} COLLATE NOCASE ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "fps_qc") {
    return `
      CASE WHEN q.bucket IS NULL THEN 1 ELSE 0 END ASC,
      CASE q.bucket WHEN 'perfect' THEN 0 WHEN 'ok' THEN 1 WHEN 'filter_out' THEN 2 ELSE 3 END ${dir},
      ${tieBreak}
    `;
  }
  if (filters.sort === "late_pct") {
    return `CASE WHEN q.gap_pct IS NULL THEN 1 ELSE 0 END ASC, q.gap_pct ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "bitrate") {
    return `CASE WHEN t.bitrate_bps IS NULL THEN 1 ELSE 0 END ASC, t.bitrate_bps ${dir}, ${tieBreak}`;
  }
  if (filters.sort === "detections") {
    return `
      COALESCE((
        SELECT dr.detection_count
        FROM detection_runs dr
        WHERE dr.video_id = t.id AND dr.status = 'completed'
        ORDER BY dr.completed_at DESC, dr.created_at DESC
        LIMIT 1
      ), 0) ${dir},
      ${tieBreak}
    `;
  }

  return `CASE WHEN t.event_timestamp IS NULL THEN 1 ELSE 0 END ASC, t.event_timestamp ${dateDir}, t.created_at DESC`;
}

function selectRowsSql(whereClause: string, orderBy: string): string {
  return `
    WITH paged AS (
      SELECT t.id
      FROM triage_results t
      LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
      WHERE ${whereClause}
      ORDER BY ${orderBy}
      LIMIT ? OFFSET ?
    )
    SELECT
      p.id,
      t.event_type,
      ${EFFECTIVE_TRIAGE_EXPR} AS effective_triage_result,
      t.speed_min,
      t.speed_max,
      t.bitrate_bps,
      t.video_length_sec,
      t.event_timestamp,
      t.road_class,
      t.created_at,
      q.bucket AS fps_qc,
      q.gap_pct AS late_frame_pct,
      q.max_delta_ms AS max_delta_ms,
      ${VRU_STATUS_EXPR} AS vru_status,
      (
        SELECT fd.label
        FROM frame_detections fd
        WHERE fd.video_id = p.id
        ORDER BY fd.confidence DESC, fd.created_at DESC
        LIMIT 1
      ) AS top_vru_label,
      (
        SELECT fd.confidence
        FROM frame_detections fd
        WHERE fd.video_id = p.id
        ORDER BY fd.confidence DESC, fd.created_at DESC
        LIMIT 1
      ) AS top_vru_confidence,
      (
        SELECT dr.detection_count
        FROM detection_runs dr
        WHERE dr.video_id = p.id AND dr.status = 'completed'
        ORDER BY dr.completed_at DESC, dr.created_at DESC
        LIMIT 1
      ) AS detection_count,
      (
        SELECT dr.started_at
        FROM detection_runs dr
        WHERE dr.video_id = p.id AND dr.status = 'completed'
        ORDER BY dr.completed_at DESC, dr.created_at DESC
        LIMIT 1
      ) AS vru_started_at,
      (
        SELECT dr.completed_at
        FROM detection_runs dr
        WHERE dr.video_id = p.id AND dr.status = 'completed'
        ORDER BY dr.completed_at DESC, dr.created_at DESC
        LIMIT 1
      ) AS vru_completed_at,
      (
        SELECT dr.last_error
        FROM detection_runs dr
        WHERE dr.video_id = p.id AND dr.status = 'failed'
        ORDER BY dr.completed_at DESC, dr.created_at DESC
        LIMIT 1
      ) AS vru_error,
      ${PRODUCTION_STATUS_EXPR} AS production_status,
      (
        SELECT pr.privacy_status
        FROM production_runs pr
        WHERE pr.video_id = p.id
        ORDER BY CASE pr.status WHEN 'processing' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'completed' THEN 3 ELSE 4 END, pr.created_at DESC
        LIMIT 1
      ) AS privacy_status,
      (
        SELECT pr.metadata_status
        FROM production_runs pr
        WHERE pr.video_id = p.id
        ORDER BY CASE pr.status WHEN 'processing' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'completed' THEN 3 ELSE 4 END, pr.created_at DESC
        LIMIT 1
      ) AS metadata_status,
      (
        SELECT pr.upload_status
        FROM production_runs pr
        WHERE pr.video_id = p.id
        ORDER BY CASE pr.status WHEN 'processing' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'completed' THEN 3 ELSE 4 END, pr.created_at DESC
        LIMIT 1
      ) AS upload_status,
      (
        SELECT pr.started_at
        FROM production_runs pr
        WHERE pr.video_id = p.id
        ORDER BY CASE pr.status WHEN 'processing' THEN 0 WHEN 'queued' THEN 1 WHEN 'failed' THEN 2 WHEN 'completed' THEN 3 ELSE 4 END, pr.created_at DESC
        LIMIT 1
      ) AS production_started_at,
      (
        SELECT pr.completed_at
        FROM production_runs pr
        WHERE pr.video_id = p.id
        ORDER BY CASE pr.status WHEN 'completed' THEN 0 WHEN 'failed' THEN 1 ELSE 2 END, pr.completed_at DESC
        LIMIT 1
      ) AS production_completed_at,
      (
        SELECT pr.last_error
        FROM production_runs pr
        WHERE pr.video_id = p.id AND pr.status = 'failed'
        ORDER BY pr.completed_at DESC, pr.created_at DESC
        LIMIT 1
      ) AS production_error,
      (
        SELECT pr.s3_video_key
        FROM production_runs pr
        WHERE pr.video_id = p.id AND pr.status = 'completed'
        ORDER BY pr.completed_at DESC
        LIMIT 1
      ) AS s3_video_key,
      (
        SELECT pr.s3_metadata_key
        FROM production_runs pr
        WHERE pr.video_id = p.id AND pr.status = 'completed'
        ORDER BY pr.completed_at DESC
        LIMIT 1
      ) AS s3_metadata_key,
      (
        SELECT pr.skip_reason
        FROM production_runs pr
        WHERE pr.video_id = p.id AND pr.status = 'completed'
        ORDER BY pr.completed_at DESC
        LIMIT 1
      ) AS production_skip_reason
    FROM paged p
    JOIN triage_results t ON t.id = p.id
    LEFT JOIN video_frame_timing_qc q ON q.video_id = p.id
    ORDER BY ${orderBy}
  `;
}

async function aggregateCounts(
  db: DbClient,
  filters: PipelineOverviewFilters,
  expressions: Record<string, string>
): Promise<Record<string, number>> {
  const base = buildBaseFilter(filters);
  const select = Object.entries(expressions)
    .map(([key, expression]) => `SUM(CASE WHEN (${expression}) THEN 1 ELSE 0 END) AS "${key}"`)
    .join(",\n");
  const result = await db.query(
    `SELECT ${select}
     FROM triage_results t
     LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
     WHERE ${base.clause}`,
    base.params
  );
  const row = result.rows[0] as Record<string, number | bigint | null | undefined> | undefined;
  return Object.fromEntries(
    Object.keys(expressions).map((key) => [key, Number(row?.[key] ?? 0)])
  );
}

async function getAwaitingCountForPeriod(
  db: DbClient,
  period: string | null,
  apiKey: string | null
): Promise<number | null> {
  const normalizedPeriod = normalizeTriagePeriod(period);
  if (!normalizedPeriod) return null;

  const [start, end] = PERIODS[normalizedPeriod];
  const triagedResult = await db.query(
    "SELECT COUNT(*) AS count FROM triage_results WHERE event_timestamp >= ? AND event_timestamp < ?",
    [start, end]
  );
  const triagedTotal = Number(
    (triagedResult.rows[0] as { count?: number | string } | undefined)?.count ?? 0
  );
  const sourceTotal = await fetchTriageSourceEventTotal(normalizedPeriod, apiKey);
  return getAwaitingTriageTotal(sourceTotal, triagedTotal);
}

export async function getPipelineOverviewRows(
  db: DbClient,
  params: PipelineOverviewRowsParams
): Promise<{ rows: Record<string, unknown>[]; total: number }> {
  const where = buildWhere(params, { includeStatus: true });
  const rowsResult = await db.query(
    selectRowsSql(where.clause, getOrderBy(params)),
    [...where.params, params.limit, params.offset]
  );

  const total =
    params.offset +
    rowsResult.rows.length +
    (rowsResult.rows.length === params.limit ? params.limit : 0);

  return { rows: rowsResult.rows, total };
}

export async function getPipelineOverviewCounts(
  db: DbClient,
  filters: PipelineOverviewFilters,
  options: { apiKey?: string | null } = {}
): Promise<PipelineOverviewCounts> {
  const triage: Record<string, number | null> = {};
  const vru: Record<string, number> = {};
  const production: Record<string, number> = {};

  if (filters.stage === "triage") {
    Object.assign(
      triage,
      await aggregateCounts(db, filters, {
        all: "1 = 1",
        signal: "t.triage_result = 'signal'",
        filtered: "t.triage_result != 'signal'",
        missing_video: "t.triage_result = 'missing_video'",
        missing_metadata: "t.triage_result = 'missing_metadata'",
        ghost: "t.triage_result = 'ghost'",
        open_road: "t.triage_result = 'open_road'",
        duplicate: "t.triage_result = 'duplicate'",
        non_linear: "t.triage_result = 'non_linear'",
        privacy: "t.triage_result = 'privacy'",
        skipped_firmware: "t.triage_result = 'skipped_firmware'",
      })
    );
    triage.awaiting = await getAwaitingCountForPeriod(db, filters.period, options.apiKey ?? null);
  }

  if (filters.stage === "vru") {
    Object.assign(
      vru,
      await aggregateCounts(db, filters, {
        queued: buildVruStatusClause("queued"),
        running: buildVruStatusClause("running"),
        completed: buildVruStatusClause("completed"),
        failed: buildVruStatusClause("failed"),
      })
    );
  }

  if (filters.stage === "production") {
    Object.assign(
      production,
      await aggregateCounts(db, filters, {
        queued: buildProductionStatusClause("queued"),
        processing: buildProductionStatusClause("processing"),
        completed: buildProductionStatusClause("completed"),
        failed: buildProductionStatusClause("failed"),
      })
    );
  }

  return { triage, vru, production };
}

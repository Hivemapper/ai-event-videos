import type { DbClient } from "@/lib/db";
import { buildVruPriorityOrderBy } from "@/lib/vru-priority";

export type PipelineDashboardTab = "queued" | "running" | "completed" | "failed";

export interface PipelineCounts {
  queued: number;
  running: number;
  completed: number;
  failed: number;
}

export type PipelineFpsQcFilter = "perfect" | "ok" | "filter_out" | "missing";

function buildFpsQcFilter(alias: string, fpsQcFilters: PipelineFpsQcFilter[]) {
  const buckets = fpsQcFilters.filter((value) => value !== "missing");
  const includeMissing = fpsQcFilters.includes("missing");

  if (buckets.length === 0 && !includeMissing) {
    return { clause: "", params: [] as string[] };
  }

  const clauses: string[] = [];
  const params: string[] = [];

  if (buckets.length > 0) {
    clauses.push(`${alias}.bucket IN (${buckets.map(() => "?").join(", ")})`);
    params.push(...buckets);
  }

  if (includeMissing) {
    clauses.push(`${alias}.bucket IS NULL`);
  }

  return {
    clause: ` AND (${clauses.join(" OR ")})`,
    params,
  };
}

export async function getPipelineCounts(db: DbClient): Promise<PipelineCounts> {
  const result = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
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
      ) as queued,
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND NOT EXISTS (
           SELECT 1 FROM detection_runs completed
           WHERE completed.video_id = t.id AND completed.status = 'completed'
         )
         AND EXISTS (
           SELECT 1 FROM detection_runs dr
           WHERE dr.video_id = t.id AND dr.status = 'running'
         )
      ) as running,
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND EXISTS (
           SELECT 1 FROM detection_runs dr
           WHERE dr.video_id = t.id AND dr.status = 'completed'
         )
      ) as completed,
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND EXISTS (
           SELECT 1 FROM detection_runs dr
           WHERE dr.video_id = t.id AND dr.status = 'failed'
         )
         AND NOT EXISTS (
           SELECT 1 FROM detection_runs active
           WHERE active.video_id = t.id AND active.status IN ('queued', 'running', 'completed')
         )
      ) as failed
  `);

  const row = result.rows[0] as Record<string, number | bigint | null | undefined>;

  return {
    queued: Number(row.queued ?? 0),
    running: Number(row.running ?? 0),
    completed: Number(row.completed ?? 0),
    failed: Number(row.failed ?? 0),
  };
}

export async function getPipelineTabCount(
  db: DbClient,
  tab: PipelineDashboardTab,
  fpsQcFilters: PipelineFpsQcFilter[] = []
): Promise<number> {
  const fpsQcFilter = buildFpsQcFilter("q", fpsQcFilters);
  let sql: string;
  let params: string[] = [];

  if (tab === "queued") {
    sql = `
      SELECT COUNT(*) as count
      FROM triage_results t
      WHERE t.triage_result = 'signal'
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
  } else if (tab === "running") {
    sql = `
      SELECT COUNT(*) as count
      FROM triage_results t
      WHERE t.triage_result = 'signal'
        AND NOT EXISTS (
          SELECT 1 FROM detection_runs completed
          WHERE completed.video_id = t.id AND completed.status = 'completed'
        )
        AND EXISTS (
          SELECT 1 FROM detection_runs dr
          WHERE dr.video_id = t.id AND dr.status = 'running'
        )
    `;
  } else if (tab === "completed") {
    sql = `
      SELECT COUNT(*) as count
      FROM triage_results t
      LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
      WHERE t.triage_result = 'signal'
        ${fpsQcFilter.clause}
        AND EXISTS (
          SELECT 1 FROM detection_runs dr
          WHERE dr.video_id = t.id AND dr.status = 'completed'
        )
    `;
    params = fpsQcFilter.params;
  } else {
    sql = `
      SELECT COUNT(*) as count
      FROM triage_results t
      WHERE t.triage_result = 'signal'
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

  const result = await db.query(sql, params);
  const row = result.rows[0] as Record<string, number | bigint | null | undefined> | undefined;
  return Number(row?.count ?? 0);
}

export type PipelineSort = "date_desc" | "detections_desc" | "detections_asc";

export async function getPipelineRows(
  db: DbClient,
  tab: PipelineDashboardTab,
  limit: number,
  offset: number,
  sort: PipelineSort = "date_desc",
  fpsQcFilters: PipelineFpsQcFilter[] = []
): Promise<Record<string, unknown>[]> {
  const fpsQcFilter = buildFpsQcFilter("q", fpsQcFilters);

  if (tab === "queued") {
    const result = await db.query(
      `WITH explicit_queued AS (
         SELECT
           dr.video_id,
           MIN(COALESCE(dr.priority, 100)) AS queued_priority,
           MIN(dr.created_at) AS queued_at
         FROM detection_runs dr
         WHERE dr.status = 'queued'
         GROUP BY dr.video_id
       )
       SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              t.firmware_version, t.firmware_version_num, q.bucket AS fps_qc,
              eq.queued_priority, eq.queued_at,
              CASE WHEN eq.video_id IS NULL THEN 'implicit' ELSE 'queued' END as run_status
       FROM triage_results t
       LEFT JOIN explicit_queued eq ON eq.video_id = t.id
       LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
       WHERE t.triage_result = 'signal'
         AND NOT EXISTS (
           SELECT 1 FROM detection_runs completed
           WHERE completed.video_id = t.id AND completed.status = 'completed'
         )
         AND (
           eq.video_id IS NOT NULL
           OR (
             (t.road_class IS NULL OR t.road_class != 'motorway')
             AND (t.speed_min IS NULL OR t.speed_min < 45)
             AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
           )
         )
       ORDER BY CASE WHEN eq.video_id IS NULL THEN 1 ELSE 0 END ASC,
                COALESCE(eq.queued_priority, 100) ASC,
                ${buildVruPriorityOrderBy("t", "q")},
                eq.queued_at ASC,
                t.event_timestamp DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    return result.rows;
  }

  if (tab === "running") {
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              dr.id as run_id, dr.status as run_status, dr.model_name, dr.started_at, dr.machine_id
       FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status = 'running'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed')
       ORDER BY dr.created_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    return result.rows;
  }

  if (tab === "completed") {
    const innerOrder =
      sort === "detections_desc"
        ? "COALESCE(detection_count, 0) DESC, completed_at DESC"
        : sort === "detections_asc"
          ? "COALESCE(detection_count, 0) ASC, completed_at DESC"
          : "completed_at DESC";
    const outerOrder =
      sort === "detections_desc"
        ? "COALESCE(dr.detection_count, 0) DESC, dr.completed_at DESC"
        : sort === "detections_asc"
          ? "COALESCE(dr.detection_count, 0) ASC, dr.completed_at DESC"
          : "dr.completed_at DESC";
    const overfetchLimit = limit * 3;
    const result = await db.query(
      `WITH recent_completed AS (
         SELECT dr.id, dr.video_id, dr.model_name, dr.detection_count, dr.started_at, dr.completed_at, dr.machine_id
         FROM detection_runs dr
         JOIN triage_results t ON t.id = dr.video_id
         LEFT JOIN video_frame_timing_qc q ON q.video_id = dr.video_id
         WHERE dr.status = 'completed'
           AND t.triage_result = 'signal'
           ${fpsQcFilter.clause}
         ORDER BY ${innerOrder}
         LIMIT ? OFFSET ?
       )
       SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              q.bucket AS fps_qc,
              dr.id as run_id, dr.model_name, dr.detection_count, dr.started_at, dr.completed_at, dr.machine_id,
              pr.id as production_run_id, pr.status as production_status, pr.priority as production_priority
       FROM recent_completed dr
       JOIN triage_results t ON t.id = dr.video_id
       LEFT JOIN video_frame_timing_qc q ON q.video_id = dr.video_id
       LEFT JOIN production_runs pr ON pr.video_id = dr.video_id
       WHERE t.triage_result = 'signal'
       ORDER BY ${outerOrder}
       LIMIT ?`,
      [...fpsQcFilter.params, overfetchLimit, offset, limit]
    );
    return result.rows;
  }

  const overfetchLimit = limit * 3;
  const result = await db.query(
    `WITH recent_failed AS (
       SELECT dr.id, dr.video_id, dr.model_name, dr.last_error, dr.completed_at, dr.machine_id
       FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE dr.status = 'failed'
         AND t.triage_result = 'signal'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed')
       ORDER BY completed_at DESC
       LIMIT ? OFFSET ?
     )
     SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
            dr.id as run_id, dr.model_name, dr.last_error, dr.completed_at, dr.machine_id
     FROM recent_failed dr
     JOIN triage_results t ON t.id = dr.video_id
     WHERE t.triage_result = 'signal'
       AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed')
     ORDER BY dr.completed_at DESC
     LIMIT ?`,
    [overfetchLimit, offset, limit]
  );
  return result.rows;
}

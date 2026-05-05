import type { DbClient } from "@/lib/db";

export type ProductionPipelineTab = "queued" | "processing" | "completed" | "failed";

export interface ProductionPipelineCounts {
  queued: number;
  processing: number;
  completed: number;
  failed: number;
}

const queuedNotEnrolledSql = `
  SELECT t.id FROM detection_runs dr
  JOIN triage_results t ON t.id = dr.video_id
  WHERE dr.status = 'completed' AND t.triage_result = 'signal'
    AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
  UNION
  SELECT t.id FROM triage_results t
  WHERE t.triage_result = 'signal'
    AND (t.road_class = 'motorway' OR (t.speed_min IS NOT NULL AND t.speed_min >= 45))
    AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
    AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
`;

const freshProcessingSql = `
  status = 'processing'
  AND COALESCE(last_heartbeat_at, started_at) >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
`;

export async function getProductionPipelineCounts(
  db: DbClient
): Promise<ProductionPipelineCounts> {
  const countsResult = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM (${queuedNotEnrolledSql})) as not_enrolled,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'queued') as queued,
      (SELECT COUNT(*) FROM production_runs WHERE ${freshProcessingSql}) as processing,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'completed') as completed,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'failed') as failed
  `);

  const raw = countsResult.rows[0] as Record<string, number | bigint | null | undefined>;

  return {
    queued: Number(raw.not_enrolled ?? 0) + Number(raw.queued ?? 0),
    processing: Number(raw.processing ?? 0),
    completed: Number(raw.completed ?? 0),
    failed: Number(raw.failed ?? 0),
  };
}

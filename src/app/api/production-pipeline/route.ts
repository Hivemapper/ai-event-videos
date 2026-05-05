import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  getProductionPipelineCounts,
  type ProductionPipelineCounts,
} from "@/lib/production-pipeline-dashboard";

export const runtime = "nodejs";

/**
 * Production pipeline: privacy blur + metadata + S3 upload.
 *
 * Tabs:
 * - queued: eligible events not yet in production_runs
 *     = VRU-completed OR VRU-skipped signals (motorway / speed >= 45)
 * - processing: production_runs with status = 'processing'
 * - completed: production_runs with status = 'completed'
 * - failed: production_runs with status = 'failed'
 */
export async function GET(request: NextRequest) {
  const tab = request.nextUrl.searchParams.get("tab") ?? "queued";
  const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
  const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);
  const includeCounts = request.nextUrl.searchParams.get("includeCounts") !== "false";

  const db = await getDb();

  const counts: ProductionPipelineCounts | null = includeCounts
    ? await getProductionPipelineCounts(db)
    : null;

  let rows: Record<string, unknown>[] = [];
  let total: number | undefined;

  if (tab === "queued") {
    total = counts?.queued;
    // Show both not-yet-enrolled and enrolled-but-queued
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.status as run_status, pr.priority
       FROM triage_results t
       LEFT JOIN production_runs pr ON pr.video_id = t.id
       WHERE t.id IN (
         -- VRU-completed
         SELECT dr.video_id FROM detection_runs dr
         WHERE dr.status = 'completed'
           AND NOT EXISTS (SELECT 1 FROM production_runs pr2 WHERE pr2.video_id = dr.video_id AND pr2.status != 'queued')
         UNION
         -- VRU-skipped signals
         SELECT t2.id FROM triage_results t2
         WHERE t2.triage_result = 'signal'
           AND (t2.road_class = 'motorway' OR (t2.speed_min IS NOT NULL AND t2.speed_min >= 45))
           AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = t2.id)
           AND NOT EXISTS (SELECT 1 FROM production_runs pr3 WHERE pr3.video_id = t2.id AND pr3.status != 'queued')
       )
       AND (pr.id IS NULL OR pr.status = 'queued')
       ORDER BY COALESCE(pr.priority, 100) ASC,
                CASE WHEN pr.id IS NOT NULL THEN pr.created_at END ASC,
                t.event_timestamp DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "processing") {
    total = counts?.processing;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.status as run_status, pr.privacy_status, pr.metadata_status, pr.upload_status,
              pr.machine_id, pr.started_at, pr.priority
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'processing'
         AND COALESCE(pr.last_heartbeat_at, pr.started_at) >= strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '-10 minutes')
       ORDER BY pr.started_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "completed") {
    total = counts?.completed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.s3_video_key, pr.s3_metadata_key, pr.skip_reason,
              pr.machine_id, pr.started_at, pr.completed_at, pr.priority
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'completed'
       ORDER BY pr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "failed") {
    total = counts?.failed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.privacy_status, pr.metadata_status, pr.upload_status,
              pr.last_error, pr.machine_id, pr.completed_at, pr.priority
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'failed'
       ORDER BY pr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  }

  return NextResponse.json({
    ...(counts ? { counts } : {}),
    rows,
    ...(total != null ? { total } : {}),
  });
}

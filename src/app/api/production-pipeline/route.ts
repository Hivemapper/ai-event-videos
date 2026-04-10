import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

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

  const db = await getDb();

  // Eligible but not yet enqueued
  const queuedNotEnrolled = `
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

  const countsResult = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM (${queuedNotEnrolled})) as not_enrolled,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'queued') as queued,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'processing') as processing,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'completed') as completed,
      (SELECT COUNT(*) FROM production_runs WHERE status = 'failed') as failed
  `);

  const raw = countsResult.rows[0] as Record<string, number>;
  const counts = {
    queued: Number(raw.not_enrolled) + Number(raw.queued),
    processing: Number(raw.processing),
    completed: Number(raw.completed),
    failed: Number(raw.failed),
  };

  let rows: Record<string, unknown>[] = [];
  let total = 0;

  if (tab === "queued") {
    total = counts.queued;
    // Show both not-yet-enrolled and enrolled-but-queued
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.status as run_status
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
       ORDER BY t.event_timestamp DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "processing") {
    total = counts.processing;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.status as run_status, pr.privacy_status, pr.metadata_status, pr.upload_status,
              pr.machine_id, pr.started_at
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'processing'
       ORDER BY pr.started_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "completed") {
    total = counts.completed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.s3_video_key, pr.s3_metadata_key, pr.skip_reason,
              pr.machine_id, pr.started_at, pr.completed_at
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'completed'
       ORDER BY pr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "failed") {
    total = counts.failed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              pr.id as run_id, pr.privacy_status, pr.metadata_status, pr.upload_status,
              pr.last_error, pr.machine_id, pr.completed_at
       FROM production_runs pr
       JOIN triage_results t ON t.id = pr.video_id
       WHERE pr.status = 'failed'
       ORDER BY pr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  }

  return NextResponse.json({ counts, rows, total });
}

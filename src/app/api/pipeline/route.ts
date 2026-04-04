import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

/**
 * Returns signal events grouped by pipeline status:
 * - queued: triage=signal, no completed/running/queued detection run
 * - running: has a detection run with status running or queued
 * - completed: has a completed detection run
 * - failed: has a failed detection run (and no completed one)
 */
export async function GET(request: NextRequest) {
  const tab = request.nextUrl.searchParams.get("tab") ?? "queued";
  const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
  const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);

  const db = await getDb();

  // Count each category
  const countsResult = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
      ) as queued,
      (SELECT COUNT(DISTINCT dr.video_id) FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status IN ('queued', 'running')
      ) as running,
      (SELECT COUNT(DISTINCT dr.video_id) FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status = 'completed'
      ) as completed,
      (SELECT COUNT(DISTINCT dr.video_id) FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status = 'failed'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed')
      ) as failed
  `);

  const counts = countsResult.rows[0] as Record<string, number>;

  // Fetch rows for the active tab
  let rows: Record<string, unknown>[] = [];
  let total = 0;

  if (tab === "queued") {
    total = counts.queued;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class
       FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
       ORDER BY t.event_timestamp DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "running") {
    total = counts.running;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              dr.id as run_id, dr.status as run_status, dr.model_name, dr.started_at
       FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status IN ('queued', 'running')
       ORDER BY dr.created_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "completed") {
    total = counts.completed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              dr.id as run_id, dr.model_name, dr.detection_count, dr.started_at, dr.completed_at
       FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status = 'completed'
       ORDER BY dr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  } else if (tab === "failed") {
    total = counts.failed;
    const result = await db.query(
      `SELECT t.id, t.event_type, t.speed_min, t.speed_max, t.event_timestamp, t.road_class,
              dr.id as run_id, dr.model_name, dr.last_error, dr.completed_at
       FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE t.triage_result = 'signal'
         AND dr.status = 'failed'
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr2 WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed')
       ORDER BY dr.completed_at DESC
       LIMIT ? OFFSET ?`,
      [limit, offset]
    );
    rows = result.rows;
  }

  return NextResponse.json({ counts, rows, total });
}

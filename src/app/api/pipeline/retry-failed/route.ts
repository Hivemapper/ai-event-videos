import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

/**
 * Delete failed detection runs for non-motorway, non-high-speed events
 * so they re-enter the queued pool.
 */
export async function POST() {
  const db = await getDb();

  // Delete frame_detections and segments tied to failed runs first
  await db.query(`
    DELETE FROM frame_detections WHERE run_id IN (
      SELECT dr.id FROM detection_runs dr
      JOIN triage_results t ON t.id = dr.video_id
      WHERE dr.status = 'failed'
        AND t.triage_result = 'signal'
        AND (t.road_class IS NULL OR t.road_class != 'motorway')
        AND (t.speed_min IS NULL OR t.speed_min < 45)
        AND NOT EXISTS (
          SELECT 1 FROM detection_runs dr2
          WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed'
        )
    )
  `);

  await db.query(`
    DELETE FROM video_detection_segments WHERE run_id IN (
      SELECT dr.id FROM detection_runs dr
      JOIN triage_results t ON t.id = dr.video_id
      WHERE dr.status = 'failed'
        AND t.triage_result = 'signal'
        AND (t.road_class IS NULL OR t.road_class != 'motorway')
        AND (t.speed_min IS NULL OR t.speed_min < 45)
        AND NOT EXISTS (
          SELECT 1 FROM detection_runs dr2
          WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed'
        )
    )
  `);

  // Delete the failed runs themselves
  const result = await db.query(`
    DELETE FROM detection_runs WHERE id IN (
      SELECT dr.id FROM detection_runs dr
      JOIN triage_results t ON t.id = dr.video_id
      WHERE dr.status = 'failed'
        AND t.triage_result = 'signal'
        AND (t.road_class IS NULL OR t.road_class != 'motorway')
        AND (t.speed_min IS NULL OR t.speed_min < 45)
        AND NOT EXISTS (
          SELECT 1 FROM detection_runs dr2
          WHERE dr2.video_id = dr.video_id AND dr2.status = 'completed'
        )
    )
  `);

  const requeued = result.changes ?? 0;

  return NextResponse.json({ requeued });
}

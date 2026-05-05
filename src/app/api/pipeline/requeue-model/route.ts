import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

/**
 * Delete detection runs for a specific model so they re-enter the queue.
 * Pass ?model=gdino-base-clip to requeue all runs from the old tiny model.
 */
export async function POST(request: NextRequest) {
  const model = request.nextUrl.searchParams.get("model");
  if (!model) {
    return NextResponse.json({ error: "model param required" }, { status: 400 });
  }

  const db = await getDb();

  // Count how many will be affected
  const countResult = await db.query(
    `SELECT COUNT(*) as cnt FROM detection_runs WHERE model_name = ?`,
    [model]
  );
  const total = (countResult.rows[0] as Record<string, number>).cnt;

  // Delete frame_detections for these runs
  await db.query(
    `DELETE FROM frame_detections WHERE run_id IN (
      SELECT id FROM detection_runs WHERE model_name = ?
    )`,
    [model]
  );

  // Delete segments for these runs
  await db.query(
    `DELETE FROM video_detection_segments WHERE run_id IN (
      SELECT id FROM detection_runs WHERE model_name = ?
    )`,
    [model]
  );

  // Delete the runs themselves
  await db.query(
    `DELETE FROM detection_runs WHERE model_name = ?`,
    [model]
  );

  return NextResponse.json({ requeued: total, model });
}

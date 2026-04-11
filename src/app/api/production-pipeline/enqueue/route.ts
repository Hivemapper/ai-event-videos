import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { randomUUID } from "crypto";

export const runtime = "nodejs";

/**
 * Bulk-enqueue eligible events into the production pipeline.
 *
 * Eligible events:
 * 1. VRU-completed: detection_runs.status = 'completed' for signal events
 * 2. VRU-skipped: triage_result = 'signal' AND (motorway OR speed >= 45)
 *    with no detection_run at all
 *
 * Uses INSERT OR IGNORE + UNIQUE(video_id) to safely handle concurrent calls.
 *
 * Query params:
 *   ?limit=500 — max events to enqueue per call (default 500, prevents timeout)
 */
export async function POST(request: NextRequest) {
  const batchLimit = parseInt(
    request.nextUrl.searchParams.get("limit") ?? "500",
    10
  );
  const db = await getDb();

  // Collect eligible video IDs (capped by limit)
  const eligibleVru = await db.query(
    `SELECT DISTINCT dr.video_id FROM detection_runs dr
     JOIN triage_results t ON t.id = dr.video_id
     WHERE dr.status = 'completed'
       AND t.triage_result = 'signal'
       AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = dr.video_id)
     LIMIT ?`,
    [batchLimit]
  );

  const remaining = batchLimit - eligibleVru.rows.length;

  const eligibleSkipped =
    remaining > 0
      ? await db.query(
          `SELECT t.id as video_id FROM triage_results t
           WHERE t.triage_result = 'signal'
             AND (t.road_class = 'motorway' OR (t.speed_min IS NOT NULL AND t.speed_min >= 45))
             AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
             AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
           LIMIT ?`,
          [remaining]
        )
      : { rows: [] };

  let enqueued = 0;

  for (const row of eligibleVru.rows as Array<{ video_id: string }>) {
    const result = await db.run(
      `INSERT OR IGNORE INTO production_runs (id, video_id, created_at)
       VALUES (?, ?, datetime('now'))`,
      [randomUUID(), row.video_id]
    );
    enqueued += result.changes;
  }

  for (const row of eligibleSkipped.rows as Array<{ video_id: string }>) {
    const result = await db.run(
      `INSERT OR IGNORE INTO production_runs (id, video_id, created_at)
       VALUES (?, ?, datetime('now'))`,
      [randomUUID(), row.video_id]
    );
    enqueued += result.changes;
  }

  // Check if there are more eligible events remaining
  const moreResult = await db.query(`
    SELECT
      (SELECT COUNT(*) FROM detection_runs dr
       JOIN triage_results t ON t.id = dr.video_id
       WHERE dr.status = 'completed' AND t.triage_result = 'signal'
         AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = dr.video_id)
      ) +
      (SELECT COUNT(*) FROM triage_results t
       WHERE t.triage_result = 'signal'
         AND (t.road_class = 'motorway' OR (t.speed_min IS NOT NULL AND t.speed_min >= 45))
         AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
         AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
      ) as remaining
  `);
  const remainingCount = Number(
    (moreResult.rows[0] as Record<string, unknown>).remaining
  );

  return NextResponse.json({
    enqueued,
    remaining: remainingCount,
    sources: {
      vruCompleted: eligibleVru.rows.length,
      vruSkipped: eligibleSkipped.rows.length,
    },
  });
}

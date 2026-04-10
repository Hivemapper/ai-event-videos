import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

function minutesAgo(min: number): string {
  return new Date(Date.now() - min * 60_000).toISOString().replace(".000", "");
}

/**
 * Returns production pipeline processing stats: active machines, rate, ETA.
 */
export async function GET() {
  const db = await getDb();

  const t10 = minutesAgo(10);
  const t30 = minutesAgo(30);
  const t60 = minutesAgo(60);

  // Active machines (completed something in last 10 minutes)
  const machinesResult = await db.query(
    `SELECT DISTINCT machine_id
     FROM production_runs
     WHERE status = 'completed'
       AND completed_at >= ?
       AND machine_id IS NOT NULL`,
    [t10]
  );
  const machines = machinesResult.rows.map(
    (r: Record<string, unknown>) => r.machine_id as string
  );

  // Count completed in last 10, 30, 60 minutes
  const r10 = await db.query(
    `SELECT COUNT(*) as cnt FROM production_runs WHERE status = 'completed' AND completed_at >= ?`,
    [t10]
  );
  const r30 = await db.query(
    `SELECT COUNT(*) as cnt FROM production_runs WHERE status = 'completed' AND completed_at >= ?`,
    [t30]
  );
  const r60 = await db.query(
    `SELECT COUNT(*) as cnt FROM production_runs WHERE status = 'completed' AND completed_at >= ?`,
    [t60]
  );

  const last10m = Number((r10.rows[0] as Record<string, number>).cnt ?? 0);
  const last30m = Number((r30.rows[0] as Record<string, number>).cnt ?? 0);
  const last60m = Number((r60.rows[0] as Record<string, number>).cnt ?? 0);

  // Use 30 min window for rate (smoothed but responsive)
  const ratePerHour = last30m * 2;

  // Queued + not-yet-enrolled count
  const queuedResult = await db.query(`
    SELECT COUNT(*) as cnt FROM (
      SELECT dr.video_id FROM detection_runs dr
      JOIN triage_results t ON t.id = dr.video_id
      WHERE dr.status = 'completed' AND t.triage_result = 'signal'
        AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
      UNION
      SELECT t.id FROM triage_results t
      WHERE t.triage_result = 'signal'
        AND (t.road_class = 'motorway' OR (t.speed_min IS NOT NULL AND t.speed_min >= 45))
        AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
        AND NOT EXISTS (SELECT 1 FROM production_runs pr WHERE pr.video_id = t.id)
    )
  `);
  const enrolledQueued = await db.query(
    `SELECT COUNT(*) as cnt FROM production_runs WHERE status = 'queued'`
  );
  const queued =
    Number((queuedResult.rows[0] as Record<string, number>).cnt ?? 0) +
    Number((enrolledQueued.rows[0] as Record<string, number>).cnt ?? 0);

  const etaHours = ratePerHour > 0 ? queued / ratePerHour : null;

  // Average processing time (last 100 completed)
  const avgResult = await db.query(`
    SELECT AVG(
      (julianday(completed_at) - julianday(started_at)) * 86400
    ) as avg_secs
    FROM (
      SELECT started_at, completed_at FROM production_runs
      WHERE status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
      ORDER BY completed_at DESC LIMIT 100
    )
  `);
  const avgSecs = Number((avgResult.rows[0] as Record<string, number>).avg_secs ?? 0);

  return NextResponse.json({
    machines,
    machineCount: machines.length,
    ratePerHour,
    last10m,
    last30m,
    last60m,
    queued,
    etaHours,
    avgSecs: Math.round(avgSecs),
  });
}

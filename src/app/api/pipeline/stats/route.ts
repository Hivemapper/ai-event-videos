import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

function minutesAgo(min: number): string {
  return new Date(Date.now() - min * 60_000).toISOString().replace(".000", "");
}

/**
 * Returns pipeline processing stats: active machines, rate per hour, ETA.
 */
const FRESH_MS = 5_000;
const MAX_STALE_MS = 30_000;

type StatsPayload = {
  machines: string[];
  machineCount: number;
  ratePerHour: number;
  last10m: number;
  last30m: number;
  last60m: number;
  queued: number | null;
  etaHours: number | null;
  lastCompletedAt: unknown;
};

const cachedStats = new Map<string, { payload: StatsPayload; updatedAt: number }>();
const refreshPromises = new Map<string, Promise<void>>();

async function buildStats(includeQueued: boolean): Promise<StatsPayload> {
  const db = await getDb();

  const t10 = minutesAgo(10);
  const t30 = minutesAgo(30);
  const t60 = minutesAgo(60);

  const machinesResult = await db.query(
    `SELECT DISTINCT machine_id
     FROM detection_runs
     WHERE status = 'completed'
       AND completed_at >= ?
       AND machine_id IS NOT NULL`,
    [t10]
  );
  const windowResult = await db.query(
    `SELECT
       SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) as last10m,
       SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) as last30m,
       COUNT(*) as last60m
     FROM detection_runs
     WHERE status = 'completed' AND completed_at >= ?`,
    [t10, t30, t60]
  );
  const lastResult = await db.query(`
    SELECT completed_at FROM detection_runs
    WHERE status = 'completed'
    ORDER BY completed_at DESC LIMIT 1
  `);
  const queuedResult = includeQueued
    ? await db.query(`
        SELECT COUNT(*) as cnt FROM triage_results t
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
      `)
    : { rows: [{ cnt: null }] };

  const machines = machinesResult.rows.map(
    (r: Record<string, unknown>) => r.machine_id as string
  );
  const windowCounts = windowResult.rows[0] as Record<string, number | null>;
  const last10m = Number(windowCounts.last10m ?? 0);
  const last30m = Number(windowCounts.last30m ?? 0);
  const last60m = Number(windowCounts.last60m ?? 0);
  const ratePerHour = last30m * 2;
  const queuedRaw = (queuedResult.rows[0] as Record<string, number | null>).cnt;
  const queued = queuedRaw === null ? null : Number(queuedRaw ?? 0);
  const etaHours = queued !== null && ratePerHour > 0 ? queued / ratePerHour : null;
  const lastCompletedAt =
    lastResult.rows.length > 0
      ? (lastResult.rows[0] as Record<string, unknown>).completed_at
      : null;

  return {
    machines,
    machineCount: machines.length,
    ratePerHour,
    last10m,
    last30m,
    last60m,
    queued,
    etaHours,
    lastCompletedAt,
  };
}

function kickRefresh(cacheKey: string, includeQueued: boolean) {
  if (!refreshPromises.has(cacheKey)) {
    refreshPromises.set(
      cacheKey,
      buildStats(includeQueued)
        .then((payload) => {
          cachedStats.set(cacheKey, { payload, updatedAt: Date.now() });
        })
        .finally(() => {
          refreshPromises.delete(cacheKey);
        })
    );
  }
}

export async function GET(request: NextRequest) {
  const includeQueued =
    request.nextUrl.searchParams.get("includeQueued") !== "false";
  const cacheKey = includeQueued ? "with-queued" : "without-queued";
  const cached = cachedStats.get(cacheKey);
  const age = cached ? Date.now() - cached.updatedAt : Infinity;

  if (!cached || age > MAX_STALE_MS) {
    const payload = await buildStats(includeQueued);
    cachedStats.set(cacheKey, { payload, updatedAt: Date.now() });
    return NextResponse.json(payload);
  }

  if (age > FRESH_MS) {
    kickRefresh(cacheKey, includeQueued);
  }

  return NextResponse.json(cached.payload);
}

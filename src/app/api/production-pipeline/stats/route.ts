import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { getProductionPipelineCounts } from "@/lib/production-pipeline-dashboard";
import type { DbClient, DbQueryResult } from "@/lib/db";

export const runtime = "nodejs";

function minutesAgo(min: number): string {
  return new Date(Date.now() - min * 60_000).toISOString().replace(".000", "");
}

/**
 * Returns production pipeline processing stats: active machines, rate, ETA.
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
  avgSecs: number;
};

const cachedStats = new Map<string, { payload: StatsPayload; updatedAt: number }>();
const refreshPromises = new Map<string, Promise<void>>();

function isMissingTableError(error: unknown): boolean {
  return String(error).toLowerCase().includes("no such table");
}

async function getWorkerMachines(
  db: DbClient,
  activeSince: string
): Promise<DbQueryResult> {
  try {
    return await db.query(
      `SELECT machine_id
       FROM production_worker_heartbeats
       WHERE last_heartbeat_at >= ?
         AND machine_id IS NOT NULL`,
      [activeSince]
    );
  } catch (error) {
    if (isMissingTableError(error)) {
      return { rows: [], lastInsertRowid: 0, changes: 0 };
    }
    throw error;
  }
}

async function buildStats(includeQueued: boolean): Promise<StatsPayload> {
  const db = await getDb();

  const t10 = minutesAgo(10);
  const t30 = minutesAgo(30);
  const t60 = minutesAgo(60);

  const [
    workerMachinesResult,
    activeMachinesResult,
    completedMachinesResult,
    windowResult,
    avgResult,
    counts,
  ] = await Promise.all([
    getWorkerMachines(db, t10),
    db.query(
      `SELECT DISTINCT machine_id
       FROM production_runs
       WHERE status = 'processing'
         AND COALESCE(last_heartbeat_at, started_at) >= ?
         AND machine_id IS NOT NULL`,
      [t10]
    ),
    db.query(
      `SELECT DISTINCT machine_id
       FROM production_runs
       WHERE status = 'completed'
         AND completed_at >= ?
         AND machine_id IS NOT NULL`,
      [t10]
    ),
    db.query(
      `SELECT
         SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) as last10m,
         SUM(CASE WHEN completed_at >= ? THEN 1 ELSE 0 END) as last30m,
         COUNT(*) as last60m
       FROM production_runs
       WHERE status = 'completed' AND completed_at >= ?`,
      [t10, t30, t60]
    ),
    db.query(`
      SELECT AVG(
        (julianday(completed_at) - julianday(started_at)) * 86400
      ) as avg_secs
      FROM (
        SELECT started_at, completed_at FROM production_runs
        WHERE status = 'completed' AND started_at IS NOT NULL AND completed_at IS NOT NULL
        ORDER BY completed_at DESC LIMIT 100
      )
    `),
    includeQueued ? getProductionPipelineCounts(db) : Promise.resolve(null),
  ]);

  const machineSet = new Set<string>();
  for (const row of workerMachinesResult.rows) {
    if (row.machine_id) machineSet.add(row.machine_id as string);
  }
  for (const row of activeMachinesResult.rows) {
    if (row.machine_id) machineSet.add(row.machine_id as string);
  }
  for (const row of completedMachinesResult.rows) {
    if (row.machine_id) machineSet.add(row.machine_id as string);
  }
  const machines = Array.from(machineSet);

  const windowCounts = windowResult.rows[0] as Record<string, number | null>;
  const last10m = Number(windowCounts.last10m ?? 0);
  const last30m = Number(windowCounts.last30m ?? 0);
  const last60m = Number(windowCounts.last60m ?? 0);
  const ratePerHour = last30m * 2;
  const queued = counts?.queued ?? null;
  const etaHours = queued !== null && ratePerHour > 0 ? queued / ratePerHour : null;
  const avgSecs = Number((avgResult.rows[0] as Record<string, number>).avg_secs ?? 0);

  return {
    machines,
    machineCount: machines.length,
    ratePerHour,
    last10m,
    last30m,
    last60m,
    queued,
    etaHours,
    avgSecs: Math.round(avgSecs),
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

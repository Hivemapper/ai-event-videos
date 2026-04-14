import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET(request: NextRequest) {
  try {
    const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
    const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);
    const filter = request.nextUrl.searchParams.get("filter"); // ghost, open_road, signal
    const period = request.nextUrl.searchParams.get("period"); // 1-5

    const db = await getDb();

    // Ensure the table exists (created by run-triage.py, may not exist yet)
    await db.exec(`CREATE TABLE IF NOT EXISTS triage_results (
      id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      triage_result TEXT NOT NULL,
      rules_triggered TEXT NOT NULL DEFAULT '[]',
      speed_min REAL,
      speed_max REAL,
      speed_mean REAL,
      speed_stddev REAL,
      gnss_displacement_m REAL,
      video_size INTEGER,
      event_timestamp TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`);

    // Period date ranges
    const PERIODS: Record<string, [string, string]> = {
      "1": ["2025-01-01", "2025-09-15"],
      "2": ["2025-09-15", "2026-01-15"],
      "3": ["2026-01-15", "2026-02-10"],
      "4": ["2026-02-11", "2026-03-15"],
      "5": ["2026-03-15", "2099-01-01"],
    };

    const conditions: string[] = [];
    const params: unknown[] = [];

    if (filter) {
      conditions.push("triage_result = ?");
      params.push(filter);
    }
    if (period && PERIODS[period]) {
      const [pStart, pEnd] = PERIODS[period];
      conditions.push("event_timestamp >= ? AND event_timestamp < ?");
      params.push(pStart + "T00:00:00Z", pEnd + "T00:00:00Z");
    }

    const whereClause = conditions.length > 0 ? "WHERE " + conditions.join(" AND ") : "";

    const countResult = await db.query(
      `SELECT COUNT(*) as count FROM triage_results ${whereClause}`,
      params
    );
    const total = (countResult.rows[0] as unknown as { count: number }).count;

    const result = await db.query(
      `SELECT * FROM triage_results ${whereClause}
       ORDER BY created_at DESC LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    // Also get summary counts (within period filter if set)
    const summaryWhere = period && PERIODS[period]
      ? `WHERE event_timestamp >= '${PERIODS[period][0]}T00:00:00Z' AND event_timestamp < '${PERIODS[period][1]}T00:00:00Z'`
      : "";
    const summaryResult = await db.query(
      `SELECT triage_result, COUNT(*) as count FROM triage_results ${summaryWhere} GROUP BY triage_result`
    );
    const summary: Record<string, number> = {};
    for (const row of summaryResult.rows as Array<{ triage_result: string; count: number }>) {
      summary[row.triage_result] = row.count;
    }

    return NextResponse.json({
      results: result.rows,
      total,
      summary,
    });
  } catch (error) {
    console.error("Triage API error:", error);
    return NextResponse.json(
      { error: String(error), results: [], total: 0, summary: {} },
      { status: 500 }
    );
  }
}

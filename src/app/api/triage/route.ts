import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET(request: NextRequest) {
  try {
    const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
    const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);
    const filter = request.nextUrl.searchParams.get("filter"); // ghost, open_road, signal

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

    const whereClause = filter ? "WHERE triage_result = ?" : "";
    const params: unknown[] = filter ? [filter] : [];

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

    // Also get summary counts
    const summaryResult = await db.query(
      `SELECT triage_result, COUNT(*) as count FROM triage_results GROUP BY triage_result`
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

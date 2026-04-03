import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

const VALID_RESULTS = ["missing_video", "missing_metadata", "ghost", "open_road", "signal", "duplicate"] as const;

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const db = await getDb();

  try {
    const result = await db.query(
      "SELECT * FROM triage_results WHERE id = ?",
      [id]
    );

    if (result.rows.length === 0) {
      return NextResponse.json({ triage: null });
    }

    return NextResponse.json({ triage: result.rows[0] });
  } catch {
    return NextResponse.json({ triage: null });
  }
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const body = await request.json();
  const triageResult = body.triage_result;
  const eventType = body.event_type ?? "UNKNOWN";

  if (!VALID_RESULTS.includes(triageResult)) {
    return NextResponse.json(
      { error: `Invalid triage_result. Must be one of: ${VALID_RESULTS.join(", ")}` },
      { status: 400 }
    );
  }

  const db = await getDb();

  try {
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

    await db.run(
      `INSERT INTO triage_results (id, event_type, triage_result, rules_triggered, created_at)
       VALUES (?, ?, ?, ?, datetime('now'))
       ON CONFLICT(id) DO UPDATE SET
         triage_result = excluded.triage_result,
         rules_triggered = '["manual"]',
         created_at = datetime('now')`,
      [id, eventType, triageResult, '["manual"]']
    );

    const result = await db.query(
      "SELECT * FROM triage_results WHERE id = ?",
      [id]
    );

    return NextResponse.json({ triage: result.rows[0] });
  } catch (error) {
    console.error("Failed to update triage:", error);
    return NextResponse.json(
      { error: "Failed to update triage result" },
      { status: 500 }
    );
  }
}

export async function DELETE(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const db = await getDb();

  try {
    await db.run("DELETE FROM triage_results WHERE id = ?", [id]);
    return NextResponse.json({ triage: null });
  } catch (error) {
    console.error("Failed to delete triage:", error);
    return NextResponse.json(
      { error: "Failed to delete triage result" },
      { status: 500 }
    );
  }
}

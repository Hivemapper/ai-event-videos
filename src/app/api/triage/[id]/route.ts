import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

const VALID_RESULTS = ["missing_video", "missing_metadata", "ghost", "open_road", "signal", "duplicate", "non_linear", "privacy", "skipped_firmware"] as const;
const VALID_EVENT_TYPES = [
  "HARSH_BRAKING",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
  "HIGH_SPEED",
  "HIGH_G_FORCE",
  "STOP_SIGN_VIOLATION",
  "TRAFFIC_LIGHT_VIOLATION",
  "TAILGATING",
  "MANUAL_REQUEST",
  "UNKNOWN",
] as const;

function isValidResult(value: unknown): value is (typeof VALID_RESULTS)[number] {
  return typeof value === "string" && (VALID_RESULTS as readonly string[]).includes(value);
}

function isValidEventType(value: unknown): value is (typeof VALID_EVENT_TYPES)[number] {
  return typeof value === "string" && (VALID_EVENT_TYPES as readonly string[]).includes(value);
}

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
  const body: unknown = await request.json();
  if (!body || typeof body !== "object" || Array.isArray(body)) {
    return NextResponse.json(
      { error: "Expected JSON object body" },
      { status: 400 }
    );
  }
  const payload = body as Record<string, unknown>;
  const hasTriageResult = Object.prototype.hasOwnProperty.call(payload, "triage_result");
  const hasEventType = Object.prototype.hasOwnProperty.call(payload, "event_type");
  const triageResult = payload.triage_result;
  const eventType = payload.event_type;

  if (!hasTriageResult && !hasEventType) {
    return NextResponse.json(
      { error: "Expected triage_result or event_type" },
      { status: 400 }
    );
  }

  if (hasTriageResult && !isValidResult(triageResult)) {
    return NextResponse.json(
      { error: `Invalid triage_result. Must be one of: ${VALID_RESULTS.join(", ")}` },
      { status: 400 }
    );
  }

  if (hasEventType && !isValidEventType(eventType)) {
    return NextResponse.json(
      { error: `Invalid event_type. Must be one of: ${VALID_EVENT_TYPES.join(", ")}` },
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
      video_length_sec REAL,
      bitrate_bps REAL,
      firmware_version TEXT,
      firmware_version_num INTEGER,
      event_timestamp TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`);

    const existing = await db.query(
      "SELECT event_type FROM triage_results WHERE id = ?",
      [id]
    );

    if (hasTriageResult) {
      const existingEventType = existing.rows[0]?.event_type;
      const eventTypeForWrite = hasEventType
        ? eventType
        : typeof existingEventType === "string"
          ? existingEventType
          : "UNKNOWN";

      await db.run(
        `INSERT INTO triage_results (id, event_type, triage_result, rules_triggered, created_at)
         VALUES (?, ?, ?, ?, datetime('now'))
         ON CONFLICT(id) DO UPDATE SET
           event_type = excluded.event_type,
           triage_result = excluded.triage_result,
           rules_triggered = '["manual"]',
           created_at = datetime('now')`,
        [id, eventTypeForWrite, triageResult, '["manual"]']
      );
    } else {
      if (existing.rows.length === 0) {
        return NextResponse.json(
          { error: "No triage result exists for this event yet" },
          { status: 404 }
        );
      }

      await db.run(
        "UPDATE triage_results SET event_type = ?, created_at = datetime('now') WHERE id = ?",
        [eventType, id]
      );
    }

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

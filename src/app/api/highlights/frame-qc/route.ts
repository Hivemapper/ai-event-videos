import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

interface FrameTimingQcSummary {
  fpsQc: string | null;
  lateFramePct: number | null;
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json().catch(() => ({}));
    const ids = Array.from(
      new Set(
        Array.isArray(body?.ids)
          ? body.ids
              .filter((id: unknown): id is string => typeof id === "string" && id.trim().length > 0)
              .map((id: string) => id.trim())
          : []
      )
    );

    if (ids.length === 0) {
      return NextResponse.json({ frameTimingQcById: {} });
    }

    const placeholders = ids.map(() => "?").join(", ");
    const db = await getDb();
    const result = await db.query(
      `SELECT video_id, bucket, gap_pct
       FROM video_frame_timing_qc
       WHERE video_id IN (${placeholders})`,
      ids
    );

    const frameTimingQcById: Record<string, FrameTimingQcSummary> = {};
    for (const row of result.rows) {
      const id = typeof row.video_id === "string" ? row.video_id : String(row.video_id ?? "");
      if (!id) continue;
      frameTimingQcById[id] = {
        fpsQc: typeof row.bucket === "string" ? row.bucket : null,
        lateFramePct: nullableNumber(row.gap_pct),
      };
    }

    return NextResponse.json({ frameTimingQcById });
  } catch (error) {
    console.error("Highlight frame QC error:", error);
    return NextResponse.json(
      { error: "Failed to load frame QC", frameTimingQcById: {} },
      { status: 500 }
    );
  }
}

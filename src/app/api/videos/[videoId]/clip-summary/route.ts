import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const db = await getDb();
  const result = await db.query(
    "SELECT summary FROM clip_summaries WHERE video_id = ?",
    [videoId]
  );
  const row = result.rows[0] as { summary: string } | undefined;

  return NextResponse.json({ summary: row?.summary ?? null });
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const { summary } = (await request.json()) as { summary: string | null };
  const db = await getDb();

  if (!summary || summary.trim() === "") {
    await db.run("DELETE FROM clip_summaries WHERE video_id = ?", [videoId]);
    return NextResponse.json({ summary: null });
  }

  await db.run(
    `INSERT INTO clip_summaries (video_id, summary, updated_at)
     VALUES (?, ?, datetime('now'))
     ON CONFLICT(video_id) DO UPDATE SET summary = excluded.summary, updated_at = excluded.updated_at`,
    [videoId, summary.trim()]
  );

  return NextResponse.json({ summary: summary.trim() });
}

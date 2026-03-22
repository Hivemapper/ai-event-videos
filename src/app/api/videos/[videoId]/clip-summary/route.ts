import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const db = getDb();
  const row = db
    .prepare("SELECT summary FROM clip_summaries WHERE video_id = ?")
    .get(videoId) as { summary: string } | undefined;

  return NextResponse.json({ summary: row?.summary ?? null });
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const { summary } = (await request.json()) as { summary: string | null };
  const db = getDb();

  if (!summary || summary.trim() === "") {
    db.prepare("DELETE FROM clip_summaries WHERE video_id = ?").run(videoId);
    return NextResponse.json({ summary: null });
  }

  db.prepare(
    `INSERT INTO clip_summaries (video_id, summary, updated_at)
     VALUES (?, ?, datetime('now'))
     ON CONFLICT(video_id) DO UPDATE SET summary = excluded.summary, updated_at = excluded.updated_at`
  ).run(videoId, summary.trim());

  return NextResponse.json({ summary: summary.trim() });
}

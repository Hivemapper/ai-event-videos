import { NextResponse } from "next/server";
import { getDetectionRun } from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ videoId: string; runId: string }> }
) {
  const { videoId, runId } = await params;
  const run = await getDetectionRun(runId);

  if (!run || run.videoId !== videoId) {
    return NextResponse.json({ error: "Run not found" }, { status: 404 });
  }

  return NextResponse.json({ run });
}

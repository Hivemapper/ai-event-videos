import { NextResponse } from "next/server";
import { stopPipeline, isPipelineRunning } from "@/lib/pipeline-manager";

export const runtime = "nodejs";

export async function POST() {
  if (!isPipelineRunning()) {
    return NextResponse.json({ status: "not_running" });
  }

  const result = await stopPipeline();
  return NextResponse.json({ status: "stopped", runId: result.runId });
}

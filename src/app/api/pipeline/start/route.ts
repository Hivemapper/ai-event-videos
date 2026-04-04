import { NextRequest, NextResponse } from "next/server";
import { startPipeline, isPipelineRunning } from "@/lib/pipeline-manager";

export const runtime = "nodejs";

export async function POST(_request: NextRequest) {
  if (isPipelineRunning()) {
    return NextResponse.json({ status: "already_running" });
  }

  const result = await startPipeline();

  if (result.started) {
    return NextResponse.json({ status: "started" }, { status: 201 });
  }

  return NextResponse.json(
    { error: result.error ?? "Failed to start pipeline" },
    { status: 500 }
  );
}

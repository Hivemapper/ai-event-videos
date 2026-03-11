import { NextResponse } from "next/server";
import { getPipelineRun, updatePipelineRunStatus } from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const run = getPipelineRun(id);

  if (!run) {
    return NextResponse.json({ error: "Run not found" }, { status: 404 });
  }

  if (["completed", "cancelled"].includes(run.status)) {
    return NextResponse.json(
      { error: `Run is already ${run.status}` },
      { status: 409 }
    );
  }

  updatePipelineRunStatus(id, "cancelled");
  return NextResponse.json({ run: getPipelineRun(id) });
}

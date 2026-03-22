import { NextResponse } from "next/server";
import { getPipelineRun, updatePipelineRunStatus } from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const run = await getPipelineRun(id);

  if (!run) {
    return NextResponse.json({ error: "Run not found" }, { status: 404 });
  }

  if (!["queued", "running"].includes(run.status)) {
    return NextResponse.json(
      { error: `Cannot pause a ${run.status} run` },
      { status: 409 }
    );
  }

  await updatePipelineRunStatus(id, "paused");
  return NextResponse.json({ run: await getPipelineRun(id) });
}

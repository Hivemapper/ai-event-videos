import { NextResponse } from "next/server";
import { spawnPipelineWorker } from "@/lib/pipeline-worker";
import {
  getPipelineRun,
  getPipelineRunBeeMapsKey,
  isRunHeartbeatStale,
  setPipelineRunWorkerPid,
  updatePipelineRunStatus,
} from "@/lib/pipeline-store";

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

  if (run.status !== "paused") {
    return NextResponse.json(
      { error: `Cannot resume a ${run.status} run` },
      { status: 409 }
    );
  }

  await updatePipelineRunStatus(id, "running");

  if (isRunHeartbeatStale(run)) {
    const beeMapsKey = await getPipelineRunBeeMapsKey(id);
    if (!beeMapsKey) {
      return NextResponse.json(
        { error: "Run is missing Bee Maps credentials" },
        { status: 500 }
      );
    }

    const worker = spawnPipelineWorker({
      runId: id,
      beeMapsKey,
      day: run.day,
      batchSize: run.batchSize,
      modelName: run.modelName,
    });
    await setPipelineRunWorkerPid(id, worker.pid);
  }

  return NextResponse.json({ run: await getPipelineRun(id) });
}

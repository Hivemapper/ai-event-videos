import { NextResponse } from "next/server";
import { spawnPipelineWorker } from "@/lib/pipeline-worker";
import {
  createRetryRunFrom,
  getActivePipelineRun,
  getPipelineRunBeeMapsKey,
  setPipelineRunWorkerPid,
} from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const activeRun = await getActivePipelineRun();

  if (activeRun) {
    return NextResponse.json(
      { error: `Run ${activeRun.id} is still ${activeRun.status}` },
      { status: 409 }
    );
  }

  try {
    const run = await createRetryRunFrom(id);
    const beeMapsKey = await getPipelineRunBeeMapsKey(run.id);
    if (!beeMapsKey) {
      throw new Error("Retry run is missing Bee Maps credentials");
    }
    const worker = spawnPipelineWorker({
      runId: run.id,
      beeMapsKey,
      day: run.day,
      batchSize: run.batchSize,
      modelName: run.modelName,
    });
    await setPipelineRunWorkerPid(run.id, worker.pid);
    return NextResponse.json({ run }, { status: 201 });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Failed to retry failed videos",
      },
      { status: 500 }
    );
  }
}

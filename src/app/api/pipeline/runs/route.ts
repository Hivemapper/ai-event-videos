import { NextRequest, NextResponse } from "next/server";
import { pipelineRunCreateSchema } from "@/lib/schemas";
import {
  createPipelineRun,
  getActivePipelineRun,
  getPipelineRun,
  listPipelineRuns,
  setPipelineRunWorkerPid,
  updatePipelineRunStatus,
} from "@/lib/pipeline-store";
import { spawnPipelineWorker } from "@/lib/pipeline-worker";

export const runtime = "nodejs";

export async function GET(request: NextRequest) {
  const day = request.nextUrl.searchParams.get("day") ?? undefined;
  const [runs, activeRun] = await Promise.all([
    listPipelineRuns(day),
    getActivePipelineRun(),
  ]);
  return NextResponse.json({ runs, activeRun });
}

export async function POST(request: NextRequest) {
  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
  }

  const parsed = pipelineRunCreateSchema.safeParse(body);
  if (!parsed.success) {
    return NextResponse.json(
      { error: parsed.error.issues.map((issue) => issue.message).join(", ") },
      { status: 400 }
    );
  }

  const activeRun = await getActivePipelineRun();
  if (activeRun) {
    return NextResponse.json(
      { error: `Run ${activeRun.id} is still ${activeRun.status}` },
      { status: 409 }
    );
  }

  const run = await createPipelineRun({
    day: parsed.data.day,
    batchSize: parsed.data.batchSize,
    beeMapsKey: parsed.data.beeMapsApiKey,
  });

  try {
    const worker = spawnPipelineWorker({
      runId: run.id,
      beeMapsKey: parsed.data.beeMapsApiKey,
      day: run.day,
      batchSize: run.batchSize,
      modelName: run.modelName,
    });
    await setPipelineRunWorkerPid(run.id, worker.pid);
  } catch (error) {
    await updatePipelineRunStatus(run.id, "failed");
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to start pipeline worker",
      },
      { status: 500 }
    );
  }

  return NextResponse.json({ run: await getPipelineRun(run.id) }, { status: 201 });
}

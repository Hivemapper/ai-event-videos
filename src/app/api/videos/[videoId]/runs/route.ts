import { NextResponse } from "next/server";
import { AVAILABLE_DETECTION_MODELS } from "@/lib/pipeline-config";
import {
  createDetectionRun,
  getActiveDetectionRun,
  listDetectionRuns,
  setDetectionRunWorkerPid,
  updateDetectionRunStatus,
} from "@/lib/pipeline-store";
import { spawnDetectionWorker } from "@/lib/detection-worker";

export const runtime = "nodejs";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const runs = await listDetectionRuns(videoId);
  return NextResponse.json({ runs });
}

export async function POST(
  request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;

  let body: unknown;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  if (
    !body ||
    typeof body !== "object" ||
    !("modelName" in body) ||
    typeof (body as { modelName: unknown }).modelName !== "string"
  ) {
    return NextResponse.json(
      { error: "modelName is required and must be a string" },
      { status: 400 }
    );
  }

  const { modelName } = body as { modelName: string };

  const validModelIds = AVAILABLE_DETECTION_MODELS.map((m) => m.id);
  if (!validModelIds.includes(modelName)) {
    return NextResponse.json(
      {
        error: `Invalid modelName. Must be one of: ${validModelIds.join(", ")}`,
      },
      { status: 400 }
    );
  }

  const modelConfig = AVAILABLE_DETECTION_MODELS.find((m) => m.id === modelName);
  const run = await createDetectionRun({
    videoId,
    modelName,
    config: {
      modelDisplayName: modelConfig?.name,
      type: modelConfig?.type,
      device: modelConfig?.device,
      classes: modelConfig?.classes,
      prompt: modelConfig?.prompt,
      features: modelConfig?.features,
      estimatedTime: modelConfig?.estimatedTime,
    },
  });

  if (!run) {
    const activeRun = await getActiveDetectionRun();
    return NextResponse.json(
      {
        error: "A detection run is already active",
        activeRun: activeRun
          ? {
              id: activeRun.id,
              videoId: activeRun.videoId,
              modelName: activeRun.modelName,
            }
          : null,
      },
      { status: 409 }
    );
  }

  try {
    const worker = spawnDetectionWorker({ runId: run.id });
    await setDetectionRunWorkerPid(run.id, worker.pid);
  } catch (error) {
    await updateDetectionRunStatus(run.id, "failed", {
      lastError:
        error instanceof Error
          ? error.message
          : "Failed to start detection worker",
    });
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to start detection worker",
      },
      { status: 500 }
    );
  }

  return NextResponse.json({ run }, { status: 201 });
}

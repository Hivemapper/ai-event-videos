import { NextResponse } from "next/server";
import { AVAILABLE_DETECTION_MODELS } from "@/lib/pipeline-config";
import {
  createDetectionRun,
  DETECTION_PRIORITY_MANUAL,
  listDetectionRuns,
  setDetectionRunWorkerPid,
  updateDetectionRunStatus,
} from "@/lib/pipeline-store";
import { spawnDetectionWorker } from "@/lib/detection-worker";
import { syncClippedEventAssetsForAws } from "@/lib/clipped-event-assets";

export const runtime = "nodejs";

const LOCAL_RUNNER_MODES = new Set(["local", "local-worker", "spawn-local"]);

function getDetectionRunnerMode(): string {
  return (process.env.DETECTION_RUNNER_MODE ?? "aws-queue").trim().toLowerCase();
}

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  try {
    const { videoId } = await params;
    const runs = await listDetectionRuns(videoId);
    return NextResponse.json({ runs });
  } catch (error) {
    console.error("Detection runs GET error:", error);
    return NextResponse.json(
      { error: String(error) },
      { status: 500 }
    );
  }
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
  const runnerMode = getDetectionRunnerMode();
  const shouldSpawnLocalWorker = LOCAL_RUNNER_MODES.has(runnerMode);
  const executionTarget = shouldSpawnLocalWorker ? "local" : "aws";
  const queuedForHost = process.env.DETECTION_AWS_HOST?.trim() || null;
  let localAssetSync: Awaited<ReturnType<typeof syncClippedEventAssetsForAws>> | null = null;

  if (!shouldSpawnLocalWorker) {
    try {
      localAssetSync = await syncClippedEventAssetsForAws(videoId);
      if (localAssetSync.skippedReason === "no-detector-hosts") {
        return NextResponse.json(
          {
            error: "Local clipped event assets exist, but no AWS detector hosts were configured or discovered",
            localAssetSync,
          },
          { status: 500 }
        );
      }
    } catch (error) {
      return NextResponse.json(
        {
          error:
            error instanceof Error
              ? error.message
              : "Failed to sync local clipped event assets",
        },
        { status: 500 }
      );
    }
  }

  const run = await createDetectionRun({
    videoId,
    modelName,
    config: {
      modelDisplayName: modelConfig?.name,
      type: modelConfig?.type,
      device: shouldSpawnLocalWorker ? modelConfig?.device : "AWS GPU",
      classes: modelConfig?.classes,
      prompt: modelConfig?.prompt,
      features: modelConfig?.features,
      estimatedTime: modelConfig?.estimatedTime,
      framesPerVideo: 300,
      frameSampling: "every_n_frames",
      frameStride: 5,
      manualPriority: true,
      executionTarget,
      runnerMode,
      ...(queuedForHost ? { queuedForHost } : {}),
      ...(localAssetSync ? { localAssetSync } : {}),
    },
    machineId: shouldSpawnLocalWorker ? undefined : null,
    priority: DETECTION_PRIORITY_MANUAL,
  });

  if (!run) {
    const activeRun = (await listDetectionRuns(videoId)).find(
      (candidate) => candidate.status === "queued" || candidate.status === "running"
    );
    return NextResponse.json(
      {
        error: "A detection run is already active for this video",
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

  if (shouldSpawnLocalWorker) {
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
  }

  return NextResponse.json(
    {
      run,
      executionTarget,
      queuedForHost,
      localAssetSync,
    },
    { status: shouldSpawnLocalWorker ? 201 : 202 }
  );
}

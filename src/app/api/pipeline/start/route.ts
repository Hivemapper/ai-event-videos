import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { createDetectionRun, getActiveDetectionRun, setDetectionRunWorkerPid, updateDetectionRunStatus } from "@/lib/pipeline-store";
import { spawnDetectionWorker } from "@/lib/detection-worker";
import { AVAILABLE_DETECTION_MODELS } from "@/lib/pipeline-config";

export const runtime = "nodejs";

/**
 * Start the pipeline: pick the next queued signal event and run detection on it.
 * Returns the created run or an error if one is already active.
 */
export async function POST(request: NextRequest) {
  const body = await request.json().catch(() => ({}));
  const modelName = (body as Record<string, unknown>).modelName as string | undefined
    ?? "gdino-base-clip";

  const validModelIds = AVAILABLE_DETECTION_MODELS.map((m) => m.id);
  if (!validModelIds.includes(modelName)) {
    return NextResponse.json(
      { error: `Invalid modelName. Must be one of: ${validModelIds.join(", ")}` },
      { status: 400 }
    );
  }

  // Check if something is already running
  const activeRun = await getActiveDetectionRun();
  if (activeRun) {
    return NextResponse.json(
      { error: "A detection run is already active", activeRun },
      { status: 409 }
    );
  }

  // Pick next signal event without a detection run
  const db = await getDb();
  const result = await db.query(
    `SELECT t.id FROM triage_results t
     WHERE t.triage_result = 'signal'
       AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
     ORDER BY t.event_timestamp DESC
     LIMIT 1`
  );

  if (result.rows.length === 0) {
    return NextResponse.json(
      { error: "No queued signal events to process" },
      { status: 404 }
    );
  }

  const videoId = (result.rows[0] as { id: string }).id;
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
    return NextResponse.json(
      { error: "Failed to create detection run (concurrent run exists)" },
      { status: 409 }
    );
  }

  try {
    const worker = spawnDetectionWorker({ runId: run.id });
    await setDetectionRunWorkerPid(run.id, worker.pid);
  } catch (error) {
    await updateDetectionRunStatus(run.id, "failed", {
      lastError: error instanceof Error ? error.message : "Failed to start worker",
    });
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to start worker" },
      { status: 500 }
    );
  }

  return NextResponse.json({ run, videoId }, { status: 201 });
}

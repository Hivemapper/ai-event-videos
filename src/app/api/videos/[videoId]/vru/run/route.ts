import { NextResponse } from "next/server";
import {
  createPipelineRun,
  getVideoPipelineState,
  getPipelineRun,
  updatePipelineRunStatus,
} from "@/lib/pipeline-store";
import { spawnSingleVideoWorker } from "@/lib/pipeline-worker";

export async function POST(
  request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;

  let body: { videoUrl: string; modelName?: string; beeMapsApiKey: string; day?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json({ error: "Invalid JSON" }, { status: 400 });
  }

  if (!body.videoUrl || !body.beeMapsApiKey) {
    return NextResponse.json(
      { error: "videoUrl and beeMapsApiKey are required" },
      { status: 400 }
    );
  }

  // Check if already running for this video
  const existing = getVideoPipelineState(videoId);
  if (existing?.status === "running" || existing?.status === "queued") {
    return NextResponse.json(
      { error: "This video is already being processed" },
      { status: 409 }
    );
  }

  const day = body.day ?? new Date().toISOString().slice(0, 10);

  const run = createPipelineRun({
    day,
    batchSize: 1,
    beeMapsKey: body.beeMapsApiKey,
    modelName: body.modelName,
  });

  try {
    const { pid } = spawnSingleVideoWorker({
      runId: run.id,
      beeMapsKey: body.beeMapsApiKey,
      day,
      videoId,
      videoUrl: body.videoUrl,
      modelName: body.modelName,
    });

    if (pid) {
      const { setPipelineRunWorkerPid } = await import("@/lib/pipeline-store");
      setPipelineRunWorkerPid(run.id, pid);
    }

    return NextResponse.json({ run: getPipelineRun(run.id), videoId });
  } catch (err) {
    updatePipelineRunStatus(run.id, "failed");
    return NextResponse.json(
      { error: err instanceof Error ? err.message : "Failed to start worker" },
      { status: 500 }
    );
  }
}

export async function DELETE(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const state = getVideoPipelineState(videoId);

  if (!state || (state.status !== "running" && state.status !== "queued")) {
    return NextResponse.json(
      { error: "No active processing for this video" },
      { status: 404 }
    );
  }

  // Find the active run and cancel it
  const { getDb } = await import("@/lib/db");
  const db = getDb();
  const row = db
    .prepare(
      `SELECT id FROM pipeline_runs
       WHERE status IN ('queued', 'running')
       ORDER BY created_at DESC LIMIT 1`
    )
    .get() as { id: string } | undefined;

  if (row) {
    updatePipelineRunStatus(row.id, "cancelled");
  }

  // Mark the video as failed/cancelled
  db.prepare(
    `UPDATE video_pipeline_state
     SET status = 'failed', last_error = 'Cancelled by user', completed_at = datetime('now')
     WHERE video_id = ?`
  ).run(videoId);

  return NextResponse.json({ cancelled: true, videoId });
}

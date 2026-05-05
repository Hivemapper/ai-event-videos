import { NextResponse } from "next/server";
import {
  getDetectionRun,
  isCurrentMachineId,
  updateDetectionRunStatus,
} from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ videoId: string; runId: string }> }
) {
  const { videoId, runId } = await params;
  const run = await getDetectionRun(runId);

  if (!run || run.videoId !== videoId) {
    return NextResponse.json({ error: "Run not found" }, { status: 404 });
  }

  if (run.status !== "queued" && run.status !== "running") {
    return NextResponse.json(
      { error: `Cannot cancel a run with status "${run.status}"` },
      { status: 400 }
    );
  }

  await updateDetectionRunStatus(runId, "cancelled");

  if (run.workerPid && isCurrentMachineId(run.machineId)) {
    try {
      // Kill the entire process group (negative PID) since worker is detached
      process.kill(-run.workerPid, "SIGTERM");
    } catch {
      // Process group may already be gone — ignore
    }
  }

  const updated = await getDetectionRun(runId);
  return NextResponse.json({ run: updated });
}

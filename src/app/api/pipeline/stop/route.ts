import { NextResponse } from "next/server";
import { getActiveDetectionRun, updateDetectionRunStatus } from "@/lib/pipeline-store";

export const runtime = "nodejs";

/**
 * Stop the currently running pipeline detection run.
 */
export async function POST() {
  const activeRun = await getActiveDetectionRun();

  if (!activeRun) {
    return NextResponse.json({ error: "No active run to stop" }, { status: 404 });
  }

  // Kill the worker process
  if (activeRun.workerPid) {
    try {
      process.kill(-activeRun.workerPid, "SIGTERM");
    } catch {
      // Process may already be gone
      try {
        process.kill(activeRun.workerPid, "SIGTERM");
      } catch {
        // ignore
      }
    }
  }

  await updateDetectionRunStatus(activeRun.id, "cancelled", {
    lastError: "Stopped by user",
  });

  return NextResponse.json({ stopped: activeRun.id });
}

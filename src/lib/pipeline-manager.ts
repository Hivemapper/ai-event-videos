/**
 * Server-side pipeline manager — runs detection pipeline continuously
 * independent of browser tabs. Polls DB for run completion and auto-starts next.
 */

import { getDb } from "@/lib/db";
import {
  createDetectionRun,
  getActiveDetectionRun,
  setDetectionRunWorkerPid,
  updateDetectionRunStatus,
} from "@/lib/pipeline-store";
import { spawnDetectionWorker } from "@/lib/detection-worker";
import { AVAILABLE_DETECTION_MODELS } from "@/lib/pipeline-config";

let running = false;
let pollTimer: ReturnType<typeof setInterval> | null = null;

const MODEL_NAME = "gdino-base-clip";
const POLL_INTERVAL = 5000; // 5 seconds

export function isPipelineRunning() {
  return running;
}

export async function startPipeline(): Promise<{ started: boolean; error?: string }> {
  if (running) return { started: true };
  running = true;

  // Try to kick off immediately
  const result = await startNextRun();

  // Start polling for completion
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(async () => {
    if (!running) {
      if (pollTimer) clearInterval(pollTimer);
      pollTimer = null;
      return;
    }

    const activeRun = await getActiveDetectionRun();
    if (!activeRun) {
      // No active run — start next one
      await startNextRun();
    }
  }, POLL_INTERVAL);

  return result;
}

export async function stopPipeline(): Promise<{ stopped: boolean; runId?: string }> {
  running = false;
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }

  // Kill active run if any
  const activeRun = await getActiveDetectionRun();
  if (activeRun) {
    if (activeRun.workerPid) {
      try {
        process.kill(-activeRun.workerPid, "SIGTERM");
      } catch {
        try {
          process.kill(activeRun.workerPid, "SIGTERM");
        } catch {
          // already gone
        }
      }
    }
    await updateDetectionRunStatus(activeRun.id, "cancelled", {
      lastError: "Stopped by user",
    });
    return { stopped: true, runId: activeRun.id };
  }

  return { stopped: true };
}

async function startNextRun(): Promise<{ started: boolean; error?: string }> {
  if (!running) return { started: false, error: "Pipeline stopped" };

  // Check if this machine already has a run going
  const activeRun = await getActiveDetectionRun();
  if (activeRun) return { started: true };

  // Pick multiple candidates to handle races with other machines
  const db = await getDb();
  const result = await db.query(
    `SELECT t.id FROM triage_results t
     WHERE t.triage_result = 'signal'
       AND NOT EXISTS (SELECT 1 FROM detection_runs dr WHERE dr.video_id = t.id)
     ORDER BY RANDOM()
     LIMIT 5`
  );

  if (result.rows.length === 0) {
    running = false;
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    return { started: false, error: "No queued signal events" };
  }

  const modelConfig = AVAILABLE_DETECTION_MODELS.find((m) => m.id === MODEL_NAME);

  // Try each candidate — first one to claim wins
  for (const row of result.rows) {
    const videoId = (row as { id: string }).id;

    const run = await createDetectionRun({
      videoId,
      modelName: MODEL_NAME,
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

    if (!run) continue; // Another machine claimed this one, try next

    try {
      const worker = spawnDetectionWorker({ runId: run.id });
      await setDetectionRunWorkerPid(run.id, worker.pid);
      return { started: true };
    } catch (error) {
      await updateDetectionRunStatus(run.id, "failed", {
        lastError: error instanceof Error ? error.message : "Failed to start worker",
      });
      // Try next candidate
    }
  }

  return { started: false, error: "Could not claim any event" };
}

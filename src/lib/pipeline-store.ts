import { randomUUID } from "crypto";
import type { Row } from "@libsql/client";
import { getDb } from "@/lib/db";
import {
  createEmptyPipelineTotals,
  CURRENT_PIPELINE_VERSION,
  DEFAULT_PIPELINE_MODEL_NAME,
} from "@/lib/pipeline-config";
import type {
  DetectionRun,
  DetectionRunStatus,
  FrameDetection,
  LabelDefinition,
  PipelineRunRecord,
  PipelineRunStatus,
  PipelineRunTotals,
  VideoDetectionSegment,
  VideoPipelineState,
  VideoPipelineStatus,
} from "@/types/pipeline";

function parseJson<T>(value: string | null | undefined, fallback: T): T {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function str(v: unknown): string {
  return v == null ? "" : String(v);
}

function num(v: unknown): number {
  return v == null ? 0 : Number(v);
}

function strOrNull(v: unknown): string | null {
  return v == null ? null : String(v);
}

function numOrNull(v: unknown): number | null {
  return v == null ? null : Number(v);
}

function mapRun(row: Row): PipelineRunRecord {
  return {
    id: str(row.id),
    day: str(row.day),
    batchSize: num(row.batch_size),
    status: str(row.status) as PipelineRunStatus,
    cursorOffset: num(row.cursor_offset),
    pipelineVersion: str(row.pipeline_version),
    modelName: strOrNull(row.model_name),
    totals: {
      ...createEmptyPipelineTotals(),
      ...parseJson<Partial<PipelineRunTotals>>(strOrNull(row.totals_json), {}),
    },
    startedAt: strOrNull(row.started_at),
    completedAt: strOrNull(row.completed_at),
    lastHeartbeatAt: strOrNull(row.last_heartbeat_at),
    lastError: strOrNull(row.last_error),
    workerPid: numOrNull(row.worker_pid),
    createdAt: str(row.created_at),
  };
}

function mapVideoState(row: Row): VideoPipelineState {
  return {
    videoId: str(row.video_id),
    day: str(row.day),
    status: str(row.status) as VideoPipelineStatus,
    pipelineVersion: str(row.pipeline_version),
    modelName: strOrNull(row.model_name),
    labelsApplied: parseJson<string[]>(strOrNull(row.labels_applied), []),
    queuedAt: strOrNull(row.queued_at),
    startedAt: strOrNull(row.started_at),
    completedAt: strOrNull(row.completed_at),
    lastHeartbeatAt: strOrNull(row.last_heartbeat_at),
    lastError: strOrNull(row.last_error),
  };
}

function mapSegment(row: Row): VideoDetectionSegment {
  return {
    id: num(row.id),
    videoId: str(row.video_id),
    label: str(row.label),
    startMs: num(row.start_ms),
    endMs: num(row.end_ms),
    maxConfidence: num(row.max_confidence),
    supportLevel: str(row.support_level) as VideoDetectionSegment["supportLevel"],
    pipelineVersion: str(row.pipeline_version),
    source: str(row.source),
    createdAt: str(row.created_at),
  };
}

function mapFrameDetection(row: Row): FrameDetection {
  return {
    id: num(row.id),
    videoId: str(row.video_id),
    frameMs: num(row.frame_ms),
    label: str(row.label),
    xMin: num(row.x_min),
    yMin: num(row.y_min),
    xMax: num(row.x_max),
    yMax: num(row.y_max),
    confidence: num(row.confidence),
    frameWidth: num(row.frame_width),
    frameHeight: num(row.frame_height),
    pipelineVersion: str(row.pipeline_version),
    modelName: str(row.model_name),
    runId: strOrNull(row.run_id),
    createdAt: str(row.created_at),
  };
}

function mapDetectionRun(row: Row): DetectionRun {
  return {
    id: str(row.id),
    videoId: str(row.video_id),
    modelName: str(row.model_name),
    status: str(row.status) as DetectionRunStatus,
    config: parseJson<Record<string, unknown>>(strOrNull(row.config_json), {}),
    detectionCount: numOrNull(row.detection_count),
    workerPid: numOrNull(row.worker_pid),
    startedAt: strOrNull(row.started_at),
    completedAt: strOrNull(row.completed_at),
    lastHeartbeatAt: strOrNull(row.last_heartbeat_at),
    lastError: strOrNull(row.last_error),
    createdAt: str(row.created_at),
  };
}

export async function listLabels(): Promise<LabelDefinition[]> {
  const db = await getDb();
  const result = await db.execute(
    `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
     FROM labels
     ORDER BY is_system DESC, id ASC`
  );
  return result.rows.map((row) => ({
    id: num(row.id),
    name: str(row.name),
    created_at: str(row.created_at),
    is_system: num(row.is_system),
    support_level: str(row.support_level),
    enabled: num(row.enabled),
    detector_aliases: strOrNull(row.detector_aliases),
  })) as LabelDefinition[];
}

export async function createCustomLabel(name: string): Promise<LabelDefinition> {
  const db = await getDb();
  const insertResult = await db.execute({
    sql: `INSERT INTO labels (name, is_system, support_level, enabled)
          VALUES (?, 0, 'custom', 1)`,
    args: [name],
  });
  const result = await db.execute({
    sql: `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
          FROM labels WHERE id = ?`,
    args: [Number(insertResult.lastInsertRowid)],
  });
  return {
    id: num(result.rows[0].id),
    name: str(result.rows[0].name),
    created_at: str(result.rows[0].created_at),
    is_system: num(result.rows[0].is_system),
    support_level: str(result.rows[0].support_level),
    enabled: num(result.rows[0].enabled),
    detector_aliases: strOrNull(result.rows[0].detector_aliases),
  } as LabelDefinition;
}

export async function deleteCustomLabel(id: number): Promise<boolean> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT is_system FROM labels WHERE id = ?",
    args: [id],
  });

  if (result.rows.length === 0) return false;
  if (num(result.rows[0].is_system)) {
    throw new Error("System labels cannot be removed");
  }

  await db.execute({ sql: "DELETE FROM labels WHERE id = ?", args: [id] });
  return true;
}

export async function getActivePipelineRun(): Promise<PipelineRunRecord | null> {
  const db = await getDb();
  const result = await db.execute(
    `SELECT * FROM pipeline_runs
     WHERE status IN ('queued', 'running', 'paused')
     ORDER BY created_at DESC
     LIMIT 1`
  );
  return result.rows.length > 0 ? mapRun(result.rows[0]) : null;
}

export async function listPipelineRuns(day?: string): Promise<PipelineRunRecord[]> {
  const db = await getDb();
  const result = day
    ? await db.execute({
        sql: `SELECT * FROM pipeline_runs WHERE day = ? ORDER BY created_at DESC LIMIT 20`,
        args: [day],
      })
    : await db.execute(
        `SELECT * FROM pipeline_runs ORDER BY created_at DESC LIMIT 20`
      );
  return result.rows.map(mapRun);
}

export async function getPipelineRun(id: string): Promise<PipelineRunRecord | null> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT * FROM pipeline_runs WHERE id = ?",
    args: [id],
  });
  return result.rows.length > 0 ? mapRun(result.rows[0]) : null;
}

export async function createPipelineRun(params: {
  day: string;
  batchSize: number;
  beeMapsKey: string;
  modelName?: string | null;
}): Promise<PipelineRunRecord> {
  const db = await getDb();
  const run = {
    id: randomUUID(),
    day: params.day,
    batchSize: params.batchSize,
    status: "queued" as PipelineRunStatus,
    totals: createEmptyPipelineTotals(),
    pipelineVersion: CURRENT_PIPELINE_VERSION,
    modelName: params.modelName ?? DEFAULT_PIPELINE_MODEL_NAME,
  };

  await db.execute({
    sql: `INSERT INTO pipeline_runs (
      id, day, batch_size, status, cursor_offset, totals_json,
      pipeline_version, model_name, bee_maps_key
    ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)`,
    args: [
      run.id,
      run.day,
      run.batchSize,
      run.status,
      JSON.stringify(run.totals),
      run.pipelineVersion,
      run.modelName,
      params.beeMapsKey,
    ],
  });

  return (await getPipelineRun(run.id))!;
}

export async function updatePipelineRunStatus(id: string, status: PipelineRunStatus) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE pipeline_runs
          SET status = ?, completed_at = CASE WHEN ? IN ('completed', 'failed', 'cancelled') THEN datetime('now') ELSE completed_at END
          WHERE id = ?`,
    args: [status, status, id],
  });
}

export async function setPipelineRunWorkerPid(id: string, pid: number | null) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE pipeline_runs
          SET worker_pid = ?, last_heartbeat_at = datetime('now')
          WHERE id = ?`,
    args: [pid, id],
  });
}

export function isRunHeartbeatStale(run: PipelineRunRecord, staleSeconds = 120): boolean {
  if (!run.lastHeartbeatAt) return true;
  const ageMs = Date.now() - new Date(run.lastHeartbeatAt).getTime();
  return ageMs > staleSeconds * 1000;
}

export async function createRetryRunFrom(sourceRunId: string): Promise<PipelineRunRecord> {
  const db = await getDb();
  const result = await db.execute({
    sql: `SELECT day, batch_size, bee_maps_key, model_name
          FROM pipeline_runs WHERE id = ?`,
    args: [sourceRunId],
  });

  if (result.rows.length === 0 || !result.rows[0].bee_maps_key) {
    throw new Error("Retry source run is missing Bee Maps credentials");
  }

  const source = result.rows[0];
  return createPipelineRun({
    day: str(source.day),
    batchSize: num(source.batch_size),
    beeMapsKey: str(source.bee_maps_key),
    modelName: strOrNull(source.model_name) ?? DEFAULT_PIPELINE_MODEL_NAME,
  });
}

export async function getPipelineRunBeeMapsKey(runId: string): Promise<string | null> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT bee_maps_key FROM pipeline_runs WHERE id = ?",
    args: [runId],
  });
  return result.rows.length > 0 ? strOrNull(result.rows[0].bee_maps_key) : null;
}

export async function listVideoPipelineStatesForDay(day: string): Promise<VideoPipelineState[]> {
  const db = await getDb();
  const result = await db.execute({
    sql: `SELECT * FROM video_pipeline_state
          WHERE day = ?
          ORDER BY COALESCE(completed_at, started_at, queued_at) DESC`,
    args: [day],
  });
  return result.rows.map(mapVideoState);
}

export async function getVideoPipelineState(videoId: string): Promise<VideoPipelineState | null> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT * FROM video_pipeline_state WHERE video_id = ?",
    args: [videoId],
  });
  return result.rows.length > 0 ? mapVideoState(result.rows[0]) : null;
}

export async function getVideoDetectionSegments(videoId: string): Promise<VideoDetectionSegment[]> {
  const db = await getDb();
  const stateResult = await db.execute({
    sql: `SELECT pipeline_version FROM video_pipeline_state WHERE video_id = ?`,
    args: [videoId],
  });

  let result;
  if (stateResult.rows.length > 0) {
    result = await db.execute({
      sql: `SELECT * FROM video_detection_segments
            WHERE video_id = ? AND pipeline_version = ?
            ORDER BY start_ms ASC`,
      args: [videoId, str(stateResult.rows[0].pipeline_version)],
    });
  } else {
    result = await db.execute({
      sql: `SELECT * FROM video_detection_segments
            WHERE video_id = ?
            ORDER BY start_ms ASC`,
      args: [videoId],
    });
  }

  return result.rows.map(mapSegment);
}

export function summarizeVideoStates(states: VideoPipelineState[]) {
  return states.reduce(
    (summary, state) => {
      summary[state.status] = (summary[state.status] ?? 0) + 1;
      return summary;
    },
    {
      unprocessed: 0,
      queued: 0,
      running: 0,
      processed: 0,
      failed: 0,
      stale: 0,
    } as Record<VideoPipelineStatus, number>
  );
}

export async function getFrameDetections(
  videoId: string,
  frameMs?: number,
  modelName?: string
): Promise<FrameDetection[]> {
  const db = await getDb();
  let result;
  if (frameMs !== undefined && modelName !== undefined) {
    result = await db.execute({
      sql: `SELECT * FROM frame_detections
            WHERE video_id = ? AND frame_ms = ? AND model_name = ?
            ORDER BY confidence DESC`,
      args: [videoId, frameMs, modelName],
    });
  } else if (frameMs !== undefined) {
    result = await db.execute({
      sql: `SELECT * FROM frame_detections
            WHERE video_id = ? AND frame_ms = ?
            ORDER BY confidence DESC`,
      args: [videoId, frameMs],
    });
  } else if (modelName !== undefined) {
    result = await db.execute({
      sql: `SELECT * FROM frame_detections
            WHERE video_id = ? AND model_name = ?
            ORDER BY frame_ms ASC, confidence DESC`,
      args: [videoId, modelName],
    });
  } else {
    result = await db.execute({
      sql: `SELECT * FROM frame_detections
            WHERE video_id = ?
            ORDER BY frame_ms ASC, confidence DESC`,
      args: [videoId],
    });
  }
  return result.rows.map(mapFrameDetection);
}

export async function getFrameDetectionTimestamps(
  videoId: string,
  modelName?: string
): Promise<number[]> {
  const db = await getDb();
  const result = modelName !== undefined
    ? await db.execute({
        sql: `SELECT DISTINCT frame_ms FROM frame_detections
              WHERE video_id = ? AND model_name = ?
              ORDER BY frame_ms ASC`,
        args: [videoId, modelName],
      })
    : await db.execute({
        sql: `SELECT DISTINCT frame_ms FROM frame_detections
              WHERE video_id = ?
              ORDER BY frame_ms ASC`,
        args: [videoId],
      });
  return result.rows.map((r) => num(r.frame_ms));
}

export async function getFrameDetectionModels(videoId: string): Promise<string[]> {
  const db = await getDb();
  const result = await db.execute({
    sql: `SELECT DISTINCT model_name FROM frame_detections WHERE video_id = ?`,
    args: [videoId],
  });
  return result.rows.map((r) => str(r.model_name));
}

// --- Detection Runs ---
// Global lock: only one detection run at a time (single GPU on local machine).
// The atomic INSERT...WHERE NOT EXISTS prevents races.

export async function createDetectionRun(params: {
  videoId: string;
  modelName: string;
  config?: Record<string, unknown>;
}): Promise<DetectionRun | null> {
  const db = await getDb();
  const id = randomUUID();
  const result = await db.execute({
    sql: `INSERT INTO detection_runs (id, video_id, model_name, status, config_json, created_at)
          SELECT ?, ?, ?, 'queued', ?, datetime('now')
          WHERE NOT EXISTS (
            SELECT 1 FROM detection_runs WHERE status IN ('queued', 'running')
          )`,
    args: [id, params.videoId, params.modelName, JSON.stringify(params.config ?? {})],
  });
  if (result.rowsAffected === 0) return null;
  return (await getDetectionRun(id))!;
}

export async function getDetectionRun(id: string): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT * FROM detection_runs WHERE id = ?",
    args: [id],
  });
  return result.rows.length > 0 ? mapDetectionRun(result.rows[0]) : null;
}

export async function listDetectionRuns(videoId: string): Promise<DetectionRun[]> {
  const db = await getDb();
  const result = await db.execute({
    sql: `SELECT * FROM detection_runs WHERE video_id = ? ORDER BY created_at DESC`,
    args: [videoId],
  });
  return result.rows.map(mapDetectionRun);
}

export async function getActiveDetectionRun(): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.execute(
    `SELECT * FROM detection_runs WHERE status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1`
  );
  return result.rows.length > 0 ? mapDetectionRun(result.rows[0]) : null;
}

export async function updateDetectionRunStatus(
  id: string,
  status: DetectionRunStatus,
  extra?: { detectionCount?: number; lastError?: string }
) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE detection_runs
          SET status = ?,
              detection_count = COALESCE(?, detection_count),
              last_error = COALESCE(?, last_error),
              started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN datetime('now') ELSE started_at END,
              completed_at = CASE WHEN ? IN ('completed', 'failed') THEN datetime('now') ELSE completed_at END
          WHERE id = ?`,
    args: [
      status,
      extra?.detectionCount ?? null,
      extra?.lastError ?? null,
      status,
      status,
      id,
    ],
  });
}

export async function setDetectionRunWorkerPid(
  id: string,
  pid: number | null
) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE detection_runs SET worker_pid = ? WHERE id = ?`,
    args: [pid, id],
  });
}

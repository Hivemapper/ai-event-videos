import { randomUUID } from "crypto";
import { execSync } from "child_process";
import os from "os";
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
  ProductionRun,
  ProductionRunStatus,
  ProductionStepStatus,
  VideoDetectionSegment,
  VideoPipelineState,
  VideoPipelineStatus,
} from "@/types/pipeline";

function getMachineId(): string {
  try {
    return execSync("scutil --get ComputerName", { timeout: 2000 })
      .toString()
      .trim();
  } catch {
    return os.hostname().split(".")[0] || "unknown";
  }
}
const MACHINE_ID = getMachineId();
export const PRODUCTION_PRIORITY_MANUAL_VRU = 0;
export const PRODUCTION_PRIORITY_DEFAULT = 100;

export function isCurrentMachineId(machineId: string | null | undefined): boolean {
  return Boolean(machineId && machineId === MACHINE_ID);
}

interface DbPipelineRunRow {
  id: string;
  day: string;
  batch_size: number;
  status: PipelineRunStatus;
  cursor_offset: number;
  totals_json: string;
  pipeline_version: string;
  model_name: string | null;
  worker_pid: number | null;
  started_at: string | null;
  completed_at: string | null;
  last_heartbeat_at: string | null;
  last_error: string | null;
  created_at: string;
}

interface DbVideoStateRow {
  video_id: string;
  day: string;
  status: VideoPipelineStatus;
  pipeline_version: string;
  model_name: string | null;
  labels_applied: string;
  queued_at: string | null;
  started_at: string | null;
  completed_at: string | null;
  last_heartbeat_at: string | null;
  last_error: string | null;
}

interface DbSegmentRow {
  id: number;
  video_id: string;
  label: string;
  start_ms: number;
  end_ms: number;
  max_confidence: number;
  support_level: VideoDetectionSegment["supportLevel"];
  pipeline_version: string;
  source: string;
  created_at: string;
}

interface DbFrameDetectionRow {
  id: number;
  video_id: string;
  frame_ms: number;
  label: string;
  x_min: number;
  y_min: number;
  x_max: number;
  y_max: number;
  confidence: number;
  frame_width: number;
  frame_height: number;
  pipeline_version: string;
  model_name: string;
  run_id: string | null;
  created_at: string;
}

interface DbDetectionRunRow {
  id: string;
  video_id: string;
  model_name: string;
  status: DetectionRunStatus;
  config_json: string;
  priority: number | null;
  detection_count: number | null;
  worker_pid: number | null;
  started_at: string | null;
  completed_at: string | null;
  last_heartbeat_at: string | null;
  last_error: string | null;
  machine_id: string | null;
  created_at: string;
}

function parseJson<T>(value: string | null | undefined, fallback: T): T {
  if (!value) return fallback;
  try {
    return JSON.parse(value) as T;
  } catch {
    return fallback;
  }
}

function mapRun(row: DbPipelineRunRow): PipelineRunRecord {
  return {
    id: row.id,
    day: row.day,
    batchSize: row.batch_size,
    status: row.status,
    cursorOffset: row.cursor_offset,
    pipelineVersion: row.pipeline_version,
    modelName: row.model_name,
    totals: {
      ...createEmptyPipelineTotals(),
      ...parseJson<Partial<PipelineRunTotals>>(row.totals_json, {}),
    },
    startedAt: row.started_at,
    completedAt: row.completed_at,
    lastHeartbeatAt: row.last_heartbeat_at,
    lastError: row.last_error,
    workerPid: row.worker_pid,
    createdAt: row.created_at,
  };
}

function mapVideoState(row: DbVideoStateRow): VideoPipelineState {
  return {
    videoId: row.video_id,
    day: row.day,
    status: row.status,
    pipelineVersion: row.pipeline_version,
    modelName: row.model_name,
    labelsApplied: parseJson<string[]>(row.labels_applied, []),
    queuedAt: row.queued_at,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    lastHeartbeatAt: row.last_heartbeat_at,
    lastError: row.last_error,
  };
}

function mapSegment(row: DbSegmentRow): VideoDetectionSegment {
  return {
    id: row.id,
    videoId: row.video_id,
    label: row.label,
    startMs: row.start_ms,
    endMs: row.end_ms,
    maxConfidence: row.max_confidence,
    supportLevel: row.support_level,
    pipelineVersion: row.pipeline_version,
    source: row.source,
    createdAt: row.created_at,
  };
}

function mapFrameDetection(row: DbFrameDetectionRow): FrameDetection {
  return {
    id: row.id,
    videoId: row.video_id,
    frameMs: row.frame_ms,
    label: row.label,
    xMin: row.x_min,
    yMin: row.y_min,
    xMax: row.x_max,
    yMax: row.y_max,
    confidence: row.confidence,
    frameWidth: row.frame_width,
    frameHeight: row.frame_height,
    pipelineVersion: row.pipeline_version,
    modelName: row.model_name,
    runId: row.run_id,
    createdAt: row.created_at,
  };
}

function mapDetectionRun(row: DbDetectionRunRow): DetectionRun {
  return {
    id: row.id,
    videoId: row.video_id,
    modelName: row.model_name,
    status: row.status,
    priority: row.priority ?? DETECTION_PRIORITY_DEFAULT,
    config: parseJson<Record<string, unknown>>(row.config_json, {}),
    detectionCount: row.detection_count,
    workerPid: row.worker_pid,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    lastHeartbeatAt: row.last_heartbeat_at,
    lastError: row.last_error,
    machineId: row.machine_id,
    createdAt: row.created_at,
  };
}

// ---------------------------------------------------------------------------
// Labels
// ---------------------------------------------------------------------------

export async function listLabels(): Promise<LabelDefinition[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
     FROM labels
     ORDER BY is_system DESC, id ASC`
  );
  return result.rows as unknown as LabelDefinition[];
}

export async function createCustomLabel(
  name: string
): Promise<LabelDefinition> {
  const db = await getDb();
  const insertResult = await db.run(
    `INSERT INTO labels (name, is_system, support_level, enabled)
     VALUES (?, 0, 'custom', 1)`,
    [name]
  );
  const result = await db.query(
    `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
     FROM labels WHERE id = ?`,
    [insertResult.lastInsertRowid]
  );
  return result.rows[0] as unknown as LabelDefinition;
}

export async function deleteCustomLabel(id: number): Promise<boolean> {
  const db = await getDb();
  const result = await db.query(
    "SELECT is_system FROM labels WHERE id = ?",
    [id]
  );
  const label = result.rows[0] as { is_system: number } | undefined;

  if (!label) {
    return false;
  }

  if (label.is_system) {
    throw new Error("System labels cannot be removed");
  }

  await db.run("DELETE FROM labels WHERE id = ?", [id]);
  return true;
}

// ---------------------------------------------------------------------------
// Pipeline Runs
// ---------------------------------------------------------------------------

export async function getActivePipelineRun(): Promise<PipelineRunRecord | null> {
  const db = await getDb();
  const result = await db.query(
    `SELECT * FROM pipeline_runs
     WHERE status IN ('queued', 'running', 'paused')
     ORDER BY created_at DESC
     LIMIT 1`
  );
  const row = result.rows[0] as unknown as DbPipelineRunRow | undefined;
  return row ? mapRun(row) : null;
}

export async function listPipelineRuns(
  day?: string
): Promise<PipelineRunRecord[]> {
  const db = await getDb();
  const result = day
    ? await db.query(
        `SELECT * FROM pipeline_runs
         WHERE day = ?
         ORDER BY created_at DESC
         LIMIT 20`,
        [day]
      )
    : await db.query(
        `SELECT * FROM pipeline_runs
         ORDER BY created_at DESC
         LIMIT 20`
      );
  return (result.rows as unknown as DbPipelineRunRow[]).map(mapRun);
}

export async function getPipelineRun(
  id: string
): Promise<PipelineRunRecord | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT * FROM pipeline_runs WHERE id = ?",
    [id]
  );
  const row = result.rows[0] as unknown as DbPipelineRunRow | undefined;
  return row ? mapRun(row) : null;
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

  await db.run(
    `INSERT INTO pipeline_runs (
      id, day, batch_size, status, cursor_offset, totals_json,
      pipeline_version, model_name, bee_maps_key
    ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)`,
    [
      run.id,
      run.day,
      run.batchSize,
      run.status,
      JSON.stringify(run.totals),
      run.pipelineVersion,
      run.modelName,
      params.beeMapsKey,
    ]
  );

  return (await getPipelineRun(run.id))!;
}

export async function updatePipelineRunStatus(
  id: string,
  status: PipelineRunStatus
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE pipeline_runs
     SET status = ?, completed_at = CASE WHEN ? IN ('completed', 'failed', 'cancelled') THEN datetime('now') ELSE completed_at END
     WHERE id = ?`,
    [status, status, id]
  );
}

export async function setPipelineRunWorkerPid(
  id: string,
  pid: number | null
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE pipeline_runs
     SET worker_pid = ?, last_heartbeat_at = datetime('now')
     WHERE id = ?`,
    [pid, id]
  );
}

export function isRunHeartbeatStale(
  run: PipelineRunRecord,
  staleSeconds = 120
): boolean {
  if (!run.lastHeartbeatAt) return true;
  const ageMs = Date.now() - new Date(run.lastHeartbeatAt).getTime();
  return ageMs > staleSeconds * 1000;
}

export async function createRetryRunFrom(
  sourceRunId: string
): Promise<PipelineRunRecord> {
  const db = await getDb();
  const result = await db.query(
    `SELECT day, batch_size, bee_maps_key, model_name
     FROM pipeline_runs
     WHERE id = ?`,
    [sourceRunId]
  );
  const source = result.rows[0] as
    | {
        day: string;
        batch_size: number;
        bee_maps_key: string | null;
        model_name: string | null;
      }
    | undefined;

  if (!source || !source.bee_maps_key) {
    throw new Error("Retry source run is missing Bee Maps credentials");
  }

  return createPipelineRun({
    day: source.day,
    batchSize: source.batch_size,
    beeMapsKey: source.bee_maps_key,
    modelName: source.model_name ?? DEFAULT_PIPELINE_MODEL_NAME,
  });
}

export async function getPipelineRunBeeMapsKey(
  runId: string
): Promise<string | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT bee_maps_key FROM pipeline_runs WHERE id = ?",
    [runId]
  );
  const row = result.rows[0] as { bee_maps_key: string | null } | undefined;
  return row?.bee_maps_key ?? null;
}

// ---------------------------------------------------------------------------
// Video Pipeline States
// ---------------------------------------------------------------------------

export async function listVideoPipelineStatesForDay(
  day: string
): Promise<VideoPipelineState[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT * FROM video_pipeline_state
     WHERE day = ?
     ORDER BY COALESCE(completed_at, started_at, queued_at) DESC`,
    [day]
  );
  return (result.rows as unknown as DbVideoStateRow[]).map(mapVideoState);
}

export async function getVideoPipelineState(
  videoId: string
): Promise<VideoPipelineState | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT * FROM video_pipeline_state WHERE video_id = ?",
    [videoId]
  );
  const row = result.rows[0] as unknown as DbVideoStateRow | undefined;
  return row ? mapVideoState(row) : null;
}

export async function getVideoDetectionSegments(
  videoId: string
): Promise<VideoDetectionSegment[]> {
  const db = await getDb();
  const stateResult = await db.query(
    `SELECT pipeline_version
     FROM video_pipeline_state
     WHERE video_id = ?`,
    [videoId]
  );
  const stateRow = stateResult.rows[0] as
    | { pipeline_version: string }
    | undefined;

  const segmentResult = stateRow
    ? await db.query(
        `SELECT * FROM video_detection_segments
         WHERE video_id = ? AND pipeline_version = ?
         ORDER BY start_ms ASC`,
        [videoId, stateRow.pipeline_version]
      )
    : await db.query(
        `SELECT * FROM video_detection_segments
         WHERE video_id = ?
         ORDER BY start_ms ASC`,
        [videoId]
      );

  return (segmentResult.rows as unknown as DbSegmentRow[]).map(mapSegment);
}

export async function getSceneAttributesByRunId(
  videoId: string,
  runId: string
): Promise<Record<string, { value: string; confidence: number | null }>> {
  const db = await getDb();
  const result = await db.query(
    `SELECT attribute, value, confidence FROM scene_attributes
     WHERE video_id = ? AND run_id = ?`,
    [videoId, runId]
  );
  const attrs: Record<string, { value: string; confidence: number | null }> = {};
  for (const row of result.rows as Array<{ attribute: string; value: string; confidence: number | null }>) {
    attrs[row.attribute] = { value: row.value, confidence: row.confidence };
  }
  return attrs;
}

export async function getTimelineByRunId(
  videoId: string,
  runId: string
): Promise<Array<{ startSec: number; endSec: number; event: string; details: string }> | null> {
  const db = await getDb();
  const result = await db.query(
    `SELECT timeline_json FROM clip_timelines
     WHERE video_id = ? AND run_id = ?
     ORDER BY created_at DESC LIMIT 1`,
    [videoId, runId]
  );
  const row = result.rows[0] as { timeline_json: string } | undefined;
  if (!row) return null;
  try {
    return JSON.parse(row.timeline_json);
  } catch {
    return null;
  }
}

export async function getDetectionSegmentsByRunId(
  videoId: string,
  runId: string
): Promise<VideoDetectionSegment[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT * FROM video_detection_segments
     WHERE video_id = ? AND run_id = ?
     ORDER BY label ASC, start_ms ASC`,
    [videoId, runId]
  );
  return (result.rows as unknown as DbSegmentRow[]).map(mapSegment);
}

export async function getVideoDetectionBoxes(videoId: string): Promise<import("@/types/pipeline").DetectionBox[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT id, video_id, timestamp_ms, label, confidence, x1, y1, x2, y2, pipeline_version
     FROM video_detection_boxes
     WHERE video_id = ?
     ORDER BY timestamp_ms ASC, id ASC`,
    [videoId]
  );
  const rows = result.rows as Array<{
    id: number;
    video_id: string;
    timestamp_ms: number;
    label: string;
    confidence: number;
    x1: number;
    y1: number;
    x2: number;
    y2: number;
    pipeline_version: string;
  }>;
  return rows.map((row) => ({
    id: row.id,
    videoId: row.video_id,
    timestampMs: row.timestamp_ms,
    label: row.label,
    confidence: row.confidence,
    x1: row.x1,
    y1: row.y1,
    x2: row.x2,
    y2: row.y2,
    pipelineVersion: row.pipeline_version,
  }));
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

// ---------------------------------------------------------------------------
// Frame Detections
// ---------------------------------------------------------------------------

export async function getFrameDetections(
  videoId: string,
  frameMs?: number,
  modelName?: string,
  runId?: string
): Promise<FrameDetection[]> {
  const db = await getDb();

  // When runId is provided, filter by run_id instead of model_name
  if (runId !== undefined) {
    if (frameMs !== undefined) {
      const result = await db.query(
        `SELECT * FROM frame_detections
         WHERE video_id = ? AND frame_ms = ? AND run_id = ?
         ORDER BY confidence DESC`,
        [videoId, frameMs, runId]
      );
      return (result.rows as unknown as DbFrameDetectionRow[]).map(
        mapFrameDetection
      );
    }
    const result = await db.query(
      `SELECT * FROM frame_detections
       WHERE video_id = ? AND run_id = ?
       ORDER BY frame_ms ASC, confidence DESC`,
      [videoId, runId]
    );
    return (result.rows as unknown as DbFrameDetectionRow[]).map(
      mapFrameDetection
    );
  }

  let result;
  if (frameMs !== undefined && modelName !== undefined) {
    result = await db.query(
      `SELECT * FROM frame_detections
       WHERE video_id = ? AND frame_ms = ? AND model_name = ?
       ORDER BY confidence DESC`,
      [videoId, frameMs, modelName]
    );
  } else if (frameMs !== undefined) {
    result = await db.query(
      `SELECT * FROM frame_detections
       WHERE video_id = ? AND frame_ms = ?
       ORDER BY confidence DESC`,
      [videoId, frameMs]
    );
  } else if (modelName !== undefined) {
    result = await db.query(
      `SELECT * FROM frame_detections
       WHERE video_id = ? AND model_name = ?
       ORDER BY frame_ms ASC, confidence DESC`,
      [videoId, modelName]
    );
  } else {
    result = await db.query(
      `SELECT * FROM frame_detections
       WHERE video_id = ?
       ORDER BY frame_ms ASC, confidence DESC`,
      [videoId]
    );
  }
  return (result.rows as unknown as DbFrameDetectionRow[]).map(
    mapFrameDetection
  );
}

export async function getFrameDetectionTimestamps(
  videoId: string,
  modelName?: string,
  runId?: string
): Promise<number[]> {
  const db = await getDb();

  // When runId is provided, filter by run_id instead of model_name
  if (runId !== undefined) {
    const result = await db.query(
      `SELECT DISTINCT frame_ms FROM frame_detections
       WHERE video_id = ? AND run_id = ?
       ORDER BY frame_ms ASC`,
      [videoId, runId]
    );
    return (result.rows as Array<{ frame_ms: number }>).map((r) => r.frame_ms);
  }

  const result =
    modelName !== undefined
      ? await db.query(
          `SELECT DISTINCT frame_ms FROM frame_detections
           WHERE video_id = ? AND model_name = ?
           ORDER BY frame_ms ASC`,
          [videoId, modelName]
        )
      : await db.query(
          `SELECT DISTINCT frame_ms FROM frame_detections
           WHERE video_id = ?
           ORDER BY frame_ms ASC`,
          [videoId]
        );
  return (result.rows as Array<{ frame_ms: number }>).map((r) => r.frame_ms);
}

export async function getFrameDetectionModels(
  videoId: string
): Promise<string[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT DISTINCT model_name FROM frame_detections WHERE video_id = ?`,
    [videoId]
  );
  return (result.rows as Array<{ model_name: string }>).map(
    (r) => r.model_name
  );
}

// ---------------------------------------------------------------------------
// Detection Runs
// ---------------------------------------------------------------------------

export const DETECTION_PRIORITY_MANUAL = 0;
export const DETECTION_PRIORITY_DEFAULT = 100;

export async function createDetectionRun(params: {
  videoId: string;
  modelName: string;
  config?: Record<string, unknown>;
  machineId?: string | null;
  priority?: number;
}): Promise<DetectionRun | null> {
  const db = await getDb();
  const id = randomUUID();
  const machineId =
    params.machineId === undefined ? MACHINE_ID : params.machineId;
  const result = await db.run(
    `INSERT INTO detection_runs (id, video_id, model_name, status, config_json, machine_id, priority, created_at)
     SELECT ?, ?, ?, 'queued', ?, ?, ?, datetime('now')
     WHERE NOT EXISTS (
       SELECT 1 FROM detection_runs WHERE video_id = ? AND status IN ('queued', 'running')
     )`,
    [
      id,
      params.videoId,
      params.modelName,
      JSON.stringify(params.config ?? {}),
      machineId,
      params.priority ?? DETECTION_PRIORITY_DEFAULT,
      params.videoId,
    ]
  );
  if (result.changes === 0) return null;
  return (await getDetectionRun(id))!;
}

export async function getDetectionRun(
  id: string
): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT * FROM detection_runs WHERE id = ?",
    [id]
  );
  const row = result.rows[0] as unknown as DbDetectionRunRow | undefined;
  return row ? mapDetectionRun(row) : null;
}

export async function listDetectionRuns(
  videoId: string
): Promise<DetectionRun[]> {
  const db = await getDb();
  const result = await db.query(
    `SELECT * FROM detection_runs WHERE video_id = ? ORDER BY created_at DESC`,
    [videoId]
  );
  return (result.rows as unknown as DbDetectionRunRow[]).map(mapDetectionRun);
}

export async function listCompletedDetectionRuns(
  limit = 50,
  offset = 0
): Promise<{ runs: DetectionRun[]; total: number }> {
  const db = await getDb();
  const countResult = await db.query(
    "SELECT COUNT(*) as count FROM detection_runs WHERE status = 'completed'"
  );
  const total = (countResult.rows[0] as unknown as { count: number }).count;
  const result = await db.query(
    `SELECT * FROM detection_runs WHERE status = 'completed'
     ORDER BY completed_at DESC LIMIT ? OFFSET ?`,
    [limit, offset]
  );
  const runs = (result.rows as unknown as DbDetectionRunRow[]).map(mapDetectionRun);
  return { runs, total };
}

export async function getActiveDetectionRun(): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.query(
    `SELECT * FROM detection_runs
     WHERE status IN ('queued', 'running')
     ORDER BY priority ASC, created_at ASC
     LIMIT 1`
  );
  const row = result.rows[0] as unknown as DbDetectionRunRow | undefined;
  return row ? mapDetectionRun(row) : null;
}

export async function updateDetectionRunStatus(
  id: string,
  status: DetectionRunStatus,
  extra?: { detectionCount?: number; lastError?: string }
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE detection_runs
     SET status = ?,
         detection_count = COALESCE(?, detection_count),
         last_error = COALESCE(?, last_error),
         started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN datetime('now') ELSE started_at END,
         completed_at = CASE WHEN ? IN ('completed', 'failed', 'cancelled') THEN datetime('now') ELSE completed_at END
     WHERE id = ?`,
    [
      status,
      extra?.detectionCount ?? null,
      extra?.lastError ?? null,
      status,
      status,
      id,
    ]
  );
}

export async function setDetectionRunWorkerPid(
  id: string,
  pid: number | null
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE detection_runs SET worker_pid = ? WHERE id = ?`,
    [pid, id]
  );
}

export async function deleteVideoDetectionSegment(
  videoId: string,
  label: string,
  startMs: number,
  endMs: number,
  runId?: string
): Promise<number> {
  const db = await getDb();
  const params: unknown[] = [videoId, label, startMs, endMs];
  let runClause = "";
  if (runId) {
    runClause = " AND run_id = ?";
    params.push(runId);
  }
  await db.run(
    `DELETE FROM frame_detections WHERE video_id = ? AND label = ? AND frame_ms >= ? AND frame_ms <= ?${runClause}`,
    params
  );
  const result = await db.run(
    `DELETE FROM video_detection_segments WHERE video_id = ? AND label = ? AND start_ms = ? AND end_ms = ?${runClause}`,
    [videoId, label, startMs, endMs, ...(runId ? [runId] : [])]
  );
  return result.changes ?? 0;
}

// ---------------------------------------------------------------------------
// Production Runs
// ---------------------------------------------------------------------------

interface DbProductionRunRow {
  id: string;
  video_id: string;
  status: ProductionRunStatus;
  privacy_status: ProductionStepStatus;
  metadata_status: ProductionStepStatus;
  upload_status: ProductionStepStatus;
  priority: number | null;
  s3_video_key: string | null;
  s3_metadata_key: string | null;
  worker_pid: number | null;
  machine_id: string | null;
  started_at: string | null;
  completed_at: string | null;
  last_heartbeat_at: string | null;
  last_error: string | null;
  created_at: string;
}

function mapProductionRun(row: DbProductionRunRow): ProductionRun {
  return {
    id: row.id,
    videoId: row.video_id,
    status: row.status,
    privacyStatus: row.privacy_status,
    metadataStatus: row.metadata_status,
    uploadStatus: row.upload_status,
    priority: row.priority ?? PRODUCTION_PRIORITY_DEFAULT,
    s3VideoKey: row.s3_video_key,
    s3MetadataKey: row.s3_metadata_key,
    workerPid: row.worker_pid,
    machineId: row.machine_id,
    startedAt: row.started_at,
    completedAt: row.completed_at,
    lastHeartbeatAt: row.last_heartbeat_at,
    lastError: row.last_error,
    createdAt: row.created_at,
  };
}

export async function createProductionRun(
  videoId: string,
  priority = PRODUCTION_PRIORITY_DEFAULT
): Promise<ProductionRun | null> {
  const db = await getDb();
  const id = randomUUID();
  const result = await db.run(
    `INSERT OR IGNORE INTO production_runs (id, video_id, priority, created_at)
     VALUES (?, ?, ?, datetime('now'))`,
    [id, videoId, priority]
  );
  if (result.changes === 0) return null;
  return (await getProductionRun(id))!;
}

export async function enqueueCompletedVruForProduction(
  videoId: string
): Promise<{
  run: ProductionRun;
  created: boolean;
  prioritized: boolean;
  requeued: boolean;
}> {
  const db = await getDb();

  const completedRun = await db.query(
    `SELECT id FROM detection_runs
     WHERE video_id = ? AND status = 'completed'
     ORDER BY COALESCE(completed_at, created_at) DESC
     LIMIT 1`,
    [videoId]
  );

  if (completedRun.rows.length === 0) {
    throw new Error("Video does not have a completed VRU detection run");
  }

  const existing = await getProductionRunByVideoId(videoId);
  if (!existing) {
    const created = await createProductionRun(
      videoId,
      PRODUCTION_PRIORITY_MANUAL_VRU
    );
    if (!created) {
      const raced = await getProductionRunByVideoId(videoId);
      if (!raced) throw new Error("Failed to enqueue production run");
      return { run: raced, created: false, prioritized: false, requeued: false };
    }
    return { run: created, created: true, prioritized: true, requeued: false };
  }

  if (existing.status === "completed" || existing.status === "processing") {
    return {
      run: existing,
      created: false,
      prioritized: false,
      requeued: false,
    };
  }

  const requeued = existing.status === "failed";
  const prioritized = existing.priority !== PRODUCTION_PRIORITY_MANUAL_VRU;
  await db.run(
    `UPDATE production_runs
     SET status = 'queued',
         priority = ?,
         privacy_status = CASE WHEN status = 'failed' THEN 'pending' ELSE privacy_status END,
         metadata_status = CASE WHEN status = 'failed' THEN 'pending' ELSE metadata_status END,
         upload_status = CASE WHEN status = 'failed' THEN 'pending' ELSE upload_status END,
         machine_id = CASE WHEN status = 'failed' THEN NULL ELSE machine_id END,
         worker_pid = CASE WHEN status = 'failed' THEN NULL ELSE worker_pid END,
         started_at = CASE WHEN status = 'failed' THEN NULL ELSE started_at END,
         completed_at = CASE WHEN status = 'failed' THEN NULL ELSE completed_at END,
         last_error = CASE WHEN status = 'failed' THEN NULL ELSE last_error END
     WHERE video_id = ? AND status IN ('queued', 'failed')`,
    [PRODUCTION_PRIORITY_MANUAL_VRU, videoId]
  );

  const updated = await getProductionRunByVideoId(videoId);
  if (!updated) throw new Error("Failed to load production run after enqueue");
  return { run: updated, created: false, prioritized, requeued };
}

export async function getProductionRun(
  id: string
): Promise<ProductionRun | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT * FROM production_runs WHERE id = ?",
    [id]
  );
  const row = result.rows[0] as unknown as DbProductionRunRow | undefined;
  return row ? mapProductionRun(row) : null;
}

export async function getProductionRunByVideoId(
  videoId: string
): Promise<ProductionRun | null> {
  const db = await getDb();
  const result = await db.query(
    "SELECT * FROM production_runs WHERE video_id = ?",
    [videoId]
  );
  const row = result.rows[0] as unknown as DbProductionRunRow | undefined;
  return row ? mapProductionRun(row) : null;
}

export async function claimNextProductionRun(
  machineId: string,
  workerPid: number
): Promise<ProductionRun | null> {
  const db = await getDb();

  // Atomic claim: UPDATE with subquery + status guard
  const result = await db.run(
    `UPDATE production_runs
     SET status = 'processing',
         machine_id = ?,
         worker_pid = ?,
         started_at = datetime('now'),
         last_heartbeat_at = datetime('now')
     WHERE id = (
       SELECT id FROM production_runs
       WHERE status = 'queued'
         AND COALESCE(priority, ?) = ?
       ORDER BY priority ASC, created_at ASC
       LIMIT 1
     ) AND status = 'queued'`,
    [
      machineId,
      workerPid,
      PRODUCTION_PRIORITY_DEFAULT,
      PRODUCTION_PRIORITY_MANUAL_VRU,
    ]
  );

  if (result.changes === 0) return null;

  // Fetch the row we just claimed
  const claimed = await db.query(
    `SELECT * FROM production_runs
     WHERE machine_id = ? AND worker_pid = ? AND status = 'processing'
     ORDER BY started_at DESC LIMIT 1`,
    [machineId, workerPid]
  );
  const row = claimed.rows[0] as unknown as DbProductionRunRow | undefined;
  return row ? mapProductionRun(row) : null;
}

export async function updateProductionRunStatus(
  id: string,
  status: ProductionRunStatus,
  extra?: { lastError?: string; s3VideoKey?: string; s3MetadataKey?: string }
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE production_runs
     SET status = ?,
         last_error = COALESCE(?, last_error),
         s3_video_key = COALESCE(?, s3_video_key),
         s3_metadata_key = COALESCE(?, s3_metadata_key),
         started_at = CASE WHEN ? = 'processing' AND started_at IS NULL THEN datetime('now') ELSE started_at END,
         completed_at = CASE WHEN ? IN ('completed', 'failed') THEN datetime('now') ELSE completed_at END
     WHERE id = ?`,
    [
      status,
      extra?.lastError ?? null,
      extra?.s3VideoKey ?? null,
      extra?.s3MetadataKey ?? null,
      status,
      status,
      id,
    ]
  );
}

export async function updateProductionStepStatus(
  id: string,
  step: "privacy" | "metadata" | "upload",
  stepStatus: ProductionStepStatus
): Promise<void> {
  const db = await getDb();
  const column = `${step}_status`;
  await db.run(
    `UPDATE production_runs SET ${column} = ? WHERE id = ?`,
    [stepStatus, id]
  );
}

export async function updateProductionRunHeartbeat(
  id: string
): Promise<void> {
  const db = await getDb();
  await db.run(
    `UPDATE production_runs SET last_heartbeat_at = datetime('now') WHERE id = ?`,
    [id]
  );
}

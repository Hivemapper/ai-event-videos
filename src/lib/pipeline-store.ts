import { randomUUID } from "crypto";
import { getDb } from "@/lib/db";
import {
  createEmptyPipelineTotals,
  CURRENT_PIPELINE_VERSION,
  DEFAULT_PIPELINE_MODEL_NAME,
} from "@/lib/pipeline-config";
import {
  LabelDefinition,
  PipelineRunRecord,
  PipelineRunStatus,
  PipelineRunTotals,
  VideoDetectionSegment,
  VideoPipelineState,
  VideoPipelineStatus,
} from "@/types/pipeline";

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

export function listLabels(): LabelDefinition[] {
  const db = getDb();
  return db
    .prepare(
      `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
       FROM labels
       ORDER BY is_system DESC, id ASC`
    )
    .all() as LabelDefinition[];
}

export function createCustomLabel(name: string): LabelDefinition {
  const db = getDb();
  const result = db
    .prepare(
      `INSERT INTO labels (name, is_system, support_level, enabled)
       VALUES (?, 0, 'custom', 1)`
    )
    .run(name);
  return db
    .prepare(
      `SELECT id, name, created_at, is_system, support_level, enabled, detector_aliases
       FROM labels WHERE id = ?`
    )
    .get(result.lastInsertRowid) as LabelDefinition;
}

export function deleteCustomLabel(id: number): boolean {
  const db = getDb();
  const label = db
    .prepare("SELECT is_system FROM labels WHERE id = ?")
    .get(id) as { is_system: number } | undefined;

  if (!label) {
    return false;
  }

  if (label.is_system) {
    throw new Error("System labels cannot be removed");
  }

  db.prepare("DELETE FROM labels WHERE id = ?").run(id);
  return true;
}

export function getActivePipelineRun(): PipelineRunRecord | null {
  const db = getDb();
  const row = db
    .prepare(
      `SELECT * FROM pipeline_runs
       WHERE status IN ('queued', 'running', 'paused')
       ORDER BY created_at DESC
       LIMIT 1`
    )
    .get() as DbPipelineRunRow | undefined;
  return row ? mapRun(row) : null;
}

export function listPipelineRuns(day?: string): PipelineRunRecord[] {
  const db = getDb();
  const rows = (day
    ? db
        .prepare(
          `SELECT * FROM pipeline_runs
           WHERE day = ?
           ORDER BY created_at DESC
           LIMIT 20`
        )
        .all(day)
    : db
        .prepare(
          `SELECT * FROM pipeline_runs
           ORDER BY created_at DESC
           LIMIT 20`
        )
        .all()) as DbPipelineRunRow[];
  return rows.map(mapRun);
}

export function getPipelineRun(id: string): PipelineRunRecord | null {
  const db = getDb();
  const row = db
    .prepare("SELECT * FROM pipeline_runs WHERE id = ?")
    .get(id) as DbPipelineRunRow | undefined;
  return row ? mapRun(row) : null;
}

export function createPipelineRun(params: {
  day: string;
  batchSize: number;
  beeMapsKey: string;
  modelName?: string | null;
}): PipelineRunRecord {
  const db = getDb();
  const run = {
    id: randomUUID(),
    day: params.day,
    batchSize: params.batchSize,
    status: "queued" as PipelineRunStatus,
    totals: createEmptyPipelineTotals(),
    pipelineVersion: CURRENT_PIPELINE_VERSION,
    modelName: params.modelName ?? DEFAULT_PIPELINE_MODEL_NAME,
  };

  db.prepare(
    `INSERT INTO pipeline_runs (
      id, day, batch_size, status, cursor_offset, totals_json,
      pipeline_version, model_name, bee_maps_key
    ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?)`
  ).run(
    run.id,
    run.day,
    run.batchSize,
    run.status,
    JSON.stringify(run.totals),
    run.pipelineVersion,
    run.modelName,
    params.beeMapsKey
  );

  return getPipelineRun(run.id)!;
}

export function updatePipelineRunStatus(id: string, status: PipelineRunStatus) {
  const db = getDb();
  db.prepare(
    `UPDATE pipeline_runs
     SET status = ?, completed_at = CASE WHEN ? IN ('completed', 'failed', 'cancelled') THEN datetime('now') ELSE completed_at END
     WHERE id = ?`
  ).run(status, status, id);
}

export function setPipelineRunWorkerPid(id: string, pid: number | null) {
  const db = getDb();
  db.prepare(
    `UPDATE pipeline_runs
     SET worker_pid = ?, last_heartbeat_at = datetime('now')
     WHERE id = ?`
  ).run(pid, id);
}

export function isRunHeartbeatStale(run: PipelineRunRecord, staleSeconds = 120): boolean {
  if (!run.lastHeartbeatAt) return true;
  const ageMs = Date.now() - new Date(run.lastHeartbeatAt).getTime();
  return ageMs > staleSeconds * 1000;
}

export function createRetryRunFrom(sourceRunId: string): PipelineRunRecord {
  const db = getDb();
  const source = db
    .prepare(
      `SELECT day, batch_size, bee_maps_key, model_name
       FROM pipeline_runs
       WHERE id = ?`
    )
    .get(sourceRunId) as
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

export function getPipelineRunBeeMapsKey(runId: string): string | null {
  const db = getDb();
  const row = db
    .prepare("SELECT bee_maps_key FROM pipeline_runs WHERE id = ?")
    .get(runId) as { bee_maps_key: string | null } | undefined;
  return row?.bee_maps_key ?? null;
}

export function listVideoPipelineStatesForDay(day: string): VideoPipelineState[] {
  const db = getDb();
  const rows = db
    .prepare(
      `SELECT * FROM video_pipeline_state
       WHERE day = ?
       ORDER BY COALESCE(completed_at, started_at, queued_at) DESC`
    )
    .all(day) as DbVideoStateRow[];
  return rows.map(mapVideoState);
}

export function getVideoPipelineState(videoId: string): VideoPipelineState | null {
  const db = getDb();
  const row = db
    .prepare("SELECT * FROM video_pipeline_state WHERE video_id = ?")
    .get(videoId) as DbVideoStateRow | undefined;
  return row ? mapVideoState(row) : null;
}

export function getVideoDetectionSegments(videoId: string): VideoDetectionSegment[] {
  const db = getDb();
  const row = db
    .prepare(
      `SELECT pipeline_version
       FROM video_pipeline_state
       WHERE video_id = ?`
    )
    .get(videoId) as { pipeline_version: string } | undefined;

  const query = row
    ? db.prepare(
        `SELECT * FROM video_detection_segments
         WHERE video_id = ? AND pipeline_version = ?
         ORDER BY start_ms ASC`
      )
    : db.prepare(
        `SELECT * FROM video_detection_segments
         WHERE video_id = ?
         ORDER BY start_ms ASC`
      );

  const rows = (row ? query.all(videoId, row.pipeline_version) : query.all(videoId)) as DbSegmentRow[];
  return rows.map(mapSegment);
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

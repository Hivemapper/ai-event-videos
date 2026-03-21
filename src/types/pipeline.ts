export type VruLabelKey =
  | "pedestrian"
  | "animal"
  | "motorcycle"
  | "scooter"
  | "wheelchair"
  | "kids"
  | "bicycle"
  | "work-zone-person";

export type VruSupportLevel =
  | "supported"
  | "experimental"
  | "manual_only"
  | "custom";

export type VideoPipelineStatus =
  | "unprocessed"
  | "queued"
  | "running"
  | "processed"
  | "failed"
  | "stale";

export type PipelineRunStatus =
  | "queued"
  | "running"
  | "paused"
  | "completed"
  | "failed"
  | "cancelled";

export interface LabelDefinition {
  id: number;
  name: string;
  created_at?: string;
  is_system: number;
  support_level: VruSupportLevel;
  enabled: number;
  detector_aliases: string | null;
}

export interface VideoDetectionSegment {
  id?: number;
  videoId: string;
  label: string;
  startMs: number;
  endMs: number;
  maxConfidence: number;
  supportLevel: VruSupportLevel;
  pipelineVersion: string;
  source: string;
  createdAt?: string;
}

export interface VideoPipelineState {
  videoId: string;
  day: string;
  status: VideoPipelineStatus;
  pipelineVersion: string;
  modelName: string | null;
  labelsApplied: string[];
  queuedAt: string | null;
  startedAt: string | null;
  completedAt: string | null;
  lastHeartbeatAt: string | null;
  lastError: string | null;
}

export interface PipelineRunTotals {
  totalDiscovered: number;
  totalProcessed: number;
  totalFailed: number;
  totalStale: number;
  totalSkipped: number;
  currentVideoId: string | null;
  currentVideoIndex: number;
  remaining: number;
  lastPageSize: number;
  reconciliationPasses: number;
  throughputPerHour: number;
}

export interface PipelineRunRecord {
  id: string;
  day: string;
  batchSize: number;
  status: PipelineRunStatus;
  cursorOffset: number;
  pipelineVersion: string;
  modelName: string | null;
  totals: PipelineRunTotals;
  startedAt: string | null;
  completedAt: string | null;
  lastHeartbeatAt: string | null;
  lastError: string | null;
  workerPid: number | null;
  createdAt: string;
}

export interface PipelineVideoRow {
  videoId: string;
  timestamp: string;
  type: string;
  videoUrl: string;
  status: VideoPipelineStatus;
  labelsApplied: string[];
  pipelineVersion: string | null;
  modelName: string | null;
  completedAt: string | null;
  lastError: string | null;
}

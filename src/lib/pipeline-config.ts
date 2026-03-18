import {
  PipelineRunTotals,
  VruLabelKey,
  VruSupportLevel,
} from "@/types/pipeline";

/** Tolerance in ms when snapping video time to the nearest detection frame. */
export const DETECTION_FRAME_TOLERANCE_MS = 200;

/** Minimum confidence to show a detection box in the UI overlay. */
export const DISPLAY_CONFIDENCE_THRESHOLD = 0.5;

export const CURRENT_PIPELINE_VERSION = "vru-yolo-v2";
export const DEFAULT_PIPELINE_MODEL_NAME = "yolo11x";
export const DEFAULT_PIPELINE_BATCH_SIZE = 50;
export const DEFAULT_PIPELINE_VIDEO_CACHE_HOURS = 24;
export const DEFAULT_DETECTION_CONFIDENCE = 0.45;

export interface SystemVruLabelConfig {
  key: VruLabelKey;
  supportLevel: VruSupportLevel;
  detectorAliases: string[];
  color: string;
}

export const SYSTEM_VRU_LABELS: SystemVruLabelConfig[] = [
  {
    key: "pedestrian",
    supportLevel: "supported",
    detectorAliases: ["person"],
    color: "#0f766e",
  },
  {
    key: "animal",
    supportLevel: "supported",
    detectorAliases: ["cat", "dog", "bird", "horse", "sheep", "cow", "bear", "elephant", "zebra", "giraffe"],
    color: "#92400e",
  },
  {
    key: "motorcycle",
    supportLevel: "supported",
    detectorAliases: ["motorcycle"],
    color: "#b91c1c",
  },
  {
    key: "bicycle",
    supportLevel: "supported",
    detectorAliases: ["bicycle"],
    color: "#1d4ed8",
  },
  {
    key: "kids",
    supportLevel: "experimental",
    detectorAliases: [],
    color: "#7c3aed",
  },
  {
    key: "wheelchair",
    supportLevel: "experimental",
    detectorAliases: [],
    color: "#4338ca",
  },
  {
    key: "scooter",
    supportLevel: "experimental",
    detectorAliases: [],
    color: "#be123c",
  },
  {
    key: "work-zone-person",
    supportLevel: "manual_only",
    detectorAliases: [],
    color: "#475569",
  },
  {
    key: "vehicle",
    supportLevel: "supported",
    detectorAliases: ["car", "truck", "bus", "train"],
    color: "#0369a1",
  },
  {
    key: "traffic_light",
    supportLevel: "supported",
    detectorAliases: ["traffic light"],
    color: "#15803d",
  },
  {
    key: "stop_sign",
    supportLevel: "supported",
    detectorAliases: ["stop sign"],
    color: "#dc2626",
  },
  {
    key: "skateboard",
    supportLevel: "experimental",
    detectorAliases: ["skateboard"],
    color: "#ca8a04",
  },
];

export const VRU_LABEL_COLOR_MAP = Object.fromEntries(
  SYSTEM_VRU_LABELS.map((label) => [label.key, label.color])
) as Record<string, string>;

export function createEmptyPipelineTotals(): PipelineRunTotals {
  return {
    totalDiscovered: 0,
    totalProcessed: 0,
    totalFailed: 0,
    totalStale: 0,
    totalSkipped: 0,
    currentVideoId: null,
    currentVideoIndex: 0,
    remaining: 0,
    lastPageSize: 0,
    reconciliationPasses: 0,
    throughputPerHour: 0,
  };
}

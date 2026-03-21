import {
  PipelineRunTotals,
  VruLabelKey,
  VruSupportLevel,
} from "@/types/pipeline";

/** Tolerance in ms when snapping video time to the nearest detection frame. */
export const DETECTION_FRAME_TOLERANCE_MS = 200;

export const CURRENT_PIPELINE_VERSION = "vru-yolo-v1";
export const DEFAULT_PIPELINE_MODEL_NAME = "yolov8n";
export const DEFAULT_PIPELINE_BATCH_SIZE = 50;
export const DEFAULT_PIPELINE_VIDEO_CACHE_HOURS = 24;
export const DEFAULT_DETECTION_CONFIDENCE = 0.7;

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
    detectorAliases: ["cat", "dog", "bird", "horse", "sheep", "cow", "bear"],
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
];

export const VRU_LABEL_COLOR_MAP: Record<string, string> = {
  // System VRU labels
  ...Object.fromEntries(SYSTEM_VRU_LABELS.map((l) => [l.key, l.color])),
  // GDINO / open-vocab detection labels
  person: "#0f766e",
  car: "#0369a1",
  truck: "#6d28d9",
  bus: "#b45309",
  motorcycle: "#b91c1c",
  cyclist: "#2563eb",
  crosswalk: "#d97706",
  "construction worker": "#ea580c",
  "traffic cone": "#e11d48",
  stroller: "#7c3aed",
  dog: "#a16207",
  skateboard: "#0891b2",
  skateboarder: "#0891b2",
  scooter: "#be185d",
  wheelchair: "#4338ca",
  pedestrian: "#0f766e",
  motorcyclist: "#dc2626",
  child: "#9333ea",
};

export interface DetectionModelConfig {
  id: string;
  name: string;
}

export const AVAILABLE_DETECTION_MODELS: DetectionModelConfig[] = [
  { id: "gdino-base-clip", name: "GDINO Base + CLIP" },
  { id: "yolo-world", name: "YOLO-World v2" },
  { id: "yolo26x", name: "YOLO26x (COCO-80)" },
  { id: "yolo11x", name: "YOLO11x (COCO-80)" },
];

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

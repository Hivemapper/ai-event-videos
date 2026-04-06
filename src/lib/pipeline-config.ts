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

export type ModelBackend = "yolo" | "grounding-dino";

export interface PipelineModelOption {
  id: string;
  label: string;
  backend: ModelBackend;
  description: string;
}

export const PIPELINE_MODEL_OPTIONS: PipelineModelOption[] = [
  {
    id: "yolov8n",
    label: "YOLOv8 Nano",
    backend: "yolo",
    description: "Fast, lightweight — best for high throughput",
  },
  {
    id: "yolo11n",
    label: "YOLO11 Nano",
    backend: "yolo",
    description: "Newer architecture, similar speed to v8",
  },
  {
    id: "yolo11s",
    label: "YOLO11 Small",
    backend: "yolo",
    description: "Better accuracy, moderate speed",
  },
  {
    id: "grounding-dino-tiny",
    label: "Grounding DINO Tiny",
    backend: "grounding-dino",
    description: "Open-vocabulary detection — detects by text prompt",
  },
  {
    id: "grounding-dino-base",
    label: "Grounding DINO Base",
    backend: "grounding-dino",
    description: "Higher accuracy open-vocabulary, slower",
  },
];

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
    supportLevel: "supported",
    detectorAliases: ["wheelchair"],
    color: "#4338ca",
  },
  {
    key: "scooter",
    supportLevel: "supported",
    detectorAliases: ["scooter"],
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
  type: string;
  device: string;
  classes?: string[];
  prompt?: string;
  features?: string[];
  estimatedTime?: string;
}

export const AVAILABLE_DETECTION_MODELS: DetectionModelConfig[] = [
  {
    id: "gdino-base-clip",
    name: "GDINO Base + CLIP",
    type: "Open-vocabulary (detect anything described in text)",
    device: "MPS (GPU)",
    prompt:
      "person. bicycle. motorcycle. wheelchair. stroller. person wearing safety vest. skateboard. dog.",
    features: ["OpenCLIP verification (filters false positives)"],
    estimatedTime: "~1.5 min for 75 frames",
  },
  {
    id: "mm-gdino",
    name: "MM-GDINO Base (V3Det)",
    type: "Open-vocabulary (V3Det trained, 13K categories)",
    device: "MPS (GPU)",
    classes: [
      "person",
      "bicycle",
      "motorcycle",
      "car",
      "truck",
      "bus",
      "electric scooter",
      "wheelchair",
      "stroller",
      "skateboard",
      "dog",
      "cat",
      "traffic cone",
      "construction worker",
    ],
    estimatedTime: "~1.3 min for 75 frames",
  },
  {
    id: "yolo-world",
    name: "YOLO-World v2",
    type: "Open-vocabulary YOLO",
    device: "MPS (GPU)",
    classes: [
      "person",
      "bicycle",
      "motorcycle",
      "scooter",
      "wheelchair",
      "stroller",
      "person wearing safety vest",
      "skateboard",
      "dog",
      "traffic cone",
      "car",
      "truck",
      "bus",
    ],
    estimatedTime: "~2 min for 75 frames",
  },
  {
    id: "yolo26x",
    name: "YOLO26x (COCO-80)",
    type: "Closed-vocabulary (COCO-80 classes only)",
    device: "MPS (GPU)",
    classes: [
      "person",
      "bicycle",
      "motorcycle",
      "car",
      "truck",
      "bus",
      "stop sign",
      "cat",
      "dog",
      "skateboard",
    ],
    features: ["CLAHE night enhancement", "imgsz=1280"],
    estimatedTime: "~1 min for 75 frames",
  },
  {
    id: "yolo11x",
    name: "YOLO11x (COCO-80)",
    type: "Closed-vocabulary (COCO-80 classes only)",
    device: "MPS (GPU)",
    classes: [
      "person",
      "bicycle",
      "motorcycle",
      "car",
      "truck",
      "bus",
      "stop sign",
      "cat",
      "dog",
      "skateboard",
    ],
    features: ["CLAHE night enhancement", "imgsz=1280"],
    estimatedTime: "~1.5 min for 75 frames",
  },
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

import type { FrameDetection } from "@/types/pipeline";

/** Gap threshold: detections of the same label within this window are the same object */
const TEMPORAL_GAP_MS = 500;

const PLURAL_MAP: Record<string, string> = {
  person: "people",
  pedestrian: "pedestrians",
  child: "children",
  bus: "buses",
};

function pluralize(label: string, count: number): string {
  if (count === 1) return label;
  return PLURAL_MAP[label] ?? `${label}s`;
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/**
 * Aggregate detections across all frames into estimated unique object counts per label.
 * Deduplicates temporally: consecutive frames with the same label (gap < 500ms) count as one object.
 */
export function summarizeDetections(
  detectionsByFrame: Map<number, FrameDetection[]>,
  minConfidence: number
): Record<string, number> {
  // Collect all (label, frameMs) pairs above threshold
  const labelFrames = new Map<string, number[]>();

  for (const [, detections] of detectionsByFrame) {
    for (const det of detections) {
      if (det.confidence < minConfidence) continue;
      const frames = labelFrames.get(det.label) ?? [];
      frames.push(det.frameMs);
      labelFrames.set(det.label, frames);
    }
  }

  // Count distinct appearance runs per label
  const counts: Record<string, number> = {};

  for (const [label, frames] of labelFrames) {
    frames.sort((a, b) => a - b);
    let runs = 1;
    for (let i = 1; i < frames.length; i++) {
      if (frames[i] - frames[i - 1] > TEMPORAL_GAP_MS) {
        runs++;
      }
    }
    counts[label] = runs;
  }

  return counts;
}

/**
 * Build a natural-language sentence from detection counts.
 * e.g. "2 pedestrians, car, and 3 trucks detected in scene."
 */
export function formatDetectionSentence(detections: Record<string, number>): string {
  const entries = Object.entries(detections).filter(([, count]) => count > 0);
  if (entries.length === 0) return "";

  const parts = entries.map(([label, count]) => {
    if (count === 1) return label;
    return `${count} ${pluralize(label, count)}`;
  });

  let joined: string;
  if (parts.length === 1) {
    joined = parts[0];
  } else if (parts.length === 2) {
    joined = `${parts[0]} and ${parts[1]}`;
  } else {
    joined = `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
  }

  return `${capitalize(joined)} detected in scene.`;
}

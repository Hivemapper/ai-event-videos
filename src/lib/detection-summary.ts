import type { FrameDetection } from "@/types/pipeline";
import { isVruDetectionLabel } from "@/lib/vru-labels";

/** Minimum peak confidence for VRU detections to appear in the clip summary (counts + timing) */
const VRU_MIN_CONFIDENCE = 0.46;

const PLURAL_MAP: Record<string, string> = {
  person: "people",
  pedestrian: "pedestrians",
  child: "children",
};

function pluralize(label: string, count: number): string {
  if (count === 1) return label;
  return PLURAL_MAP[label] ?? `${label}s`;
}

function capitalize(s: string): string {
  return s.charAt(0).toUpperCase() + s.slice(1);
}

/** Check if two bounding boxes overlap (IoU > 0) or are within a margin */
function bboxNear(a: FrameDetection, b: FrameDetection, margin: number): boolean {
  return !(
    a.xMax + margin < b.xMin ||
    b.xMax + margin < a.xMin ||
    a.yMax + margin < b.yMin ||
    b.yMax + margin < a.yMin
  );
}

export interface VruTiming {
  label: string;
  firstSeenSec: number;
  lastSeenSec: number;
  peakConfidence: number;
}

export interface DetectionSummary {
  counts: Record<string, number>;
  vruTimings: VruTiming[];
}

/**
 * Aggregate detections across all frames into estimated unique object counts per label,
 * plus timing info for high-confidence VRU detections.
 */
export function summarizeDetections(
  detectionsByFrame: Map<number, FrameDetection[]>,
  minConfidence: number
): DetectionSummary {
  // Collect detections above threshold, keeping only VRU labels for user-facing summaries.
  const threshold = Math.max(minConfidence, VRU_MIN_CONFIDENCE);
  const byLabel = new Map<string, FrameDetection[]>();

  for (const [, detections] of detectionsByFrame) {
    for (const det of detections) {
      if (!isVruDetectionLabel(det.label)) continue;
      if (det.confidence < threshold) continue;
      const arr = byLabel.get(det.label) ?? [];
      arr.push(det);
      byLabel.set(det.label, arr);
    }
  }

  const counts: Record<string, number> = {};
  const vruTimings: VruTiming[] = [];

  for (const [label, dets] of byLabel) {
    // Sort by time
    dets.sort((a, b) => a.frameMs - b.frameMs);

    // Track unique objects: each "track" is the most recent detection for that object.
    // A new detection matches an existing track if it's in a nearby frame AND overlaps spatially.
    const tracks: { first: FrameDetection; last: FrameDetection; peakConfidence: number }[] = [];

    for (const det of dets) {
      let matched = false;
      for (let t = 0; t < tracks.length; t++) {
        const prev = tracks[t].last;
        const timeDelta = det.frameMs - prev.frameMs;
        // Allow up to 2 seconds gap (handles missed frames in sparse sampling)
        if (timeDelta <= 2000 && bboxNear(det, prev, 80)) {
          tracks[t].last = det;
          tracks[t].peakConfidence = Math.max(tracks[t].peakConfidence, det.confidence);
          matched = true;
          break;
        }
      }
      if (!matched) {
        tracks.push({ first: det, last: det, peakConfidence: det.confidence });
      }
    }

    counts[label] = tracks.length;

    for (const track of tracks) {
      vruTimings.push({
        label,
        firstSeenSec: track.first.frameMs / 1000,
        lastSeenSec: track.last.frameMs / 1000,
        peakConfidence: track.peakConfidence,
      });
    }
  }

  // Sort VRU timings by first appearance
  vruTimings.sort((a, b) => a.firstSeenSec - b.firstSeenSec);

  return { counts, vruTimings };
}

/**
 * Build a natural-language sentence from detection counts + VRU timings.
 */
export function formatDetectionSentence(summary: DetectionSummary): string {
  const { counts, vruTimings } = summary;
  const entries = Object.entries(counts).filter(([, count]) => count > 0);
  if (entries.length === 0) return "";

  const parts = entries.map(([label, count]) => {
    if (count === 1) return label;
    return `multiple ${pluralize(label, count)}`;
  });

  let joined: string;
  if (parts.length === 1) {
    joined = parts[0];
  } else if (parts.length === 2) {
    joined = `${parts[0]} and ${parts[1]}`;
  } else {
    joined = `${parts.slice(0, -1).join(", ")}, and ${parts[parts.length - 1]}`;
  }

  let sentence = `${capitalize(joined)} detected in scene.`;

  // Add timing callouts for high-confidence VRU detections
  if (vruTimings.length > 0) {
    // Group by label, take the most prominent (highest confidence) per label
    const byLabel = new Map<string, VruTiming>();
    for (const t of vruTimings) {
      const existing = byLabel.get(t.label);
      if (!existing || t.peakConfidence > existing.peakConfidence) {
        byLabel.set(t.label, t);
      }
    }

    const timingParts: string[] = [];
    for (const [label, t] of byLabel) {
      const duration = t.lastSeenSec - t.firstSeenSec;
      if (duration > 0.5) {
        timingParts.push(
          `${capitalize(label)} visible from ${t.firstSeenSec.toFixed(1)}–${t.lastSeenSec.toFixed(1)}s`
        );
      } else {
        timingParts.push(`${capitalize(label)} at ${t.firstSeenSec.toFixed(1)}s`);
      }
    }

    if (timingParts.length > 0) {
      sentence += ` ${timingParts.join(". ")}.`;
    }
  }

  return sentence;
}

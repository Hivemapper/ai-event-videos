"use client";

import { useMemo } from "react";
import { DetectionBox } from "@/types/pipeline";
const OVERLAY_COLORS: Record<string, string> = {
  pedestrian: "#e040fb", // vivid pink-purple
  bicycle: "#00e5ff",    // electric cyan
  motorcycle: "#ff4081", // hot pink
  animal: "#ffab00",     // vivid amber
  kids: "#d500f9",       // bright purple
  wheelchair: "#651fff", // deep violet
  scooter: "#f50057",    // neon rose
  "work-zone-person": "#ff6d00", // vivid orange
};

interface DetectionOverlayProps {
  boxes: DetectionBox[];
  currentTime: number;
  /** How far (in ms) to look around currentTime to find matching boxes */
  toleranceMs?: number;
}

/**
 * SVG overlay that renders bounding boxes on the video
 * for detected VRU classes at the current playback time.
 */
export function DetectionOverlay({
  boxes,
  currentTime,
  toleranceMs = 150,
}: DetectionOverlayProps) {
  const currentMs = currentTime * 1000;

  // Find boxes near the current timestamp
  const visibleBoxes = useMemo(() => {
    if (!boxes.length) return [];

    // Binary search for the approximate start position
    let lo = 0;
    let hi = boxes.length - 1;
    const target = currentMs - toleranceMs;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (boxes[mid].timestampMs < target) lo = mid + 1;
      else hi = mid;
    }

    const result: DetectionBox[] = [];
    for (let i = lo; i < boxes.length; i++) {
      const box = boxes[i];
      if (box.timestampMs > currentMs + toleranceMs) break;
      if (box.timestampMs >= currentMs - toleranceMs) {
        result.push(box);
      }
    }
    return result;
  }, [boxes, currentMs, toleranceMs]);

  if (!visibleBoxes.length) return null;

  return (
    <svg
      className="absolute inset-0 w-full h-full pointer-events-none"
      viewBox="0 0 1 1"
      preserveAspectRatio="none"
    >
      {visibleBoxes.map((box) => {
        const color = OVERLAY_COLORS[box.label] ?? "#e040fb";
        const x = Math.max(0, box.x1);
        const y = Math.max(0, box.y1);
        const w = Math.min(1, box.x2) - x;
        const h = Math.min(1, box.y2) - y;
        return (
          <g key={`${box.id}-${box.timestampMs}`}>
            <rect
              x={x}
              y={y}
              width={w}
              height={h}
              fill="none"
              stroke={color}
              strokeWidth={0.003}
              rx={0.002}
            />
            {/* Label background */}
            <rect
              x={x}
              y={Math.max(0, y - 0.028)}
              width={Math.max(w, 0.08)}
              height={0.026}
              fill={color}
              rx={0.002}
            />
            {/* Label text */}
            <text
              x={x + 0.004}
              y={Math.max(0.018, y - 0.008)}
              fill="white"
              fontSize={0.016}
              fontFamily="system-ui, sans-serif"
              fontWeight="600"
            >
              {box.label} {Math.round(box.confidence * 100)}%
            </text>
          </g>
        );
      })}
    </svg>
  );
}

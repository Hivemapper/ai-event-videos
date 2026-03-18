import { useMemo } from "react";
import useSWR from "swr";
import type { FrameDetection } from "@/types/pipeline";

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export interface DetectionTimestampsResponse {
  detections: FrameDetection[];
  timestamps: number[];
}

export function useDetectionTimestamps(videoId: string | null) {
  const { data, error, isLoading } = useSWR<DetectionTimestampsResponse>(
    videoId ? `/api/videos/${videoId}/detections` : null,
    fetcher
  );

  const detectionsByFrame = useMemo(() => {
    const map = new Map<number, FrameDetection[]>();
    if (!data?.detections) return map;
    for (const det of data.detections) {
      const existing = map.get(det.frameMs);
      if (existing) {
        existing.push(det);
      } else {
        map.set(det.frameMs, [det]);
      }
    }
    return map;
  }, [data?.detections]);

  return {
    timestamps: data?.timestamps ?? [],
    detectionsByFrame,
    isLoading,
    error,
  };
}

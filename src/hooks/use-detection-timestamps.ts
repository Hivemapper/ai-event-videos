import { useMemo } from "react";
import useSWR from "swr";
import type { FrameDetection, VideoDetectionSegment } from "@/types/pipeline";

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export interface DetectionTimestampsResponse {
  detections: FrameDetection[];
  timestamps: number[];
  models: string[];
  segments: VideoDetectionSegment[];
  sceneAttributes: Record<string, { value: string; confidence: number | null }>;
  timeline: Array<{ startSec: number; endSec: number; event: string; details: string }> | null;
}

export function useDetectionTimestamps(videoId: string | null, runId?: string) {
  const url = videoId
    ? `/api/videos/${videoId}/detections${runId ? `?runId=${encodeURIComponent(runId)}` : ""}`
    : null;

  const { data, error, isLoading, mutate } = useSWR<DetectionTimestampsResponse>(
    url,
    fetcher,
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
    }
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
  }, [data]);

  return {
    timestamps: data?.timestamps ?? [],
    models: data?.models ?? [],
    segments: data?.segments ?? [],
    sceneAttributes: data?.sceneAttributes ?? {},
    timeline: data?.timeline ?? null,
    detectionsByFrame,
    isLoading,
    error,
    mutate,
  };
}

import useSWR from "swr";
import {
  DetectionBox,
  PipelineRunRecord,
  PipelineVideoRow,
  VideoDetectionSegment,
  VideoPipelineState,
} from "@/types/pipeline";

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export interface VideoVruResponse {
  state: VideoPipelineState | null;
  segments: VideoDetectionSegment[];
  boxes: DetectionBox[];
}

export function useVideoVru(videoId: string) {
  return useSWR<VideoVruResponse>(
    videoId ? `/api/videos/${videoId}/vru` : null,
    fetcher
  );
}

export interface PipelineVideosResponse {
  day: string;
  videos: PipelineVideoRow[];
  summary: {
    total: number;
    processed: number;
    failed: number;
    stale: number;
    running: number;
    queued: number;
    remaining: number;
  };
  latestRun: PipelineRunRecord | null;
  activeRun: PipelineRunRecord | null;
}

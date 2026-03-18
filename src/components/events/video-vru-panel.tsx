"use client";

import { useMemo, useState } from "react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useVideoVru } from "@/hooks/use-video-vru";
import { DETECTION_FRAME_TOLERANCE_MS, VRU_LABEL_COLOR_MAP } from "@/lib/pipeline-config";
import { cn } from "@/lib/utils";
import type { VideoDetectionSegment } from "@/types/pipeline";

interface VideoVruPanelProps {
  videoId: string;
  currentTime: number;
  duration: number;
  isPlaying?: boolean;
  detectionTimestamps?: number[];
  onSeek: (time: number) => void;
}

function formatTimestamp(seconds: number) {
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.floor(seconds % 60);
  return `${minutes}:${remainder.toString().padStart(2, "0")}`;
}

function statusTone(status: string | undefined) {
  switch (status) {
    case "processed":
      return "default";
    case "running":
    case "queued":
      return "secondary";
    case "failed":
      return "destructive";
    default:
      return "outline";
  }
}

export function VideoVruPanel({
  videoId,
  currentTime,
  duration,
  isPlaying,
  detectionTimestamps,
  onSeek,
}: VideoVruPanelProps) {
  const { data, isLoading } = useVideoVru(videoId);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  // Frame stepping helpers
  const currentMs = currentTime * 1000;
  const sortedTimestamps = useMemo(
    () => (detectionTimestamps ? [...detectionTimestamps].sort((a, b) => a - b) : []),
    [detectionTimestamps]
  );

  const currentFrameIndex = useMemo(() => {
    if (sortedTimestamps.length === 0) return -1;
    let bestIdx = 0;
    let bestDist = Math.abs(sortedTimestamps[0] - currentMs);
    for (let i = 1; i < sortedTimestamps.length; i++) {
      const dist = Math.abs(sortedTimestamps[i] - currentMs);
      if (dist < bestDist) {
        bestIdx = i;
        bestDist = dist;
      }
    }
    return bestDist < DETECTION_FRAME_TOLERANCE_MS ? bestIdx : -1;
  }, [sortedTimestamps, currentMs]);

  const canStepPrev = currentFrameIndex > 0;
  const canStepNext =
    currentFrameIndex >= 0 && currentFrameIndex < sortedTimestamps.length - 1;

  const stepPrev = () => {
    if (canStepPrev) {
      onSeek(sortedTimestamps[currentFrameIndex - 1] / 1000);
    }
  };

  const stepNext = () => {
    if (canStepNext) {
      onSeek(sortedTimestamps[currentFrameIndex + 1] / 1000);
    }
  };

  const groupedSegments = useMemo(() => {
    const groups = new Map<string, VideoDetectionSegment[]>();
    for (const segment of data?.segments ?? []) {
      const row = groups.get(segment.label) ?? [];
      row.push(segment);
      groups.set(segment.label, row);
    }
    return [...groups.entries()];
  }, [data?.segments]);

  if (isLoading) {
    return <Skeleton className="h-40 rounded-xl" />;
  }

  const state = data?.state;
  const status = state?.status ?? "unprocessed";
  const labelsApplied = state?.labelsApplied ?? [];

  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center justify-between gap-3 text-lg">
          <span>VRU Labels</span>
          <Badge variant={statusTone(status)}>{status}</Badge>
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="flex flex-wrap gap-2">
          {labelsApplied.length > 0 ? (
            labelsApplied.map((label) => (
              <Badge key={label} variant="outline" className="text-xs">
                {label}
              </Badge>
            ))
          ) : (
            <p className="text-sm text-muted-foreground">
              {status === "processed"
                ? "No VRU detections cleared the confidence threshold."
                : "This video has not completed the VRU pipeline yet."}
            </p>
          )}
        </div>

        {state?.lastError && (
          <div className="rounded-lg border border-destructive/40 bg-destructive/5 px-3 py-2 text-sm text-destructive">
            {state.lastError}
          </div>
        )}

        {groupedSegments.length > 0 && duration > 0 && (
          <div className="space-y-3">
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>Timeline</span>
              <span>{formatTimestamp(duration)}</span>
            </div>
            {groupedSegments.map(([label, segments]) => (
              <div key={label} className="grid grid-cols-[120px_1fr] items-center gap-3">
                <div className="text-sm font-medium">{label}</div>
                <div className="relative h-8 rounded-md bg-muted">
                  {segments.map((segment, index) => {
                    const key = `${segment.label}-${segment.startMs}-${segment.endMs}-${index}`;
                    const start = (segment.startMs / 1000 / duration) * 100;
                    const end = (segment.endMs / 1000 / duration) * 100;
                    const width = Math.max(1.5, end - start);
                    const isCurrent =
                      currentTime * 1000 >= segment.startMs &&
                      currentTime * 1000 <= segment.endMs;
                    const isSelected = key === selectedKey;
                    return (
                      <button
                        key={key}
                        type="button"
                        className={cn(
                          "absolute top-1/2 h-5 -translate-y-1/2 rounded-sm border transition-transform",
                          (isSelected || isCurrent) && "scale-y-110"
                        )}
                        style={{
                          left: `${start}%`,
                          width: `${width}%`,
                          backgroundColor:
                            VRU_LABEL_COLOR_MAP[segment.label] ?? "#334155",
                          borderColor: isSelected || isCurrent ? "#ffffff" : "transparent",
                        }}
                        title={`${label} ${formatTimestamp(segment.startMs / 1000)}-${formatTimestamp(segment.endMs / 1000)} (${Math.round(segment.maxConfidence * 100)}%)`}
                        onClick={() => {
                          setSelectedKey(key);
                          onSeek(segment.startMs / 1000);
                        }}
                      />
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}

        {/* Frame stepping controls - show when paused on a detection frame */}
        {!isPlaying && sortedTimestamps.length > 0 && currentFrameIndex >= 0 && (
          <div className="flex items-center justify-between rounded-lg border bg-muted/50 px-3 py-2">
            <span className="text-sm text-muted-foreground">
              Frame at {(sortedTimestamps[currentFrameIndex] / 1000).toFixed(2)}s
            </span>
            <div className="flex items-center gap-1">
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                disabled={!canStepPrev}
                onClick={stepPrev}
                title="Previous detection frame"
              >
                <ChevronLeft className="h-4 w-4" />
              </Button>
              <span className="text-xs tabular-nums text-muted-foreground min-w-[4rem] text-center">
                {currentFrameIndex + 1} / {sortedTimestamps.length}
              </span>
              <Button
                variant="ghost"
                size="icon"
                className="h-7 w-7"
                disabled={!canStepNext}
                onClick={stepNext}
                title="Next detection frame"
              >
                <ChevronRight className="h-4 w-4" />
              </Button>
            </div>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

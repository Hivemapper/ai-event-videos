"use client";

import { useMemo, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useVideoVru } from "@/hooks/use-video-vru";
import { VRU_LABEL_COLOR_MAP } from "@/lib/pipeline-config";
import { cn } from "@/lib/utils";
import { VideoDetectionSegment } from "@/types/pipeline";

interface VideoVruPanelProps {
  videoId: string;
  currentTime: number;
  duration: number;
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
  onSeek,
}: VideoVruPanelProps) {
  const { data, isLoading } = useVideoVru(videoId);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

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
      </CardContent>
    </Card>
  );
}

"use client";

import { useMemo, useState } from "react";
import { ChevronLeft, ChevronRight, Loader2, Play } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Slider } from "@/components/ui/slider";
import { useVideoVru } from "@/hooks/use-video-vru";
import {
  AVAILABLE_DETECTION_MODELS,
  DETECTION_FRAME_TOLERANCE_MS,
  VRU_LABEL_COLOR_MAP,
} from "@/lib/pipeline-config";
import { cn } from "@/lib/utils";
import type { DetectionRun, FrameDetection, VideoDetectionSegment } from "@/types/pipeline";

interface VideoVruPanelProps {
  videoId: string;
  currentTime: number;
  duration: number;
  isPlaying?: boolean;
  detectionTimestamps?: number[];
  detectionsByFrame?: Map<number, FrameDetection[]>;
  availableModels?: string[];
  selectedModel?: string;
  onModelChange?: (model: string) => void;
  minConfidence?: number;
  onMinConfidenceChange?: (value: number) => void;
  activeDetectionRun?: DetectionRun | null;
  detectionRuns?: DetectionRun[];
  onRunDetection?: (modelName: string) => void;
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

function formatRelativeTime(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const seconds = Math.floor(diff / 1000);
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatElapsedTime(startedAt: string): string {
  const elapsed = Math.floor((Date.now() - new Date(startedAt).getTime()) / 1000);
  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;
  return `${minutes}:${seconds.toString().padStart(2, "0")}`;
}

export function VideoVruPanel({
  videoId,
  currentTime,
  duration,
  isPlaying,
  detectionTimestamps,
  detectionsByFrame,
  availableModels,
  selectedModel,
  onModelChange,
  minConfidence,
  onMinConfidenceChange,
  activeDetectionRun,
  detectionRuns,
  onRunDetection,
  onSeek,
}: VideoVruPanelProps) {
  const { data, isLoading } = useVideoVru(videoId);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [selectedRunModel, setSelectedRunModel] = useState(
    AVAILABLE_DETECTION_MODELS[0].id
  );

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

  const uniqueLabels = useMemo(() => {
    if (!detectionsByFrame) return [];
    const labels = new Set<string>();
    const threshold = minConfidence ?? 0;
    for (const detections of detectionsByFrame.values()) {
      for (const det of detections) {
        if (det.confidence >= threshold) {
          labels.add(det.label);
        }
      }
    }
    return [...labels].sort();
  }, [detectionsByFrame, minConfidence]);

  if (isLoading) {
    return <Skeleton className="h-40 rounded-xl" />;
  }

  const state = data?.state;
  const status = state?.status ?? "unprocessed";
  const labelsApplied = state?.labelsApplied ?? [];

  return (
    <div className="space-y-3">
      {/* Detection controls — model selector + confidence slider */}
      {availableModels &&
        availableModels.length >= 1 &&
        selectedModel &&
        onModelChange && (
          <div className="space-y-3 rounded-lg border bg-card px-4 py-3">
            <h3 className="text-lg font-semibold">Detections</h3>
            <div className="flex items-center gap-3">
              <label
                htmlFor="model-select"
                className="text-sm font-medium text-muted-foreground"
              >
                Model
              </label>
              <Select value={selectedModel} onValueChange={onModelChange}>
                <SelectTrigger id="model-select" className="flex-1" size="sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {availableModels.map((model) => (
                    <SelectItem key={model} value={model}>
                      {model}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            {onMinConfidenceChange && (
              <div className="flex items-center gap-3">
                <label className="text-sm font-medium text-muted-foreground whitespace-nowrap">
                  Min conf
                </label>
                <Slider
                  aria-label="Minimum confidence threshold"
                  value={[minConfidence ?? 0]}
                  onValueChange={([v]) => onMinConfidenceChange(v)}
                  min={0}
                  max={1}
                  step={0.05}
                  className="flex-1"
                />
                <span className="text-sm tabular-nums text-muted-foreground w-10 text-right">
                  {Math.round((minConfidence ?? 0) * 100)}%
                </span>
              </div>
            )}
            {uniqueLabels.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {uniqueLabels.map((label) => (
                  <Badge
                    key={label}
                    className="text-xs text-white border-transparent"
                    style={{
                      backgroundColor:
                        VRU_LABEL_COLOR_MAP[label] ?? "#334155",
                    }}
                  >
                    {label}
                  </Badge>
                ))}
              </div>
            )}
          </div>
        )}

      {/* Run Detection — always visible */}
      {onRunDetection && (
        <div className="space-y-2 rounded-lg border bg-card px-4 py-3">
          <h4 className="text-sm font-medium text-muted-foreground">Run Detection</h4>
          {activeDetectionRun &&
          (activeDetectionRun.status === "queued" ||
            activeDetectionRun.status === "running") ? (
            <div className="flex items-center gap-2 text-sm">
              <Loader2 className="h-4 w-4 animate-spin text-muted-foreground" />
              <span>
                Running{" "}
                <span className="font-medium">
                  {AVAILABLE_DETECTION_MODELS.find(
                    (m) => m.id === activeDetectionRun.modelName
                  )?.name ?? activeDetectionRun.modelName}
                </span>
                ...
              </span>
              {activeDetectionRun.startedAt && (
                <span className="text-xs text-muted-foreground ml-auto tabular-nums">
                  {formatElapsedTime(activeDetectionRun.startedAt)}
                </span>
              )}
            </div>
          ) : (
            <div className="flex items-center gap-2">
              <Select
                value={selectedRunModel}
                onValueChange={setSelectedRunModel}
              >
                <SelectTrigger className="flex-1" size="sm">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {AVAILABLE_DETECTION_MODELS.map((model) => (
                    <SelectItem key={model.id} value={model.id}>
                      {model.name}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
              <Button
                size="sm"
                onClick={() => onRunDetection(selectedRunModel)}
              >
                <Play className="h-3.5 w-3.5 mr-1" />
                Run
              </Button>
            </div>
          )}

          {/* Run History */}
          {detectionRuns &&
            detectionRuns.filter(
              (r) => r.status === "completed" || r.status === "failed"
            ).length > 0 && (
              <div className="space-y-1 border-t pt-2">
                <p className="text-xs font-medium text-muted-foreground">
                  Recent runs
                </p>
                {detectionRuns
                  .filter(
                    (r) =>
                      r.status === "completed" || r.status === "failed"
                  )
                  .slice(0, 3)
                  .map((run) => (
                    <div
                      key={run.id}
                      className="flex items-center justify-between text-xs text-muted-foreground"
                    >
                      <span>
                        {AVAILABLE_DETECTION_MODELS.find(
                          (m) => m.id === run.modelName
                        )?.name ?? run.modelName}
                      </span>
                      <span className="flex items-center gap-2">
                        {run.status === "failed" ? (
                          <span className="text-destructive">Failed</span>
                        ) : (
                          <span>
                            {run.detectionCount ?? 0} detections
                          </span>
                        )}
                        {run.completedAt && (
                          <span className="tabular-nums">
                            {formatRelativeTime(run.completedAt)}
                          </span>
                        )}
                      </span>
                    </div>
                  ))}
              </div>
            )}
        </div>
      )}

      <Card>
        <CardHeader>
          <CardTitle className="flex items-center justify-between gap-3 text-lg">
            VRU Labels
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
                <div
                  key={label}
                  className="grid grid-cols-[120px_1fr] items-center gap-3"
                >
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
                            (isSelected || isCurrent) && "scale-y-110",
                          )}
                          style={{
                            left: `${start}%`,
                            width: `${width}%`,
                            backgroundColor:
                              VRU_LABEL_COLOR_MAP[segment.label] ?? "#334155",
                            borderColor:
                              isSelected || isCurrent
                                ? "#ffffff"
                                : "transparent",
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
                Frame at{" "}
                {(sortedTimestamps[currentFrameIndex] / 1000).toFixed(2)}s
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
    </div>
  );
}

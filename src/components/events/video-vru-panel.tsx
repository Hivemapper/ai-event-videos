"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import { ChevronDown, ChevronLeft, ChevronRight, Clock, Cpu, Info, Loader2, Play, Plus, X } from "lucide-react";
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
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
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
  minConfidence?: number;
  onMinConfidenceChange?: (value: number) => void;
  activeDetectionRun?: DetectionRun | null;
  detectionRuns?: DetectionRun[];
  onRunDetection?: (modelName: string) => void;
  selectedRunId?: string | null;
  onSelectRun?: (runId: string) => void;
  onCancelRun?: (runId: string) => void;
  logs?: string;
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
    case "cancelled":
      return "outline";
    default:
      return "outline";
  }
}

function parseTimestamp(dateStr: string): number {
  // SQLite datetime('now') returns "2026-03-22 06:10:02" (no T, no Z)
  // Python ISO format returns "2026-03-22T06:14:11Z"
  // Normalize: if no T or Z, treat as UTC by appending Z
  let normalized = dateStr;
  if (!normalized.includes("T")) {
    normalized = normalized.replace(" ", "T");
  }
  if (!normalized.endsWith("Z") && !normalized.includes("+")) {
    normalized += "Z";
  }
  return new Date(normalized).getTime();
}

function formatRelativeTime(dateStr: string): string {
  const diff = Date.now() - parseTimestamp(dateStr);
  const seconds = Math.max(0, Math.floor(diff / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

function formatElapsedTime(startedAt: string): string {
  const elapsed = Math.max(0, Math.floor((Date.now() - parseTimestamp(startedAt)) / 1000));
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
  minConfidence,
  onMinConfidenceChange,
  activeDetectionRun,
  detectionRuns,
  onRunDetection,
  selectedRunId,
  onSelectRun,
  onCancelRun,
  logs,
  onSeek,
}: VideoVruPanelProps) {
  const { data, isLoading } = useVideoVru(videoId);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [selectedRunModel, setSelectedRunModel] = useState(
    AVAILABLE_DETECTION_MODELS[0].id
  );
  const [logsOpen, setLogsOpen] = useState(true);
  const [dialogOpen, setDialogOpen] = useState(false);
  const logsEndRef = useRef<HTMLDivElement>(null);

  // Auto-scroll logs to bottom
  useEffect(() => {
    if (logsOpen && logsEndRef.current) {
      logsEndRef.current.scrollIntoView({ behavior: "smooth" });
    }
  }, [logs, logsOpen]);

  const isRunActive =
    activeDetectionRun &&
    (activeDetectionRun.status === "queued" || activeDetectionRun.status === "running");

  const selectedModelConfig = useMemo(
    () => AVAILABLE_DETECTION_MODELS.find((m) => m.id === selectedRunModel) ?? null,
    [selectedRunModel]
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

  // Determine if selected run is completed (for showing controls)
  const selectedRun = useMemo(() => {
    if (!selectedRunId || !detectionRuns) return null;
    return detectionRuns.find((r) => r.id === selectedRunId) ?? null;
  }, [selectedRunId, detectionRuns]);

  const selectedRunCompleted = selectedRun?.status === "completed";


  if (isLoading) {
    return <Skeleton className="h-40 rounded-xl" />;
  }

  const state = data?.state;
  const status = state?.status ?? "unprocessed";
  const labelsApplied = state?.labelsApplied ?? [];

  return (
    <div className="space-y-3">
      {/* Consolidated Detections card */}
      {onRunDetection && (
        <div className="space-y-3 rounded-lg border bg-card px-4 py-3">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Detections</h3>
            <Dialog open={dialogOpen} onOpenChange={setDialogOpen}>
              <DialogTrigger asChild>
                <Button size="sm" variant="outline" disabled={!!isRunActive}>
                  <Plus className="h-3.5 w-3.5 mr-1" />
                  New Run
                </Button>
              </DialogTrigger>
              <DialogContent className="sm:max-w-md">
                <DialogHeader>
                  <DialogTitle>New Detection Run</DialogTitle>
                  <DialogDescription>
                    Select a detection model and start a new run.
                  </DialogDescription>
                </DialogHeader>
                <div className="space-y-4">
                  <Select
                    value={selectedRunModel}
                    onValueChange={setSelectedRunModel}
                  >
                    <SelectTrigger className="w-full">
                      <SelectValue placeholder="Select a model" />
                    </SelectTrigger>
                    <SelectContent>
                      {AVAILABLE_DETECTION_MODELS.map((model) => (
                        <SelectItem key={model.id} value={model.id}>
                          {model.name}
                        </SelectItem>
                      ))}
                    </SelectContent>
                  </Select>
                  {selectedModelConfig && (
                    <div className="rounded-md border bg-muted/30 px-3 py-3 space-y-2 text-sm">
                      <div className="flex items-center gap-1.5 font-medium">
                        <Info className="h-3.5 w-3.5 text-muted-foreground" />
                        Model Info
                      </div>
                      <div className="grid grid-cols-[auto_1fr] gap-x-3 gap-y-1.5 text-xs">
                        <span className="text-muted-foreground">Type</span>
                        <span>{selectedModelConfig.type}</span>
                        <span className="text-muted-foreground">Device</span>
                        <span className="flex items-center gap-1">
                          <Cpu className="h-3 w-3" />
                          {selectedModelConfig.device}
                        </span>
                        {selectedModelConfig.prompt && (
                          <>
                            <span className="text-muted-foreground">Prompt</span>
                            <span className="break-words font-mono text-[11px] leading-relaxed">
                              {selectedModelConfig.prompt}
                            </span>
                          </>
                        )}
                        {selectedModelConfig.classes && (
                          <>
                            <span className="text-muted-foreground">Classes</span>
                            <span>{selectedModelConfig.classes.join(", ")}</span>
                          </>
                        )}
                        {selectedModelConfig.features && (
                          <>
                            <span className="text-muted-foreground">Features</span>
                            <span>{selectedModelConfig.features.join(", ")}</span>
                          </>
                        )}
                        {selectedModelConfig.estimatedTime && (
                          <>
                            <span className="text-muted-foreground">Est. time</span>
                            <span className="flex items-center gap-1">
                              <Clock className="h-3 w-3" />
                              {selectedModelConfig.estimatedTime}
                            </span>
                          </>
                        )}
                      </div>
                    </div>
                  )}
                </div>
                <DialogFooter>
                  <DialogClose asChild>
                    <Button variant="outline">Cancel</Button>
                  </DialogClose>
                  <Button
                    disabled={!!isRunActive}
                    onClick={() => {
                      onRunDetection(selectedRunModel);
                      setDialogOpen(false);
                    }}
                  >
                    <Play className="h-3.5 w-3.5 mr-1" />
                    Run
                  </Button>
                </DialogFooter>
              </DialogContent>
            </Dialog>
          </div>

          {/* Run list */}
          {detectionRuns && detectionRuns.length > 0 && (
            <div className="space-y-1">
              <p className="text-xs font-medium text-muted-foreground">Previous runs</p>
              {detectionRuns.slice(0, 10).map((run) => {
                const isSelected = run.id === selectedRunId;
                const isClickable =
                  run.status === "completed" && onSelectRun;
                const isCancellable =
                  (run.status === "queued" || run.status === "running") &&
                  onCancelRun;
                const isRunning = run.status === "queued" || run.status === "running";
                const showRunLogs = isRunning && !!logs;
                return (
                  <div key={run.id} className="space-y-1">
                    <div
                      className={cn(
                        "flex w-full items-center justify-between rounded-md px-2.5 py-1.5 text-xs transition-colors",
                        isSelected
                          ? "bg-accent text-accent-foreground"
                          : "text-muted-foreground hover:bg-muted/50",
                        isClickable
                          ? "cursor-pointer"
                          : "cursor-default"
                      )}
                      role={isClickable ? "button" : undefined}
                      tabIndex={isClickable ? 0 : undefined}
                      onClick={() => {
                        if (isClickable) {
                          onSelectRun(run.id);
                        }
                      }}
                      onKeyDown={(e) => {
                        if (isClickable && (e.key === "Enter" || e.key === " ")) {
                          e.preventDefault();
                          onSelectRun!(run.id);
                        }
                      }}
                    >
                      <span className="flex items-center gap-1.5">
                        {isRunning ? (
                          <Loader2 className="h-3 w-3 animate-spin shrink-0" />
                        ) : run.status === "completed" ? (
                          <span className="h-2 w-2 rounded-full bg-green-500 inline-block shrink-0" />
                        ) : run.status === "cancelled" ? (
                          <span className="h-2 w-2 rounded-full bg-yellow-500 inline-block shrink-0" />
                        ) : (
                          <span className="h-2 w-2 rounded-full bg-red-500 inline-block shrink-0" />
                        )}
                        {run.completedAt && (
                          <span className="tabular-nums text-muted-foreground">
                            {formatRelativeTime(run.completedAt)}
                          </span>
                        )}
                        {isRunning && run.startedAt && (
                          <span className="tabular-nums text-muted-foreground">
                            {formatElapsedTime(run.startedAt)}
                          </span>
                        )}
                        <span className={cn(isSelected && "font-medium")}>
                          {(run.config?.modelDisplayName as string) ??
                            AVAILABLE_DETECTION_MODELS.find(
                              (m) => m.id === run.modelName
                            )?.name ??
                            run.modelName}
                        </span>
                      </span>
                      <span className="flex items-center gap-2">
                        {run.status === "failed" ? (
                          <span className="text-destructive">Failed</span>
                        ) : run.status === "cancelled" ? (
                          <span className="text-yellow-600 dark:text-yellow-500">Cancelled</span>
                        ) : isRunning ? (
                          <span>
                            {run.status === "queued" ? "Queued" : "Running"}
                          </span>
                        ) : (
                          <span>{run.detectionCount ?? 0} detections</span>
                        )}
                        {isCancellable && (
                          <Button
                            variant="ghost"
                            size="icon"
                            className="h-5 w-5 text-destructive hover:text-destructive hover:bg-destructive/10"
                            onClick={(e) => {
                              e.stopPropagation();
                              onCancelRun(run.id);
                            }}
                            title="Cancel run"
                          >
                            <X className="h-3 w-3" />
                          </Button>
                        )}
                      </span>
                    </div>
                    {/* Inline logs for active run */}
                    {showRunLogs && (
                      <div className="ml-4">
                        <button
                          type="button"
                          className="flex items-center gap-1 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors mb-1"
                          onClick={() => setLogsOpen((v) => !v)}
                        >
                          <ChevronDown
                            className={cn(
                              "h-3.5 w-3.5 transition-transform",
                              !logsOpen && "-rotate-90"
                            )}
                          />
                          Logs
                        </button>
                        {logsOpen && (
                          <div className="max-h-40 overflow-y-auto rounded-md border bg-muted/50 px-3 py-2">
                            <pre className="text-xs font-mono whitespace-pre-wrap break-words text-muted-foreground">
                              {logs || "Waiting for output..."}
                            </pre>
                            <div ref={logsEndRef} />
                          </div>
                        )}
                      </div>
                    )}
                  </div>
                );
              })}
            </div>
          )}

          {/* Selected run controls -- confidence + label badges */}
          {selectedRunCompleted && onMinConfidenceChange && (
            <div className="space-y-2.5 rounded-md border bg-muted/30 px-3 py-2.5">
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

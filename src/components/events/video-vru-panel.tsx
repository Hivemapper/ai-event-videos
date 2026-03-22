"use client";

import { useMemo, useState, useEffect } from "react";
import {
  Play,
  Square,
  Loader2,
  CheckCircle2,
  XCircle,
  RotateCcw,
  Cpu,
  Clock,
  Info,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { useVideoVru } from "@/hooks/use-video-vru";
import {
  PIPELINE_MODEL_OPTIONS,
  DEFAULT_PIPELINE_MODEL_NAME,
  DEFAULT_DETECTION_CONFIDENCE,
  VRU_LABEL_COLOR_MAP,
} from "@/lib/pipeline-config";
import { cn } from "@/lib/utils";
import { getApiKey } from "@/lib/api";
import { VideoDetectionSegment } from "@/types/pipeline";

interface VideoVruPanelProps {
  videoId: string;
  videoUrl: string;
  currentTime: number;
  duration: number;
  onSeek: (time: number) => void;
  embedded?: boolean;
}

function formatTimestamp(seconds: number) {
  const minutes = Math.floor(seconds / 60);
  const remainder = Math.floor(seconds % 60);
  return `${minutes}:${remainder.toString().padStart(2, "0")}`;
}

function StatusIndicator({ status }: { status: string }) {
  switch (status) {
    case "processed":
      return (
        <div className="inline-flex items-center gap-1.5 rounded-full bg-emerald-500/10 px-2.5 py-0.5 text-xs font-medium text-emerald-700 dark:text-emerald-400">
          <CheckCircle2 className="w-3 h-3" />
          Processed
        </div>
      );
    case "running":
      return (
        <div className="inline-flex items-center gap-1.5 rounded-full bg-sky-500/10 px-2.5 py-0.5 text-xs font-medium text-sky-700 dark:text-sky-400">
          <Loader2 className="w-3 h-3 animate-spin" />
          Running
        </div>
      );
    case "queued":
      return (
        <div className="inline-flex items-center gap-1.5 rounded-full bg-amber-500/10 px-2.5 py-0.5 text-xs font-medium text-amber-700 dark:text-amber-400">
          <Loader2 className="w-3 h-3 animate-spin" />
          Queued
        </div>
      );
    case "failed":
      return (
        <div className="inline-flex items-center gap-1.5 rounded-full bg-red-500/10 px-2.5 py-0.5 text-xs font-medium text-red-600 dark:text-red-400">
          <XCircle className="w-3 h-3" />
          Failed
        </div>
      );
    default:
      return (
        <div className="inline-flex items-center gap-1.5 rounded-full bg-muted px-2.5 py-0.5 text-xs font-medium text-muted-foreground">
          Unprocessed
        </div>
      );
  }
}

function formatRunDuration(startedAt: string | null, completedAt: string | null) {
  if (!startedAt || !completedAt) return null;
  const ms = new Date(completedAt).getTime() - new Date(startedAt).getTime();
  if (ms < 1000) return `${ms}ms`;
  const secs = Math.round(ms / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  return `${mins}m ${remSecs}s`;
}

function RunLog({
  state,
  boxes,
  segments,
}: {
  state: import("@/types/pipeline").VideoPipelineState;
  boxes: import("@/types/pipeline").DetectionBox[];
  segments: import("@/types/pipeline").VideoDetectionSegment[];
}) {
  const modelLabel =
    PIPELINE_MODEL_OPTIONS.find((o) => o.id === state.modelName)?.label ??
    state.modelName ??
    "Unknown";
  const duration = formatRunDuration(state.startedAt, state.completedAt);
  const thresholdPct = Math.round(DEFAULT_DETECTION_CONFIDENCE * 100);
  const segmentLabels = [...new Set(segments.map((s) => s.label))];
  const segmentCount = segments.length;

  // Find labels that appear in boxes but NOT in segments (below threshold)
  const allBoxLabels = [...new Set(boxes.map((b) => b.label))];
  const belowThresholdLabels = allBoxLabels.filter((l) => !segmentLabels.includes(l));

  let summaryLines: string[] = [];
  if (state.status === "failed") {
    summaryLines = [state.lastError ?? "Unknown error"];
  } else {
    if (segmentCount > 0) {
      summaryLines.push(
        `${segmentCount} segment${segmentCount !== 1 ? "s" : ""} above ${thresholdPct}% threshold (${segmentLabels.join(", ")})`
      );
    }
    if (belowThresholdLabels.length > 0) {
      const counts = belowThresholdLabels.map((label) => {
        const maxConf = Math.max(...boxes.filter((b) => b.label === label).map((b) => b.confidence));
        return `${label} ${Math.round(maxConf * 100)}%`;
      });
      summaryLines.push(
        `${belowThresholdLabels.length} class${belowThresholdLabels.length !== 1 ? "es" : ""} below ${thresholdPct}% threshold (${counts.join(", ")})`
      );
    }
    if (summaryLines.length === 0) {
      summaryLines = ["No VRU objects detected in any frame"];
    }
  }

  return (
    <div className="rounded-lg border border-border/60 bg-muted/30 px-3 py-2.5 text-xs space-y-1">
      <div className="flex items-center gap-4 text-muted-foreground">
        <span className="inline-flex items-center gap-1">
          <Cpu className="w-3 h-3" />
          {modelLabel}
        </span>
        {duration && (
          <span className="inline-flex items-center gap-1">
            <Clock className="w-3 h-3" />
            {duration}
          </span>
        )}
        {state.completedAt && (
          <span>
            {new Date(state.completedAt).toLocaleString()}
          </span>
        )}
      </div>
      {summaryLines.map((line, i) => (
        <div
          key={i}
          className={cn(
            "flex items-start gap-1.5",
            state.status === "failed"
              ? "text-red-600 dark:text-red-400"
              : i === 0 && segmentCount > 0
                ? "text-foreground"
                : "text-muted-foreground"
          )}
        >
          {i === 0 && state.status === "failed" ? (
            <XCircle className="w-3 h-3 mt-0.5 shrink-0" />
          ) : (
            <Info className="w-3 h-3 mt-0.5 shrink-0" />
          )}
          <span>{line}</span>
        </div>
      ))}
    </div>
  );
}

export function VideoVruPanel({
  videoId,
  videoUrl,
  currentTime,
  duration,
  onSeek,
  embedded = false,
}: VideoVruPanelProps) {
  const { data, isLoading, mutate } = useVideoVru(videoId);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);
  const [modelName, setModelName] = useState(DEFAULT_PIPELINE_MODEL_NAME);
  const [modelInitialized, setModelInitialized] = useState(false);

  // Default to the last model used for this video
  useEffect(() => {
    if (!modelInitialized && data?.state?.modelName) {
      setModelName(data.state.modelName);
      setModelInitialized(true);
    }
  }, [data?.state?.modelName, modelInitialized]);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const state = data?.state;
  const status = state?.status ?? "unprocessed";
  const isRunning = status === "running" || status === "queued";
  const labelsApplied = state?.labelsApplied ?? [];

  // Poll faster while running
  useEffect(() => {
    if (!isRunning) return;
    const id = setInterval(() => mutate(), 2000);
    return () => clearInterval(id);
  }, [isRunning, mutate]);

  const groupedSegments = useMemo(() => {
    const groups = new Map<string, VideoDetectionSegment[]>();
    for (const segment of data?.segments ?? []) {
      const row = groups.get(segment.label) ?? [];
      row.push(segment);
      groups.set(segment.label, row);
    }
    return [...groups.entries()];
  }, [data?.segments]);

  async function handleRun() {
    const apiKey = getApiKey();
    if (!apiKey) {
      setActionError("Configure your Bee Maps API key in Settings first.");
      return;
    }
    if (!videoUrl) {
      setActionError("Video URL not available yet. Try again in a moment.");
      return;
    }
    setIsSubmitting(true);
    setActionError(null);
    try {
      const res = await fetch(`/api/videos/${videoId}/vru/run`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          videoUrl,
          modelName,
          beeMapsApiKey: apiKey,
        }),
      });
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.error || "Failed to start");
      // Optimistically show running state — don't revalidate immediately
      // because the worker hasn't updated the DB yet. The polling effect
      // will pick up the real state once isRunning becomes true.
      await mutate(
        {
          state: {
            videoId,
            day: new Date().toISOString().slice(0, 10),
            status: "running" as const,
            pipelineVersion: "",
            modelName,
            labelsApplied: [],
            queuedAt: new Date().toISOString(),
            startedAt: new Date().toISOString(),
            completedAt: null,
            lastHeartbeatAt: null,
            lastError: null,
          },
          segments: [],
          boxes: [],
        },
        { revalidate: false }
      );
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to start");
    } finally {
      setIsSubmitting(false);
    }
  }

  async function handleCancel() {
    setIsSubmitting(true);
    setActionError(null);
    try {
      const res = await fetch(`/api/videos/${videoId}/vru/run`, {
        method: "DELETE",
      });
      const payload = await res.json();
      if (!res.ok) throw new Error(payload.error || "Failed to cancel");
      await mutate();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to cancel");
    } finally {
      setIsSubmitting(false);
    }
  }

  if (isLoading) {
    return <Skeleton className="h-40 rounded-xl" />;
  }

  const showRunControls = !isRunning;
  const selectedModel = PIPELINE_MODEL_OPTIONS.find((o) => o.id === modelName);

  const content = (
    <div className="space-y-4">
      {embedded && (
        <div className="flex items-center gap-3">
          <span className="text-lg font-semibold">VRU Labels</span>
          <StatusIndicator status={status} />
        </div>
      )}
        {/* Labels display */}
        {labelsApplied.length > 0 && (
          <div className="flex flex-wrap gap-1.5">
            {labelsApplied.map((label) => (
              <span
                key={label}
                className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-xs font-medium"
                style={{
                  borderColor: `${VRU_LABEL_COLOR_MAP[label] ?? "#888"}33`,
                  backgroundColor: `${VRU_LABEL_COLOR_MAP[label] ?? "#888"}11`,
                  color: VRU_LABEL_COLOR_MAP[label] ?? "#888",
                }}
              >
                <span
                  className="w-1.5 h-1.5 rounded-full"
                  style={{
                    backgroundColor: VRU_LABEL_COLOR_MAP[label] ?? "#888",
                  }}
                />
                {label}
              </span>
            ))}
          </div>
        )}

        {/* Run log */}
        {(status === "processed" || status === "failed") && state && (
          <RunLog state={state} boxes={data?.boxes ?? []} segments={data?.segments ?? []} />
        )}

        {/* Running state */}
        {isRunning && (
          <div className="space-y-3">
            <div className="flex items-center justify-between text-sm">
              <span className="text-muted-foreground">Processing video...</span>
              <Button
                variant="outline"
                size="sm"
                disabled={isSubmitting}
                onClick={handleCancel}
                className="gap-1.5 h-7 text-xs"
              >
                <Square className="w-3 h-3" />
                Cancel
              </Button>
            </div>
            <div className="h-1.5 rounded-full bg-muted overflow-hidden">
              <div className="h-full rounded-full bg-sky-500 animate-progress" />
            </div>
            {state?.modelName && (
              <p className="text-[11px] text-muted-foreground">
                Model: {PIPELINE_MODEL_OPTIONS.find((o) => o.id === state.modelName)?.label ?? state.modelName}
              </p>
            )}
          </div>
        )}

        {/* Error display */}
        {state?.lastError && status === "failed" && (
          <div className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/5 px-3 py-2 text-xs text-red-600 dark:text-red-400">
            <XCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
            <span className="break-all">{state.lastError}</span>
          </div>
        )}

        {actionError && (
          <div className="flex items-start gap-2 rounded-lg border border-red-500/30 bg-red-500/5 px-3 py-2 text-xs text-red-600 dark:text-red-400">
            <XCircle className="w-3.5 h-3.5 mt-0.5 shrink-0" />
            <span>{actionError}</span>
          </div>
        )}

        {/* Run controls */}
        {showRunControls && (
          <div className="flex items-end gap-2">
            <div className="space-y-1.5">
              <label className="flex items-center gap-1.5 text-[11px] font-medium text-muted-foreground">
                <Cpu className="w-3 h-3" />
                Model
              </label>
              <select
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                className="flex h-8 w-[200px] rounded-md border border-input bg-background px-2.5 py-1 text-xs shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                {PIPELINE_MODEL_OPTIONS.map((option) => (
                  <option key={option.id} value={option.id}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>
            <Button
              size="sm"
              disabled={isSubmitting}
              onClick={handleRun}
              className="gap-1.5 h-8 shrink-0"
            >
              {isSubmitting ? (
                <Loader2 className="w-3.5 h-3.5 animate-spin" />
              ) : status === "failed" || status === "processed" ? (
                <RotateCcw className="w-3.5 h-3.5" />
              ) : (
                <Play className="w-3.5 h-3.5" />
              )}
              {status === "failed"
                ? "Retry"
                : status === "processed"
                  ? "Re-run"
                  : "Run"}
            </Button>
            {selectedModel && (
              <span className="text-[10px] text-muted-foreground pb-1.5">
                {selectedModel.description}
              </span>
            )}
          </div>
        )}

        {/* Timeline visualization */}
        {groupedSegments.length > 0 && duration > 0 && (
          <div className="space-y-3 pt-1">
            <div className="flex items-center justify-between text-xs text-muted-foreground">
              <span>Timeline</span>
              <span>{formatTimestamp(duration)}</span>
            </div>
            {groupedSegments.map(([label, segments]) => (
              <div
                key={label}
                className="grid grid-cols-[100px_1fr] items-center gap-3"
              >
                <div className="flex items-center gap-1.5 text-sm font-medium">
                  <span
                    className="w-2 h-2 rounded-full shrink-0"
                    style={{
                      backgroundColor: VRU_LABEL_COLOR_MAP[label] ?? "#334155",
                    }}
                  />
                  {label}
                </div>
                <div className="relative h-7 rounded-md bg-muted mb-5">
                  {segments.map((segment, index) => {
                    const key = `${segment.label}-${segment.startMs}-${segment.endMs}-${index}`;
                    const start = (segment.startMs / 1000 / duration) * 100;
                    const end = (segment.endMs / 1000 / duration) * 100;
                    const width = Math.max(1.5, end - start);
                    const isCurrent =
                      currentTime * 1000 >= segment.startMs &&
                      currentTime * 1000 <= segment.endMs;
                    const isSelected = key === selectedKey;
                    const segLabel = `${formatTimestamp(segment.startMs / 1000)}–${formatTimestamp(segment.endMs / 1000)} · ${Math.round(segment.maxConfidence * 100)}%`;
                    return (
                      <button
                        key={key}
                        type="button"
                        className={cn(
                          "absolute top-1/2 -translate-y-1/2 rounded-sm border transition-all group/seg",
                          (isSelected || isCurrent) ? "h-6 ring-1 ring-white" : "h-4"
                        )}
                        style={{
                          left: `${start}%`,
                          width: `${width}%`,
                          backgroundColor:
                            VRU_LABEL_COLOR_MAP[segment.label] ?? "#334155",
                          borderColor:
                            isSelected || isCurrent ? "#ffffff" : "transparent",
                        }}
                        onClick={() => {
                          setSelectedKey(key);
                          onSeek(segment.startMs / 1000);
                        }}
                      >
                        <span className="absolute left-1/2 -translate-x-1/2 top-full mt-1 whitespace-nowrap rounded bg-popover px-1.5 py-0.5 text-[10px] text-popover-foreground shadow-sm border border-border/50">
                          {segLabel}
                        </span>
                      </button>
                    );
                  })}
                </div>
              </div>
            ))}
          </div>
        )}
    </div>
  );

  if (embedded) return content;

  return (
    <Card className="gap-4">
      <CardHeader>
        <CardTitle className="flex items-center gap-3 text-lg">
          <span>VRU Labels</span>
          <StatusIndicator status={status} />
        </CardTitle>
      </CardHeader>
      <CardContent>{content}</CardContent>
    </Card>
  );
}

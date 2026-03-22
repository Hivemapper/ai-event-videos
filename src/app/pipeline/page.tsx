"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import Image from "next/image";
import Link from "next/link";
import {
  Play,
  Pause,
  Square,
  RotateCcw,
  Clock,
  CheckCircle2,
  XCircle,
  Loader2,
  AlertTriangle,
  Video,
  Zap,
  ChevronDown,
  ChevronRight,
  Calendar,
  Layers,
  Cpu,
} from "lucide-react";
import { Header } from "@/components/layout/header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";
import {
  DEFAULT_PIPELINE_MODEL_NAME,
  PIPELINE_MODEL_OPTIONS,
  VRU_LABEL_COLOR_MAP,
} from "@/lib/pipeline-config";
import { PipelineRunRecord, PipelineVideoRow } from "@/types/pipeline";

/* ─── Formatters ─── */

const videoTimestampFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
});

const pipelineTimeFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

function localDateInputValue() {
  const now = new Date();
  const offsetMs = now.getTimezoneOffset() * 60 * 1000;
  return new Date(now.getTime() - offsetMs).toISOString().slice(0, 10);
}

function formatRelativeDate(value: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function formatVideoTimestamp(value: string) {
  return videoTimestampFormatter.format(new Date(value));
}

function formatPipelineTimestamp(value: string | null) {
  if (!value) return "—";
  return pipelineTimeFormatter.format(new Date(value));
}

function formatEventType(value: string) {
  return value
    .toLowerCase()
    .split("_")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(" ");
}

function formatDuration(startedAt: string | null, completedAt: string | null) {
  if (!startedAt) return "—";
  const start = new Date(startedAt).getTime();
  const end = completedAt ? new Date(completedAt).getTime() : Date.now();
  const secs = Math.floor((end - start) / 1000);
  if (secs < 60) return `${secs}s`;
  const mins = Math.floor(secs / 60);
  const remSecs = secs % 60;
  if (mins < 60) return `${mins}m ${remSecs}s`;
  const hrs = Math.floor(mins / 60);
  const remMins = mins % 60;
  return `${hrs}h ${remMins}m`;
}

/* ─── Status helpers ─── */

function videoStatusConfig(status: string) {
  switch (status) {
    case "processed":
      return {
        bg: "bg-emerald-500/10",
        border: "border-emerald-500/20",
        text: "text-emerald-700 dark:text-emerald-400",
        dot: "bg-emerald-500",
      };
    case "running":
      return {
        bg: "bg-sky-500/10",
        border: "border-sky-500/20",
        text: "text-sky-700 dark:text-sky-400",
        dot: "bg-sky-500",
      };
    case "queued":
      return {
        bg: "bg-amber-500/10",
        border: "border-amber-500/20",
        text: "text-amber-700 dark:text-amber-400",
        dot: "bg-amber-500",
      };
    case "failed":
      return {
        bg: "bg-red-500/10",
        border: "border-red-500/20",
        text: "text-red-600 dark:text-red-400",
        dot: "bg-red-500",
      };
    default:
      return {
        bg: "bg-muted/50",
        border: "border-border",
        text: "text-muted-foreground",
        dot: "bg-muted-foreground/50",
      };
  }
}

function runStatusConfig(status: string | null | undefined) {
  switch (status) {
    case "running":
      return {
        bg: "bg-sky-500/10",
        border: "border-sky-500/30",
        text: "text-sky-700 dark:text-sky-400",
        icon: Loader2,
        iconClass: "animate-spin",
        label: "Running",
      };
    case "paused":
      return {
        bg: "bg-amber-500/10",
        border: "border-amber-500/30",
        text: "text-amber-700 dark:text-amber-400",
        icon: Pause,
        iconClass: "",
        label: "Paused",
      };
    case "completed":
      return {
        bg: "bg-emerald-500/10",
        border: "border-emerald-500/30",
        text: "text-emerald-700 dark:text-emerald-400",
        icon: CheckCircle2,
        iconClass: "",
        label: "Completed",
      };
    case "failed":
      return {
        bg: "bg-red-500/10",
        border: "border-red-500/30",
        text: "text-red-600 dark:text-red-400",
        icon: XCircle,
        iconClass: "",
        label: "Failed",
      };
    case "cancelled":
      return {
        bg: "bg-muted",
        border: "border-border",
        text: "text-muted-foreground",
        icon: Square,
        iconClass: "",
        label: "Cancelled",
      };
    default:
      return {
        bg: "bg-muted/50",
        border: "border-border",
        text: "text-muted-foreground",
        icon: Clock,
        iconClass: "",
        label: "Idle",
      };
  }
}

/* ─── Progress Ring ─── */

function ProgressRing({
  value,
  max,
  size = 120,
  strokeWidth = 10,
  failed = 0,
}: {
  value: number;
  max: number;
  size?: number;
  strokeWidth?: number;
  failed?: number;
}) {
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const pct = max > 0 ? value / max : 0;
  const failPct = max > 0 ? failed / max : 0;
  const successOffset = circumference * (1 - pct);
  const failOffset = circumference * (1 - failPct);

  return (
    <div className="relative" style={{ width: size, height: size }}>
      <svg width={size} height={size} className="-rotate-90">
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth={strokeWidth}
          className="text-muted/60"
        />
        {failed > 0 && (
          <circle
            cx={size / 2}
            cy={size / 2}
            r={radius}
            fill="none"
            stroke="currentColor"
            strokeWidth={strokeWidth}
            strokeDasharray={circumference}
            strokeDashoffset={failOffset}
            strokeLinecap="round"
            className="text-red-500/70 transition-all duration-700"
          />
        )}
        <circle
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          stroke="currentColor"
          strokeWidth={strokeWidth}
          strokeDasharray={circumference}
          strokeDashoffset={successOffset}
          strokeLinecap="round"
          className="text-emerald-500 transition-all duration-700"
        />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-2xl font-bold tabular-nums">
          {max > 0 ? Math.round(pct * 100) : 0}%
        </span>
        <span className="text-[10px] text-muted-foreground">
          {value}/{max}
        </span>
      </div>
    </div>
  );
}

/* ─── Elapsed Timer ─── */

function ElapsedTimer({ startedAt }: { startedAt: string }) {
  const [elapsed, setElapsed] = useState("");
  useEffect(() => {
    const update = () => setElapsed(formatDuration(startedAt, null));
    update();
    const id = setInterval(update, 1000);
    return () => clearInterval(id);
  }, [startedAt]);
  return <span className="tabular-nums">{elapsed}</span>;
}

/* ─── Types ─── */

interface PipelineResponse {
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

/* ─── Main Page ─── */

export default function PipelinePage() {
  const [day, setDay] = useState(localDateInputValue());
  const [batchSize, setBatchSize] = useState("50");
  const [modelName, setModelName] = useState(DEFAULT_PIPELINE_MODEL_NAME);
  const [data, setData] = useState<PipelineResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();
  const [showHistory, setShowHistory] = useState(false);

  const load = async (selectedDay = day) => {
    const apiKey = getApiKey();
    if (!apiKey) {
      setError(
        "Configure your Bee Maps API key in Settings to use the pipeline."
      );
      return;
    }
    try {
      setError(null);
      const response = await fetch(`/api/pipeline/videos?day=${selectedDay}`, {
        headers: { Authorization: apiKey },
      });
      const payload = await response.json();
      if (!response.ok)
        throw new Error(payload.error || "Failed to load pipeline videos");
      setData(payload);
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Failed to load pipeline data"
      );
    }
  };

  useEffect(() => {
    load(day);
    const interval = window.setInterval(() => load(day), 5000);
    return () => window.clearInterval(interval);
  }, [day]);

  const runAction = (
    url: string,
    init?: RequestInit,
    onSuccess?: (payload: { run?: PipelineRunRecord }) => void
  ) => {
    startTransition(() => {
      void (async () => {
        try {
          setError(null);
          const response = await fetch(url, init);
          const payload = await response.json();
          if (!response.ok)
            throw new Error(payload.error || "Pipeline action failed");
          onSuccess?.(payload);
          await load(day);
        } catch (err) {
          setError(
            err instanceof Error ? err.message : "Pipeline action failed"
          );
        }
      })();
    });
  };

  const activeRun = data?.activeRun ?? null;
  const latestRun =
    activeRun?.day === day ? activeRun : (data?.latestRun ?? null);
  const canStart = !activeRun;
  const summary = data?.summary;
  const total = summary?.total ?? 0;
  const processed = summary?.processed ?? 0;
  const failed = summary?.failed ?? 0;
  const remaining = summary?.remaining ?? 0;
  const running = summary?.running ?? 0;
  const queued = summary?.queued ?? 0;
  const isActive = !!activeRun;
  const runConfig = runStatusConfig(latestRun?.status);
  const RunStatusIcon = runConfig.icon;
  const selectedModel = PIPELINE_MODEL_OPTIONS.find(
    (o) => o.id === modelName
  );

  // Sort videos: failed first, then running, queued, unprocessed, processed last
  const sortedVideos = useMemo(() => {
    const order: Record<string, number> = {
      failed: 0,
      running: 1,
      queued: 2,
      stale: 3,
      unprocessed: 4,
      processed: 5,
    };
    return [...(data?.videos ?? [])].sort(
      (a, b) => (order[a.status] ?? 4) - (order[b.status] ?? 4)
    );
  }, [data?.videos]);

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto px-4 py-6 space-y-5">
        {/* ── Header + Controls ── */}
        <div className="space-y-4">
          <div>
            <h1 className="text-2xl font-bold tracking-tight">Pipeline</h1>
            <p className="text-sm text-muted-foreground mt-0.5">
              Auto-drain one day of Bee Maps videos through the local VRU
              worker.
            </p>
          </div>

          <div className="flex flex-wrap items-end gap-3">
            <div className="space-y-1.5">
              <label className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                <Calendar className="w-3 h-3" />
                Day
              </label>
              <Input
                type="date"
                value={day}
                onChange={(e) => setDay(e.target.value)}
                className="w-[160px] h-9"
              />
            </div>
            <div className="space-y-1.5">
              <label className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                <Layers className="w-3 h-3" />
                Batch Size
              </label>
              <Input
                type="number"
                min={1}
                max={500}
                value={batchSize}
                onChange={(e) => setBatchSize(e.target.value)}
                className="w-[100px] h-9"
              />
            </div>
            <div className="space-y-1.5">
              <label className="flex items-center gap-1.5 text-xs font-medium text-muted-foreground">
                <Cpu className="w-3 h-3" />
                Model
              </label>
              <select
                value={modelName}
                onChange={(e) => setModelName(e.target.value)}
                className="flex h-9 w-[200px] rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                {PIPELINE_MODEL_OPTIONS.map((option) => (
                  <option key={option.id} value={option.id}>
                    {option.label}
                  </option>
                ))}
              </select>
            </div>

            <div className="flex items-end gap-2">
              {canStart ? (
                <Button
                  disabled={isPending}
                  onClick={() =>
                    runAction("/api/pipeline/runs", {
                      method: "POST",
                      headers: { "Content-Type": "application/json" },
                      body: JSON.stringify({
                        day,
                        batchSize: Number(batchSize),
                        beeMapsApiKey: getApiKey(),
                        modelName,
                      }),
                    })
                  }
                  className="gap-2"
                >
                  <Play className="w-3.5 h-3.5" />
                  Start Run
                </Button>
              ) : (
                <>
                  {activeRun?.status === "running" && (
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={isPending}
                      className="gap-1.5"
                      onClick={() =>
                        runAction(
                          `/api/pipeline/runs/${activeRun.id}/pause`,
                          { method: "POST" }
                        )
                      }
                    >
                      <Pause className="w-3.5 h-3.5" />
                      Pause
                    </Button>
                  )}
                  {activeRun?.status === "paused" && (
                    <Button
                      variant="outline"
                      size="sm"
                      disabled={isPending}
                      className="gap-1.5"
                      onClick={() =>
                        runAction(
                          `/api/pipeline/runs/${activeRun.id}/resume`,
                          { method: "POST" }
                        )
                      }
                    >
                      <Play className="w-3.5 h-3.5" />
                      Resume
                    </Button>
                  )}
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={isPending}
                    className="gap-1.5"
                    onClick={() =>
                      runAction(
                        `/api/pipeline/runs/${activeRun!.id}/cancel`,
                        { method: "POST" }
                      )
                    }
                  >
                    <Square className="w-3 h-3" />
                    Cancel
                  </Button>
                </>
              )}
              {latestRun && !activeRun && (failed > 0) && (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={isPending}
                  className="gap-1.5"
                  onClick={() =>
                    runAction(
                      `/api/pipeline/runs/${latestRun.id}/retry-failed`,
                      { method: "POST" }
                    )
                  }
                >
                  <RotateCcw className="w-3.5 h-3.5" />
                  Retry Failed
                </Button>
              )}
            </div>
          </div>

          {selectedModel && (
            <p className="text-xs text-muted-foreground -mt-1">
              {selectedModel.description}
            </p>
          )}
        </div>

        {/* ── Alerts ── */}
        {error && (
          <div className="flex items-center gap-2 rounded-lg border border-red-500/30 bg-red-500/5 px-4 py-3 text-sm text-red-600 dark:text-red-400">
            <XCircle className="w-4 h-4 shrink-0" />
            {error}
          </div>
        )}

        {activeRun && activeRun.day !== day && (
          <div className="flex items-center gap-2 rounded-lg border border-amber-500/30 bg-amber-500/5 px-4 py-3 text-sm text-amber-700 dark:text-amber-400">
            <AlertTriangle className="w-4 h-4 shrink-0" />
            A run is active for <strong>{activeRun.day}</strong>. Finish or
            cancel it before starting a new one.
          </div>
        )}

        {/* ── Stats + Progress ── */}
        <div className="grid gap-4 lg:grid-cols-[1fr_auto]">
          <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
            {/* Total */}
            <Card className="border-border/60">
              <CardContent className="pt-4 pb-4 px-4">
                <div className="flex items-center gap-2 text-xs font-medium text-muted-foreground mb-1">
                  <Video className="w-3.5 h-3.5" />
                  Total
                </div>
                <div className="text-2xl font-bold tabular-nums">
                  {total.toLocaleString()}
                </div>
              </CardContent>
            </Card>
            {/* Processed */}
            <Card className="border-emerald-500/20 bg-emerald-500/[0.03]">
              <CardContent className="pt-4 pb-4 px-4">
                <div className="flex items-center gap-2 text-xs font-medium text-emerald-600 dark:text-emerald-400 mb-1">
                  <CheckCircle2 className="w-3.5 h-3.5" />
                  Processed
                </div>
                <div className="text-2xl font-bold tabular-nums text-emerald-700 dark:text-emerald-400">
                  {processed.toLocaleString()}
                </div>
              </CardContent>
            </Card>
            {/* Remaining */}
            <Card className="border-amber-500/20 bg-amber-500/[0.03]">
              <CardContent className="pt-4 pb-4 px-4">
                <div className="flex items-center gap-2 text-xs font-medium text-amber-600 dark:text-amber-400 mb-1">
                  <Clock className="w-3.5 h-3.5" />
                  Remaining
                </div>
                <div className="text-2xl font-bold tabular-nums text-amber-700 dark:text-amber-400">
                  {remaining.toLocaleString()}
                </div>
                {(running > 0 || queued > 0) && (
                  <div className="text-[10px] text-muted-foreground mt-0.5">
                    {running > 0 && `${running} running`}
                    {running > 0 && queued > 0 && " · "}
                    {queued > 0 && `${queued} queued`}
                  </div>
                )}
              </CardContent>
            </Card>
            {/* Failed */}
            <Card
              className={cn(
                "border-border/60",
                failed > 0 && "border-red-500/20 bg-red-500/[0.03]"
              )}
            >
              <CardContent className="pt-4 pb-4 px-4">
                <div
                  className={cn(
                    "flex items-center gap-2 text-xs font-medium mb-1",
                    failed > 0
                      ? "text-red-600 dark:text-red-400"
                      : "text-muted-foreground"
                  )}
                >
                  <XCircle className="w-3.5 h-3.5" />
                  Failed
                </div>
                <div
                  className={cn(
                    "text-2xl font-bold tabular-nums",
                    failed > 0
                      ? "text-red-600 dark:text-red-400"
                      : "text-muted-foreground"
                  )}
                >
                  {failed.toLocaleString()}
                </div>
              </CardContent>
            </Card>
          </div>

          {/* Progress Ring */}
          <Card className="border-border/60 flex items-center justify-center px-6">
            <CardContent className="p-0">
              <ProgressRing
                value={processed}
                max={total}
                failed={failed}
                size={130}
                strokeWidth={12}
              />
            </CardContent>
          </Card>
        </div>

        {/* ── Latest Run ── */}
        <Card
          className={cn(
            "overflow-hidden transition-colors",
            runConfig.border,
            latestRun ? runConfig.bg : ""
          )}
        >
          <div className="px-5 py-4">
            <div className="flex items-center justify-between mb-3">
              <div className="flex items-center gap-2.5">
                <h2 className="text-sm font-semibold">Latest Run</h2>
                <div
                  className={cn(
                    "inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium",
                    runConfig.bg,
                    runConfig.text
                  )}
                >
                  <RunStatusIcon
                    className={cn("w-3 h-3", runConfig.iconClass)}
                  />
                  {runConfig.label}
                </div>
              </div>
              {latestRun?.modelName && (
                <span className="text-xs text-muted-foreground">
                  {
                    PIPELINE_MODEL_OPTIONS.find(
                      (o) => o.id === latestRun.modelName
                    )?.label ?? latestRun.modelName
                  }
                </span>
              )}
            </div>

            <div className="grid gap-4 sm:grid-cols-2 lg:grid-cols-4">
              <div>
                <div className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                  Started
                </div>
                <div className="mt-0.5 text-sm">
                  {formatRelativeDate(latestRun?.startedAt ?? null)}
                </div>
              </div>
              <div>
                <div className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                  {latestRun?.status === "running" ? "Elapsed" : "Duration"}
                </div>
                <div className="mt-0.5 text-sm">
                  {latestRun?.status === "running" && latestRun.startedAt ? (
                    <ElapsedTimer startedAt={latestRun.startedAt} />
                  ) : latestRun?.startedAt ? (
                    formatDuration(latestRun.startedAt, latestRun.completedAt)
                  ) : (
                    "—"
                  )}
                </div>
              </div>
              <div>
                <div className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                  Throughput
                </div>
                <div className="mt-0.5 text-sm flex items-center gap-1.5">
                  <Zap className="w-3 h-3 text-amber-500" />
                  <span className="tabular-nums">
                    {latestRun?.totals.throughputPerHour ?? 0}
                  </span>{" "}
                  videos/hr
                </div>
              </div>
              <div>
                <div className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                  Progress
                </div>
                <div className="mt-0.5 text-sm tabular-nums">
                  {latestRun
                    ? `${latestRun.totals.totalProcessed} / ${latestRun.totals.totalDiscovered}`
                    : "—"}
                </div>
                {latestRun && latestRun.totals.totalDiscovered > 0 && (
                  <div className="mt-1.5 h-1.5 rounded-full bg-muted overflow-hidden">
                    <div
                      className="h-full rounded-full bg-emerald-500 transition-all duration-700"
                      style={{
                        width: `${Math.round(
                          (latestRun.totals.totalProcessed /
                            latestRun.totals.totalDiscovered) *
                            100
                        )}%`,
                      }}
                    />
                  </div>
                )}
              </div>
            </div>
          </div>
        </Card>

        {/* ── Video List ── */}
        <div>
          <div className="flex items-center justify-between mb-3">
            <h2 className="text-sm font-semibold">
              Videos for {day}
              {total > 0 && (
                <span className="ml-2 text-xs font-normal text-muted-foreground">
                  {total} total
                </span>
              )}
            </h2>
            {total > 0 && (
              <div className="flex gap-1.5 text-[10px] text-muted-foreground">
                {processed > 0 && (
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-emerald-500" />
                    {processed} done
                  </span>
                )}
                {failed > 0 && (
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-red-500" />
                    {failed} failed
                  </span>
                )}
                {remaining > 0 && (
                  <span className="flex items-center gap-1">
                    <span className="w-2 h-2 rounded-full bg-muted-foreground/40" />
                    {remaining} remaining
                  </span>
                )}
              </div>
            )}
          </div>

          {sortedVideos.length === 0 ? (
            <div className="rounded-xl border border-dashed border-border/70 bg-muted/10 py-16 text-center">
              <Video className="w-10 h-10 mx-auto text-muted-foreground/30 mb-3" />
              <p className="text-sm text-muted-foreground">
                No events recorded on this day.
              </p>
              <p className="text-xs text-muted-foreground/60 mt-1">
                Try selecting a different date above.
              </p>
            </div>
          ) : (
            <div className="space-y-2">
              {sortedVideos.map((video) => {
                const sc = videoStatusConfig(video.status);
                return (
                  <Link
                    key={video.videoId}
                    href={`/event/${video.videoId}`}
                    target="_blank"
                    rel="noreferrer"
                    className={cn(
                      "group block rounded-xl border p-3 transition-all hover:shadow-sm hover:border-border",
                      sc.border,
                      video.status === "running" || video.status === "failed"
                        ? sc.bg
                        : "bg-card/80 hover:bg-card"
                    )}
                    style={{
                      contentVisibility: "auto",
                      containIntrinsicSize: "80px",
                    }}
                  >
                    <div className="grid gap-3 lg:grid-cols-[100px_minmax(0,1.5fr)_minmax(0,0.8fr)_minmax(0,1fr)] lg:items-center">
                      {/* Thumbnail */}
                      <div className="relative overflow-hidden rounded-lg bg-muted aspect-video lg:aspect-[16/10]">
                        <Image
                          src={`/api/thumbnail?url=${encodeURIComponent(video.videoUrl)}`}
                          alt=""
                          fill
                          unoptimized
                          className="object-cover transition-transform duration-200 group-hover:scale-105"
                        />
                      </div>

                      {/* Event info */}
                      <div className="min-w-0">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-medium truncate">
                            {formatEventType(video.type)}
                          </span>
                          <span className="text-xs text-muted-foreground shrink-0">
                            {formatVideoTimestamp(video.timestamp)}
                          </span>
                        </div>
                        <div className="mt-0.5 text-[11px] font-mono text-muted-foreground/60 truncate">
                          {video.videoId}
                        </div>
                      </div>

                      {/* Status */}
                      <div className="flex items-center gap-3">
                        <div className="flex items-center gap-1.5">
                          <span
                            className={cn(
                              "w-2 h-2 rounded-full shrink-0",
                              sc.dot,
                              video.status === "running" && "animate-pulse"
                            )}
                          />
                          <span
                            className={cn(
                              "text-xs font-medium capitalize",
                              sc.text
                            )}
                          >
                            {video.status}
                          </span>
                        </div>
                        {video.completedAt && (
                          <span className="text-[11px] text-muted-foreground">
                            {formatPipelineTimestamp(video.completedAt)}
                          </span>
                        )}
                        {video.lastError && (
                          <span
                            className="text-[11px] text-red-500 truncate max-w-[180px]"
                            title={video.lastError}
                          >
                            {video.lastError}
                          </span>
                        )}
                      </div>

                      {/* Labels */}
                      <div className="flex flex-wrap gap-1.5">
                        {video.labelsApplied.length > 0 ? (
                          video.labelsApplied.map((label) => (
                            <span
                              key={label}
                              className="inline-flex items-center gap-1 rounded-full border px-2 py-0.5 text-[11px] font-medium"
                              style={{
                                borderColor: `${VRU_LABEL_COLOR_MAP[label] ?? "#888"}33`,
                                backgroundColor: `${VRU_LABEL_COLOR_MAP[label] ?? "#888"}11`,
                                color: VRU_LABEL_COLOR_MAP[label] ?? "#888",
                              }}
                            >
                              <span
                                className="w-1.5 h-1.5 rounded-full"
                                style={{
                                  backgroundColor:
                                    VRU_LABEL_COLOR_MAP[label] ?? "#888",
                                }}
                              />
                              {label}
                            </span>
                          ))
                        ) : (
                          <span className="text-[11px] text-muted-foreground/50">
                            —
                          </span>
                        )}
                      </div>
                    </div>
                  </Link>
                );
              })}
            </div>
          )}
        </div>
      </main>
    </div>
  );
}

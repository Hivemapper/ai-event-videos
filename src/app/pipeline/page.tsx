"use client";

import { KeyboardEvent, useEffect, useMemo, useState, useTransition } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { Header } from "@/components/layout/header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";
import { PipelineDaySummary, PipelineRunRecord } from "@/types/pipeline";

const DEFAULT_BATCH_SIZE = "50";
const dayFormatter = new Intl.DateTimeFormat(undefined, {
  weekday: "short",
  month: "short",
  day: "numeric",
  year: "numeric",
});
const detailTimeFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

interface PipelineDaysResponse {
  days: PipelineDaySummary[];
  activeRun: PipelineRunRecord | null;
}

function formatDayLabel(day: string) {
  return dayFormatter.format(new Date(`${day}T12:00:00`));
}

function formatDetailTime(value: string | null) {
  if (!value) return "—";
  return detailTimeFormatter.format(new Date(value));
}

function formatPercent(value: number | null) {
  if (value === null) return "Count unavailable";
  return `${value.toFixed(value % 1 === 0 ? 0 : 1)}% processed`;
}

function runBadgeClass(status: string | null | undefined) {
  switch (status) {
    case "running":
      return "border-sky-500/20 bg-sky-500/10 text-sky-700";
    case "paused":
      return "border-amber-500/20 bg-amber-500/10 text-amber-700";
    case "failed":
    case "cancelled":
      return "border-destructive/20 bg-destructive/10 text-destructive";
    case "completed":
      return "border-emerald-500/20 bg-emerald-500/10 text-emerald-700";
    default:
      return "border-border bg-background text-foreground";
  }
}

function dayStatus(summary: PipelineDaySummary) {
  if (summary.latestRun?.status) return summary.latestRun.status;
  if ((summary.totalVideos ?? 0) > 0 && summary.processedCount >= (summary.totalVideos ?? 0)) {
    return "completed";
  }
  if (
    summary.processedCount > 0 ||
    summary.failedCount > 0 ||
    summary.runningCount > 0 ||
    summary.queuedCount > 0 ||
    summary.staleCount > 0
  ) {
    return "partial";
  }
  return "idle";
}

function currentWorkLabel(summary: PipelineDaySummary) {
  if (summary.latestRun?.status === "running" && summary.currentVideoId) {
    return `Processing ${summary.currentVideoId}`;
  }
  if (summary.latestRun?.status === "paused" && summary.currentVideoId) {
    return `Paused on ${summary.currentVideoId}`;
  }
  if (summary.latestRun?.status === "queued") {
    return "Queued";
  }
  if (summary.lastCompletedAt) {
    return `Last completed ${formatDetailTime(summary.lastCompletedAt)}`;
  }
  return "No active work";
}

function progressSegments(summary: PipelineDaySummary) {
  const total = summary.totalVideos ?? 0;
  if (total <= 0) {
    return [{ className: "bg-muted-foreground/10", width: 100 }];
  }

  const processed = (summary.processedCount / total) * 100;
  const inProgress = ((summary.runningCount + summary.queuedCount) / total) * 100;
  const failed = (summary.failedCount / total) * 100;
  const stale = (summary.staleCount / total) * 100;
  const accounted = processed + inProgress + failed + stale;

  return [
    { className: "bg-emerald-500", width: processed },
    { className: "bg-sky-500", width: inProgress },
    { className: "bg-destructive", width: failed },
    { className: "bg-amber-500", width: stale },
    { className: "bg-muted-foreground/15", width: Math.max(100 - accounted, 0) },
  ].filter((segment) => segment.width > 0);
}

export default function PipelinePage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const [data, setData] = useState<PipelineDaysResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [batchSizes, setBatchSizes] = useState<Record<string, string>>({});
  const [isPending, startTransition] = useTransition();

  useEffect(() => {
    const legacyDay = searchParams.get("day");
    if (legacyDay) {
      router.replace(`/pipeline/${legacyDay}`);
    }
  }, [router, searchParams]);

  const load = async () => {
    const apiKey = getApiKey();
    if (!apiKey) {
      setError("Configure your Bee Maps API key in Settings to use the pipeline.");
      return;
    }

    try {
      setError(null);
      const response = await fetch("/api/pipeline/days?window=30", {
        headers: { Authorization: apiKey },
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Failed to load pipeline day summaries");
      }
      setData(payload);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to load pipeline data");
    }
  };

  useEffect(() => {
    void load();
    const interval = window.setInterval(() => void load(), 5000);
    return () => window.clearInterval(interval);
  }, []);

  const runAction = (url: string, init?: RequestInit) => {
    startTransition(() => {
      void (async () => {
        try {
          setError(null);
          const response = await fetch(url, init);
          const payload = await response.json();
          if (!response.ok) {
            throw new Error(payload.error || "Pipeline action failed");
          }
          await load();
        } catch (err) {
          setError(err instanceof Error ? err.message : "Pipeline action failed");
        }
      })();
    });
  };

  const activeRun = data?.activeRun ?? null;
  const stats = useMemo(() => {
    const summaries = data?.days ?? [];
    return {
      daysTracked: summaries.length,
      totalVideos: summaries.reduce((sum, day) => sum + (day.totalVideos ?? 0), 0),
      processedVideos: summaries.reduce((sum, day) => sum + day.processedCount, 0),
    };
  }, [data]);

  const navigateToDay = (day: string) => {
    router.push(`/pipeline/${day}`);
  };

  const openDayInNewTab = (day: string) => {
    window.open(`/pipeline/${day}`, "_blank", "noopener,noreferrer");
  };

  const handleRowKeyDown = (event: KeyboardEvent<HTMLElement>, day: string) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      navigateToDay(day);
    }
  };

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto space-y-6 px-4 py-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-1">
            <h1 className="text-2xl font-semibold">Pipeline</h1>
            <p className="text-sm text-muted-foreground">
              Track the last 30 days of Bee Maps videos, see what is in flight, and jump into a
              single day only when you need detail.
            </p>
          </div>
          <div className="flex flex-wrap gap-3 text-sm text-muted-foreground">
            <span>{stats.daysTracked} active days</span>
            <span>{stats.totalVideos.toLocaleString()} videos</span>
            <span>{stats.processedVideos.toLocaleString()} processed</span>
          </div>
        </div>

        {error && (
          <div className="rounded-xl border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        )}

        {activeRun && (
          <div className="rounded-xl border border-sky-500/30 bg-sky-500/8 px-4 py-3 text-sm text-sky-800">
            Active run: {activeRun.day} is currently {activeRun.status}.
          </div>
        )}

        <Card>
          <CardHeader className="pb-3">
            <CardTitle className="text-lg">Daily Queue</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            {(data?.days ?? []).length === 0 ? (
              <div className="rounded-2xl border border-dashed border-border/70 bg-muted/20 px-6 py-10 text-center text-sm text-muted-foreground">
                No days with Bee Maps videos or pipeline activity were found in the last 30 days.
              </div>
            ) : (
              data?.days.map((summary) => {
                const status = dayStatus(summary);
                const batchSize = batchSizes[summary.day] ?? DEFAULT_BATCH_SIZE;
                const isActiveDay = activeRun?.day === summary.day;
                const canStart = !activeRun;
                const canRetry = !activeRun && summary.latestRun && summary.failedCount > 0;

                return (
                  <article
                    key={summary.day}
                    role="link"
                    tabIndex={0}
                    onClick={() => navigateToDay(summary.day)}
                    onKeyDown={(event) => handleRowKeyDown(event, summary.day)}
                    className="cursor-pointer rounded-2xl border border-border/70 bg-card/90 p-4 shadow-sm transition-colors hover:border-border hover:bg-card focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    <div className="grid gap-5 xl:grid-cols-[220px_minmax(0,1fr)_340px] xl:items-center">
                      <div className="space-y-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <h2 className="text-lg font-semibold tracking-tight">
                            {formatDayLabel(summary.day)}
                          </h2>
                          <Badge
                            variant="outline"
                            className={cn("rounded-full px-2.5 py-1 text-xs", runBadgeClass(status))}
                          >
                            {status}
                          </Badge>
                        </div>
                        <div className="space-y-1 text-sm text-muted-foreground">
                          <div>
                            {summary.totalVideos === null
                              ? "Video count unavailable"
                              : `${summary.totalVideos.toLocaleString()} videos`}
                          </div>
                          <div>{formatPercent(summary.processedPercent)}</div>
                          {summary.countError && (
                            <div className="text-amber-700">Bee Maps count temporarily unavailable</div>
                          )}
                        </div>
                      </div>

                      <div className="space-y-3">
                        <div className="flex items-center justify-between gap-4">
                          <div className="text-sm font-medium text-foreground">
                            {formatPercent(summary.processedPercent)}
                          </div>
                          <div className="text-xs uppercase tracking-[0.18em] text-muted-foreground">
                            {summary.processedCount.toLocaleString()} done
                          </div>
                        </div>
                        <div className="flex h-2 overflow-hidden rounded-full bg-muted">
                          {progressSegments(summary).map((segment, index) => (
                            <div
                              key={`${summary.day}-${index}`}
                              className={segment.className}
                              style={{ width: `${segment.width}%` }}
                            />
                          ))}
                        </div>
                        <div className="flex flex-wrap gap-x-4 gap-y-2 text-sm text-muted-foreground">
                          <span>{summary.processedCount.toLocaleString()} processed</span>
                          <span>{(summary.runningCount + summary.queuedCount).toLocaleString()} in flight</span>
                          <span>{summary.failedCount.toLocaleString()} failed</span>
                          <span>{summary.staleCount.toLocaleString()} stale</span>
                          {summary.remainingCount !== null && (
                            <span>{summary.remainingCount.toLocaleString()} remaining</span>
                          )}
                        </div>
                      </div>

                      <div
                        className="space-y-3"
                        onClick={(event) => event.stopPropagation()}
                        onKeyDown={(event) => event.stopPropagation()}
                      >
                        <div className="rounded-xl border border-border/70 bg-background/60 p-3">
                          <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                            Current Work
                          </div>
                          <div className="mt-1 text-sm font-medium text-foreground">
                            {currentWorkLabel(summary)}
                          </div>
                          <div className="mt-1 text-xs text-muted-foreground">
                            {summary.lastCompletedAt
                              ? `Last completion ${formatDetailTime(summary.lastCompletedAt)}`
                              : "Open day for per-video detail"}
                          </div>
                        </div>

                        <div className="flex flex-wrap items-end gap-2">
                          <div className="space-y-1">
                            <label className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                              Batch
                            </label>
                            <Input
                              type="number"
                              min={1}
                              max={500}
                              value={batchSize}
                              onChange={(event) =>
                                setBatchSizes((current) => ({
                                  ...current,
                                  [summary.day]: event.target.value,
                                }))
                              }
                              className="w-24"
                            />
                          </div>

                          {isActiveDay && activeRun?.status === "running" && (
                            <Button
                              variant="outline"
                              disabled={isPending}
                              onClick={() =>
                                runAction(`/api/pipeline/runs/${activeRun.id}/pause`, {
                                  method: "POST",
                                })
                              }
                            >
                              Pause
                            </Button>
                          )}

                          {isActiveDay && activeRun?.status === "paused" && (
                            <Button
                              variant="outline"
                              disabled={isPending}
                              onClick={() =>
                                runAction(`/api/pipeline/runs/${activeRun.id}/resume`, {
                                  method: "POST",
                                })
                              }
                            >
                              Resume
                            </Button>
                          )}

                          {isActiveDay && activeRun && (
                            <Button
                              variant="outline"
                              disabled={isPending}
                              onClick={() =>
                                runAction(`/api/pipeline/runs/${activeRun.id}/cancel`, {
                                  method: "POST",
                                })
                              }
                            >
                              Cancel
                            </Button>
                          )}

                          {!isActiveDay && canStart && (
                            <Button
                              disabled={isPending}
                              onClick={() =>
                                runAction("/api/pipeline/runs", {
                                  method: "POST",
                                  headers: { "Content-Type": "application/json" },
                                  body: JSON.stringify({
                                    day: summary.day,
                                    batchSize: Number(batchSize),
                                    beeMapsApiKey: getApiKey(),
                                  }),
                                })
                              }
                            >
                              Start
                            </Button>
                          )}

                          {!isActiveDay && canRetry && summary.latestRun && (
                            <Button
                              variant="outline"
                              disabled={isPending}
                              onClick={() =>
                                runAction(`/api/pipeline/runs/${summary.latestRun!.id}/retry-failed`, {
                                  method: "POST",
                                })
                              }
                            >
                              Retry Failed
                            </Button>
                          )}

                          <Button
                            variant="ghost"
                            disabled={isPending}
                            onClick={() => openDayInNewTab(summary.day)}
                          >
                            Open Day
                          </Button>
                        </div>
                      </div>
                    </div>
                  </article>
                );
              })
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

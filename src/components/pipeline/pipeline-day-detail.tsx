"use client";

import { useEffect, useMemo, useState, useTransition } from "react";
import Image from "next/image";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";
import { PipelineRunRecord, PipelineVideoRow } from "@/types/pipeline";

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

function formatRelativeDate(value: string | null) {
  if (!value) return "—";
  return new Date(value).toLocaleString();
}

function runStatusTone(status: string | null | undefined) {
  switch (status) {
    case "running":
      return "default";
    case "paused":
      return "secondary";
    case "failed":
    case "cancelled":
      return "destructive";
    default:
      return "outline";
  }
}

function videoStatusTone(status: string) {
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

function videoStatusClass(status: string) {
  switch (status) {
    case "processed":
      return "border-emerald-500/20 bg-emerald-500/10 text-emerald-700";
    case "running":
      return "border-sky-500/20 bg-sky-500/10 text-sky-700";
    case "queued":
      return "border-amber-500/20 bg-amber-500/10 text-amber-700";
    case "failed":
      return "border-destructive/20 bg-destructive/10 text-destructive";
    default:
      return "border-border bg-background text-foreground";
  }
}

function rowClassName(video: PipelineVideoRow) {
  switch (video.status) {
    case "running":
      return "border-sky-500/20 bg-sky-500/[0.04]";
    case "processed":
      return "border-emerald-500/15 bg-emerald-500/[0.03]";
    case "failed":
      return "border-destructive/20 bg-destructive/[0.03]";
    default:
      return "border-border/70 bg-card/90";
  }
}

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

export function PipelineDayDetail({ day }: { day: string }) {
  const [batchSize, setBatchSize] = useState("50");
  const [data, setData] = useState<PipelineResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const load = async () => {
    const apiKey = getApiKey();
    if (!apiKey) {
      setError("Configure your Bee Maps API key in Settings to use the pipeline.");
      return;
    }

    try {
      setError(null);
      const response = await fetch(`/api/pipeline/videos?day=${day}`, {
        headers: { Authorization: apiKey },
      });
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || "Failed to load pipeline videos");
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
  }, [day]);

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
  const latestRun = activeRun?.day === day ? activeRun : data?.latestRun ?? null;
  const canStart = !activeRun;
  const summaryCards = useMemo(
    () => [
      { label: "Total", value: data?.summary.total ?? 0 },
      { label: "Processed", value: data?.summary.processed ?? 0 },
      { label: "Remaining", value: data?.summary.remaining ?? 0 },
      { label: "Failed", value: data?.summary.failed ?? 0 },
    ],
    [data]
  );

  return (
    <div className="min-h-screen bg-background">
      <Header />
      <main className="container mx-auto space-y-6 px-4 py-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-2">
            <Link
              href="/pipeline"
              className="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground"
            >
              ← Back to pipeline days
            </Link>
            <div className="space-y-1">
              <h1 className="text-2xl font-semibold">Pipeline for {day}</h1>
              <p className="text-sm text-muted-foreground">
                Inspect one day of Bee Maps videos and manage its local VRU run.
              </p>
            </div>
          </div>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">Batch Size</label>
              <Input
                type="number"
                min={1}
                max={500}
                value={batchSize}
                onChange={(event) => setBatchSize(event.target.value)}
                className="w-[120px]"
              />
            </div>
            <Button
              disabled={!canStart || isPending}
              onClick={() =>
                runAction("/api/pipeline/runs", {
                  method: "POST",
                  headers: { "Content-Type": "application/json" },
                  body: JSON.stringify({
                    day,
                    batchSize: Number(batchSize),
                    beeMapsApiKey: getApiKey(),
                  }),
                })
              }
            >
              Start Run
            </Button>
            {activeRun?.status === "running" && (
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
            {activeRun?.status === "paused" && (
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
            {activeRun && activeRun.day === day && (
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
            {latestRun && !activeRun && (
              <Button
                variant="outline"
                disabled={isPending}
                onClick={() =>
                  runAction(`/api/pipeline/runs/${latestRun.id}/retry-failed`, {
                    method: "POST",
                  })
                }
              >
                Retry Failed
              </Button>
            )}
          </div>
        </div>

        {error && (
          <div className="rounded-xl border border-destructive/40 bg-destructive/5 px-4 py-3 text-sm text-destructive">
            {error}
          </div>
        )}

        {activeRun && activeRun.day !== day && (
          <div className="rounded-xl border border-amber-500/40 bg-amber-500/10 px-4 py-3 text-sm text-amber-800">
            Another run is active for {activeRun.day}. Finish or cancel it before starting {day}.
          </div>
        )}

        <div className="grid gap-4 md:grid-cols-4">
          {summaryCards.map((card) => (
            <Card key={card.label}>
              <CardHeader className="pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  {card.label}
                </CardTitle>
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-semibold">{card.value.toLocaleString()}</div>
              </CardContent>
            </Card>
          ))}
        </div>

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between gap-3 text-lg">
              <span>Latest Run</span>
              <Badge variant={runStatusTone(latestRun?.status)}>
                {latestRun?.status ?? "idle"}
              </Badge>
            </CardTitle>
          </CardHeader>
          <CardContent className="grid gap-4 md:grid-cols-4">
            <div>
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Started
              </div>
              <div className="mt-1 text-sm">{formatRelativeDate(latestRun?.startedAt ?? null)}</div>
            </div>
            <div>
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Completed
              </div>
              <div className="mt-1 text-sm">{formatRelativeDate(latestRun?.completedAt ?? null)}</div>
            </div>
            <div>
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Throughput
              </div>
              <div className="mt-1 text-sm">
                {latestRun?.totals.throughputPerHour ?? 0} videos/hour
              </div>
            </div>
            <div>
              <div className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Current Video
              </div>
              <div className="mt-1 text-sm font-mono">
                {latestRun?.totals.currentVideoId ?? "—"}
              </div>
            </div>
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle className="text-lg">Videos for {day}</CardTitle>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="hidden items-center gap-4 rounded-xl border border-border/60 bg-muted/30 px-4 py-2 text-[11px] font-medium uppercase tracking-[0.18em] text-muted-foreground lg:grid lg:grid-cols-[136px_minmax(0,1.6fr)_minmax(0,0.9fr)_minmax(0,1.1fr)]">
              <div>Preview</div>
              <div>Event</div>
              <div>Pipeline</div>
              <div>Labels & Errors</div>
            </div>

            {(data?.videos ?? []).length === 0 ? (
              <div className="rounded-2xl border border-dashed border-border/70 bg-muted/20 px-6 py-10 text-center text-sm text-muted-foreground">
                No videos found for this day.
              </div>
            ) : (
              <div className="space-y-3">
                {(data?.videos ?? []).map((video) => (
                  <Link
                    key={video.videoId}
                    href={`/event/${video.videoId}`}
                    target="_blank"
                    rel="noreferrer"
                    className={cn(
                      "block rounded-2xl border p-3 shadow-sm transition-colors hover:border-border hover:bg-card lg:p-4",
                      rowClassName(video)
                    )}
                    style={{
                      contentVisibility: "auto",
                      containIntrinsicSize: "144px",
                    }}
                  >
                    <div className="grid gap-4 lg:grid-cols-[136px_minmax(0,1.6fr)_minmax(0,0.9fr)_minmax(0,1.1fr)] lg:items-center">
                      <div className="group/preview relative block overflow-hidden rounded-xl border border-border/70 bg-muted">
                        <Image
                          src={`/api/thumbnail?url=${encodeURIComponent(video.videoUrl)}`}
                          alt=""
                          width={136}
                          height={80}
                          unoptimized
                          className="h-24 w-full object-cover transition-transform duration-200 group-hover/preview:scale-[1.03] lg:h-20"
                        />
                        <div className="pointer-events-none absolute inset-x-0 bottom-0 bg-gradient-to-t from-black/65 via-black/15 to-transparent px-3 py-2 text-xs font-medium text-white">
                          Open video
                        </div>
                      </div>

                      <div className="min-w-0 space-y-3">
                        <div className="flex flex-wrap items-center gap-2">
                          <Badge
                            variant="outline"
                            className="rounded-full px-2.5 py-1 text-[11px] uppercase tracking-[0.14em] text-muted-foreground"
                          >
                            {video.type.toLowerCase().replaceAll("_", " ")}
                          </Badge>
                          <span className="text-sm text-muted-foreground">
                            {formatVideoTimestamp(video.timestamp)}
                          </span>
                        </div>
                        <div className="min-w-0">
                          <div className="block truncate text-lg font-semibold tracking-tight text-foreground">
                            {formatEventType(video.type)}
                          </div>
                          <div className="mt-1 text-xs font-mono text-muted-foreground/80">
                            {video.videoId}
                          </div>
                        </div>
                      </div>

                      <div className="space-y-3">
                        <div>
                          <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                            Pipeline
                          </div>
                          <div className="mt-2 flex flex-wrap items-center gap-2">
                            <Badge
                              variant={videoStatusTone(video.status)}
                              className={cn(
                                "rounded-full px-3 py-1 text-sm",
                                videoStatusClass(video.status)
                              )}
                            >
                              {video.status}
                            </Badge>
                          </div>
                        </div>
                        <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-1">
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                              Processed
                            </div>
                            <div className="mt-1 text-sm text-foreground">
                              {formatPipelineTimestamp(video.completedAt)}
                            </div>
                          </div>
                          <div>
                            <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                              Error
                            </div>
                            <div className="mt-1 text-sm text-destructive/90">
                              {video.lastError ? (
                                <span className="line-clamp-2">{video.lastError}</span>
                              ) : (
                                <span className="text-muted-foreground">None</span>
                              )}
                            </div>
                          </div>
                        </div>
                      </div>

                      <div className="space-y-3">
                        <div>
                          <div className="text-[11px] font-medium uppercase tracking-[0.16em] text-muted-foreground">
                            VRU Labels
                          </div>
                          <div className="mt-2 flex flex-wrap gap-2">
                            {video.labelsApplied.length > 0 ? (
                              video.labelsApplied.map((label) => (
                                <Badge
                                  key={label}
                                  variant="outline"
                                  className="rounded-full border-border/80 bg-background/80 px-2.5 py-1 text-xs font-medium"
                                >
                                  {label}
                                </Badge>
                              ))
                            ) : (
                              <span className="text-sm text-muted-foreground">
                                No VRU labels yet
                              </span>
                            )}
                          </div>
                        </div>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            )}
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

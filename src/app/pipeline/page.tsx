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
import { PipelineRunRecord, PipelineVideoRow } from "@/types/pipeline";

function localDateInputValue() {
  const now = new Date();
  const offsetMs = now.getTimezoneOffset() * 60 * 1000;
  return new Date(now.getTime() - offsetMs).toISOString().slice(0, 10);
}

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

export default function PipelinePage() {
  const [day, setDay] = useState(localDateInputValue());
  const [batchSize, setBatchSize] = useState("50");
  const [data, setData] = useState<PipelineResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [isPending, startTransition] = useTransition();

  const load = async (selectedDay = day) => {
    const apiKey = getApiKey();
    if (!apiKey) {
      setError("Configure your Bee Maps API key in Settings to use the pipeline.");
      return;
    }

    try {
      setError(null);
      const response = await fetch(`/api/pipeline/videos?day=${selectedDay}`, {
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
          if (!response.ok) {
            throw new Error(payload.error || "Pipeline action failed");
          }
          onSuccess?.(payload);
          await load(day);
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
      <main className="container mx-auto px-4 py-6 space-y-6">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="space-y-1">
            <h1 className="text-2xl font-semibold">Pipeline</h1>
            <p className="text-sm text-muted-foreground">
              Auto-drain one day of Bee Maps videos through the local VRU worker.
            </p>
          </div>
          <div className="flex flex-col gap-3 sm:flex-row sm:items-end">
            <div className="space-y-1">
              <label className="text-xs font-medium text-muted-foreground">Day</label>
              <Input
                type="date"
                value={day}
                onChange={(event) => setDay(event.target.value)}
                className="w-[180px]"
              />
            </div>
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
                Pause {activeRun.day !== day ? activeRun.day : ""}
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
                Resume {activeRun.day !== day ? activeRun.day : ""}
              </Button>
            )}
            {activeRun && (
              <Button
                variant="outline"
                disabled={isPending}
                onClick={() =>
                  runAction(`/api/pipeline/runs/${activeRun.id}/cancel`, {
                    method: "POST",
                  })
                }
              >
                Cancel {activeRun.day !== day ? activeRun.day : ""}
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
          <CardContent>
            <div className="overflow-x-auto">
              <table className="w-full min-w-[900px] text-sm">
                <thead>
                  <tr className="border-b text-left text-muted-foreground">
                    <th className="px-2 py-3 font-medium">Preview</th>
                    <th className="px-2 py-3 font-medium">Timestamp</th>
                    <th className="px-2 py-3 font-medium">Type</th>
                    <th className="px-2 py-3 font-medium">Status</th>
                    <th className="px-2 py-3 font-medium">Labels</th>
                    <th className="px-2 py-3 font-medium">Processed At</th>
                    <th className="px-2 py-3 font-medium">Error</th>
                  </tr>
                </thead>
                <tbody>
                  {(data?.videos ?? []).map((video) => (
                    <tr key={video.videoId} className="border-b align-top">
                      <td className="px-2 py-3">
                        <Link href={`/event/${video.videoId}`} className="block">
                          <Image
                            src={`/api/thumbnail?url=${encodeURIComponent(video.videoUrl)}`}
                            alt=""
                            width={112}
                            height={64}
                            unoptimized
                            className="h-16 w-28 rounded-md object-cover bg-muted"
                          />
                        </Link>
                      </td>
                      <td className="px-2 py-3">{new Date(video.timestamp).toLocaleString()}</td>
                      <td className="px-2 py-3">{video.type}</td>
                      <td className="px-2 py-3">
                        <Badge variant={videoStatusTone(video.status)}>{video.status}</Badge>
                      </td>
                      <td className="px-2 py-3">
                        <div className="flex flex-wrap gap-1">
                          {video.labelsApplied.length > 0 ? (
                            video.labelsApplied.map((label) => (
                              <Badge key={label} variant="outline" className="text-xs">
                                {label}
                              </Badge>
                            ))
                          ) : (
                            <span className="text-muted-foreground">—</span>
                          )}
                        </div>
                      </td>
                      <td className="px-2 py-3">{formatRelativeDate(video.completedAt)}</td>
                      <td className="px-2 py-3 text-destructive/90">
                        {video.lastError ?? "—"}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </CardContent>
        </Card>
      </main>
    </div>
  );
}

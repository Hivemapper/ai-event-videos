"use client";

import { useState, useCallback, useEffect, use } from "react";
import Link from "next/link";
import useSWR from "swr";
import {
  ChevronLeft,
  ChevronRight,
  Pencil,
  CircleAlert,
  CircleCheck,
  Clock,
  Loader2,
  Play,
  Server,
  Shield,
  FileJson,
  Timer,
  TrendingUp,
  Upload,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { cn } from "@/lib/utils";
import { getS3Bucket } from "@/lib/api";

const PAGE_SIZE = 50;
const fetcher = (url: string) => fetch(url).then((r) => r.json());

type Tab = "queued" | "processing" | "completed" | "failed";
const VALID_TABS: Tab[] = ["queued", "processing", "completed", "failed"];

const TABS: { value: Tab; label: string; icon: typeof Clock; color: string }[] = [
  { value: "queued", label: "Queued", icon: Clock, color: "" },
  { value: "processing", label: "Processing", icon: Loader2, color: "text-blue-600" },
  { value: "completed", label: "Completed", icon: CircleCheck, color: "text-green-600" },
  { value: "failed", label: "Failed", icon: CircleAlert, color: "text-red-600" },
];

function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr.endsWith("Z") ? dateStr : dateStr + "Z");
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatDuration(
  startedAt: string | null | undefined,
  completedAt: string | null | undefined
): string {
  if (!startedAt || !completedAt) return "—";
  const start = new Date(startedAt.endsWith("Z") ? startedAt : startedAt + "Z");
  const end = new Date(completedAt.endsWith("Z") ? completedAt : completedAt + "Z");
  const sec = Math.round((end.getTime() - start.getTime()) / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

function StepBadge({ status, label }: { status: string; label: string }) {
  const colors: Record<string, string> = {
    pending: "bg-muted text-muted-foreground border-muted",
    processing: "bg-blue-50 text-blue-700 border-blue-200",
    completed: "bg-green-50 text-green-700 border-green-200",
    failed: "bg-red-50 text-red-700 border-red-200",
  };
  return (
    <Badge variant="outline" className={cn("text-xs gap-1", colors[status] ?? "")}>
      {status === "processing" && <Loader2 className="w-3 h-3 animate-spin" />}
      {status === "completed" && <CircleCheck className="w-3 h-3" />}
      {status === "failed" && <CircleAlert className="w-3 h-3" />}
      {label}
    </Badge>
  );
}

export default function ProductionPipelineTabPage({
  params,
}: {
  params: Promise<{ tab: string }>;
}) {
  const { tab: rawTab } = use(params);
  const tab: Tab = VALID_TABS.includes(rawTab as Tab)
    ? (rawTab as Tab)
    : "queued";

  const [offset, setOffset] = useState(0);
  const [isEnqueuing, setIsEnqueuing] = useState(false);
  const [enqueueResult, setEnqueueResult] = useState<{ enqueued: number; remaining: number } | null>(null);
  const [s3Bucket, setS3BucketState] = useState("");

  useEffect(() => {
    setS3BucketState(getS3Bucket());
  }, []);

  useEffect(() => {
    setOffset(0);
  }, [tab]);

  const { data: stats } = useSWR<{
    machines: string[];
    machineCount: number;
    ratePerHour: number;
    last10m: number;
    last30m: number;
    last60m: number;
    queued: number;
    etaHours: number | null;
    avgSecs: number;
  }>("/api/production-pipeline/stats", fetcher, {
    refreshInterval: 10000,
    revalidateOnFocus: false,
  });

  const url = `/api/production-pipeline?tab=${tab}&limit=${PAGE_SIZE}&offset=${offset}`;
  const { data, isLoading, mutate } = useSWR<{
    counts: Record<string, number>;
    rows: Record<string, unknown>[];
    total: number;
  }>(url, fetcher, {
    refreshInterval: 5000,
    revalidateOnFocus: false,
  });

  const counts = data?.counts ?? {};
  const rows = data?.rows ?? [];
  const total = data?.total ?? 0;
  const pageNum = Math.floor(offset / PAGE_SIZE) + 1;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  const handleEnqueue = useCallback(async () => {
    setIsEnqueuing(true);
    setEnqueueResult(null);
    try {
      const resp = await fetch("/api/production-pipeline/enqueue?limit=500", { method: "POST" });
      const result = await resp.json();
      setEnqueueResult(result);
      mutate();
    } finally {
      setIsEnqueuing(false);
    }
  }, [mutate]);

  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-xl font-semibold">Production Pipeline</h1>
            <p className="text-sm text-muted-foreground mt-1">
              Privacy blur, metadata generation, and S3 upload
            </p>
            {s3Bucket && (
              <p className="text-xs text-muted-foreground mt-0.5 font-mono flex items-center gap-1.5">
                s3://{s3Bucket}/
                <Link href="/settings?tab=pipeline" className="text-primary hover:text-primary/80">
                  <Pencil className="w-3 h-3" />
                </Link>
              </p>
            )}
          </div>
          <div className="flex items-center gap-2">
            <Button
              variant="default"
              size="sm"
              onClick={handleEnqueue}
              disabled={isEnqueuing}
            >
              {isEnqueuing ? (
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              ) : (
                <Play className="w-4 h-4 mr-2" />
              )}
              Enqueue Events
            </Button>
          </div>
        </div>

        {enqueueResult && (
          <div className="mb-4 flex items-center gap-2 rounded-lg border border-green-200 bg-green-50 px-4 py-2 text-sm text-green-800">
            <CircleCheck className="w-4 h-4" />
            Enqueued {enqueueResult.enqueued.toLocaleString()} events.
            {enqueueResult.remaining > 0 && (
              <span className="text-green-600">
                {" "}({enqueueResult.remaining.toLocaleString()} remaining — click again to enqueue more)
              </span>
            )}
          </div>
        )}

        {/* Tab cards */}
        <div className="grid grid-cols-4 gap-3 mb-6">
          {TABS.map(({ value, label, icon: Icon, color }) => {
            const count = counts[value] ?? 0;
            const isActive = tab === value;
            return (
              <Link
                key={value}
                href={`/production-pipeline/${value}`}
                className={cn(
                  "rounded-lg border px-4 py-3 text-left transition-all",
                  isActive
                    ? "ring-2 ring-primary border-primary"
                    : "hover:border-foreground/20",
                  value === "processing" && count > 0 && "bg-blue-50 border-blue-200",
                  value === "completed" && count > 0 && "bg-green-50/50 border-green-200"
                )}
              >
                <div className="flex items-center gap-2">
                  <Icon
                    className={cn(
                      "w-4 h-4",
                      color,
                      value === "processing" && count > 0 && "animate-spin"
                    )}
                  />
                  <span className="text-xs text-muted-foreground">{label}</span>
                </div>
                <p className="text-2xl font-semibold tabular-nums mt-1">
                  {count.toLocaleString()}
                </p>
              </Link>
            );
          })}
        </div>

        {/* Processing stats */}
        {stats && (
          <div className="mb-4 grid grid-cols-4 gap-3">
            <div className="flex items-center gap-3 rounded-lg border px-4 py-3">
              <Server className={cn("w-5 h-5", stats.machineCount > 0 ? "text-green-600" : "text-muted-foreground")} />
              <div>
                <p className="text-xs text-muted-foreground">Active Servers</p>
                <p className="text-lg font-semibold tabular-nums">{stats.machineCount}</p>
                {stats.machines.length > 0 && (
                  <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                    {stats.machines.map((m: string) => m.replace("ip-", "")).join(", ")}
                  </p>
                )}
              </div>
            </div>
            <div className="flex items-center gap-3 rounded-lg border px-4 py-3">
              <TrendingUp className={cn("w-5 h-5", stats.ratePerHour > 0 ? "text-blue-600" : "text-muted-foreground")} />
              <div>
                <p className="text-xs text-muted-foreground">Processing Rate</p>
                <p className="text-lg font-semibold tabular-nums">
                  {stats.ratePerHour.toLocaleString()}<span className="text-sm font-normal text-muted-foreground">/hr</span>
                </p>
                <p className="text-xs text-muted-foreground">
                  {stats.last10m} last 10m · {stats.last30m} last 30m
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3 rounded-lg border px-4 py-3">
              <Timer className={cn("w-5 h-5", stats.etaHours ? "text-orange-500" : "text-muted-foreground")} />
              <div>
                <p className="text-xs text-muted-foreground">Est. Time Remaining</p>
                <p className="text-lg font-semibold tabular-nums">
                  {stats.etaHours != null && stats.ratePerHour > 0
                    ? stats.etaHours < 24
                      ? `${stats.etaHours.toFixed(1)} hrs`
                      : `${(stats.etaHours / 24).toFixed(1)} days`
                    : "—"}
                </p>
                <p className="text-xs text-muted-foreground">
                  {stats.queued.toLocaleString()} remaining
                </p>
              </div>
            </div>
            <div className="flex items-center gap-3 rounded-lg border px-4 py-3">
              <Clock className={cn("w-5 h-5", stats.avgSecs > 0 ? "text-purple-500" : "text-muted-foreground")} />
              <div>
                <p className="text-xs text-muted-foreground">Avg. Time / Video</p>
                <p className="text-lg font-semibold tabular-nums">
                  {stats.avgSecs > 0
                    ? stats.avgSecs < 60
                      ? `${stats.avgSecs}s`
                      : `${Math.floor(stats.avgSecs / 60)}m ${stats.avgSecs % 60}s`
                    : "—"}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Table */}
        {isLoading ? (
          <div className="space-y-3">
            {Array.from({ length: 10 }).map((_, i) => (
              <Skeleton key={i} className="h-12 w-full" />
            ))}
          </div>
        ) : rows.length === 0 ? (
          <div className="text-center py-16 text-muted-foreground">
            {tab === "queued" && "No events waiting for production processing."}
            {tab === "processing" && "No events currently being processed."}
            {tab === "completed" && "No completed production runs yet."}
            {tab === "failed" && "No failed production runs."}
          </div>
        ) : (
          <>
            <div className="rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40">
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">#</th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Event</th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Type</th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Road</th>
                    <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Speed</th>
                    {(tab === "processing" || tab === "failed") && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Steps</th>
                    )}
                    {tab === "completed" && (
                      <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Duration</th>
                    )}
                    {tab === "failed" && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Error</th>
                    )}
                    {tab !== "queued" && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Machine</th>
                    )}
                    <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Date</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => (
                    <tr
                      key={String(r.id) + String(r.run_id ?? i)}
                      className="border-b last:border-0 hover:bg-muted/20 transition-colors"
                    >
                      <td className="px-4 py-2.5 text-muted-foreground tabular-nums">
                        {offset + i + 1}
                      </td>
                      <td className="px-4 py-2.5">
                        <Link
                          href={`/event/${r.id}`}
                          className="font-mono text-primary hover:underline"
                        >
                          {String(r.id).slice(0, 16)}…
                        </Link>
                      </td>
                      <td className="px-4 py-2.5 text-xs text-muted-foreground">
                        {String(r.event_type ?? "").replace(/_/g, " ")}
                      </td>
                      <td className="px-4 py-2.5 text-xs text-muted-foreground">
                        {r.road_class ? String(r.road_class).replace(/_/g, " ") : "—"}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                        {r.speed_min != null && r.speed_max != null
                          ? `${r.speed_min}–${r.speed_max} mph`
                          : "—"}
                      </td>
                      {(tab === "processing" || tab === "failed") && (
                        <td className="px-4 py-2.5">
                          <div className="flex items-center gap-1.5">
                            <StepBadge status={String(r.privacy_status ?? "pending")} label="Privacy" />
                            <StepBadge status={String(r.metadata_status ?? "pending")} label="Meta" />
                            <StepBadge status={String(r.upload_status ?? "pending")} label="S3" />
                          </div>
                        </td>
                      )}
                      {tab === "completed" && (
                        <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                          {formatDuration(r.started_at as string, r.completed_at as string)}
                        </td>
                      )}
                      {tab === "failed" && (
                        <td className="px-4 py-2.5 text-xs text-red-600 max-w-[200px] truncate">
                          {String(r.last_error ?? "Unknown error")}
                        </td>
                      )}
                      {tab !== "queued" && (
                        <td className="px-4 py-2.5 text-xs text-muted-foreground">
                          {r.machine_id ? String(r.machine_id) : "—"}
                        </td>
                      )}
                      <td className="px-4 py-2.5 text-right text-muted-foreground">
                        {formatDate(
                          tab === "completed"
                            ? (r.completed_at as string)
                            : (r.event_timestamp as string)
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {totalPages > 1 && (
              <div className="flex items-center justify-between mt-4">
                <p className="text-sm text-muted-foreground">
                  {total.toLocaleString()} results
                </p>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={offset === 0}
                    onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                  >
                    <ChevronLeft className="w-4 h-4 mr-1" />
                    Prev
                  </Button>
                  <span className="text-sm text-muted-foreground">
                    Page {pageNum} of {totalPages}
                  </span>
                  <Button
                    variant="outline"
                    size="sm"
                    disabled={offset + PAGE_SIZE >= total}
                    onClick={() => setOffset(offset + PAGE_SIZE)}
                  >
                    Next
                    <ChevronRight className="w-4 h-4 ml-1" />
                  </Button>
                </div>
              </div>
            )}
          </>
        )}
      </main>
    </>
  );
}

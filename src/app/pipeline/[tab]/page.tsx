"use client";

import { useState, useCallback, useEffect, use } from "react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import useSWR from "swr";
import {
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  CircleCheck,
  Clock,
  Loader2,
  Play,
  Server,
  Square,
  TrendingUp,
  Timer,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { cn } from "@/lib/utils";

const PAGE_SIZE = 50;
const fetcher = (url: string) => fetch(url).then((r) => r.json());

type Tab = "queued" | "running" | "completed" | "failed";
const VALID_TABS: Tab[] = ["queued", "running", "completed", "failed"];

const TABS: { value: Tab; label: string; icon: typeof Clock; color: string }[] = [
  { value: "queued", label: "Queued", icon: Clock, color: "" },
  { value: "running", label: "In Progress", icon: Loader2, color: "text-blue-600" },
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
  const start = new Date(
    startedAt.endsWith("Z") ? startedAt : startedAt + "Z"
  );
  const end = new Date(
    completedAt.endsWith("Z") ? completedAt : completedAt + "Z"
  );
  const sec = Math.round((end.getTime() - start.getTime()) / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

export default function PipelineTabPage({
  params,
}: {
  params: Promise<{ tab: string }>;
}) {
  const { tab: rawTab } = use(params);
  const tab: Tab = VALID_TABS.includes(rawTab as Tab)
    ? (rawTab as Tab)
    : "queued";
  const router = useRouter();

  const [offset, setOffset] = useState(0);
  const [isStarting, setIsStarting] = useState(false);
  const [isStopping, setIsStopping] = useState(false);

  // Reset offset when tab changes via URL
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
    lastCompletedAt: string | null;
  }>("/api/pipeline/stats", fetcher, {
    refreshInterval: 10000,
    revalidateOnFocus: false,
  });

  const url = `/api/pipeline?tab=${tab}&limit=${PAGE_SIZE}&offset=${offset}`;
  const { data, isLoading, mutate } = useSWR<{
    counts: Record<string, number>;
    rows: Record<string, unknown>[];
    total: number;
    pipelineRunning: boolean;
  }>(url, fetcher, {
    refreshInterval: 3000,
    revalidateOnFocus: false,
  });

  const counts = data?.counts ?? {};
  const autoRun = data?.pipelineRunning ?? false;
  const rows = data?.rows ?? [];
  const total = data?.total ?? 0;
  const pageNum = Math.floor(offset / PAGE_SIZE) + 1;
  const totalPages = Math.ceil(total / PAGE_SIZE);
  const isRunning = (counts.running ?? 0) > 0;
  const hasQueued = (counts.queued ?? 0) > 0;

  const handleStartPipeline = useCallback(async () => {
    setIsStarting(true);
    try {
      await fetch("/api/pipeline/start", { method: "POST" });
      router.push("/pipeline/running");
      mutate();
    } finally {
      setIsStarting(false);
    }
  }, [mutate, router]);

  const handleRunOne = useCallback(async () => {
    setIsStarting(true);
    try {
      // Use the old direct approach for single run
      const db = await fetch("/api/pipeline/start", { method: "POST" });
      if (db.ok) router.push("/pipeline/running");
      mutate();
    } finally {
      setIsStarting(false);
    }
  }, [mutate, router]);

  const handleStop = useCallback(async () => {
    setIsStopping(true);
    try {
      await fetch("/api/pipeline/stop", { method: "POST" });
      mutate();
    } finally {
      setIsStopping(false);
    }
  }, [mutate]);

  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1 className="text-xl font-semibold">VRU Pipeline</h1>
            <p className="text-sm text-muted-foreground mt-1">
              VRU detection on triaged signal events
            </p>
          </div>
          <div className="flex items-center gap-2">
            {autoRun ? (
              <Button
                variant="destructive"
                size="sm"
                onClick={handleStop}
                disabled={isStopping}
              >
                {isStopping ? (
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                ) : (
                  <Square className="w-4 h-4 mr-2" />
                )}
                Stop Pipeline
              </Button>
            ) : (
              <>
                <Button
                  variant="default"
                  size="sm"
                  onClick={handleStartPipeline}
                  disabled={isStarting || !hasQueued}
                >
                  <Play className="w-4 h-4 mr-2" />
                  Run Pipeline
                </Button>
                <Button
                  variant="outline"
                  size="sm"
                  onClick={handleRunOne}
                  disabled={isStarting || isRunning || !hasQueued}
                >
                  {isStarting ? (
                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  ) : (
                    <Play className="w-4 h-4 mr-2" />
                  )}
                  Run One
                </Button>
                {isRunning && (
                  <Button
                    variant="destructive"
                    size="sm"
                    onClick={handleStop}
                    disabled={isStopping}
                  >
                    <Square className="w-4 h-4 mr-2" />
                    Stop
                  </Button>
                )}
              </>
            )}
          </div>
        </div>

        {/* Tab cards */}
        <div className="grid grid-cols-4 gap-3 mb-6">
          {TABS.map(({ value, label, icon: Icon, color }) => {
            const count = counts[value] ?? 0;
            const isActive = tab === value;
            return (
              <Link
                key={value}
                href={`/pipeline/${value}`}
                className={cn(
                  "rounded-lg border px-4 py-3 text-left transition-all",
                  isActive
                    ? "ring-2 ring-primary border-primary"
                    : "hover:border-foreground/20",
                  value === "running" &&
                    count > 0 &&
                    "bg-blue-50 border-blue-200",
                  value === "completed" &&
                    count > 0 &&
                    "bg-green-50/50 border-green-200"
                )}
              >
                <div className="flex items-center gap-2">
                  <Icon
                    className={cn(
                      "w-4 h-4",
                      color,
                      value === "running" && count > 0 && "animate-spin"
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
          <div className="mb-4 grid grid-cols-3 gap-3">
            <div className="flex items-center gap-3 rounded-lg border px-4 py-3">
              <Server className={cn("w-5 h-5", stats.machineCount > 0 ? "text-green-600" : "text-muted-foreground")} />
              <div>
                <p className="text-xs text-muted-foreground">Active Servers</p>
                <p className="text-lg font-semibold tabular-nums">{stats.machineCount}</p>
                {stats.machines.length > 0 && (
                  <p className="text-xs text-muted-foreground truncate max-w-[200px]">
                    {stats.machines.map((m) => m.replace("ip-", "")).join(", ")}
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
          </div>
        )}

        {/* Auto-run indicator */}
        {autoRun && (
          <div className="mb-4 flex items-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-4 py-2 text-sm text-blue-800">
            <Loader2 className="w-4 h-4 animate-spin" />
            Pipeline is running continuously — processing events automatically.
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
            {tab === "queued" && "No signal events waiting for detection."}
            {tab === "running" && "No detection runs in progress."}
            {tab === "completed" && "No completed detection runs yet."}
            {tab === "failed" && "No failed detection runs."}
          </div>
        ) : (
          <>
            <div className="rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40">
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                      #
                    </th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                      Event
                    </th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                      Type
                    </th>
                    <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                      Road
                    </th>
                    <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">
                      Speed
                    </th>
                    {tab === "completed" && (
                      <>
                        <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">
                          Detections
                        </th>
                        <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">
                          Duration
                        </th>
                      </>
                    )}
                    {tab === "failed" && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                        Error
                      </th>
                    )}
                    {tab === "running" && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                        Status
                      </th>
                    )}
                    {tab !== "queued" && (
                      <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">
                        Machine
                      </th>
                    )}
                    <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">
                      Date
                    </th>
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
                        {r.road_class
                          ? String(r.road_class).replace(/_/g, " ")
                          : "—"}
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                        {r.speed_min != null && r.speed_max != null
                          ? `${r.speed_min}–${r.speed_max} mph`
                          : "—"}
                      </td>
                      {tab === "completed" && (
                        <>
                          <td className="px-4 py-2.5 text-right tabular-nums">
                            <Badge
                              variant="outline"
                              className="bg-green-50 text-green-700 border-green-200"
                            >
                              {String(r.detection_count ?? 0)}
                            </Badge>
                          </td>
                          <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                            {formatDuration(
                              r.started_at as string,
                              r.completed_at as string
                            )}
                          </td>
                        </>
                      )}
                      {tab === "failed" && (
                        <td className="px-4 py-2.5 text-xs text-red-600 max-w-[200px] truncate">
                          {String(r.last_error ?? "Unknown error")}
                        </td>
                      )}
                      {tab === "running" && (
                        <td className="px-4 py-2.5">
                          <Badge
                            variant="outline"
                            className="bg-blue-50 text-blue-700 border-blue-200 gap-1"
                          >
                            <Loader2 className="w-3 h-3 animate-spin" />
                            {String(r.run_status ?? "running")}
                          </Badge>
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

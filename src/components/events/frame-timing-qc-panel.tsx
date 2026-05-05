"use client";

import { useEffect, useMemo, useState } from "react";
import useSWR from "swr";
import { Activity, AlertTriangle, CheckCircle2, Loader2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export interface FrameTimingQc {
  video_id: string | null;
  firmware_version: string | null;
  bucket: "perfect" | "ok" | "filter_out";
  frame_count: number;
  duration_s: number;
  effective_fps: number;
  gap_pct: number;
  single_gaps: number;
  double_gaps: number;
  triple_plus_gaps: number;
  max_delta_ms: number;
  late_frames: number;
  max_late_frames_per_2s: number;
  late_frame_clusters: number;
  non_monotonic_deltas: number;
  failed_rules: string[];
  probe_status: "ok" | "failed";
  probe_error: string | null;
  deltas_ms: number[];
  updated_at?: string;
}

const fetcher = (url: string) => fetch(url).then((r) => r.json() as Promise<{ qc: FrameTimingQc | null }>);

export async function probeFrameTimingQc(
  videoId: string,
  videoUrl: string,
  firmwareVersion: string | null | undefined
): Promise<{ qc: FrameTimingQc | null }> {
  const response = await fetch(`/api/videos/${videoId}/frame-timing-qc`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ videoUrl, firmwareVersion, force: true }),
  });
  const json = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(typeof json.error === "string" ? json.error : `Frame timing QC failed: ${response.status}`);
  }
  return { qc: json.qc ?? null };
}

export function bucketLabel(bucket: FrameTimingQc["bucket"]): string {
  if (bucket === "filter_out") return "Filter Out";
  return bucket === "perfect" ? "Perfect" : "Acceptable";
}

function bucketClasses(bucket: FrameTimingQc["bucket"]): string {
  if (bucket === "perfect") return "bg-green-50 text-green-700 border-green-200";
  if (bucket === "ok") return "bg-amber-50 text-amber-700 border-amber-200";
  return "bg-teal-50 text-teal-700 border-teal-200";
}

function ruleLabel(rule: string): string {
  return rule.replace(/^frame_timing:/, "").replace(/_/g, " ");
}

function fmt(value: number, digits = 1): string {
  if (!Number.isFinite(value)) return "—";
  return value.toFixed(digits);
}

function deltaColor(delta: number): string {
  if (delta > 100) return "rgb(239, 68, 68)";
  if (delta > 90) return "rgb(239, 68, 68)";
  if (delta > 66) return "rgb(249, 115, 22)";
  if (delta > 50) return "rgb(245, 158, 11)";
  return "rgb(34, 197, 94)";
}

const X_AXIS_TICK_SECONDS = 2;

function FrameDeltaChart({
  deltas,
  durationSeconds,
  currentTimeSeconds,
  incidentTimeSeconds,
}: {
  deltas: number[];
  durationSeconds: number;
  currentTimeSeconds?: number;
  incidentTimeSeconds?: number | null;
}) {
  const width = 720;
  const height = 220;
  const padding = { top: 14, right: 16, bottom: 28, left: 48 };
  const plotWidth = width - padding.left - padding.right;
  const plotHeight = height - padding.top - padding.bottom;
  const maxDelta = Math.max(140, ...deltas);
  const yMax = Math.ceil(maxDelta / 20) * 20;
  const derivedDurationSeconds = deltas.reduce((sum, delta) => sum + Math.max(delta, 0), 0) / 1000;
  const chartDurationSeconds = Number.isFinite(durationSeconds) && durationSeconds > 0
    ? durationSeconds
    : derivedDurationSeconds;
  const elapsedSeconds = deltas.reduce<number[]>((times, delta, index) => {
    const previous = index === 0 ? 0 : times[index - 1];
    times.push(previous + Math.max(delta, 0) / 1000);
    return times;
  }, []);
  const xForSecond = (second: number) => padding.left + (
    chartDurationSeconds <= 0
      ? 0
      : Math.min(Math.max(second / chartDurationSeconds, 0), 1) * plotWidth
  );
  const xFor = (index: number) => {
    const fallbackSecond = deltas.length <= 1 ? 0 : (index / (deltas.length - 1)) * chartDurationSeconds;
    return xForSecond(elapsedSeconds[index] ?? fallbackSecond);
  };
  const yFor = (delta: number) => padding.top + plotHeight * (1 - Math.min(delta / yMax, 1));
  const path = deltas
    .map((delta, index) => `${index === 0 ? "M" : "L"}${xFor(index).toFixed(1)},${yFor(delta).toFixed(1)}`)
    .join(" ");
  const gridTicks = [33.33, 50, 66.67, 100, 133.33].filter((tick) => tick <= yMax);
  const xLabels = Array.from(
    { length: Math.floor(chartDurationSeconds / X_AXIS_TICK_SECONDS) + 1 },
    (_, index) => index * X_AXIS_TICK_SECONDS
  );
  const currentSecond =
    typeof currentTimeSeconds === "number" && Number.isFinite(currentTimeSeconds)
      ? Math.min(Math.max(currentTimeSeconds, 0), chartDurationSeconds)
      : null;
  const currentX = currentSecond === null ? null : xForSecond(currentSecond);
  const incidentSecond =
    typeof incidentTimeSeconds === "number" && Number.isFinite(incidentTimeSeconds)
      ? Math.min(Math.max(incidentTimeSeconds, 0), chartDurationSeconds)
      : null;
  const incidentX = incidentSecond === null ? null : xForSecond(incidentSecond);
  const xAxisY = yFor(33.33);

  if (deltas.length === 0) {
    return <p className="py-6 text-center text-sm text-muted-foreground">No frame deltas available.</p>;
  }

  return (
    <div>
      <svg
        viewBox={`0 0 ${width} ${height}`}
        className="h-[220px] w-full overflow-visible"
        role="img"
        aria-label="Frame-to-frame delta chart"
      >
        {gridTicks.map((tick) => {
          const y = yFor(tick);
          return (
            <g key={tick}>
              <line
                x1={padding.left}
                x2={width - padding.right}
                y1={y}
                y2={y}
                className="stroke-border"
                strokeWidth={tick === 50 ? 1.25 : 0.75}
                strokeDasharray={tick === 50 ? "4 4" : undefined}
              />
              <text
                x={padding.left - 8}
                y={y + 4}
                textAnchor="end"
                className="fill-muted-foreground"
                fontSize={10}
                fontFamily="ui-monospace, monospace"
              >
                {tick % 1 === 0 ? tick.toFixed(0) : tick.toFixed(1)}ms
              </text>
            </g>
          );
        })}
        <path d={path} fill="none" stroke="rgb(148, 163, 184)" strokeWidth={1} strokeOpacity={0.55} />
        {deltas.map((delta, index) => (
          delta > 50 ? (
            <g key={index}>
              <line
                x1={xFor(index)}
                x2={xFor(index)}
                y1={yFor(33.33)}
                y2={yFor(delta)}
                stroke="rgb(148, 163, 184)"
                strokeWidth={0.75}
                strokeOpacity={0.55}
              />
              <circle
                cx={xFor(index)}
                cy={yFor(delta)}
                r={2.6}
                fill={deltaColor(delta)}
              />
            </g>
          ) : null
        ))}
        {xLabels.map((second) => (
          <text
            key={second}
            x={xForSecond(second)}
            y={height - 8}
            textAnchor="middle"
            className="fill-muted-foreground"
            fontSize={10}
            fontFamily="ui-monospace, monospace"
          >
            {second}
          </text>
        ))}
        {currentX !== null && (
          <circle
            cx={currentX}
            cy={xAxisY}
            r={5}
            fill="rgb(37, 99, 235)"
            stroke="white"
            strokeWidth={2}
          >
            <title>{`Current video time: ${currentSecond?.toFixed(1)}s`}</title>
          </circle>
        )}
        {incidentX !== null && (
          <g>
            <line
              x1={incidentX}
              x2={incidentX}
              y1={padding.top}
              y2={xAxisY}
              stroke="rgb(239, 68, 68)"
              strokeWidth={1}
              strokeDasharray="3 3"
              strokeOpacity={0.55}
            />
            <circle
              cx={incidentX}
              cy={xAxisY}
              r={5}
              fill="rgb(239, 68, 68)"
              stroke="white"
              strokeWidth={2}
            >
              <title>{`Incident location: ${incidentSecond?.toFixed(1)}s`}</title>
            </circle>
          </g>
        )}
      </svg>
      <div className="mt-2 flex flex-wrap items-center gap-3 text-xs text-muted-foreground">
        <span className="inline-flex items-center gap-1"><span className="h-2.5 w-4 rounded-sm bg-green-500" />33ms</span>
        <span className="inline-flex items-center gap-1"><span className="h-2.5 w-4 rounded-sm bg-amber-500" />50ms+</span>
        <span className="inline-flex items-center gap-1"><span className="h-2.5 w-4 rounded-sm bg-orange-500" />66ms+</span>
        <span className="inline-flex items-center gap-1"><span className="h-2.5 w-4 rounded-sm bg-red-500" />100ms+</span>
        {incidentX !== null && (
          <span className="inline-flex items-center gap-1">
            <span className="h-2.5 w-2.5 rounded-full bg-red-500 ring-2 ring-red-500/20" />
            Incident
          </span>
        )}
      </div>
    </div>
  );
}

export function FrameTimingQcPanel({
  videoId,
  videoUrl,
  firmwareVersion,
  runToken = 0,
  isProbingExternal = false,
  embedded = false,
  title = "Frame-Timing QC",
  className,
  currentTime,
  incidentTimeSeconds,
}: {
  videoId: string;
  videoUrl: string | null | undefined;
  firmwareVersion: string | null | undefined;
  runToken?: number;
  isProbingExternal?: boolean;
  embedded?: boolean;
  title?: string | null;
  className?: string;
  currentTime?: number;
  incidentTimeSeconds?: number | null;
}) {
  const { data, isLoading, mutate } = useSWR<{ qc: FrameTimingQc | null }>(
    `/api/videos/${videoId}/frame-timing-qc`,
    fetcher,
    { revalidateOnFocus: false, revalidateOnReconnect: false }
  );
  const [probeData, setProbeData] = useState<{ qc: FrameTimingQc | null } | null>(null);
  const [probeError, setProbeError] = useState<unknown>(null);
  const [isProbing, setIsProbing] = useState(false);

  useEffect(() => {
    if (!runToken || !videoUrl) return;

    let cancelled = false;
    queueMicrotask(() => {
      if (cancelled) return;
      setIsProbing(true);
      setProbeError(null);
    });
    probeFrameTimingQc(videoId, videoUrl, firmwareVersion)
      .then((result) => {
        if (cancelled) return;
        setProbeData(result);
        mutate(result, false);
      })
      .catch((error) => {
        if (!cancelled) setProbeError(error);
      })
      .finally(() => {
        if (!cancelled) setIsProbing(false);
      });

    return () => {
      cancelled = true;
    };
  }, [firmwareVersion, mutate, runToken, videoId, videoUrl]);

  const qc = probeData?.qc ?? data?.qc ?? null;
  const isChecking = isProbing || isLoading || isProbingExternal;
  const probeErrorMessage = probeError instanceof Error ? probeError.message : probeError ? String(probeError) : null;
  const metricItems = useMemo(() => {
    if (!qc) return [];
    return [
      ["Frames", qc.frame_count.toLocaleString()],
      ["Duration", `${fmt(qc.duration_s, 2)}s`],
      ["Effective FPS", fmt(qc.effective_fps, 2)],
      ["Late Frames", `${fmt(qc.gap_pct, 2)}%`],
      ["Single Gaps", qc.single_gaps.toLocaleString()],
      ["Double Gaps", qc.double_gaps.toLocaleString()],
      ["Triple+ Gaps", qc.triple_plus_gaps.toLocaleString()],
      ["Max Δt", `${fmt(qc.max_delta_ms, 1)}ms`],
    ];
  }, [qc]);

  return (
    <section className={cn(embedded ? "space-y-3" : "rounded-lg border bg-card px-4 py-3", className)}>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2">
          {title && <h3 className="text-sm font-semibold">{title}</h3>}
          {qc ? (
            <Badge variant="outline" className={cn("gap-1", bucketClasses(qc.bucket))}>
              {qc.bucket === "filter_out" ? (
                <AlertTriangle className="h-3 w-3" />
              ) : qc.bucket === "perfect" ? (
                <CheckCircle2 className="h-3 w-3" />
              ) : (
                <Activity className="h-3 w-3" />
              )}
              {bucketLabel(qc.bucket)}
            </Badge>
          ) : (
              <Badge variant="outline" className="gap-1">
                <Loader2 className="h-3 w-3 animate-spin" />
              {isChecking ? "Checking" : "Pending"}
            </Badge>
          )}
        </div>
        <span className="font-mono text-xs text-muted-foreground">
          {firmwareVersion ? `fw ${firmwareVersion}` : "fw unknown"}
        </span>
      </div>

      {probeErrorMessage && (
        <p className="mb-3 text-sm text-destructive">{probeErrorMessage}</p>
      )}

      {!qc ? (
        <p className="text-sm text-muted-foreground">
          {isChecking ? "Extracting frame timestamps with ffprobe..." : "Frame timing report has not been generated yet."}
        </p>
      ) : (
        <div className="space-y-4">
          <div className="grid grid-cols-2 gap-2 sm:grid-cols-4 xl:grid-cols-8">
            {metricItems.map(([label, value]) => (
              <div key={label} className="rounded-md border bg-background px-2.5 py-2">
                <p className="text-[11px] text-muted-foreground">{label}</p>
                <p className="mt-0.5 font-mono text-sm text-foreground">{value}</p>
              </div>
            ))}
          </div>

          {qc.failed_rules.length > 0 && (
            <div className="flex flex-wrap gap-1.5">
              {qc.failed_rules.map((rule) => (
                <span
                  key={rule}
                  className="rounded bg-muted px-1.5 py-0.5 text-[11px] text-muted-foreground"
                >
                  {ruleLabel(rule)}
                </span>
              ))}
            </div>
          )}

          {qc.probe_status === "failed" && qc.probe_error && (
            <p className="text-sm text-destructive">{qc.probe_error}</p>
          )}

          <FrameDeltaChart
            deltas={qc.deltas_ms}
            durationSeconds={qc.duration_s}
            currentTimeSeconds={currentTime}
            incidentTimeSeconds={incidentTimeSeconds}
          />
        </div>
      )}
    </section>
  );
}

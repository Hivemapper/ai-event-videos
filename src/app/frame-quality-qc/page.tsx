import Link from "next/link";
import { Header } from "@/components/layout/header";
import { Badge } from "@/components/ui/badge";
import { getDb } from "@/lib/db";
import { cn } from "@/lib/utils";

export const dynamic = "force-dynamic";

const FIRMWARE_VERSION = "7.4.3";
const LOCAL_EVENT_BASE_URL = "http://localhost:3000/event";

interface FrameQualityRow {
  id: string;
  event_type: string | null;
  event_timestamp: string | null;
  firmware_version: string | null;
  bucket: string | null;
  gap_pct: number | null;
  max_delta_ms: number | null;
  late_frames: number | null;
  max_late_frames_per_2s: number | null;
  single_gaps: number | null;
  double_gaps: number | null;
  triple_plus_gaps: number | null;
  non_monotonic_deltas: number | null;
  probe_status: string | null;
  probe_error: string | null;
  updated_at: string | null;
}

interface SummaryRow {
  total_videos: number;
  qc_complete: number;
  probe_ok: number;
  perfectly_linear: number;
  missing_qc: number;
  failed_probe: number;
}

function numberValue(value: unknown): number {
  const num = Number(value ?? 0);
  return Number.isFinite(num) ? num : 0;
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function rowToSummary(row: Record<string, unknown> | undefined): SummaryRow {
  return {
    total_videos: numberValue(row?.total_videos),
    qc_complete: numberValue(row?.qc_complete),
    probe_ok: numberValue(row?.probe_ok),
    perfectly_linear: numberValue(row?.perfectly_linear),
    missing_qc: numberValue(row?.missing_qc),
    failed_probe: numberValue(row?.failed_probe),
  };
}

function rowToFrameQuality(row: Record<string, unknown>): FrameQualityRow {
  return {
    id: String(row.id ?? ""),
    event_type: typeof row.event_type === "string" ? row.event_type : null,
    event_timestamp: typeof row.event_timestamp === "string" ? row.event_timestamp : null,
    firmware_version: typeof row.firmware_version === "string" ? row.firmware_version : null,
    bucket: typeof row.bucket === "string" ? row.bucket : null,
    gap_pct: nullableNumber(row.gap_pct),
    max_delta_ms: nullableNumber(row.max_delta_ms),
    late_frames: nullableNumber(row.late_frames),
    max_late_frames_per_2s: nullableNumber(row.max_late_frames_per_2s),
    single_gaps: nullableNumber(row.single_gaps),
    double_gaps: nullableNumber(row.double_gaps),
    triple_plus_gaps: nullableNumber(row.triple_plus_gaps),
    non_monotonic_deltas: nullableNumber(row.non_monotonic_deltas),
    probe_status: typeof row.probe_status === "string" ? row.probe_status : null,
    probe_error: typeof row.probe_error === "string" ? row.probe_error : null,
    updated_at: typeof row.updated_at === "string" ? row.updated_at : null,
  };
}

function formatDate(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value.endsWith("Z") ? value : `${value}Z`);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleString("en-US", {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function formatPercent(value: number | null): string {
  return value === null ? "-" : `${value.toFixed(2)}%`;
}

function formatMs(value: number | null): string {
  return value === null ? "-" : `${value.toFixed(1)}ms`;
}

function formatInt(value: number | null): string {
  return value === null ? "-" : Math.round(value).toLocaleString();
}

function bucketLabel(value: string | null): string {
  if (!value) return "Missing";
  if (value === "filter_out") return "Filter Out";
  return value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function bucketClass(value: string | null): string {
  if (value === "perfect") return "border-emerald-200 bg-emerald-50 text-emerald-700";
  if (value === "ok") return "border-sky-200 bg-sky-50 text-sky-700";
  if (value === "filter_out") return "border-rose-200 bg-rose-50 text-rose-700";
  return "border-muted bg-muted text-muted-foreground";
}

function formatProbeError(value: string | null): string | null {
  if (!value) return null;
  return value
    .replace(/https:\/\/video\.beemaps\.com\/v\?t=[^\s:]+/g, "[video URL]")
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 180);
}

async function loadFrameQualityRows() {
  const db = await getDb();
  const [summaryResult, rowsResult] = await Promise.all([
    db.query(
      `SELECT
         COUNT(*) AS total_videos,
         SUM(CASE WHEN q.video_id IS NOT NULL THEN 1 ELSE 0 END) AS qc_complete,
         SUM(CASE WHEN q.probe_status = 'ok' THEN 1 ELSE 0 END) AS probe_ok,
         SUM(CASE
           WHEN q.probe_status = 'ok'
            AND q.late_frames = 0
            AND q.single_gaps = 0
            AND q.double_gaps = 0
            AND q.triple_plus_gaps = 0
            AND q.non_monotonic_deltas = 0
           THEN 1 ELSE 0 END) AS perfectly_linear,
         SUM(CASE WHEN q.video_id IS NULL THEN 1 ELSE 0 END) AS missing_qc,
         SUM(CASE WHEN q.probe_status = 'failed' THEN 1 ELSE 0 END) AS failed_probe
       FROM triage_results t
       LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
       WHERE t.firmware_version = ?`,
      [FIRMWARE_VERSION]
    ),
    db.query(
      `SELECT
         t.id,
         t.event_type,
         t.event_timestamp,
         t.firmware_version,
         q.bucket,
         q.gap_pct,
         q.max_delta_ms,
         q.late_frames,
         q.max_late_frames_per_2s,
         q.single_gaps,
         q.double_gaps,
         q.triple_plus_gaps,
         q.non_monotonic_deltas,
         q.probe_status,
         q.probe_error,
         q.updated_at
       FROM triage_results t
       LEFT JOIN video_frame_timing_qc q ON q.video_id = t.id
       WHERE t.firmware_version = ?
       ORDER BY
         CASE WHEN q.probe_status = 'ok'
           AND q.late_frames = 0
           AND q.single_gaps = 0
           AND q.double_gaps = 0
           AND q.triple_plus_gaps = 0
           AND q.non_monotonic_deltas = 0
         THEN 0 ELSE 1 END,
         q.gap_pct ASC,
         q.max_delta_ms ASC,
         t.event_timestamp DESC`,
      [FIRMWARE_VERSION]
    ),
  ]);

  return {
    summary: rowToSummary(summaryResult.rows[0]),
    rows: rowsResult.rows.map(rowToFrameQuality),
  };
}

export default async function FrameQualityQcPage() {
  const { summary, rows } = await loadFrameQualityRows();
  const completionPct =
    summary.total_videos > 0 ? (summary.qc_complete / summary.total_videos) * 100 : 0;

  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6 space-y-6">
        <section className="space-y-2">
          <div className="flex flex-wrap items-end justify-between gap-4">
            <div>
              <p className="text-sm font-medium text-muted-foreground">Frame Quality QC</p>
              <h1 className="text-3xl font-semibold tracking-tight">Firmware {FIRMWARE_VERSION}</h1>
            </div>
            <div className="text-sm text-muted-foreground">
              {summary.qc_complete.toLocaleString()} / {summary.total_videos.toLocaleString()} QC complete ({completionPct.toFixed(1)}%)
            </div>
          </div>
        </section>

        <section className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-5">
          {[
            ["Videos", summary.total_videos],
            ["QC Complete", summary.qc_complete],
            ["Perfectly Linear", summary.perfectly_linear],
            ["Failed Probe", summary.failed_probe],
            ["Missing QC", summary.missing_qc],
          ].map(([label, value]) => (
            <div key={label} className="rounded-lg border bg-card p-4">
              <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">{label}</p>
              <p className="mt-2 text-2xl font-semibold tabular-nums">{Number(value).toLocaleString()}</p>
            </div>
          ))}
        </section>

        <section className="overflow-hidden rounded-lg border bg-card">
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/60">
                <tr className="border-b">
                  <th className="px-3 py-2 text-left font-medium text-muted-foreground">URL</th>
                  <th className="px-3 py-2 text-left font-medium text-muted-foreground">Event</th>
                  <th className="px-3 py-2 text-left font-medium text-muted-foreground">Time</th>
                  <th className="px-3 py-2 text-right font-medium text-muted-foreground">Late Frame %</th>
                  <th className="px-3 py-2 text-right font-medium text-muted-foreground">Max Δt</th>
                  <th className="px-3 py-2 text-right font-medium text-muted-foreground">Late</th>
                  <th className="px-3 py-2 text-right font-medium text-muted-foreground">Max 2s Late</th>
                  <th className="px-3 py-2 text-right font-medium text-muted-foreground">Dropped</th>
                  <th className="px-3 py-2 text-left font-medium text-muted-foreground">QC</th>
                  <th className="px-3 py-2 text-left font-medium text-muted-foreground">Updated</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((row) => {
                  const eventUrl = `${LOCAL_EVENT_BASE_URL}/${row.id}`;
                  const dropped =
                    row.single_gaps === null && row.double_gaps === null && row.triple_plus_gaps === null
                      ? null
                      : (row.single_gaps ?? 0) + (row.double_gaps ?? 0) + (row.triple_plus_gaps ?? 0);
                  const probeError = formatProbeError(row.probe_error);
                  return (
                    <tr key={row.id} className="border-b last:border-0 hover:bg-muted/40">
                      <td className="max-w-[360px] px-3 py-2 font-mono text-xs">
                        <Link className="text-primary underline-offset-4 hover:underline" href={`/event/${row.id}`}>
                          {eventUrl}
                        </Link>
                      </td>
                      <td className="px-3 py-2">{row.event_type ?? "-"}</td>
                      <td className="px-3 py-2 whitespace-nowrap text-muted-foreground">
                        {formatDate(row.event_timestamp)}
                      </td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatPercent(row.gap_pct)}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatMs(row.max_delta_ms)}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatInt(row.late_frames)}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatInt(row.max_late_frames_per_2s)}</td>
                      <td className="px-3 py-2 text-right tabular-nums">{formatInt(dropped)}</td>
                      <td className="px-3 py-2">
                        <Badge variant="outline" className={cn("whitespace-nowrap", bucketClass(row.bucket))}>
                          {row.probe_status === "failed" ? "Probe Failed" : bucketLabel(row.bucket)}
                        </Badge>
                        {probeError && (
                          <p className="mt-1 max-w-[260px] truncate text-xs text-muted-foreground" title={probeError}>
                            {probeError}
                          </p>
                        )}
                      </td>
                      <td className="px-3 py-2 whitespace-nowrap text-muted-foreground">
                        {formatDate(row.updated_at)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>
      </main>
    </>
  );
}

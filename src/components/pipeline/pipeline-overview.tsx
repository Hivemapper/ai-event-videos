"use client";

import { Fragment, useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import useSWR from "swr";
import {
  ArrowDown,
  ArrowUp,
  ArrowUpDown,
  Check,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  CircleAlert,
  CircleCheck,
  Clock,
  Filter,
  Loader2,
  Server,
  Timer,
  TrendingUp,
  Upload,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { VideoBitrateCell } from "@/components/pipeline/video-bitrate-cell";
import { ALL_EVENT_TYPES, EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getS3Bucket } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useRetainedValue } from "@/hooks/use-retained-value";
import type { AIEventType } from "@/types/events";

type Stage = "triage" | "vru" | "production";
type SortKey =
  | "date"
  | "event_type"
  | "triage"
  | "vru"
  | "production"
  | "fps_qc"
  | "late_pct"
  | "bitrate"
  | "detections";
type SortDir = "asc" | "desc";

const PIPELINE_BROWSER_STATE_KEY = "ai-event-videos:pipeline-selection:v1";
const PIPELINE_STATE_PARAMS = [
  "stage",
  "status",
  "period",
  "fpsQc",
  "eventTypes",
  "vruLabels",
  "sort",
  "dir",
] as const;

interface OverviewRow {
  id: string;
  event_type: string | null;
  effective_triage_result: string | null;
  speed_min: number | null;
  speed_max: number | null;
  bitrate_bps: number | null;
  event_timestamp: string | null;
  fps_qc: string | null;
  late_frame_pct: number | null;
  max_delta_ms: number | null;
  vru_status: string | null;
  top_vru_label: string | null;
  top_vru_confidence: number | null;
  detection_count: number | null;
  vru_started_at: string | null;
  vru_completed_at: string | null;
  vru_error: string | null;
  production_status: string | null;
  privacy_status: string | null;
  metadata_status: string | null;
  upload_status: string | null;
  production_started_at: string | null;
  production_completed_at: string | null;
  production_error: string | null;
  s3_video_key: string | null;
  s3_metadata_key: string | null;
  production_skip_reason: string | null;
}

interface CountsResponse {
  counts: {
    triage: Record<string, number | null>;
    vru: Record<string, number>;
    production: Record<string, number>;
  };
  autoTriage?: {
    started: boolean;
    threshold: number;
    awaitingCount: number;
    reason?: string;
    period?: string;
    numEvents?: number;
    pid?: number;
    logPath?: string;
  } | null;
}

interface RowsResponse {
  rows: OverviewRow[];
  total: number;
}

interface StatsResponse {
  machines: string[];
  machineCount: number;
  ratePerHour: number;
  last10m: number;
  last30m: number;
  last60m: number;
  queued: number | null;
  etaHours: number | null;
  avgSecs?: number;
}

const PAGE_SIZE = 50;

function getRowsRefreshInterval(stage: Stage, status: string): number {
  if (stage === "triage") return 30000;
  if (status === "completed" || status === "failed") return 30000;
  return 10000;
}

function getCountsRefreshInterval(stage: Stage): number {
  return stage === "triage" ? 30000 : 10000;
}

function getStatsRefreshInterval(): number {
  return 15000;
}

async function fetcher<T>(url: string): Promise<T> {
  const response = await fetch(url);
  const data = await response.json().catch(() => null);
  if (!response.ok) {
    const message =
      data && typeof data === "object" && "error" in data
        ? String((data as { error: unknown }).error)
        : `Request failed with ${response.status}`;
    throw new Error(message);
  }
  return data as T;
}

const STAGES: Array<{ value: Stage; label: string }> = [
  { value: "triage", label: "Triage" },
  { value: "vru", label: "VRU" },
  { value: "production", label: "Production" },
];

const TRIAGE_STATUS_OPTIONS = [
  { value: "all", label: "All" },
  { value: "filtered", label: "Filtered" },
  { value: "signal", label: "Signal" },
  { value: "awaiting", label: "Awaiting" },
] as const;

const TRIAGE_CATEGORY_OPTIONS = [
  "missing_video",
  "missing_metadata",
  "ghost",
  "open_road",
  "duplicate",
  "non_linear",
  "privacy",
  "skipped_firmware",
] as const;

const VRU_STATUS_OPTIONS = [
  { value: "queued", label: "Queued" },
  { value: "running", label: "Running" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
] as const;

const PRODUCTION_STATUS_OPTIONS = [
  { value: "queued", label: "Queued" },
  { value: "processing", label: "Processing" },
  { value: "completed", label: "Completed" },
  { value: "failed", label: "Failed" },
] as const;

const PERIOD_OPTIONS = [
  { value: "all", label: "All Periods" },
  { value: "1", label: "Period 1: Jan 1-Sep 15, 2025" },
  { value: "2", label: "Period 2: Sep 15, 2025-Jan 20, 2026" },
  { value: "3", label: "Period 3: Jan 20-Feb 25, 2026" },
  { value: "4", label: "Period 4: Feb 25-Mar 15, 2026" },
  { value: "5", label: "Period 5: Mar 15-Apr 17, 2026" },
  { value: "6", label: "Period 6: Apr 17-Apr 22, 2026" },
  { value: "7", label: "Period 7: Apr 22, 2026 onward" },
] as const;

const FPS_QC_OPTIONS = [
  { value: "perfect", label: "Perfect" },
  { value: "ok", label: "OK" },
  { value: "filter_out", label: "Filter Out" },
  { value: "missing", label: "No QC" },
] as const;

const VRU_LABEL_OPTIONS = [
  "person",
  "pedestrian",
  "stroller",
  "wheelchair",
  "scooter",
  "bicycle",
  "motorcycle",
  "skateboard",
  "dog",
  "animal",
  "construction worker",
  "work-zone-person",
].map((value) => ({
  value,
  label: titleize(value),
}));

const TRIAGE_CONFIG: Record<string, { label: string; color: string }> = {
  missing_video: { label: "Missing Video", color: "bg-blue-50 text-blue-700 border-blue-200" },
  missing_metadata: { label: "Missing Metadata", color: "bg-violet-50 text-violet-700 border-violet-200" },
  ghost: { label: "Ghost", color: "bg-red-50 text-red-700 border-red-200" },
  open_road: { label: "Open Road", color: "bg-amber-50 text-amber-700 border-amber-200" },
  signal: { label: "Signal", color: "bg-green-50 text-green-700 border-green-200" },
  duplicate: { label: "Duplicate", color: "bg-orange-50 text-orange-700 border-orange-200" },
  non_linear: { label: "Non Linear", color: "bg-teal-50 text-teal-700 border-teal-200" },
  privacy: { label: "Privacy", color: "bg-indigo-50 text-indigo-700 border-indigo-200" },
  skipped_firmware: { label: "Firmware Skip", color: "bg-slate-50 text-slate-700 border-slate-200" },
};

const STATUS_CONFIG: Record<string, { label: string; color: string; icon?: typeof CircleCheck }> = {
  queued: { label: "Queued", color: "bg-amber-50 text-amber-700 border-amber-200", icon: Clock },
  running: { label: "Running", color: "bg-blue-50 text-blue-700 border-blue-200", icon: Loader2 },
  processing: { label: "Processing", color: "bg-blue-50 text-blue-700 border-blue-200", icon: Loader2 },
  completed: { label: "Completed", color: "bg-emerald-50 text-emerald-700 border-emerald-200", icon: CircleCheck },
  failed: { label: "Failed", color: "bg-red-50 text-red-700 border-red-200", icon: CircleAlert },
  not_queued: { label: "Not Queued", color: "bg-muted text-muted-foreground border-muted-foreground/20", icon: Clock },
};

const FPS_QC_CONFIG: Record<string, { label: string; color: string }> = {
  perfect: { label: "Perfect", color: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  ok: { label: "OK", color: "bg-sky-50 text-sky-700 border-sky-200" },
  filter_out: { label: "Filter Out", color: "bg-rose-50 text-rose-700 border-rose-200" },
  missing: { label: "No QC", color: "bg-muted text-muted-foreground border-muted-foreground/20" },
};

function titleize(value: string): string {
  return value.replace(/[-_]/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatDate(dateStr: string | null | undefined): string {
  if (!dateStr) return "-";
  const date = new Date(dateStr.endsWith("Z") ? dateStr : `${dateStr}Z`);
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function formatDuration(startedAt: string | null | undefined, completedAt: string | null | undefined): string {
  if (!startedAt || !completedAt) return "-";
  const start = new Date(startedAt.endsWith("Z") ? startedAt : `${startedAt}Z`).getTime();
  const end = new Date(completedAt.endsWith("Z") ? completedAt : `${completedAt}Z`).getTime();
  const seconds = Math.max(0, Math.round((end - start) / 1000));
  if (seconds < 60) return `${seconds}s`;
  return `${Math.floor(seconds / 60)}m ${String(seconds % 60).padStart(2, "0")}s`;
}

function formatLatePct(value: number | null | undefined): string {
  return typeof value === "number" && Number.isFinite(value) ? `${value.toFixed(2)}%` : "-";
}

function formatMbps(value: number | null | undefined): string | null {
  return typeof value === "number" && Number.isFinite(value) && value > 0
    ? (value / 1_000_000).toFixed(2)
    : null;
}

function formatCount(value: number | null | undefined): string {
  return typeof value === "number" ? value.toLocaleString() : "-";
}

function parseCsv(searchParams: URLSearchParams, key: string): string[] {
  return Array.from(
    new Set(
      (searchParams.get(key) ?? "")
        .split(",")
        .map((value) => value.trim())
        .filter(Boolean)
    )
  );
}

function getStage(searchParams: URLSearchParams): Stage {
  const stage = searchParams.get("stage");
  return stage === "vru" || stage === "production" ? stage : "triage";
}

function getDefaultStatus(stage: Stage): string {
  if (stage === "triage") return "all";
  return "queued";
}

function getStatus(stage: Stage, searchParams: URLSearchParams): string {
  const requested = searchParams.get("status") ?? getDefaultStatus(stage);
  if (stage === "triage") {
    const values = new Set<string>([
      ...TRIAGE_STATUS_OPTIONS.map((option) => option.value),
      ...TRIAGE_CATEGORY_OPTIONS,
    ]);
    return values.has(requested) && requested !== "awaiting" ? requested : "all";
  }
  if (stage === "production") {
    return PRODUCTION_STATUS_OPTIONS.some((option) => option.value === requested) ? requested : "queued";
  }
  return VRU_STATUS_OPTIONS.some((option) => option.value === requested) ? requested : "queued";
}

function getSort(searchParams: URLSearchParams): SortKey {
  const sort = searchParams.get("sort");
  const valid = new Set<SortKey>([
    "date",
    "event_type",
    "triage",
    "vru",
    "production",
    "fps_qc",
    "late_pct",
    "bitrate",
    "detections",
  ]);
  return sort && valid.has(sort as SortKey) ? (sort as SortKey) : "date";
}

function getSortDir(searchParams: URLSearchParams): SortDir {
  return searchParams.get("dir") === "asc" ? "asc" : "desc";
}

function getPeriod(searchParams: URLSearchParams): string {
  const period = searchParams.get("period");
  return PERIOD_OPTIONS.some((option) => option.value === period) ? period ?? "all" : "all";
}

function getEventTypes(searchParams: URLSearchParams): AIEventType[] {
  const requested = parseCsv(searchParams, "eventTypes");
  return requested.filter((value): value is AIEventType =>
    ALL_EVENT_TYPES.includes(value as AIEventType)
  );
}

function getFpsQc(searchParams: URLSearchParams): string[] {
  const valid = new Set(FPS_QC_OPTIONS.map((option) => option.value));
  return parseCsv(searchParams, "fpsQc").filter((value) => valid.has(value as never));
}

function getVruLabels(searchParams: URLSearchParams): string[] {
  const valid = new Set(VRU_LABEL_OPTIONS.map((option) => option.value));
  return parseCsv(searchParams, "vruLabels").filter((value) => valid.has(value));
}

function hasPipelineStateParams(searchParams: URLSearchParams): boolean {
  return PIPELINE_STATE_PARAMS.some((key) => searchParams.has(key));
}

function getPipelineQueryModel(searchParams: URLSearchParams) {
  const stage = getStage(searchParams);
  const status = getStatus(stage, searchParams);
  const period = getPeriod(searchParams);
  const sort = getSort(searchParams);
  const dir = getSortDir(searchParams);
  const fpsQc = getFpsQc(searchParams);
  const eventTypes = getEventTypes(searchParams);
  const vruLabels = getVruLabels(searchParams);

  return { stage, status, period, sort, dir, fpsQc, eventTypes, vruLabels };
}

function buildQuery(params: {
  stage: Stage;
  status?: string;
  period: string;
  fpsQc: string[];
  eventTypes: string[];
  vruLabels: string[];
  sort: SortKey;
  dir: SortDir;
  includeStatus?: boolean;
  limit?: number;
  offset?: number;
}) {
  const query = new URLSearchParams();
  query.set("stage", params.stage);
  if (params.includeStatus !== false && params.status) query.set("status", params.status);
  if (params.period !== "all") query.set("period", params.period);
  if (params.fpsQc.length > 0 && params.fpsQc.length < FPS_QC_OPTIONS.length) {
    query.set("fpsQc", params.fpsQc.join(","));
  }
  if (params.eventTypes.length > 0 && params.eventTypes.length < ALL_EVENT_TYPES.length) {
    query.set("eventTypes", params.eventTypes.join(","));
  }
  if (params.vruLabels.length > 0) query.set("vruLabels", params.vruLabels.join(","));
  if (params.sort !== "date") query.set("sort", params.sort);
  if (params.sort !== "date" || params.dir !== "desc") query.set("dir", params.dir);
  if (params.limit != null) query.set("limit", String(params.limit));
  if (params.offset != null) query.set("offset", String(params.offset));
  return query.toString();
}

function normalizePipelineQueryString(value: string | null): string | null {
  if (!value) return null;
  try {
    const search = value.startsWith("?") ? value.slice(1) : value;
    const params = new URLSearchParams(search);
    if (!hasPipelineStateParams(params)) return null;
    return buildQuery(getPipelineQueryModel(params));
  } catch {
    return null;
  }
}

function readStoredPipelineQuery(): string | null {
  if (typeof window === "undefined") return null;
  try {
    return normalizePipelineQueryString(
      window.localStorage.getItem(PIPELINE_BROWSER_STATE_KEY)
    );
  } catch {
    return null;
  }
}

function writeStoredPipelineQuery(query: string): void {
  if (typeof window === "undefined") return;
  try {
    window.localStorage.setItem(PIPELINE_BROWSER_STATE_KEY, query);
  } catch {
    // Ignore storage-denied browsers; URL state still works.
  }
}

function getStageStatusOptions(stage: Stage): Array<{ value: string; label: string }> {
  if (stage === "production") return [...PRODUCTION_STATUS_OPTIONS];
  if (stage === "vru") return [...VRU_STATUS_OPTIONS];
  return [...TRIAGE_STATUS_OPTIONS];
}

function SortButton({
  sortKey,
  activeSort,
  activeDir,
  onSort,
  children,
  align = "left",
}: {
  sortKey: SortKey;
  activeSort: SortKey;
  activeDir: SortDir;
  onSort: (sort: SortKey) => void;
  children: React.ReactNode;
  align?: "left" | "right";
}) {
  const isActive = activeSort === sortKey;
  const Icon = isActive ? (activeDir === "asc" ? ArrowUp : ArrowDown) : ArrowUpDown;
  return (
    <button
      type="button"
      onClick={() => onSort(sortKey)}
      className={cn(
        "inline-flex items-center gap-1 rounded px-1 py-0.5 text-muted-foreground transition-colors hover:bg-muted hover:text-foreground",
        isActive && "text-foreground",
        align === "right" && "justify-end"
      )}
    >
      {children}
      <Icon className={cn("h-3 w-3", !isActive && "opacity-50")} />
    </button>
  );
}

function MultiFilter({
  label,
  options,
  selectedValues,
  onApply,
}: {
  label: string;
  options: Array<{ value: string; label: string }>;
  selectedValues: string[];
  onApply: (values: string[]) => void;
}) {
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState<string[]>([]);
  const activeSelected = selectedValues.length > 0 ? selectedValues : options.map((option) => option.value);
  const isActive = selectedValues.length > 0 && selectedValues.length < options.length;

  const draftSet = new Set(draft);
  const toggle = (value: string) => {
    setDraft((current) => {
      const next = new Set(current);
      if (next.has(value)) next.delete(value);
      else next.add(value);
      return options.map((option) => option.value).filter((value) => next.has(value));
    });
  };

  const apply = () => {
    if (draft.length === 0) return;
    onApply(draft.length === options.length ? [] : draft);
    setOpen(false);
  };

  const handleOpenChange = (nextOpen: boolean) => {
    if (nextOpen) setDraft(activeSelected);
    setOpen(nextOpen);
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogTrigger asChild>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className={cn("h-8", isActive && "border-primary text-primary")}
        >
          <Filter className="mr-1.5 h-3.5 w-3.5" />
          {label}
          {isActive && (
            <Badge variant="outline" className="ml-1.5 h-5 bg-primary/5 px-1.5 text-[11px] text-primary">
              {selectedValues.length}/{options.length}
            </Badge>
          )}
        </Button>
      </DialogTrigger>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>{label}</DialogTitle>
        </DialogHeader>
        <div className="flex items-center justify-between gap-3">
          <span className="text-sm text-muted-foreground">{draft.length} selected</span>
          <div className="flex items-center gap-1">
            <Button type="button" variant="ghost" size="sm" onClick={() => setDraft(options.map((option) => option.value))}>
              All
            </Button>
            <Button type="button" variant="ghost" size="sm" onClick={() => setDraft([])}>
              None
            </Button>
          </div>
        </div>
        <div className="grid max-h-80 gap-1 overflow-y-auto">
          {options.map((option) => {
            const checked = draftSet.has(option.value);
            return (
              <button
                key={option.value}
                type="button"
                onClick={() => toggle(option.value)}
                className={cn(
                  "flex items-center gap-2 rounded-md border px-3 py-2 text-left text-sm transition-colors",
                  checked ? "border-primary/40 bg-primary/5" : "hover:bg-muted/40"
                )}
              >
                <span
                  className={cn(
                    "flex h-4 w-4 shrink-0 items-center justify-center rounded border",
                    checked ? "border-primary bg-primary text-primary-foreground" : "border-muted-foreground/30"
                  )}
                >
                  {checked && <Check className="h-3 w-3" />}
                </span>
                <span>{option.label}</span>
              </button>
            );
          })}
        </div>
        {draft.length === 0 && (
          <p className="text-sm text-destructive">Select at least one value.</p>
        )}
        <DialogFooter>
          <Button type="button" onClick={apply} disabled={draft.length === 0}>
            Apply
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function TriageBadge({ value }: { value: string | null | undefined }) {
  const config = value ? TRIAGE_CONFIG[value] : null;
  if (!config) return <span className="text-muted-foreground">-</span>;
  return (
    <Badge variant="outline" className={cn("whitespace-nowrap", config.color)}>
      {config.label}
    </Badge>
  );
}

function StatusBadge({ value }: { value: string | null | undefined }) {
  const config = STATUS_CONFIG[value ?? "not_queued"] ?? STATUS_CONFIG.not_queued;
  const Icon = config.icon;
  return (
    <Badge variant="outline" className={cn("gap-1 whitespace-nowrap", config.color)}>
      {Icon && (
        <Icon
          className={cn(
            "h-3 w-3",
            (value === "running" || value === "processing") && "animate-spin"
          )}
        />
      )}
      {config.label}
    </Badge>
  );
}

function FpsQcBadge({ value }: { value: string | null | undefined }) {
  const config = value ? FPS_QC_CONFIG[value] : null;
  if (!config) return <span className="text-muted-foreground">-</span>;
  return (
    <Badge variant="outline" className={cn("whitespace-nowrap", config.color)}>
      {config.label}
    </Badge>
  );
}

function TypeBadge({ value }: { value: string | null | undefined }) {
  if (!value) return <span className="text-muted-foreground">-</span>;
  const config = EVENT_TYPE_CONFIG[value as AIEventType];
  if (!config) return <span className="text-muted-foreground">{value.replace(/_/g, " ")}</span>;
  return (
    <Badge variant="outline" className={cn("whitespace-nowrap", config.bgColor, config.color, config.borderColor)}>
      {config.label}
    </Badge>
  );
}

function VruLabelCell({ row }: { row: OverviewRow }) {
  if (!row.top_vru_label) return <span className="text-muted-foreground">-</span>;
  const confidence =
    typeof row.top_vru_confidence === "number" && Number.isFinite(row.top_vru_confidence)
      ? `${Math.round(row.top_vru_confidence * 100)}%`
      : null;
  return (
    <span className="whitespace-nowrap text-sm text-muted-foreground">
      {row.top_vru_label} {confidence && <span className="tabular-nums">{confidence}</span>}
    </span>
  );
}

function BitrateCell({ row }: { row: OverviewRow }) {
  const stored = formatMbps(row.bitrate_bps);
  if (stored) {
    return <span className="font-mono text-xs tabular-nums text-muted-foreground">{stored}</span>;
  }
  return <VideoBitrateCell eventId={row.id} />;
}

function StepBadge({ status, label }: { status: string | null | undefined; label: string }) {
  const normalized = status ?? "pending";
  const colors: Record<string, string> = {
    pending: "bg-muted text-muted-foreground border-muted-foreground/20",
    processing: "bg-blue-50 text-blue-700 border-blue-200",
    completed: "bg-green-50 text-green-700 border-green-200",
    failed: "bg-red-50 text-red-700 border-red-200",
  };
  return (
    <Badge variant="outline" className={cn("gap-1 whitespace-nowrap text-xs", colors[normalized] ?? colors.pending)}>
      {normalized === "processing" && <Loader2 className="h-3 w-3 animate-spin" />}
      {normalized === "completed" && <CircleCheck className="h-3 w-3" />}
      {normalized === "failed" && <CircleAlert className="h-3 w-3" />}
      {label}
    </Badge>
  );
}

function ProductionCell({
  row,
  onPush,
  busy,
}: {
  row: OverviewRow;
  onPush: (videoId: string) => void;
  busy: boolean;
}) {
  const status = row.production_status ?? "not_queued";
  const canPush =
    status === "not_queued" || status === "queued" || status === "failed";

  if (!canPush || row.vru_status !== "completed") {
    return <StatusBadge value={status} />;
  }

  return (
    <Button
      type="button"
      variant="outline"
      size="sm"
      className="h-7 whitespace-nowrap text-xs"
      onClick={() => onPush(row.id)}
      disabled={busy}
    >
      {busy ? (
        <Loader2 className="mr-1 h-3 w-3 animate-spin" />
      ) : (
        <Upload className="mr-1 h-3 w-3" />
      )}
      {status === "failed" ? "Requeue" : status === "queued" ? "Prioritize" : "Push"}
    </Button>
  );
}

function MetricCards({
  stage,
  status,
  counts,
  onStatusChange,
}: {
  stage: Stage;
  status: string;
  counts: CountsResponse["counts"] | undefined;
  onStatusChange: (status: string) => void;
}) {
  const options = getStageStatusOptions(stage);
  const stageCounts = counts?.[stage] as Record<string, number | null> | undefined;

  return (
    <div className="space-y-3">
      <div className="grid grid-cols-2 gap-2 lg:grid-cols-4">
        {options.map((option) => {
          const active = status === option.value;
          const value = stageCounts?.[option.value];
          return (
            <button
              key={option.value}
              type="button"
              onClick={() => option.value !== "awaiting" && onStatusChange(option.value)}
              disabled={option.value === "awaiting"}
              className={cn(
                "rounded-lg border px-3 py-2 text-left transition-colors",
                active ? "border-primary ring-2 ring-primary" : "hover:border-foreground/20",
                option.value === "awaiting" && "cursor-default opacity-80"
              )}
            >
              <p className="text-xl font-semibold tabular-nums">{formatCount(value)}</p>
              <p className="mt-0.5 text-xs text-muted-foreground">{option.label}</p>
            </button>
          );
        })}
      </div>
      {stage === "triage" && (
        <div className="grid grid-cols-2 gap-2 md:grid-cols-4 xl:grid-cols-8">
          {TRIAGE_CATEGORY_OPTIONS.map((category) => {
            const config = TRIAGE_CONFIG[category];
            const active = status === category;
            return (
              <button
                key={category}
                type="button"
                onClick={() => onStatusChange(category)}
                className={cn(
                  "rounded-lg border px-3 py-2 text-left transition-colors",
                  active ? "border-primary ring-2 ring-primary" : "hover:border-foreground/20"
                )}
              >
                <p className="text-base font-semibold tabular-nums">{formatCount(stageCounts?.[category])}</p>
                <p className={cn("mt-0.5 truncate text-xs", config.color.replace("bg-", "text-").split(" ")[1] ?? "text-muted-foreground")}>
                  {config.label}
                </p>
              </button>
            );
          })}
        </div>
      )}
    </div>
  );
}

function StageStats({
  stage,
  stats,
  queuedCount,
}: {
  stage: Stage;
  stats: StatsResponse | undefined;
  queuedCount: number | null | undefined;
}) {
  if (stage === "triage" || !stats) return null;

  const queuedRemaining = queuedCount ?? stats.queued;
  const etaHours =
    queuedRemaining != null && stats.ratePerHour > 0
      ? queuedRemaining / stats.ratePerHour
      : null;

  return (
    <div className="grid gap-2 md:grid-cols-4">
      <div className="flex items-center gap-3 rounded-lg border px-3 py-2">
        <Server className={cn("h-4 w-4", stats.machineCount > 0 ? "text-green-600" : "text-muted-foreground")} />
        <div className="min-w-0">
          <p className="text-xs text-muted-foreground">Active Servers</p>
          <p className="text-lg font-semibold tabular-nums">{stats.machineCount}</p>
        </div>
      </div>
      <div className="flex items-center gap-3 rounded-lg border px-3 py-2">
        <TrendingUp className={cn("h-4 w-4", stats.ratePerHour > 0 ? "text-blue-600" : "text-muted-foreground")} />
        <div>
          <p className="text-xs text-muted-foreground">Processing Rate</p>
          <p className="text-lg font-semibold tabular-nums">{stats.ratePerHour.toLocaleString()}<span className="text-sm font-normal text-muted-foreground">/hr</span></p>
        </div>
      </div>
      <div className="flex items-center gap-3 rounded-lg border px-3 py-2">
        <Timer className={cn("h-4 w-4", etaHours ? "text-orange-500" : "text-muted-foreground")} />
        <div>
          <p className="text-xs text-muted-foreground">Est. Remaining</p>
          <p className="text-lg font-semibold tabular-nums">
            {etaHours == null ? "-" : etaHours < 24 ? `${etaHours.toFixed(1)} hrs` : `${(etaHours / 24).toFixed(1)} days`}
          </p>
        </div>
      </div>
      <div className="flex items-center gap-3 rounded-lg border px-3 py-2">
        <Clock className="h-4 w-4 text-muted-foreground" />
        <div>
          <p className="text-xs text-muted-foreground">{stage === "production" ? "Avg. Time / Video" : "Last 30m"}</p>
          <p className="text-lg font-semibold tabular-nums">
            {stage === "production" && stats.avgSecs
              ? stats.avgSecs < 60
                ? `${stats.avgSecs}s`
                : `${Math.floor(stats.avgSecs / 60)}m ${stats.avgSecs % 60}s`
              : stats.last30m.toLocaleString()}
          </p>
        </div>
      </div>
    </div>
  );
}

export function PipelineOverview() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const searchParamsString = searchParams.toString();
  const hasPipelineSelection = hasPipelineStateParams(searchParams);
  const [barePipelineReady, setBarePipelineReady] = useState(hasPipelineSelection);
  const [offset, setOffset] = useState(0);
  const [pushingToProduction, setPushingToProduction] = useState<Record<string, boolean>>({});
  const [expandedRow, setExpandedRow] = useState<string | null>(null);
  const [notice, setNotice] = useState<{ type: "success" | "error"; message: string } | null>(null);
  const [s3Bucket, setS3Bucket] = useState("");

  const queryModel = getPipelineQueryModel(searchParams);
  const { stage, status, period, sort, dir, fpsQc, eventTypes, vruLabels } = queryModel;
  const queryKey = [
    stage,
    status,
    period,
    sort,
    dir,
    fpsQc.join(","),
    eventTypes.join(","),
    vruLabels.join(","),
  ].join("|");
  const selectionQuery = buildQuery(queryModel);
  const shouldLoadPipeline = hasPipelineSelection || barePipelineReady;

  useEffect(() => {
    if (hasPipelineSelection) {
      setBarePipelineReady(false);
      return;
    }

    const storedQuery = readStoredPipelineQuery();
    if (storedQuery) {
      router.replace(`/pipeline?${storedQuery}`, { scroll: false });
      return;
    }

    setBarePipelineReady(true);
  }, [hasPipelineSelection, router, searchParamsString]);

  useEffect(() => {
    if (!shouldLoadPipeline || !hasPipelineSelection) return;
    writeStoredPipelineQuery(selectionQuery);
  }, [hasPipelineSelection, selectionQuery, shouldLoadPipeline]);

  useEffect(() => {
    setOffset(0);
    setExpandedRow(null);
  }, [queryKey]);

  useEffect(() => {
    setS3Bucket(getS3Bucket());
  }, []);

  const rowsQuery = buildQuery({
    ...queryModel,
    limit: PAGE_SIZE,
    offset,
  });
  const countsQuery = buildQuery({
    ...queryModel,
    includeStatus: false,
  });

  const { data: rowsData, isLoading, isValidating, mutate } = useSWR<RowsResponse>(
    shouldLoadPipeline ? `/api/pipeline/overview?${rowsQuery}` : null,
    fetcher,
    {
      refreshInterval: getRowsRefreshInterval(stage, status),
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      keepPreviousData: true,
    }
  );
  const { data: countsData, isValidating: countsRefreshing, mutate: mutateCounts } = useSWR<CountsResponse>(
    shouldLoadPipeline ? `/api/pipeline/overview/counts?${countsQuery}` : null,
    fetcher,
    {
      refreshInterval: getCountsRefreshInterval(stage),
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      keepPreviousData: true,
    }
  );
  const { data: vruStats } = useSWR<StatsResponse>(
    shouldLoadPipeline && stage === "vru" ? "/api/pipeline/stats?includeQueued=false" : null,
    fetcher,
    {
      refreshInterval: getStatsRefreshInterval(),
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      keepPreviousData: true,
    }
  );
  const { data: productionStats } = useSWR<StatsResponse>(
    shouldLoadPipeline && stage === "production" ? "/api/production-pipeline/stats?includeQueued=false" : null,
    fetcher,
    {
      refreshInterval: getStatsRefreshInterval(),
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      keepPreviousData: true,
    }
  );

  const retainedCounts = useRetainedValue(countsData);
  const retainedRows = useRetainedValue(rowsData);
  const retainedVruStats = useRetainedValue(vruStats);
  const retainedProductionStats = useRetainedValue(productionStats);
  const counts = retainedCounts?.counts;
  const autoTriage = retainedCounts?.autoTriage;
  const rows = retainedRows?.rows ?? [];
  const stageStatusCount = stage === "triage"
    ? counts?.triage[status]
    : stage === "vru"
      ? counts?.vru[status]
      : counts?.production[status];
  const total = typeof stageStatusCount === "number" ? stageStatusCount : (retainedRows?.total ?? 0);
  const totalPages = total > 0 ? Math.ceil(total / PAGE_SIZE) : 0;
  const pageNum = Math.floor(offset / PAGE_SIZE) + 1;
  const metricRefreshing = (countsRefreshing && Boolean(counts)) || (isValidating && Boolean(retainedRows));
  const queuedCount = stage === "vru"
    ? counts?.vru.queued
    : stage === "production"
      ? counts?.production.queued
      : null;

  useEffect(() => {
    if (!autoTriage?.started || !autoTriage.pid) return;
    setNotice({
      type: "success",
      message: `Auto triage started for ${autoTriage.numEvents?.toLocaleString() ?? autoTriage.awaitingCount.toLocaleString()} Period ${autoTriage.period} events.`,
    });
  }, [autoTriage?.awaitingCount, autoTriage?.numEvents, autoTriage?.period, autoTriage?.pid, autoTriage?.started]);

  const pushUrl = useCallback(
    (patch: Record<string, string | null>) => {
      const next = new URLSearchParams(searchParams.toString());
      for (const [key, value] of Object.entries(patch)) {
        if (value === null || value === "") next.delete(key);
        else next.set(key, value);
      }
      const nextQuery = next.toString();
      router.push(`/pipeline${nextQuery ? `?${nextQuery}` : ""}`, { scroll: false });
    },
    [router, searchParams]
  );

  const changeStage = (nextStage: Stage) => {
    pushUrl({
      stage: nextStage,
      status: getDefaultStatus(nextStage),
      sort: null,
      dir: null,
    });
  };

  const changeStatus = (nextStatus: string) => {
    pushUrl({ status: nextStatus });
  };

  const changePeriod = (nextPeriod: string) => {
    pushUrl({ period: nextPeriod === "all" ? null : nextPeriod });
  };

  const changeMultiFilter = (key: "eventTypes" | "fpsQc" | "vruLabels", values: string[]) => {
    pushUrl({ [key]: values.length > 0 ? values.join(",") : null });
  };

  const handleSort = (nextSort: SortKey) => {
    if (sort !== nextSort) {
      pushUrl({ sort: nextSort === "date" ? null : nextSort, dir: nextSort === "date" ? "desc" : "asc" });
      return;
    }
    if (dir === "asc") {
      pushUrl({ dir: "desc" });
      return;
    }
    pushUrl({ sort: null, dir: null });
  };

  const handlePushToProduction = useCallback(async (videoId: string) => {
    setPushingToProduction((current) => ({ ...current, [videoId]: true }));
    setNotice(null);
    try {
      const response = await fetch(`/api/videos/${videoId}/production-pipeline`, { method: "POST" });
      const result = await response.json();
      if (!response.ok) {
        throw new Error(result.error ?? "Failed to update production queue");
      }
      const message =
        result.requeued
          ? "Video requeued at the front of production."
          : result.created
            ? "Video pushed to production."
            : result.prioritized
              ? "Video moved to the front of production."
              : "Production queue already has this video.";
      setNotice({ type: "success", message });
      mutate();
      mutateCounts();
    } catch (error) {
      setNotice({ type: "error", message: error instanceof Error ? error.message : String(error) });
    } finally {
      setPushingToProduction((current) => ({ ...current, [videoId]: false }));
    }
  }, [mutate, mutateCounts]);

  const eventTypeOptions = ALL_EVENT_TYPES.map((value) => ({
    value,
    label: EVENT_TYPE_CONFIG[value].label,
  }));

  if (!shouldLoadPipeline) {
    return <Skeleton className="h-96 w-full" />;
  }

  return (
    <div className="space-y-5">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
        <div className="flex flex-wrap items-center gap-3">
          <div className="hidden items-center gap-1 rounded-lg border bg-muted/30 p-1 md:inline-flex">
            {STAGES.map((item, index) => {
              const active = stage === item.value;
              return (
                <button
                  key={item.value}
                  type="button"
                  onClick={() => changeStage(item.value)}
                  className={cn(
                    "inline-flex items-center gap-2 rounded-md px-3 py-1.5 text-sm font-medium transition-colors",
                    active ? "bg-background text-foreground shadow-sm" : "text-muted-foreground hover:text-foreground"
                  )}
                >
                  <span className="flex h-5 w-5 items-center justify-center rounded-full border text-[11px] tabular-nums">
                    {index + 1}
                  </span>
                  {item.label}
                </button>
              );
            })}
          </div>
          <Select value={stage} onValueChange={(value) => changeStage(value as Stage)}>
            <SelectTrigger className="h-9 md:hidden">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {STAGES.map((item) => (
                <SelectItem key={item.value} value={item.value}>
                  {item.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {metricRefreshing && (
            <div className="flex items-center text-xs text-muted-foreground" aria-live="polite">
              <Loader2 className="mr-1 h-3 w-3 animate-spin" />
              Updating metrics
            </div>
          )}
        </div>

        <div className="flex flex-wrap items-center gap-2">
          <Select value={period} onValueChange={changePeriod}>
            <SelectTrigger className="h-8 w-full sm:w-[19rem]">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {PERIOD_OPTIONS.map((option) => (
                <SelectItem key={option.value} value={option.value}>
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <MultiFilter
            label="FPS QC"
            options={[...FPS_QC_OPTIONS]}
            selectedValues={fpsQc}
            onApply={(values) => changeMultiFilter("fpsQc", values)}
          />
          <MultiFilter
            label="Event Type"
            options={eventTypeOptions}
            selectedValues={eventTypes}
            onApply={(values) => changeMultiFilter("eventTypes", values)}
          />
          <MultiFilter
            label="VRU"
            options={VRU_LABEL_OPTIONS}
            selectedValues={vruLabels}
            onApply={(values) => changeMultiFilter("vruLabels", values)}
          />
        </div>
      </div>

      <MetricCards
        stage={stage}
        status={status}
        counts={counts}
        onStatusChange={changeStatus}
      />

      <StageStats
        stage={stage}
        stats={stage === "production" ? retainedProductionStats : retainedVruStats}
        queuedCount={queuedCount}
      />

      {notice && (
        <div
          className={cn(
            "flex items-center gap-2 rounded-lg border px-3 py-2 text-sm",
            notice.type === "success"
              ? "border-green-200 bg-green-50 text-green-800"
              : "border-red-200 bg-red-50 text-red-800"
          )}
        >
          {notice.type === "success" ? <CircleCheck className="h-4 w-4" /> : <CircleAlert className="h-4 w-4" />}
          {notice.message}
        </div>
      )}

      {isLoading && !retainedRows ? (
        <div className="space-y-2">
          {Array.from({ length: 10 }).map((_, index) => (
            <Skeleton key={index} className="h-12 w-full" />
          ))}
        </div>
      ) : rows.length === 0 ? (
        <div className="rounded-lg border py-16 text-center text-sm text-muted-foreground">
          No rows match the selected filters.
        </div>
      ) : (
        <div className="overflow-x-auto rounded-lg border">
          <table className="w-full min-w-[1180px] text-sm">
            <thead>
              <tr className="border-b bg-muted/40">
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">#</th>
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">Event</th>
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">
                  <SortButton sortKey="event_type" activeSort={sort} activeDir={dir} onSort={handleSort}>Type</SortButton>
                </th>
                {stage === "triage" && (
                  <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">
                    <SortButton sortKey="triage" activeSort={sort} activeDir={dir} onSort={handleSort}>Triage</SortButton>
                  </th>
                )}
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">
                  <SortButton sortKey="vru" activeSort={sort} activeDir={dir} onSort={handleSort}>VRU</SortButton>
                </th>
                {stage === "vru" && (
                  <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">Top VRU</th>
                )}
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">
                  <SortButton sortKey="production" activeSort={sort} activeDir={dir} onSort={handleSort}>Production</SortButton>
                </th>
                <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">
                  <SortButton sortKey="fps_qc" activeSort={sort} activeDir={dir} onSort={handleSort}>FPS QC</SortButton>
                </th>
                <th className="px-3 py-2.5 text-right font-medium text-muted-foreground">
                  <SortButton sortKey="late_pct" activeSort={sort} activeDir={dir} onSort={handleSort} align="right">Late %</SortButton>
                </th>
                <th className="px-3 py-2.5 text-right font-medium text-muted-foreground">
                  <SortButton sortKey="bitrate" activeSort={sort} activeDir={dir} onSort={handleSort} align="right">Bitrate</SortButton>
                </th>
                {stage === "vru" && (
                  <th className="px-3 py-2.5 text-right font-medium text-muted-foreground">
                    <SortButton sortKey="detections" activeSort={sort} activeDir={dir} onSort={handleSort} align="right">Detections</SortButton>
                  </th>
                )}
                {stage === "production" && (
                  <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">Steps/Duration</th>
                )}
                <th className="px-3 py-2.5 text-right font-medium text-muted-foreground">
                  <SortButton sortKey="date" activeSort={sort} activeDir={dir} onSort={handleSort} align="right">Date</SortButton>
                </th>
                {stage === "production" && (
                  <th className="px-3 py-2.5 text-left font-medium text-muted-foreground">Details</th>
                )}
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => {
                const expanded = expandedRow === row.id;
                return (
                  <Fragment key={row.id}>
                    <tr className="border-b last:border-0 hover:bg-muted/20">
                      <td className="px-3 py-2.5 text-muted-foreground tabular-nums">{offset + index + 1}</td>
                      <td className="px-3 py-2.5">
                        <Link href={`/event/${row.id}`} className="font-mono text-primary hover:underline" title={row.id}>
                          {row.id}
                        </Link>
                      </td>
                      <td className="px-3 py-2.5"><TypeBadge value={row.event_type} /></td>
                      {stage === "triage" && (
                        <td className="px-3 py-2.5"><TriageBadge value={row.effective_triage_result} /></td>
                      )}
                      <td className="px-3 py-2.5"><StatusBadge value={row.vru_status} /></td>
                      {stage === "vru" && (
                        <td className="px-3 py-2.5"><VruLabelCell row={row} /></td>
                      )}
                      <td className="px-3 py-2.5">
                        <ProductionCell
                          row={row}
                          onPush={handlePushToProduction}
                          busy={Boolean(pushingToProduction[row.id])}
                        />
                      </td>
                      <td className="px-3 py-2.5"><FpsQcBadge value={row.fps_qc} /></td>
                      <td className="px-3 py-2.5 text-right font-mono text-xs tabular-nums text-muted-foreground">
                        {formatLatePct(row.late_frame_pct)}
                      </td>
                      <td className="px-3 py-2.5 text-right"><BitrateCell row={row} /></td>
                      {stage === "vru" && (
                        <td className="px-3 py-2.5 text-right">
                          <Badge variant="outline" className="bg-green-50 text-green-700 border-green-200">
                            {formatCount(row.detection_count)}
                          </Badge>
                        </td>
                      )}
                      {stage === "production" && (
                        <td className="px-3 py-2.5">
                          {row.production_status === "completed" ? (
                            <span className="font-mono text-xs text-muted-foreground">
                              {formatDuration(row.production_started_at, row.production_completed_at)}
                            </span>
                          ) : (
                            <div className="flex items-center gap-1.5">
                              <StepBadge status={row.privacy_status} label="Privacy" />
                              <StepBadge status={row.metadata_status} label="Meta" />
                              <StepBadge status={row.upload_status} label="S3" />
                            </div>
                          )}
                        </td>
                      )}
                      <td className="px-3 py-2.5 text-right text-muted-foreground">{formatDate(row.event_timestamp)}</td>
                      {stage === "production" && (
                        <td className="px-3 py-2.5">
                          <Button
                            type="button"
                            variant="ghost"
                            size="sm"
                            className="h-7 text-xs text-muted-foreground"
                            onClick={() => setExpandedRow(expanded ? null : row.id)}
                          >
                            {expanded ? <ChevronDown className="mr-1 h-3 w-3 rotate-180" /> : <ChevronDown className="mr-1 h-3 w-3" />}
                            Details
                          </Button>
                        </td>
                      )}
                    </tr>
                    {stage === "production" && expanded && (
                      <tr key={`${row.id}-details`} className="border-b bg-muted/30">
                        <td colSpan={11} className="px-3 py-3">
                          <div className="space-y-1.5 text-xs">
                            {row.production_error && (
                              <p className="text-red-600">{row.production_error}</p>
                            )}
                            {row.production_skip_reason && (
                              <p><span className="text-muted-foreground">Skipped:</span> {row.production_skip_reason}</p>
                            )}
                            {row.s3_video_key && s3Bucket && (
                              <a
                                href={`https://${s3Bucket}.s3.us-west-2.amazonaws.com/${row.s3_video_key}`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="block font-mono text-primary hover:underline"
                              >
                                s3://{s3Bucket}/{row.s3_video_key}
                              </a>
                            )}
                            {row.s3_metadata_key && s3Bucket && (
                              <a
                                href={`https://${s3Bucket}.s3.us-west-2.amazonaws.com/${row.s3_metadata_key}`}
                                target="_blank"
                                rel="noopener noreferrer"
                                className="block font-mono text-primary hover:underline"
                              >
                                s3://{s3Bucket}/{row.s3_metadata_key}
                              </a>
                            )}
                            {!row.production_error && !row.production_skip_reason && !row.s3_video_key && !row.s3_metadata_key && (
                              <p className="text-muted-foreground">No production details recorded.</p>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {totalPages > 1 && (
        <div className="flex items-center justify-between">
          <p className="text-sm text-muted-foreground">{total.toLocaleString()} results</p>
          <div className="flex items-center gap-2">
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={offset === 0}
              onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
            >
              <ChevronLeft className="mr-1 h-4 w-4" />
              Prev
            </Button>
            <span className="text-sm text-muted-foreground">Page {pageNum} of {totalPages}</span>
            <Button
              type="button"
              variant="outline"
              size="sm"
              disabled={offset + PAGE_SIZE >= total}
              onClick={() => setOffset(offset + PAGE_SIZE)}
            >
              Next
              <ChevronRight className="ml-1 h-4 w-4" />
            </Button>
          </div>
        </div>
      )}
    </div>
  );
}

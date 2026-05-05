import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  getPipelineOverviewRows,
  type PipelineOverviewFilters,
  type PipelineOverviewSort,
  type PipelineOverviewSortDir,
  type PipelineOverviewStage,
} from "@/lib/pipeline-overview-dashboard";

export const runtime = "nodejs";

const VALID_STAGES = new Set<PipelineOverviewStage>(["triage", "vru", "production"]);
const VALID_SORTS = new Set<PipelineOverviewSort>([
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
const VALID_FPS_QC = new Set(["perfect", "ok", "filter_out", "missing"]);
const MAX_LIMIT = 100;
const MAX_CACHE_ENTRIES = 200;

type RowsResponse = Awaited<ReturnType<typeof getPipelineOverviewRows>>;

const rowsCache = new Map<string, { expiresAt: number; data: RowsResponse }>();

function parseCsv(value: string | null): string[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
    )
  );
}

function parseEventTypes(value: string | null): string[] {
  return parseCsv(value).filter((type) => /^[A-Z0-9_]+$/.test(type));
}

function parseFpsQc(value: string | null): string[] {
  return parseCsv(value).filter((bucket) => VALID_FPS_QC.has(bucket));
}

function parseVruLabels(value: string | null): string[] {
  return parseCsv(value).filter((label) => /^[\w\s-]+$/.test(label));
}

function parseStage(value: string | null): PipelineOverviewStage {
  return value && VALID_STAGES.has(value as PipelineOverviewStage)
    ? (value as PipelineOverviewStage)
    : "triage";
}

function parseSort(value: string | null): PipelineOverviewSort {
  return value && VALID_SORTS.has(value as PipelineOverviewSort)
    ? (value as PipelineOverviewSort)
    : "date";
}

function parseDir(value: string | null): PipelineOverviewSortDir {
  return value === "asc" ? "asc" : "desc";
}

function parseLimit(value: string | null): number {
  const parsed = Number.parseInt(value ?? "50", 10);
  if (!Number.isFinite(parsed) || parsed <= 0) return 50;
  return Math.min(parsed, MAX_LIMIT);
}

function parseOffset(value: string | null): number {
  const parsed = Number.parseInt(value ?? "0", 10);
  return Number.isFinite(parsed) && parsed > 0 ? parsed : 0;
}

function parseFilters(request: NextRequest): PipelineOverviewFilters {
  const params = request.nextUrl.searchParams;
  return {
    stage: parseStage(params.get("stage")),
    status: params.get("status") ?? "all",
    period: params.get("period"),
    fpsQc: parseFpsQc(params.get("fpsQc")),
    eventTypes: parseEventTypes(params.get("eventTypes")),
    vruLabels: parseVruLabels(params.get("vruLabels")),
    sort: parseSort(params.get("sort")),
    dir: parseDir(params.get("dir")),
  };
}

function getRowsCacheTtlMs(filters: PipelineOverviewFilters): number {
  if (filters.stage === "triage") return 30_000;
  if (filters.status === "completed" || filters.status === "failed") return 30_000;
  return 10_000;
}

function pruneRowsCache(now: number): void {
  for (const [key, entry] of rowsCache) {
    if (entry.expiresAt <= now) rowsCache.delete(key);
  }
  if (rowsCache.size > MAX_CACHE_ENTRIES) rowsCache.clear();
}

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;
    const filters = parseFilters(request);
    const cacheKey = params.toString();
    const now = Date.now();
    const cached = rowsCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return NextResponse.json(cached.data);
    }

    const db = await getDb();
    const data = await getPipelineOverviewRows(db, {
      ...filters,
      limit: parseLimit(params.get("limit")),
      offset: parseOffset(params.get("offset")),
    });

    pruneRowsCache(now);
    rowsCache.set(cacheKey, {
      data,
      expiresAt: now + getRowsCacheTtlMs(filters),
    });

    return NextResponse.json(data);
  } catch (error) {
    console.error("Pipeline overview API error:", error);
    return NextResponse.json(
      { error: String(error), rows: [], total: 0 },
      { status: 500 }
    );
  }
}

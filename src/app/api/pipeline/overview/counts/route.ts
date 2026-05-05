import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  getPipelineOverviewCounts,
  type PipelineOverviewFilters,
  type PipelineOverviewSort,
  type PipelineOverviewSortDir,
  type PipelineOverviewStage,
} from "@/lib/pipeline-overview-dashboard";
import { maybeStartAutoTriage } from "@/lib/triage-auto-runner";

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
const MAX_CACHE_ENTRIES = 200;

type CountsResponse = {
  counts: Awaited<ReturnType<typeof getPipelineOverviewCounts>>;
  autoTriage: Awaited<ReturnType<typeof maybeStartAutoTriage>> | null;
};

const countsCache = new Map<string, { expiresAt: number; data: CountsResponse }>();

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

function parseFilters(request: NextRequest): PipelineOverviewFilters {
  const params = request.nextUrl.searchParams;
  return {
    stage: parseStage(params.get("stage")),
    status: params.get("status") ?? "all",
    period: params.get("period"),
    fpsQc: parseCsv(params.get("fpsQc")).filter((bucket) => VALID_FPS_QC.has(bucket)),
    eventTypes: parseCsv(params.get("eventTypes")).filter((type) => /^[A-Z0-9_]+$/.test(type)),
    vruLabels: parseCsv(params.get("vruLabels")).filter((label) => /^[\w\s-]+$/.test(label)),
    sort: parseSort(params.get("sort")),
    dir: parseDir(params.get("dir")),
  };
}

function getCountsCacheTtlMs(filters: PipelineOverviewFilters): number {
  if (filters.stage === "triage") return 30_000;
  return 10_000;
}

function pruneCountsCache(now: number): void {
  for (const [key, entry] of countsCache) {
    if (entry.expiresAt <= now) countsCache.delete(key);
  }
  if (countsCache.size > MAX_CACHE_ENTRIES) countsCache.clear();
}

export async function GET(request: NextRequest) {
  try {
    const filters = parseFilters(request);
    const cacheKey = request.nextUrl.searchParams.toString();
    const now = Date.now();
    const cached = countsCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return NextResponse.json(cached.data);
    }

    const db = await getDb();
    const apiKey = request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY || null;
    const counts = await getPipelineOverviewCounts(db, filters, { apiKey });
    const autoTriage =
      filters.stage === "triage"
        ? await maybeStartAutoTriage({
            period: filters.period,
            awaitingCount: counts.triage.awaiting,
          })
        : null;
    const data = { counts, autoTriage };

    pruneCountsCache(now);
    countsCache.set(cacheKey, {
      data,
      expiresAt: now + getCountsCacheTtlMs(filters),
    });

    return NextResponse.json(data);
  } catch (error) {
    console.error("Pipeline overview counts API error:", error);
    return NextResponse.json(
      {
        error: String(error),
        counts: {
          triage: {},
          vru: {},
          production: {},
        },
      },
      { status: 500 }
    );
  }
}

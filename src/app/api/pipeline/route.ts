import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  getPipelineCounts,
  getPipelineRows,
  getPipelineTabCount,
} from "@/lib/pipeline-dashboard";
import type {
  PipelineDashboardTab,
  PipelineFpsQcFilter,
  PipelineSort,
} from "@/lib/pipeline-dashboard";
import { isPipelineRunning } from "@/lib/pipeline-manager";

export const runtime = "nodejs";

const VALID_SORTS: PipelineSort[] = ["date_desc", "detections_desc", "detections_asc"];
const VALID_FPS_QC_FILTERS = new Set<string>(["perfect", "ok", "filter_out", "missing"]);

function parseFpsQcFilters(value: string | null): PipelineFpsQcFilter[] {
  if (!value) return [];

  return value
    .split(",")
    .map((filter) => filter.trim())
    .filter((filter): filter is PipelineFpsQcFilter => VALID_FPS_QC_FILTERS.has(filter));
}

/**
 * Returns signal events grouped by pipeline status:
 * - queued: has an explicit queued run, or is eligible and has no run yet
 * - running: has a detection run with status running
 * - completed: has a completed detection run
 * - failed: has a failed detection run with no active/completed retry
 */
export async function GET(request: NextRequest) {
  const tab = request.nextUrl.searchParams.get("tab") ?? "queued";
  const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
  const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);
  const includeCounts = request.nextUrl.searchParams.get("includeCounts") !== "false";
  const sortParam = request.nextUrl.searchParams.get("sort");
  const sort: PipelineSort = VALID_SORTS.includes(sortParam as PipelineSort)
    ? (sortParam as PipelineSort)
    : "date_desc";
  const fpsQcFilters = parseFpsQcFilters(request.nextUrl.searchParams.get("fpsQc"));

  const db = await getDb();

  const activeTab: PipelineDashboardTab =
    tab === "running" || tab === "completed" || tab === "failed" ? tab : "queued";
  const rows = await getPipelineRows(db, activeTab, limit, offset, sort, fpsQcFilters);
  const filteredTotal =
    activeTab === "completed" && fpsQcFilters.length > 0
      ? await getPipelineTabCount(db, activeTab, fpsQcFilters)
      : null;

  if (!includeCounts) {
    return NextResponse.json({
      rows,
      ...(filteredTotal !== null ? { total: filteredTotal } : {}),
      pipelineRunning: isPipelineRunning(),
    });
  }

  const counts = await getPipelineCounts(db);
  return NextResponse.json({
    counts,
    rows,
    total: filteredTotal ?? counts[activeTab],
    pipelineRunning: isPipelineRunning(),
  });
}

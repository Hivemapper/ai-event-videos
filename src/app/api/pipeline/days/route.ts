import { createHash } from "crypto";
import { mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { NextRequest, NextResponse } from "next/server";
import { fetchBeeMapsEventCountForDay } from "@/lib/beemaps";
import {
  getActivePipelineRun,
  listPipelineRuns,
  listVideoPipelineStatesForDay,
  summarizeVideoStates,
} from "@/lib/pipeline-store";
import { PipelineDaySummary, VideoPipelineState } from "@/types/pipeline";

export const runtime = "nodejs";

const DEFAULT_WINDOW_DAYS = 30;
const MAX_WINDOW_DAYS = 60;
const COUNT_CACHE_TTL_MS = 5 * 60 * 1000;
const COUNT_CACHE_DIR = join(process.cwd(), "data", "pipeline-cache", "day-counts");

interface CachedDayCount {
  totalVideos: number;
  cachedAt: number;
}

function clampWindowDays(raw: string | null) {
  const parsed = Number(raw ?? DEFAULT_WINDOW_DAYS);
  if (!Number.isFinite(parsed)) return DEFAULT_WINDOW_DAYS;
  return Math.min(MAX_WINDOW_DAYS, Math.max(1, Math.trunc(parsed)));
}

function formatLocalDay(date: Date) {
  const offsetMs = date.getTimezoneOffset() * 60 * 1000;
  return new Date(date.getTime() - offsetMs).toISOString().slice(0, 10);
}

function listRecentDays(windowDays: number) {
  return Array.from({ length: windowDays }, (_, index) => {
    const date = new Date();
    date.setDate(date.getDate() - index);
    return formatLocalDay(date);
  });
}

function ensureCacheDir() {
  mkdirSync(COUNT_CACHE_DIR, { recursive: true });
}

function dayCountCachePath(apiKey: string, day: string) {
  const key = createHash("sha1").update(`${apiKey}:${day}`).digest("hex");
  return join(COUNT_CACHE_DIR, `${key}.json`);
}

function readCachedDayCount(apiKey: string, day: string): number | null {
  const cachePath = dayCountCachePath(apiKey, day);
  try {
    const cached = JSON.parse(readFileSync(cachePath, "utf-8")) as CachedDayCount;
    if (Date.now() - cached.cachedAt > COUNT_CACHE_TTL_MS) {
      return null;
    }
    return cached.totalVideos;
  } catch {
    return null;
  }
}

function writeCachedDayCount(apiKey: string, day: string, totalVideos: number) {
  const cachePath = dayCountCachePath(apiKey, day);
  const payload: CachedDayCount = {
    totalVideos,
    cachedAt: Date.now(),
  };
  writeFileSync(cachePath, JSON.stringify(payload), "utf-8");
}

async function getDayCount(apiKey: string, day: string) {
  ensureCacheDir();
  const cached = readCachedDayCount(apiKey, day);
  if (cached !== null) {
    return { totalVideos: cached, countError: null };
  }

  try {
    const totalVideos = await fetchBeeMapsEventCountForDay({ apiKey, day });
    writeCachedDayCount(apiKey, day, totalVideos);
    return { totalVideos, countError: null };
  } catch (error) {
    return {
      totalVideos: null,
      countError:
        error instanceof Error ? error.message : "Failed to load Bee Maps day count",
    };
  }
}

function getLastCompletedAt(states: VideoPipelineState[], latestRunCompletedAt: string | null) {
  const completedState = states.find((state) => state.completedAt)?.completedAt ?? null;
  return latestRunCompletedAt ?? completedState;
}

function buildDaySummary(params: {
  day: string;
  states: VideoPipelineState[];
  totalVideos: number | null;
  latestRun: PipelineDaySummary["latestRun"];
  countError: string | null;
}): PipelineDaySummary {
  const counts = summarizeVideoStates(params.states);
  const trackedCount =
    counts.processed +
    counts.failed +
    counts.queued +
    counts.running +
    counts.stale +
    counts.unprocessed;

  const totalVideos = params.totalVideos;
  const unprocessedCount =
    totalVideos === null ? counts.unprocessed : Math.max(totalVideos - trackedCount, 0);
  const remainingCount = totalVideos === null ? null : Math.max(totalVideos - counts.processed, 0);
  const processedPercent =
    totalVideos === null
      ? null
      : totalVideos > 0
        ? Number(((counts.processed / totalVideos) * 100).toFixed(1))
        : 0;

  return {
    day: params.day,
    totalVideos,
    processedCount: counts.processed,
    failedCount: counts.failed,
    queuedCount: counts.queued,
    runningCount: counts.running,
    staleCount: counts.stale,
    unprocessedCount,
    remainingCount,
    processedPercent,
    latestRun: params.latestRun,
    currentVideoId: params.latestRun?.totals.currentVideoId ?? null,
    lastCompletedAt: getLastCompletedAt(params.states, params.latestRun?.completedAt ?? null),
    countError: params.countError,
  };
}

function shouldIncludeDay(summary: PipelineDaySummary) {
  return (
    (summary.totalVideos ?? 0) > 0 ||
    summary.processedCount > 0 ||
    summary.failedCount > 0 ||
    summary.queuedCount > 0 ||
    summary.runningCount > 0 ||
    summary.staleCount > 0 ||
    summary.latestRun !== null
  );
}

export async function GET(request: NextRequest) {
  const apiKey =
    request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY || "";

  if (!apiKey) {
    return NextResponse.json(
      { error: "Bee Maps API key is required" },
      { status: 401 }
    );
  }

  const windowDays = clampWindowDays(request.nextUrl.searchParams.get("window"));
  const days = listRecentDays(windowDays);
  const activeRun = await getActivePipelineRun();

  try {
    const summaries = await Promise.all(
      days.map(async (day) => {
        const [countResult, states, runs] = await Promise.all([
          getDayCount(apiKey, day),
          listVideoPipelineStatesForDay(day),
          listPipelineRuns(day),
        ]);

        return buildDaySummary({
          day,
          states,
          totalVideos: countResult.totalVideos,
          latestRun: runs[0] ?? null,
          countError: countResult.countError,
        });
      })
    );

    return NextResponse.json({
      days: summaries.filter(shouldIncludeDay),
      activeRun,
    });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Failed to load pipeline day summaries",
      },
      { status: 500 }
    );
  }
}

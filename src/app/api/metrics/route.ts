import { NextRequest, NextResponse } from "next/server";
import { ALL_EVENT_TYPES } from "@/lib/constants";
import {
  fetchSearchCount,
  getAuthCacheKey,
  normalizeBeeMapsAuthHeader,
  runLimited,
} from "@/lib/metrics-counts";

const CACHE_TTL_MS = 5 * 60 * 1000;
const SUMMARY_CACHE_TTL_MS = 5 * 60 * 1000;
const SUMMARY_CONCURRENCY = 5;
const SUMMARY_CHUNK_CONCURRENCY = 8;
const BREAKDOWN_CONCURRENCY = 2;

type PeriodMetrics = {
  total: number;
  byType: Record<string, number>;
  partial?: boolean;
};

type MetricsData = Record<string, PeriodMetrics>;

const cache = new Map<string, { expiresAt: number; data: MetricsData }>();
const inFlight = new Map<string, Promise<MetricsData>>();
const summaryCache = new Map<string, { expiresAt: number; data: MetricsData }>();
const summaryInFlight = new Map<string, Promise<MetricsData>>();

function getPeriods(now: Date) {
  return [
    {
      key: "all",
      start: "2025-02-01T00:00:00.000Z",
      end: now.toISOString(),
    },
    {
      key: "60d",
      start: new Date(
        now.getTime() - 60 * 24 * 60 * 60 * 1000
      ).toISOString(),
      end: now.toISOString(),
    },
    {
      key: "30d",
      start: new Date(
        now.getTime() - 30 * 24 * 60 * 60 * 1000
      ).toISOString(),
      end: now.toISOString(),
    },
    {
      key: "7d",
      start: new Date(
        now.getTime() - 7 * 24 * 60 * 60 * 1000
      ).toISOString(),
      end: now.toISOString(),
    },
    {
      key: "24h",
      start: new Date(
        now.getTime() - 1 * 24 * 60 * 60 * 1000
      ).toISOString(),
      end: now.toISOString(),
    },
  ];
}

function emptyMetrics(periods: ReturnType<typeof getPeriods>): MetricsData {
  const metrics: MetricsData = {};
  for (const period of periods) {
    metrics[period.key] = { total: 0, byType: {} };
  }
  return metrics;
}

async function buildSummaryMetrics(authHeader: string): Promise<MetricsData> {
  const now = new Date();
  const periods = getPeriods(now);
  const windows = getBreakdownWindows(periods);
  const metrics = emptyMetrics(periods);

  const results = await runLimited(
    windows.map((window) => () =>
      fetchSearchCount(authHeader, window.start, window.end, undefined, {
        chunkConcurrency: SUMMARY_CHUNK_CONCURRENCY,
      })
    ),
    SUMMARY_CONCURRENCY
  );

  const countsByWindow = new Map<string, { count: number; partial: boolean }>();
  windows.forEach((window, index) => countsByWindow.set(window.key, results[index]));

  for (const period of periods) {
    for (const windowKey of windowsForPeriod(period.key)) {
      const result = countsByWindow.get(windowKey);
      if (!result) continue;
      metrics[period.key].total += result.count;
      if (result.partial) metrics[period.key].partial = true;
    }
  }

  return metrics;
}

function getBreakdownWindows(periods: ReturnType<typeof getPeriods>) {
  const byKey = Object.fromEntries(periods.map((period) => [period.key, period]));
  return [
    { key: "before60d", start: byKey.all.start, end: byKey["60d"].start },
    { key: "60to30d", start: byKey["60d"].start, end: byKey["30d"].start },
    { key: "30to7d", start: byKey["30d"].start, end: byKey["7d"].start },
    { key: "7dto24h", start: byKey["7d"].start, end: byKey["24h"].start },
    { key: "24h", start: byKey["24h"].start, end: byKey["24h"].end },
  ].filter((window) => new Date(window.start).getTime() < new Date(window.end).getTime());
}

function windowsForPeriod(period: string): string[] {
  switch (period) {
    case "all":
      return ["before60d", "60to30d", "30to7d", "7dto24h", "24h"];
    case "60d":
      return ["60to30d", "30to7d", "7dto24h", "24h"];
    case "30d":
      return ["30to7d", "7dto24h", "24h"];
    case "7d":
      return ["7dto24h", "24h"];
    case "24h":
      return ["24h"];
    default:
      return [];
  }
}

async function buildBreakdownMetrics(authHeader: string): Promise<MetricsData> {
  const now = new Date();
  const periods = getPeriods(now);
  const windows = getBreakdownWindows(periods);
  const metrics = emptyMetrics(periods);

  const tasks: {
    type: string;
    windowKey: string;
    run: () => Promise<{ count: number; partial: boolean }>;
  }[] = [];

  for (const type of ALL_EVENT_TYPES) {
    for (const window of windows) {
      tasks.push({
        type,
        windowKey: window.key,
        run: () => fetchSearchCount(authHeader, window.start, window.end, [type]),
      });
    }
  }

  const results = await runLimited(
    tasks.map((task) => task.run),
    BREAKDOWN_CONCURRENCY
  );

  const countsByTypeWindow = new Map<string, { count: number; partial: boolean }>();

  tasks.forEach((task, i) => {
    const { count, partial } = results[i];
    countsByTypeWindow.set(`${task.type}:${task.windowKey}`, { count, partial });
  });

  for (const period of periods) {
    for (const type of ALL_EVENT_TYPES) {
      let typeTotal = 0;
      let typePartial = false;
      for (const windowKey of windowsForPeriod(period.key)) {
        const result = countsByTypeWindow.get(`${type}:${windowKey}`);
        if (!result) continue;
        typeTotal += result.count;
        typePartial = typePartial || result.partial;
      }
      metrics[period.key].byType[type] = typeTotal;
      metrics[period.key].total += typeTotal;
      if (typePartial) metrics[period.key].partial = true;
    }
  }

  return metrics;
}

function isPartial(data: MetricsData): boolean {
  for (const period of Object.values(data)) {
    if (period.partial) return true;
  }
  return false;
}

async function getCachedMetrics(
  cacheKey: string,
  mode: "summary" | "breakdown",
  build: () => Promise<MetricsData>
): Promise<{ metrics: MetricsData; cacheStatus: string }> {
  const selectedCache = mode === "summary" ? summaryCache : cache;
  const selectedInFlight = mode === "summary" ? summaryInFlight : inFlight;
  const ttl = mode === "summary" ? SUMMARY_CACHE_TTL_MS : CACHE_TTL_MS;

  const cached = selectedCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return { metrics: cached.data, cacheStatus: "hit" };
  }

  let promise = selectedInFlight.get(cacheKey);
  if (!promise) {
    promise = build();
    selectedInFlight.set(cacheKey, promise);
    promise.finally(() => selectedInFlight.delete(cacheKey)).catch(() => {});
  }

  const metrics = await promise;
  const partial = isPartial(metrics);
  if (!partial || mode === "breakdown") {
    selectedCache.set(cacheKey, {
      data: metrics,
      expiresAt: Date.now() + (partial ? 60 * 1000 : ttl),
    });
  }

  return {
    metrics,
    cacheStatus: partial ? "miss-partial" : "miss",
  };
}

export async function GET(request: NextRequest) {
  try {
    const apiKey =
      request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;
    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }
    const authHeader = normalizeBeeMapsAuthHeader(apiKey);
    const mode =
      request.nextUrl.searchParams.get("mode") === "summary"
        ? "summary"
        : "breakdown";
    const cacheKey = `${mode}:${getAuthCacheKey(authHeader)}`;
    const { metrics, cacheStatus } = await getCachedMetrics(cacheKey, mode, () =>
      mode === "summary"
        ? buildSummaryMetrics(authHeader)
        : buildBreakdownMetrics(authHeader)
    );
    const partial = isPartial(metrics);

    return NextResponse.json(metrics, {
      headers: {
        "Cache-Control": partial
          ? "private, no-store"
          : "private, max-age=60, stale-while-revalidate=300",
        "X-Metrics-Cache": cacheStatus,
        "X-Metrics-Mode": mode,
      },
    });
  } catch (error) {
    console.error("Metrics API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

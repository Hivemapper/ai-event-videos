import { NextRequest, NextResponse } from "next/server";
import {
  fetchSearchCount,
  getAuthCacheKey,
  normalizeBeeMapsAuthHeader,
  runLimited,
} from "@/lib/metrics-counts";

const MONTH_NAMES = [
  "January", "February", "March", "April", "May", "June",
  "July", "August", "September", "October", "November", "December",
];

const FIRST_YEAR = 2026;
const FIRST_MONTH = 0; // January
const MONTHLY_CACHE_TTL_MS = 5 * 60 * 1000;

interface MonthEntry {
  key: string;        // "2026-01"
  label: string;      // "Jan 2026"
  startDate: string;  // ISO start of month UTC
  endDate: string;    // ISO start of next month UTC (exclusive)
  total: number;
  partial: boolean;
}

interface MonthlyResult {
  months: MonthEntry[];
  partial: boolean;
}

const monthlyCache = new Map<string, { expiresAt: number; data: MonthlyResult }>();

function buildMonths(now: Date): Omit<MonthEntry, "total" | "partial">[] {
  const months: Omit<MonthEntry, "total" | "partial">[] = [];
  let year = FIRST_YEAR;
  let month = FIRST_MONTH;
  while (year < now.getUTCFullYear() || (year === now.getUTCFullYear() && month <= now.getUTCMonth())) {
    const start = new Date(Date.UTC(year, month, 1));
    const end = new Date(Date.UTC(year, month + 1, 1));
    months.push({
      key: `${year}-${String(month + 1).padStart(2, "0")}`,
      label: `${MONTH_NAMES[month].slice(0, 3)} ${year}`,
      startDate: start.toISOString(),
      endDate: end.toISOString(),
    });
    month += 1;
    if (month > 11) { month = 0; year += 1; }
  }
  return months;
}

export async function GET(request: NextRequest) {
  try {
    const apiKey =
      request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: "API key is required" }, { status: 401 });
    }
    const authHeader = normalizeBeeMapsAuthHeader(apiKey);
    const cacheKey = getAuthCacheKey(authHeader);
    const cached = monthlyCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return NextResponse.json(cached.data, {
        headers: {
          "Cache-Control": "private, max-age=300, stale-while-revalidate=600",
          "X-Metrics-Cache": "hit",
        },
      });
    }

    const months = buildMonths(new Date());

    const results = await runLimited(
      months.map((month) => async (): Promise<MonthEntry> => {
        let total = 0;
        let partial = false;
        try {
          const result = await fetchSearchCount(
            authHeader,
            month.startDate,
            month.endDate
          );
          total = result.count;
          partial = result.partial;
        } catch (err) {
          console.error(
            `[metrics/monthly] ${month.key} failed:`,
            err instanceof Error ? err.message : err
          );
          partial = true;
        }
        return { ...month, total, partial };
      }),
      3
    );

    const anyPartial = results.some((r) => r.partial);
    const data = { months: results, partial: anyPartial };
    if (!anyPartial) {
      monthlyCache.set(cacheKey, {
        data,
        expiresAt: Date.now() + MONTHLY_CACHE_TTL_MS,
      });
    }

    return NextResponse.json(
      data,
      {
        headers: {
          "Cache-Control": anyPartial
            ? "private, no-store"
            : "private, max-age=300, stale-while-revalidate=600",
          "X-Metrics-Cache": anyPartial ? "miss-partial" : "miss",
        },
      }
    );
  } catch (error) {
    console.error("Monthly metrics API error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

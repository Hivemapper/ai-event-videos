import { NextRequest, NextResponse } from "next/server";
import {
  fetchSearchCount,
  getAuthCacheKey,
  normalizeBeeMapsAuthHeader,
} from "@/lib/metrics-counts";

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const DAILY_CACHE_TTL_MS = 5 * 60 * 1000;

interface DailyResult {
  date: string;
  day: string;
  total: number;
}

const dailyCache = new Map<string, { expiresAt: number; data: DailyResult[] }>();

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
    const cacheKey = getAuthCacheKey(authHeader);
    const cached = dailyCache.get(cacheKey);
    if (cached && cached.expiresAt > Date.now()) {
      return NextResponse.json(cached.data, {
        headers: {
          "Cache-Control": "private, max-age=60, stale-while-revalidate=300",
          "X-Metrics-Cache": "hit",
        },
      });
    }

    const now = new Date();
    const days: { date: string; day: string; startDate: string; endDate: string }[] = [];

    for (let i = 6; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const dateStr = `${yyyy}-${mm}-${dd}`;
      days.push({
        date: dateStr,
        day: DAY_NAMES[d.getDay()],
        startDate: `${dateStr}T00:00:00.000Z`,
        endDate: `${dateStr}T23:59:59.999Z`,
      });
    }

    const counts = await Promise.all(
      days.map(async (day) => {
        const result = await fetchSearchCount(
          authHeader,
          day.startDate,
          day.endDate
        );
        return result.count;
      })
    );

    const result = days.map((day, i) => ({
      date: day.date,
      day: day.day,
      total: counts[i],
    }));
    dailyCache.set(cacheKey, {
      data: result,
      expiresAt: Date.now() + DAILY_CACHE_TTL_MS,
    });

    return NextResponse.json(result, {
      headers: {
        "Cache-Control": "private, max-age=60, stale-while-revalidate=300",
        "X-Metrics-Cache": "miss",
      },
    });
  } catch (error) {
    console.error("Daily metrics API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

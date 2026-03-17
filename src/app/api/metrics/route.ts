import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL, ALL_EVENT_TYPES } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

function splitDateRange(start: string, end: string) {
  const s = new Date(start).getTime();
  const e = new Date(end).getTime();
  if (e - s <= MAX_RANGE_MS) return [{ startDate: start, endDate: end }];
  const chunks: { startDate: string; endDate: string }[] = [];
  let cur = s;
  while (cur < e) {
    const ce = Math.min(cur + MAX_RANGE_MS, e);
    chunks.push({
      startDate: new Date(cur).toISOString(),
      endDate: new Date(ce).toISOString(),
    });
    cur = ce;
  }
  return chunks;
}

/** Fetch the total count for a date range + optional type filter, splitting into ≤31-day chunks. */
async function fetchCount(
  authHeader: string,
  startDate: string,
  endDate: string,
  types?: string[]
): Promise<number> {
  const chunks = splitDateRange(startDate, endDate);
  const counts = await Promise.all(
    chunks.map(async (chunk) => {
      const body: Record<string, unknown> = {
        startDate: chunk.startDate,
        endDate: chunk.endDate,
        limit: 1,
        offset: 0,
      };
      if (types) body.types = types;
      const res = await fetchWithRetry(`${API_BASE_URL}/search`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: authHeader,
        },
        body: JSON.stringify(body),
      });
      if (!res.ok) return 0;
      const data = await res.json();
      return data.pagination?.total ?? 0;
    })
  );
  return counts.reduce((a, b) => a + b, 0);
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
    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    const now = new Date();
    const periods = [
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

    // Fire all count requests in parallel: 3 periods × 10 types = 30 lightweight calls
    const tasks: {
      period: string;
      type: string;
      promise: Promise<number>;
    }[] = [];
    for (const period of periods) {
      for (const type of ALL_EVENT_TYPES) {
        tasks.push({
          period: period.key,
          type,
          promise: fetchCount(authHeader, period.start, period.end, [type]),
        });
      }
    }

    const results = await Promise.all(tasks.map((t) => t.promise));

    const metrics: Record<
      string,
      { total: number; byType: Record<string, number> }
    > = {};
    for (const period of periods) {
      metrics[period.key] = { total: 0, byType: {} };
    }

    tasks.forEach((task, i) => {
      const count = results[i];
      metrics[task.period].byType[task.type] = count;
      metrics[task.period].total += count;
    });

    return NextResponse.json(metrics);
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

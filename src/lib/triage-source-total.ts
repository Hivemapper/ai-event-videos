import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

export const TRIAGE_PERIODS = {
  "1": ["2025-01-01T00:00:00.000Z", "2025-09-15T00:00:00.000Z"],
  "2": ["2025-09-15T00:00:00.000Z", "2026-01-20T00:00:00.000Z"],
  "3": ["2026-01-20T00:00:00.000Z", "2026-02-25T00:00:00.000Z"],
  "4": ["2026-02-25T00:00:00.000Z", "2026-03-15T00:00:00.000Z"],
  "5": ["2026-03-15T00:00:00.000Z", "2026-04-17T00:00:00.000Z"],
  "6": ["2026-04-17T00:00:00.000Z", "2026-04-22T20:45:00.500Z"],
  "7": ["2026-04-22T20:45:00.500Z", "2099-01-01T00:00:00.000Z"],
} as const;

export type TriagePeriod = keyof typeof TRIAGE_PERIODS;

export const TRIAGE_MIN_PERIOD = "4" satisfies TriagePeriod;
export const TRIAGE_MIN_START = TRIAGE_PERIODS[TRIAGE_MIN_PERIOD][0];
export const TRIAGE_END = TRIAGE_PERIODS["7"][1];
export const SUPPORTED_TRIAGE_PERIODS = new Set<TriagePeriod>(["4", "5", "6", "7"]);

const EVENT_TOTAL_CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_EVENT_TOTAL_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

const eventTotalCache = new Map<string, { total: number; expiresAt: number }>();

export function normalizeTriagePeriod(period: string | null): TriagePeriod | null {
  return period && SUPPORTED_TRIAGE_PERIODS.has(period as TriagePeriod)
    ? (period as TriagePeriod)
    : null;
}

export function normalizeTriagePeriodForSource(period: string | null): TriagePeriod | null {
  return normalizeTriagePeriod(period);
}

function normalizeAuthHeader(value: string): string {
  return value.startsWith("Basic ") ? value : `Basic ${value}`;
}

function getSourceDateRange(period: string | null): { startDate: string; endDate: string } {
  const normalizedPeriod = normalizeTriagePeriodForSource(period);
  const [periodStart, periodEnd] = normalizedPeriod
    ? TRIAGE_PERIODS[normalizedPeriod]
    : [TRIAGE_MIN_START, TRIAGE_END];
  const now = Date.now();
  const endMs = Math.min(new Date(periodEnd).getTime(), now);

  return {
    startDate: periodStart,
    endDate: new Date(endMs).toISOString(),
  };
}

function splitDateRange(startDate: string, endDate: string): Array<{ startDate: string; endDate: string }> {
  const startMs = new Date(startDate).getTime();
  const endMs = new Date(endDate).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return [];
  }

  const chunks: Array<{ startDate: string; endDate: string }> = [];
  for (let chunkStart = startMs; chunkStart < endMs; chunkStart += MAX_EVENT_TOTAL_RANGE_MS) {
    const chunkEnd = Math.min(chunkStart + MAX_EVENT_TOTAL_RANGE_MS, endMs);
    chunks.push({
      startDate: new Date(chunkStart).toISOString(),
      endDate: new Date(chunkEnd).toISOString(),
    });
  }
  return chunks;
}

export async function fetchTriageSourceEventTotal(
  period: string | null,
  apiKey: string | null
): Promise<number | null> {
  if (!apiKey) return null;

  const normalizedPeriod = normalizeTriagePeriodForSource(period);
  const cacheKey = normalizedPeriod ? `period-${normalizedPeriod}` : "period-4-plus";
  const cached = eventTotalCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.total;
  }

  const authHeader = normalizeAuthHeader(apiKey);
  const { startDate, endDate } = getSourceDateRange(period);
  const chunks = splitDateRange(startDate, endDate);
  if (chunks.length === 0) return 0;

  try {
    const totals = await Promise.all(
      chunks.map(async (chunk) => {
        const response = await fetchWithRetry(
          `${API_BASE_URL}/search`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: authHeader,
            },
            body: JSON.stringify({ ...chunk, limit: 1, offset: 0 }),
          },
          { retries: 1, backoffMs: 1000 }
        );

        if (!response.ok) {
          throw new Error(`Bee Maps event count failed with HTTP ${response.status}`);
        }

        const data = await response.json();
        return Number(data.pagination?.total ?? data.events?.length ?? 0);
      })
    );

    const total = totals.reduce((sum, value) => sum + value, 0);
    eventTotalCache.set(cacheKey, {
      total,
      expiresAt: Date.now() + EVENT_TOTAL_CACHE_TTL_MS,
    });
    return total;
  } catch (error) {
    console.warn("Failed to load Bee Maps source total for triage:", error);
    if (cached) return cached.total;
    return null;
  }
}

export function getAwaitingTriageTotal(sourceTotal: number | null, triagedTotal: number): number | null {
  if (sourceTotal === null) return null;
  return Math.max(0, sourceTotal - triagedTotal);
}

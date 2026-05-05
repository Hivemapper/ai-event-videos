import { createHash } from "crypto";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000;
const LIVE_RANGE_TTL_MS = 5 * 60 * 1000;
const CLOSED_RANGE_TTL_MS = 12 * 60 * 60 * 1000;
const CHUNK_CONCURRENCY = 4;

export interface CountRange {
  startDate: string;
  endDate: string;
}

export interface CountResult {
  count: number;
  partial: boolean;
}

interface CachedCount {
  expiresAt: number;
  result: CountResult;
}

const countCache = new Map<string, CachedCount>();
const inFlightCounts = new Map<string, Promise<CountResult>>();

interface FetchSearchCountOptions {
  chunkConcurrency?: number;
}

export function normalizeBeeMapsAuthHeader(apiKey: string): string {
  return apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;
}

export function getAuthCacheKey(authHeader: string): string {
  return createHash("sha256").update(authHeader).digest("hex");
}

function splitDateRange(startDate: string, endDate: string): CountRange[] {
  const startMs = new Date(startDate).getTime();
  const endMs = new Date(endDate).getTime();
  if (endMs - startMs <= MAX_RANGE_MS) return [{ startDate, endDate }];

  const chunks: CountRange[] = [];
  let currentStartMs = startMs;
  while (currentStartMs < endMs) {
    const currentEndMs = Math.min(currentStartMs + MAX_RANGE_MS, endMs);
    chunks.push({
      startDate: new Date(currentStartMs).toISOString(),
      endDate: new Date(currentEndMs).toISOString(),
    });
    currentStartMs = currentEndMs;
  }
  return chunks;
}

function getCountCacheKey(
  authHeader: string,
  range: CountRange,
  types?: readonly string[]
): string {
  return JSON.stringify({
    auth: getAuthCacheKey(authHeader),
    startDate: range.startDate,
    endDate: range.endDate,
    types: types?.slice().sort() ?? null,
  });
}

function getCountTtlMs(endDate: string): number {
  const endMs = new Date(endDate).getTime();
  return Date.now() - endMs < 10 * 60 * 1000
    ? LIVE_RANGE_TTL_MS
    : CLOSED_RANGE_TTL_MS;
}

async function runLimited<T>(
  tasks: Array<() => Promise<T>>,
  concurrency: number
): Promise<T[]> {
  const results: T[] = new Array(tasks.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < tasks.length) {
      const index = nextIndex;
      nextIndex += 1;
      results[index] = await tasks[index]();
    }
  }

  await Promise.all(
    Array.from({ length: Math.min(concurrency, tasks.length) }, () => worker())
  );

  return results;
}

async function fetchSingleCount(
  authHeader: string,
  range: CountRange,
  types?: readonly string[]
): Promise<CountResult> {
  const cacheKey = getCountCacheKey(authHeader, range, types);
  const cached = countCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) return cached.result;

  let promise = inFlightCounts.get(cacheKey);
  if (!promise) {
    promise = (async () => {
      const body: Record<string, unknown> = {
        startDate: range.startDate,
        endDate: range.endDate,
        limit: 1,
        offset: 0,
      };
      if (types && types.length > 0) body.types = types;

      try {
        const response = await fetchWithRetry(`${API_BASE_URL}/search`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: authHeader,
          },
          body: JSON.stringify(body),
        });

        if (!response.ok) {
          console.error(
            `[metrics] count returned ${response.status} (${range.startDate}..${range.endDate}, types=${types?.join(",") ?? "all"})`
          );
          return { count: 0, partial: true };
        }

        const data = await response.json();
        const result = {
          count: data.pagination?.total ?? 0,
          partial: false,
        };
        countCache.set(cacheKey, {
          result,
          expiresAt: Date.now() + getCountTtlMs(range.endDate),
        });
        return result;
      } catch (error) {
        console.error(
          `[metrics] count failed (${range.startDate}..${range.endDate}, types=${types?.join(",") ?? "all"}):`,
          error instanceof Error ? error.message : error
        );
        return { count: 0, partial: true };
      }
    })();
    inFlightCounts.set(cacheKey, promise);
    promise.finally(() => inFlightCounts.delete(cacheKey)).catch(() => {});
  }

  return promise;
}

export async function fetchSearchCount(
  authHeader: string,
  startDate: string,
  endDate: string,
  types?: readonly string[],
  options: FetchSearchCountOptions = {}
): Promise<CountResult> {
  const chunks = splitDateRange(startDate, endDate);
  const results = await runLimited(
    chunks.map((chunk) => () => fetchSingleCount(authHeader, chunk, types)),
    options.chunkConcurrency ?? CHUNK_CONCURRENCY
  );

  return results.reduce<CountResult>(
    (acc, result) => ({
      count: acc.count + result.count,
      partial: acc.partial || result.partial,
    }),
    { count: 0, partial: false }
  );
}

export { runLimited };

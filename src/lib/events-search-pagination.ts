export const MAX_EVENT_SEARCH_RANGE_MS = 31 * 24 * 60 * 60 * 1000;
export const MAX_EVENT_SEARCH_PAGE_LIMIT = 500;

export interface EventSearchDateChunk {
  startDate: string;
  endDate: string;
}

export interface EventSearchChunkTotal extends EventSearchDateChunk {
  total: number;
}

export interface EventSearchPageFetch {
  startDate: string;
  endDate: string;
  offset: number;
  limit: number;
}

export function splitEventSearchDateRange(
  startDate: string,
  endDate: string,
  maxRangeMs = MAX_EVENT_SEARCH_RANGE_MS
): EventSearchDateChunk[] {
  const start = new Date(startDate).getTime();
  const end = new Date(endDate).getTime();

  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return [{ startDate, endDate }];
  }

  if (end - start <= maxRangeMs) {
    return [{ startDate, endDate }];
  }

  const chunks: EventSearchDateChunk[] = [];
  let chunkStart = start;
  while (chunkStart <= end) {
    const chunkEnd = Math.min(chunkStart + maxRangeMs, end);
    chunks.push({
      startDate: new Date(chunkStart).toISOString(),
      endDate: new Date(chunkEnd).toISOString(),
    });
    if (chunkEnd >= end) break;
    chunkStart = chunkEnd + 1;
  }

  return chunks;
}

export function newestFirstEventSearchChunks(
  chunks: EventSearchDateChunk[]
): EventSearchDateChunk[] {
  return [...chunks].reverse();
}

export function planEventSearchPageFetches(
  chunkTotals: EventSearchChunkTotal[],
  offset: number,
  limit: number
): EventSearchPageFetch[] {
  let remainingOffset = Math.max(0, offset);
  let remainingLimit = Math.max(0, Math.min(limit, MAX_EVENT_SEARCH_PAGE_LIMIT));
  const fetches: EventSearchPageFetch[] = [];

  for (const chunk of chunkTotals) {
    const total = Math.max(0, chunk.total);
    if (total === 0) continue;

    if (remainingOffset >= total) {
      remainingOffset -= total;
      continue;
    }

    const available = total - remainingOffset;
    const pageLimit = Math.min(remainingLimit, available, MAX_EVENT_SEARCH_PAGE_LIMIT);
    if (pageLimit > 0) {
      fetches.push({
        startDate: chunk.startDate,
        endDate: chunk.endDate,
        offset: remainingOffset,
        limit: pageLimit,
      });
      remainingLimit -= pageLimit;
    }

    remainingOffset = 0;
    if (remainingLimit <= 0) break;
  }

  return fetches;
}

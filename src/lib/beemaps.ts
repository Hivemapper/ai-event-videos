import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";
import { AIEvent } from "@/types/events";

interface BeeMapsEventsPage {
  events: AIEvent[];
  pagination?: {
    total?: number;
    limit?: number;
    offset?: number;
  };
}

export function normalizeBeeMapsAuthHeader(apiKey: string): string {
  return apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;
}

export function getUtcDayBounds(day: string) {
  return {
    startDate: new Date(`${day}T00:00:00.000Z`).toISOString(),
    endDate: new Date(`${day}T23:59:59.999Z`).toISOString(),
  };
}

export async function fetchBeeMapsEventsPage(params: {
  apiKey: string;
  day: string;
  limit: number;
  offset: number;
}): Promise<BeeMapsEventsPage> {
  const { apiKey, day, limit, offset } = params;
  const { startDate, endDate } = getUtcDayBounds(day);
  const response = await fetchWithRetry(`${API_BASE_URL}/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: normalizeBeeMapsAuthHeader(apiKey),
    },
    body: JSON.stringify({
      startDate,
      endDate,
      limit,
      offset,
    }),
  });

  if (!response.ok) {
    const message = await response.text();
    throw new Error(message || `Bee Maps error: ${response.status}`);
  }

  return response.json();
}

export async function fetchAllBeeMapsEventsForDay(params: {
  apiKey: string;
  day: string;
  pageSize?: number;
}): Promise<AIEvent[]> {
  const { apiKey, day, pageSize = 500 } = params;
  const results: AIEvent[] = [];
  let offset = 0;
  let total = Number.POSITIVE_INFINITY;

  while (offset < total) {
    const page = await fetchBeeMapsEventsPage({
      apiKey,
      day,
      limit: pageSize,
      offset,
    });
    results.push(...page.events);
    const pageTotal = page.pagination?.total ?? page.events.length;
    total = pageTotal;
    if (page.events.length === 0) {
      break;
    }
    offset += page.events.length;
  }

  return results;
}

export async function fetchBeeMapsEventCountForDay(params: {
  apiKey: string;
  day: string;
}): Promise<number> {
  const page = await fetchBeeMapsEventsPage({
    apiKey: params.apiKey,
    day: params.day,
    limit: 1,
    offset: 0,
  });

  return page.pagination?.total ?? page.events.length;
}

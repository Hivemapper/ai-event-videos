import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";
import { eventsSearchSchema } from "@/lib/schemas";

const MAX_RANGE_MS = 31 * 24 * 60 * 60 * 1000; // 31 days in ms

/** Split a date range into ≤31-day chunks. */
function splitDateRange(startDate: string, endDate: string): { startDate: string; endDate: string }[] {
  const start = new Date(startDate).getTime();
  const end = new Date(endDate).getTime();

  if (end - start <= MAX_RANGE_MS) {
    return [{ startDate, endDate }];
  }

  const chunks: { startDate: string; endDate: string }[] = [];
  let chunkStart = start;
  while (chunkStart < end) {
    const chunkEnd = Math.min(chunkStart + MAX_RANGE_MS, end);
    chunks.push({
      startDate: new Date(chunkStart).toISOString(),
      endDate: new Date(chunkEnd).toISOString(),
    });
    chunkStart = chunkEnd;
  }
  return chunks;
}

function parseApiError(errorText: string, status: number): string {
  try {
    const errorJson = JSON.parse(errorText);
    if (typeof errorJson === 'string') return errorJson;
    if (Array.isArray(errorJson)) {
      return errorJson.map((e: unknown) => {
        if (typeof e === 'string') return e;
        const err = e as Record<string, unknown>;
        return err.message || JSON.stringify(e);
      }).join(', ');
    }
    if (errorJson.message) return typeof errorJson.message === 'string' ? errorJson.message : JSON.stringify(errorJson.message);
    if (errorJson.error) return typeof errorJson.error === 'string' ? errorJson.error : JSON.stringify(errorJson.error);
    if (errorJson.errors && Array.isArray(errorJson.errors)) {
      return errorJson.errors.map((e: unknown) =>
        typeof e === 'string' ? e : (e as Record<string, unknown>).message || JSON.stringify(e)
      ).join(', ');
    }
    return JSON.stringify(errorJson);
  } catch {
    return errorText || `API error: ${status}`;
  }
}

async function fetchChunk(
  body: Record<string, unknown>,
  authHeader: string,
): Promise<{ events: Record<string, unknown>[]; total: number }> {
  const response = await fetchWithRetry(`${API_BASE_URL}/search`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: authHeader,
    },
    body: JSON.stringify(body),
  });

  if (!response.ok) {
    const errorText = await response.text();
    const msg = parseApiError(errorText, response.status);
    throw new Error(msg);
  }

  const data = await response.json();
  return { events: data.events, total: data.pagination?.total ?? data.events.length };
}

export async function POST(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    const raw = await request.json();
    const parsed = eventsSearchSchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    const body = parsed.data;
    console.log("Request body:", JSON.stringify(body));

    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;
    const chunks = splitDateRange(body.startDate, body.endDate);

    // Single chunk — pass through directly (preserves offset/limit for pagination)
    if (chunks.length === 1) {
      const result = await fetchChunk(body, authHeader);
      return NextResponse.json({
        events: result.events,
        pagination: { total: result.total, limit: body.limit ?? 50, offset: body.offset ?? 0 },
      });
    }

    // Multiple chunks — fetch all in parallel, merge, then apply offset/limit
    console.log(`Splitting ${body.startDate} → ${body.endDate} into ${chunks.length} chunks`);

    const results = await Promise.all(
      chunks.map((chunk) =>
        fetchChunk({ ...body, startDate: chunk.startDate, endDate: chunk.endDate, limit: 500, offset: 0 }, authHeader)
      )
    );

    // Merge and deduplicate by event id
    const seen = new Set<string>();
    const allEvents: Record<string, unknown>[] = [];
    for (const result of results) {
      for (const event of result.events) {
        const id = (event as { id?: string }).id ?? JSON.stringify(event);
        if (!seen.has(id)) {
          seen.add(id);
          allEvents.push(event);
        }
      }
    }

    // Sort by timestamp descending (newest first)
    allEvents.sort((a, b) => {
      const ta = (a as { timestamp?: string }).timestamp ?? "";
      const tb = (b as { timestamp?: string }).timestamp ?? "";
      return tb.localeCompare(ta);
    });

    const total = results.reduce((sum, r) => sum + r.total, 0);
    const offset = body.offset ?? 0;
    const limit = body.limit ?? 50;
    const paged = allEvents.slice(offset, offset + limit);

    return NextResponse.json({
      events: paged,
      pagination: { total, limit, offset },
    });
  } catch (error) {
    console.error("API proxy error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

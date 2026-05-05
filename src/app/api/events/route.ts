import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { getDb } from "@/lib/db";
import { fetchWithRetry } from "@/lib/fetch-retry";
import { loadLocalEditedMetadata, localEditedMetadataToEvent } from "@/lib/local-edited-events";
import {
  newestFirstEventSearchChunks,
  planEventSearchPageFetches,
  splitEventSearchDateRange,
} from "@/lib/events-search-pagination";
import { eventsSearchSchema } from "@/lib/schemas";
import { expandVruObjectFilterAliases } from "@/lib/vru-labels";
import type { z } from "zod";

export const runtime = "nodejs";

type EventsSearchBody = z.infer<typeof eventsSearchSchema>;

function placeholders(values: unknown[]): string {
  return values.map(() => "?").join(", ");
}

function polygonBounds(polygon: number[][]) {
  return polygon.reduce(
    (bounds, [lon, lat]) => ({
      minLon: Math.min(bounds.minLon, lon),
      maxLon: Math.max(bounds.maxLon, lon),
      minLat: Math.min(bounds.minLat, lat),
      maxLat: Math.max(bounds.maxLat, lat),
    }),
    {
      minLon: Number.POSITIVE_INFINITY,
      maxLon: Number.NEGATIVE_INFINITY,
      minLat: Number.POSITIVE_INFINITY,
      maxLat: Number.NEGATIVE_INFINITY,
    }
  );
}

function pointInPolygon(lon: number, lat: number, polygon: number[][]): boolean {
  let inside = false;
  for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
    const [xi, yi] = polygon[i];
    const [xj, yj] = polygon[j];
    const intersects =
      yi > lat !== yj > lat &&
      lon < ((xj - xi) * (lat - yi)) / (yj - yi) + xi;
    if (intersects) inside = !inside;
  }
  return inside;
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

async function fetchEventById(
  id: string,
  authHeader: string
): Promise<Record<string, unknown> | null> {
  const localMetadata = await loadLocalEditedMetadata(id);
  if (localMetadata) {
    return localEditedMetadataToEvent(id, localMetadata) as unknown as Record<string, unknown>;
  }

  const response = await fetchWithRetry(`${API_BASE_URL}/${id}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: authHeader,
    },
  });

  if (!response.ok) {
    console.warn(`Skipping VRU/object search result ${id}: ${response.status}`);
    return null;
  }

  return response.json();
}

async function fetchVruLabelFilteredEvents(
  body: EventsSearchBody,
  authHeader: string
): Promise<{ events: Record<string, unknown>[]; total: number }> {
  const labels = expandVruObjectFilterAliases(body.vruLabels ?? []);
  if (labels.length === 0) {
    return { events: [], total: 0 };
  }

  const db = await getDb();
  const clauses = [
    `lower(replace(trim(fd.label), '_', ' ')) IN (${placeholders(labels)})`,
    "t.event_timestamp >= ?",
    "t.event_timestamp <= ?",
  ];
  const params: unknown[] = [...labels, body.startDate, body.endDate];

  if (body.types && body.types.length > 0) {
    clauses.push(`t.event_type IN (${placeholders(body.types)})`);
    params.push(...body.types);
  }

  if (body.bbox) {
    const [west, south, east, north] = body.bbox;
    clauses.push("t.lon >= ? AND t.lon <= ? AND t.lat >= ? AND t.lat <= ?");
    params.push(west, east, south, north);
  }

  if (body.polygon && body.polygon.length > 0) {
    const bounds = polygonBounds(body.polygon);
    clauses.push("t.lon >= ? AND t.lon <= ? AND t.lat >= ? AND t.lat <= ?");
    params.push(bounds.minLon, bounds.maxLon, bounds.minLat, bounds.maxLat);
  }

  const whereClause = clauses.join(" AND ");
  const baseSql = `
    FROM frame_detections fd
    JOIN triage_results t ON t.id = fd.video_id
    WHERE ${whereClause}
  `;
  const limit = body.limit ?? 50;
  const offset = body.offset ?? 0;

  let total: number;
  let pageIds: string[];

  if (body.polygon && body.polygon.length > 0) {
    const candidates = await db.query(
      `
        SELECT fd.video_id AS id, t.lat AS lat, t.lon AS lon, MAX(t.event_timestamp) AS sort_timestamp
        ${baseSql}
        GROUP BY fd.video_id
        ORDER BY sort_timestamp DESC, fd.video_id ASC
      `,
      params
    );
    const matchingRows = candidates.rows.filter((row) => {
      const lat = Number(row.lat);
      const lon = Number(row.lon);
      return Number.isFinite(lat) && Number.isFinite(lon) && pointInPolygon(lon, lat, body.polygon!);
    });
    total = matchingRows.length;
    pageIds = matchingRows
      .slice(offset, offset + limit)
      .map((row) => String(row.id))
      .filter(Boolean);
  } else {
    const countResult = await db.query(
      `SELECT COUNT(DISTINCT fd.video_id) AS total ${baseSql}`,
      params
    );
    total = Number(countResult.rows[0]?.total ?? 0);

    const pageResult = await db.query(
      `
        SELECT fd.video_id AS id, MAX(t.event_timestamp) AS sort_timestamp
        ${baseSql}
        GROUP BY fd.video_id
        ORDER BY sort_timestamp DESC, fd.video_id ASC
        LIMIT ? OFFSET ?
      `,
      [...params, limit, offset]
    );
    pageIds = pageResult.rows.map((row) => String(row.id)).filter(Boolean);
  }

  const fetched = await Promise.all(pageIds.map((id) => fetchEventById(id, authHeader)));
  return {
    events: fetched.filter((event): event is Record<string, unknown> => event !== null),
    total,
  };
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

    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    if (body.vruLabels && body.vruLabels.length > 0) {
      const result = await fetchVruLabelFilteredEvents(body, authHeader);
      return NextResponse.json(
        {
          events: result.events,
          pagination: {
            total: result.total,
            limit: body.limit ?? 50,
            offset: body.offset ?? 0,
          },
        },
        { headers: { "Cache-Control": "no-store" } }
      );
    }

    const chunks = splitEventSearchDateRange(body.startDate, body.endDate);

    // Single chunk — pass through directly (preserves offset/limit for pagination)
    if (chunks.length === 1) {
      const result = await fetchChunk(body, authHeader);
      return NextResponse.json({
        events: result.events,
        pagination: { total: result.total, limit: body.limit ?? 50, offset: body.offset ?? 0 },
      });
    }

    const limit = body.limit ?? 50;
    const offset = body.offset ?? 0;
    const newestFirstChunks = newestFirstEventSearchChunks(chunks);

    // Multiple chunks — page across chronological chunks instead of fetching
    // the first 500 rows from every chunk and slicing an incomplete merge.
    const chunkTotals = await Promise.all(
      newestFirstChunks.map(async (chunk) => {
        const result = await fetchChunk(
          {
            ...body,
            startDate: chunk.startDate,
            endDate: chunk.endDate,
            limit: 1,
            offset: 0,
          },
          authHeader
        );
        return { ...chunk, total: result.total };
      })
    );
    const total = chunkTotals.reduce((sum, chunk) => sum + chunk.total, 0);
    const pageFetches = planEventSearchPageFetches(chunkTotals, offset, limit);

    const results = await Promise.all(
      pageFetches.map((chunk) =>
        fetchChunk(
          {
            ...body,
            startDate: chunk.startDate,
            endDate: chunk.endDate,
            limit: chunk.limit,
            offset: chunk.offset,
          },
          authHeader
        )
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

    allEvents.sort((a, b) => {
      const ta = (a as { timestamp?: string }).timestamp ?? "";
      const tb = (b as { timestamp?: string }).timestamp ?? "";
      return tb.localeCompare(ta);
    });

    return NextResponse.json({
      events: allEvents,
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

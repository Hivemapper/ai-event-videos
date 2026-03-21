import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent, AIEventType } from "@/types/events";
import { HighlightEvent } from "@/lib/highlights";
import { parseSpeedArray, getAcceleration } from "@/lib/highlights-utils";
import { fetchWithRetry } from "@/lib/fetch-retry";

const ALL_TYPES: AIEventType[] = [
  "HARSH_BRAKING",
  "HIGH_G_FORCE",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
  "HIGH_SPEED",
  "STOP_SIGN_VIOLATION",
  "TRAFFIC_LIGHT_VIOLATION",
  "TAILGATING",
];

interface ScoredEvent {
  event: AIEvent;
  maxSpeed: number;
  minSpeed: number;
  acceleration: number;
  score: number;
}

function scoreEvent(maxSpeed: number, minSpeed: number, acceleration: number): number {
  const speedDrop = maxSpeed - minSpeed;
  return speedDrop * 1.5 + acceleration * 25 + maxSpeed * 0.4;
}

/**
 * Reverse-geocode via Mapbox, returning { place, region, country }.
 * region is the state/province — key for US diversity.
 */
async function reverseGeocode(
  lat: number,
  lon: number,
  token: string
): Promise<{ label: string; region: string; country: string }> {
  const fallback = {
    label: `${lat.toFixed(2)}, ${lon.toFixed(2)}`,
    region: "",
    country: "",
  };
  try {
    const response = await fetch(
      `https://api.mapbox.com/geocoding/v5/mapbox.places/${lon},${lat}.json?types=place,region,country&limit=1&access_token=${token}`
    );
    if (!response.ok) return fallback;
    const data = await response.json();
    const feature = data.features?.[0];
    if (!feature) return fallback;

    const place = feature.text || "";
    const regionCtx = feature.context?.find((c: { id: string }) =>
      c.id.startsWith("region.")
    );
    const countryCtx = feature.context?.find((c: { id: string }) =>
      c.id.startsWith("country.")
    );
    const region = regionCtx?.text || "";
    const country = countryCtx?.text || "";

    const label = [place, country].filter(Boolean).join(", ");
    return { label: label || fallback.label, region, country };
  } catch {
    return fallback;
  }
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

    const mapboxToken =
      request.nextUrl.searchParams.get("mapboxToken") ||
      process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 31);

    // Fetch a large pool (Bee Maps requires full ISO datetime)
    const body = {
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString(),
      types: ALL_TYPES,
      limit: 500,
      offset: 0,
    };

    const response = await fetchWithRetry(`${API_BASE_URL}/search`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      return NextResponse.json(
        { error: `API error: ${response.status}` },
        { status: response.status }
      );
    }

    const data = await response.json();
    const events: AIEvent[] = data.events || [];

    // Score all events
    const scored: ScoredEvent[] = events
      .map((event) => {
        const { maxSpeed, minSpeed } = parseSpeedArray(event.metadata);
        const acceleration = getAcceleration(event.metadata);
        const score = scoreEvent(maxSpeed, minSpeed, acceleration);
        return { event, maxSpeed, minSpeed, acceleration, score };
      })
      .filter((s) => s.score > 10);

    // Sort by score descending
    scored.sort((a, b) => b.score - a.score);

    if (!mapboxToken) {
      // Without geocoding we can't determine countries — just return top 20
      const top = scored.slice(0, 20);
      const results: HighlightEvent[] = top.map((s) => ({
        id: s.event.id,
        type: s.event.type,
        location: `${s.event.location.lat.toFixed(2)}, ${s.event.location.lon.toFixed(2)}`,
        coords: { lat: s.event.location.lat, lon: s.event.location.lon },
        date: new Date(s.event.timestamp).toLocaleDateString("en-US", {
          month: "short",
          day: "2-digit",
          year: "numeric",
        }),
        maxSpeed: s.maxSpeed,
        minSpeed: s.minSpeed,
        acceleration: s.acceleration,
      }));
      return NextResponse.json(results);
    }

    // Geocode top ~80 events in parallel (Mapbox has no rate limit like Nominatim)
    const candidates = scored.slice(0, 80);
    const geocoded = await Promise.all(
      candidates.map(async (s) => {
        const geo = await reverseGeocode(
          s.event.location.lat,
          s.event.location.lon,
          mapboxToken
        );
        return { ...s, geo };
      })
    );

    // Geodiversity selection: pick the best event per unique "bucket".
    // For US events, bucket = state (e.g. "California"). For non-US, bucket = country.
    // This maximizes geographic spread.
    const picked: typeof geocoded = [];
    const usedBuckets = new Set<string>();
    const TARGET = 25;

    for (const candidate of geocoded) {
      if (picked.length >= TARGET) break;

      const { country, region } = candidate.geo;
      // Bucket: US states get their own bucket, others by country
      const isUS = country === "United States";
      const bucket = isUS && region ? `US:${region}` : country || "unknown";

      if (usedBuckets.has(bucket)) continue;
      usedBuckets.add(bucket);
      picked.push(candidate);
    }

    // If we haven't hit TARGET yet, do a second pass allowing 2 per bucket
    if (picked.length < TARGET) {
      const usedBuckets2 = new Map<string, number>();
      for (const p of picked) {
        const isUS = p.geo.country === "United States";
        const bucket = isUS && p.geo.region ? `US:${p.geo.region}` : p.geo.country || "unknown";
        usedBuckets2.set(bucket, 1);
      }
      const pickedIds = new Set(picked.map((p) => p.event.id));

      for (const candidate of geocoded) {
        if (picked.length >= TARGET) break;
        if (pickedIds.has(candidate.event.id)) continue;

        const isUS = candidate.geo.country === "United States";
        const bucket = isUS && candidate.geo.region
          ? `US:${candidate.geo.region}`
          : candidate.geo.country || "unknown";
        const count = usedBuckets2.get(bucket) || 0;
        if (count >= 2) continue;

        usedBuckets2.set(bucket, count + 1);
        picked.push(candidate);
      }
    }

    const results: HighlightEvent[] = picked.map((s) => ({
      id: s.event.id,
      type: s.event.type,
      location: s.geo.label,
      coords: { lat: s.event.location.lat, lon: s.event.location.lon },
      date: new Date(s.event.timestamp).toLocaleDateString("en-US", {
        month: "short",
        day: "2-digit",
        year: "numeric",
      }),
      maxSpeed: s.maxSpeed,
      minSpeed: s.minSpeed,
      acceleration: s.acceleration,
    }));

    return NextResponse.json(results, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  } catch (error) {
    console.error("International highlights error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent } from "@/types/events";
import { HighlightEvent } from "@/lib/highlights";
import { parseSpeedArray, getAcceleration } from "@/lib/highlights-utils";
import { fetchWithRetry } from "@/lib/fetch-retry";

// Filter thresholds — the "emergency stop at parking-lot speed" profile
const MAX_SPEED_KMH = 16;        // 10 mph — upper bound on initial speed
const MIN_SPEED_KMH = 2;         // must come nearly to a stop
const MIN_SPEED_DROP_KMH = 5;    // ignore events that were barely moving
const LOOKBACK_DAYS = 90;
const WINDOW_DAYS = 30;          // Bee Maps API caps each query at 31 days
const PAGE_SIZE = 500;           // Bee Maps API caps per-call
const MAX_PAGES_PER_WINDOW = 4;  // up to 2000 events per 30-day window
const RESULT_LIMIT = 20;
const SPATIAL_DEDUP_KM = 0.5;    // tighter than Trending — low-speed events cluster at intersections

interface ScoredEvent {
  event: AIEvent;
  maxSpeed: number;
  minSpeed: number;
  acceleration: number;
  peakDeceleration: number;
}

function spatialDedup(scored: ScoredEvent[], radiusKm = SPATIAL_DEDUP_KM): ScoredEvent[] {
  const kept: ScoredEvent[] = [];
  for (const candidate of scored) {
    const lat1 = candidate.event.location.lat;
    const lon1 = candidate.event.location.lon;
    const tooClose = kept.some((k) => {
      const dlat = lat1 - k.event.location.lat;
      const dlon = (lon1 - k.event.location.lon) * Math.cos((lat1 * Math.PI) / 180);
      const distKm = Math.sqrt(dlat * dlat + dlon * dlon) * 111.32;
      return distKm < radiusKm;
    });
    if (!tooClose) kept.push(candidate);
  }
  return kept;
}

async function reverseGeocodeServer(lat: number, lon: number): Promise<string> {
  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      { headers: { "User-Agent": "AI-Event-Videos-App" } }
    );
    if (!response.ok) return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
    const data = await response.json();
    const address = data.address;
    const name =
      address?.city || address?.town || address?.village ||
      address?.county || address?.state ||
      `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
    const country = address?.country;
    return country ? `${name}, ${country}` : name;
  } catch {
    return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
  }
}

function delay(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export async function GET(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;
    if (!apiKey) {
      return NextResponse.json({ error: "API key is required" }, { status: 401 });
    }
    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    // The Bee Maps API caps each query at a 31-day range, so we sweep the
    // lookback in 30-day windows and paginate within each.
    const events: AIEvent[] = [];
    const now = new Date();
    const windowCount = Math.ceil(LOOKBACK_DAYS / WINDOW_DAYS);

    for (let w = 0; w < windowCount; w++) {
      const windowEnd = new Date(now);
      windowEnd.setDate(windowEnd.getDate() - w * WINDOW_DAYS);
      const windowStart = new Date(now);
      windowStart.setDate(windowStart.getDate() - Math.min(LOOKBACK_DAYS, (w + 1) * WINDOW_DAYS));

      for (let page = 0; page < MAX_PAGES_PER_WINDOW; page++) {
        const body = {
          startDate: windowStart.toISOString(),
          endDate: windowEnd.toISOString(),
          types: ["HARSH_BRAKING"],
          limit: PAGE_SIZE,
          offset: page * PAGE_SIZE,
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
          // Soft-fail on a single window so one bad page doesn't sink everything
          console.error(
            `Low-Speed Stop: window ${w} page ${page} → ${response.status}`
          );
          break;
        }

        const data = await response.json();
        const batch: AIEvent[] = data.events || [];
        events.push(...batch);
        if (batch.length < PAGE_SIZE) break;
      }
    }

    // Filter to the low-speed emergency-stop profile — a HARSH_BRAKING event
    // that went from ≤10 mph to near-zero with a meaningful speed drop. We
    // don't require a computed peak-decel threshold because SPEED_ARRAY is
    // averaged per sample; the event's classification as HARSH_BRAKING plus
    // the ACCELERATION_MS2 metadata carry that signal.
    const scored: ScoredEvent[] = events
      .map((event) => {
        const { maxSpeed, minSpeed, peakDeceleration } = parseSpeedArray(event.metadata);
        const acceleration = getAcceleration(event.metadata);
        return { event, maxSpeed, minSpeed, acceleration, peakDeceleration };
      })
      .filter(
        (s) =>
          s.maxSpeed > 0 &&
          s.maxSpeed <= MAX_SPEED_KMH &&
          s.minSpeed <= MIN_SPEED_KMH &&
          s.maxSpeed - s.minSpeed >= MIN_SPEED_DROP_KMH
      );

    // Rank by the event's own peak g-force (authoritative), tiebreak on speed drop
    scored.sort((a, b) => {
      if (b.acceleration !== a.acceleration) return b.acceleration - a.acceleration;
      return (b.maxSpeed - b.minSpeed) - (a.maxSpeed - a.minSpeed);
    });

    // Spatial dedup so one parking lot doesn't dominate
    const deduped = spatialDedup(scored);

    // Top N
    const top = deduped.slice(0, RESULT_LIMIT);

    // Reverse-geocode with Nominatim rate limiting
    const results: HighlightEvent[] = [];
    for (let i = 0; i < top.length; i++) {
      const { event, maxSpeed, minSpeed, acceleration } = top[i];

      if (i > 0) await delay(1100);
      const location = await reverseGeocodeServer(event.location.lat, event.location.lon);

      const date = new Date(event.timestamp).toLocaleDateString("en-US", {
        month: "short",
        day: "2-digit",
        year: "numeric",
      });

      results.push({
        id: event.id,
        type: event.type,
        location,
        coords: { lat: event.location.lat, lon: event.location.lon },
        date,
        maxSpeed,
        minSpeed,
        acceleration,
      });
    }

    return NextResponse.json(results, {
      headers: { "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400" },
    });
  } catch (error) {
    console.error("Low-Speed Stop API error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

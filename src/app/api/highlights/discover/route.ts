import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent, AIEventType } from "@/types/events";
import { HighlightEvent } from "@/lib/highlights";
import { parseSpeedArray, getAcceleration } from "@/lib/highlights-utils";

const DISCOVER_TYPES: AIEventType[] = [
  "HARSH_BRAKING",
  "HIGH_SPEED",
  "HIGH_G_FORCE",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
];

// US bounding box (continental + Alaska + Hawaii approximate)
function isUSCoords(lat: number, lon: number): boolean {
  // Continental US
  if (lat >= 24.5 && lat <= 49.5 && lon >= -125 && lon <= -66.5) return true;
  // Alaska
  if (lat >= 51 && lat <= 72 && lon >= -180 && lon <= -129) return true;
  // Hawaii
  if (lat >= 18.5 && lat <= 22.5 && lon >= -161 && lon <= -154) return true;
  return false;
}

async function reverseGeocodeMapbox(
  lat: number,
  lon: number,
  token: string
): Promise<string> {
  try {
    const response = await fetch(
      `https://api.mapbox.com/geocoding/v5/mapbox.places/${lon},${lat}.json?types=place,region,country&limit=1&access_token=${token}`
    );
    if (!response.ok) return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
    const data = await response.json();
    const feature = data.features?.[0];
    if (!feature) return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;

    const place = feature.text;
    const country = feature.context?.find((c: { id: string }) =>
      c.id.startsWith("country.")
    )?.text;
    return country ? `${place}, ${country}` : place;
  } catch {
    return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;
  }
}

interface ScoredEvent {
  event: AIEvent;
  maxSpeed: number;
  minSpeed: number;
  acceleration: number;
}

function rankSection(
  events: ScoredEvent[],
  sectionIndex: number
): ScoredEvent[] {
  const sorted = [...events];
  switch (sectionIndex) {
    case 0: // Extreme Braking: sort by speed drop
      sorted.sort(
        (a, b) => b.maxSpeed - b.minSpeed - (a.maxSpeed - a.minSpeed)
      );
      break;
    case 1: // High Speed: sort by maxSpeed
      sorted.sort((a, b) => b.maxSpeed - a.maxSpeed);
      break;
    case 2: // G-Force: sort by acceleration
      sorted.sort((a, b) => b.acceleration - a.acceleration);
      break;
    case 3: // Acceleration: sort by acceleration
      sorted.sort((a, b) => b.acceleration - a.acceleration);
      break;
    case 4: // Swerving: sort by acceleration
      sorted.sort((a, b) => b.acceleration - a.acceleration);
      break;
    case 5: // International: composite score
      sorted.sort((a, b) => {
        const scoreA =
          (a.maxSpeed - a.minSpeed) * 1.0 +
          a.acceleration * 30 +
          a.maxSpeed * 0.3;
        const scoreB =
          (b.maxSpeed - b.minSpeed) * 1.0 +
          b.acceleration * 30 +
          b.maxSpeed * 0.3;
        return scoreB - scoreA;
      });
      break;
  }
  return sorted.slice(0, 5);
}

const SECTION_TYPES: Record<number, AIEventType[]> = {
  0: ["HARSH_BRAKING"],
  1: ["HIGH_SPEED"],
  2: ["HIGH_G_FORCE"],
  3: ["AGGRESSIVE_ACCELERATION"],
  4: ["SWERVING"],
  5: DISCOVER_TYPES, // International: all types
};

export async function POST(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization");
    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    const mapboxToken = request.headers.get("X-Mapbox-Token");
    const { excludeIds = [] }: { excludeIds?: string[] } =
      await request.json();
    const excludeSet = new Set(excludeIds);

    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 31);

    // Query all 5 event types in parallel
    const typeResults = await Promise.all(
      DISCOVER_TYPES.map(async (type) => {
        const body = {
          startDate: startDate.toISOString().split("T")[0],
          endDate: endDate.toISOString().split("T")[0],
          types: [type],
          limit: 100,
          offset: 0,
        };

        const response = await fetch(`${API_BASE_URL}/search`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: authHeader,
          },
          body: JSON.stringify(body),
        });

        if (!response.ok) return { type, events: [] as AIEvent[] };
        const data = await response.json();
        return { type, events: (data.events || []) as AIEvent[] };
      })
    );

    // Build a map of type -> scored events (excluding known IDs)
    const byType: Record<string, ScoredEvent[]> = {};
    for (const { type, events } of typeResults) {
      byType[type] = events
        .filter((e) => !excludeSet.has(e.id))
        .map((event) => {
          const { maxSpeed, minSpeed } = parseSpeedArray(event.metadata);
          const acceleration = getAcceleration(event.metadata);
          return { event, maxSpeed, minSpeed, acceleration };
        });
    }

    // Rank per section
    const sections: Record<number, ScoredEvent[]> = {};
    for (let i = 0; i <= 5; i++) {
      const types = SECTION_TYPES[i];
      let pool: ScoredEvent[] = [];
      for (const t of types) {
        pool = pool.concat(byType[t] || []);
      }
      // Section 5 (International): filter to non-US coords only
      if (i === 5) {
        pool = pool.filter(
          (s) => !isUSCoords(s.event.location.lat, s.event.location.lon)
        );
      }
      sections[i] = rankSection(pool, i);
    }

    // Collect all unique events that need geocoding
    const allEvents = new Map<string, ScoredEvent>();
    for (const sectionEvents of Object.values(sections)) {
      for (const se of sectionEvents) {
        allEvents.set(se.event.id, se);
      }
    }

    // Geocode in parallel via Mapbox (or fallback to coords)
    const locationMap = new Map<string, string>();
    if (mapboxToken) {
      const entries = Array.from(allEvents.entries());
      const geocodeResults = await Promise.all(
        entries.map(async ([id, se]) => {
          const loc = await reverseGeocodeMapbox(
            se.event.location.lat,
            se.event.location.lon,
            mapboxToken
          );
          return [id, loc] as const;
        })
      );
      for (const [id, loc] of geocodeResults) {
        locationMap.set(id, loc);
      }
    } else {
      for (const [id, se] of allEvents) {
        locationMap.set(
          id,
          `${se.event.location.lat.toFixed(2)}, ${se.event.location.lon.toFixed(2)}`
        );
      }
    }

    // Build response keyed by section index
    const result: Record<number, HighlightEvent[]> = {};
    for (let i = 0; i <= 5; i++) {
      result[i] = sections[i].map((se) => {
        const date = new Date(se.event.timestamp).toLocaleDateString("en-US", {
          month: "short",
          day: "2-digit",
          year: "numeric",
        });
        return {
          id: se.event.id,
          type: se.event.type,
          location: locationMap.get(se.event.id) || "",
          coords: {
            lat: se.event.location.lat,
            lon: se.event.location.lon,
          },
          date,
          maxSpeed: se.maxSpeed,
          minSpeed: se.minSpeed,
          acceleration: se.acceleration,
        };
      });
    }

    return NextResponse.json(result);
  } catch (error) {
    console.error("Discover API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

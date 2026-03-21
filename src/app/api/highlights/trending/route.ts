import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent, AIEventType } from "@/types/events";
import { HighlightEvent } from "@/lib/highlights";
import { parseSpeedArray, getAcceleration } from "@/lib/highlights-utils";
import { fetchWithRetry } from "@/lib/fetch-retry";

/** All event types — violations and tailgating are inherently "crazy" */
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
  reason: string;
}

/**
 * Multi-dimensional "craziness" score.
 *
 * Philosophy: truly wild events have at least ONE dimension that's an extreme
 * outlier. We score each dimension independently and take the max, with bonuses
 * for combinations. This prevents a "pretty fast, pretty hard braking" event
 * from beating a "stopped from 140 km/h in 2 seconds" event.
 */
function scoreEvent(
  type: AIEventType,
  maxSpeed: number,      // km/h
  minSpeed: number,      // km/h
  acceleration: number,  // m/s²
  peakDeceleration: number, // m/s² (positive = harder braking)
): { score: number; reason: string } {
  const speedDrop = maxSpeed - minSpeed;
  const scores: { value: number; reason: string }[] = [];

  // 1. Extreme deceleration rate (emergency stops, near-crashes)
  //    8+ m/s² is ABS-limit territory, 10+ is a crash or near-miss
  if (peakDeceleration > 0) {
    scores.push({
      value: Math.pow(peakDeceleration, 1.8) * 5,
      reason: `${peakDeceleration.toFixed(1)} m/s² peak decel`,
    });
  }

  // 2. Huge speed drop (going from highway speed to near-stop)
  if (speedDrop > 20) {
    // Bonus multiplier for high initial speed — dropping from 140 is scarier than from 60
    const initialSpeedMultiplier = 1 + Math.max(0, maxSpeed - 80) / 100;
    scores.push({
      value: speedDrop * 1.5 * initialSpeedMultiplier,
      reason: `${speedDrop.toFixed(0)} km/h drop from ${maxSpeed.toFixed(0)}`,
    });
  }

  // 3. Very high speed (100+ km/h events are rare; 150+ are wild)
  if (maxSpeed > 80) {
    scores.push({
      value: Math.pow(Math.max(0, maxSpeed - 60), 1.4) * 0.4,
      reason: `${maxSpeed.toFixed(0)} km/h top speed`,
    });
  }

  // 4. Extreme acceleration (launch-like events)
  if (acceleration > 1.0) {
    scores.push({
      value: Math.pow(acceleration, 2) * 20,
      reason: `${acceleration.toFixed(2)} m/s² accel`,
    });
  }

  // 5. Type-specific bonuses — violations are inherently notable
  if (type === "STOP_SIGN_VIOLATION" && maxSpeed > 30) {
    scores.push({
      value: 80 + maxSpeed * 0.5,
      reason: `stop sign violation at ${maxSpeed.toFixed(0)} km/h`,
    });
  }
  if (type === "TRAFFIC_LIGHT_VIOLATION" && maxSpeed > 30) {
    scores.push({
      value: 80 + maxSpeed * 0.5,
      reason: `red light at ${maxSpeed.toFixed(0)} km/h`,
    });
  }
  if (type === "TAILGATING" && maxSpeed > 60) {
    scores.push({
      value: 60 + maxSpeed * 0.4,
      reason: `tailgating at ${maxSpeed.toFixed(0)} km/h`,
    });
  }
  if (type === "SWERVING" && maxSpeed > 60) {
    scores.push({
      value: 50 + maxSpeed * 0.6,
      reason: `swerving at ${maxSpeed.toFixed(0)} km/h`,
    });
  }

  // Take the best single dimension as the base, add 20% of the second-best
  // This rewards events that are extreme in ONE way, with a bonus for being extreme in multiple
  scores.sort((a, b) => b.value - a.value);

  if (scores.length === 0) {
    return { score: 0, reason: "no notable dimensions" };
  }

  let finalScore = scores[0].value;
  if (scores.length >= 2) finalScore += scores[1].value * 0.2;

  return { score: finalScore, reason: scores[0].reason };
}

/**
 * Spatial dedup: only keep the highest-scored event within ~2km.
 * Prevents the leaderboard from being dominated by one stretch of highway.
 */
function spatialDedup(scored: ScoredEvent[], radiusKm = 2): ScoredEvent[] {
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

    // Query last 31 days
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 31);

    // Fetch a larger pool to find true outliers
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
      const errorText = await response.text();
      return NextResponse.json(
        { error: `API error: ${response.status}` },
        { status: response.status }
      );
    }

    const data = await response.json();
    const events: AIEvent[] = data.events || [];

    // Score every event
    const scored: ScoredEvent[] = events
      .map((event) => {
        const { maxSpeed, minSpeed, peakDeceleration } = parseSpeedArray(event.metadata);
        const acceleration = getAcceleration(event.metadata);
        const { score, reason } = scoreEvent(
          event.type, maxSpeed, minSpeed, acceleration, peakDeceleration
        );
        return { event, maxSpeed, minSpeed, acceleration, score, reason };
      })
      .filter((s) => s.score > 0);

    // Sort by score descending
    scored.sort((a, b) => b.score - a.score);

    // Spatial dedup so one road doesn't dominate
    const deduped = spatialDedup(scored);

    // Take top 15
    const top = deduped.slice(0, 15);

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
    console.error("Trending API error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

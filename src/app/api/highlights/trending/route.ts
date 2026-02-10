import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent, AIEventType } from "@/types/events";
import { HighlightEvent } from "@/lib/highlights";

const TRENDING_TYPES: AIEventType[] = [
  "HARSH_BRAKING",
  "HIGH_G_FORCE",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
  "HIGH_SPEED",
];

function parseSpeedArray(metadata: Record<string, unknown> | undefined): {
  maxSpeed: number;
  minSpeed: number;
} {
  if (!metadata?.SPEED_ARRAY) return { maxSpeed: 0, minSpeed: 0 };

  try {
    const speeds = metadata.SPEED_ARRAY as number[];
    if (!Array.isArray(speeds) || speeds.length === 0)
      return { maxSpeed: 0, minSpeed: 0 };

    // Speeds are in m/s, convert to km/h
    const kmhSpeeds = speeds.map((s) => s * 3.6);
    return {
      maxSpeed: Math.max(...kmhSpeeds),
      minSpeed: Math.min(...kmhSpeeds),
    };
  } catch {
    return { maxSpeed: 0, minSpeed: 0 };
  }
}

function getAcceleration(metadata: Record<string, unknown> | undefined): number {
  if (!metadata?.ACCELERATION_MS2) return 0;
  const val = Number(metadata.ACCELERATION_MS2);
  return isNaN(val) ? 0 : val;
}

function scoreEvent(maxSpeed: number, minSpeed: number, acceleration: number): number {
  const speedDrop = maxSpeed - minSpeed;
  return speedDrop * 1.0 + acceleration * 30 + maxSpeed * 0.3;
}

async function reverseGeocodeServer(
  lat: number,
  lon: number
): Promise<string> {
  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      {
        headers: { "User-Agent": "AI-Event-Videos-App" },
      }
    );

    if (!response.ok) return `${lat.toFixed(2)}, ${lon.toFixed(2)}`;

    const data = await response.json();
    const address = data.address;
    const name =
      address?.city ||
      address?.town ||
      address?.village ||
      address?.county ||
      address?.state ||
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
    const apiKey = request.headers.get("Authorization");

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    // Query last 31 days (max allowed window)
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 31);

    const body = {
      startDate: startDate.toISOString().split("T")[0],
      endDate: endDate.toISOString().split("T")[0],
      types: TRENDING_TYPES,
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

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Beemaps API error:", response.status, errorText);
      return NextResponse.json(
        { error: `API error: ${response.status}` },
        { status: response.status }
      );
    }

    const data = await response.json();
    const events: AIEvent[] = data.events || [];

    // Score and rank events
    const scored = events.map((event) => {
      const { maxSpeed, minSpeed } = parseSpeedArray(event.metadata);
      const acceleration = getAcceleration(event.metadata);
      const score = scoreEvent(maxSpeed, minSpeed, acceleration);
      return { event, maxSpeed, minSpeed, acceleration, score };
    });

    scored.sort((a, b) => b.score - a.score);
    const top = scored.slice(0, 10);

    // Reverse-geocode locations with rate limiting (1 req/sec for Nominatim)
    const results: HighlightEvent[] = [];
    for (let i = 0; i < top.length; i++) {
      const { event, maxSpeed, minSpeed, acceleration } = top[i];

      if (i > 0) await delay(1100);
      const location = await reverseGeocodeServer(
        event.location.lat,
        event.location.lon
      );

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

    return NextResponse.json(results);
  } catch (error) {
    console.error("Trending API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

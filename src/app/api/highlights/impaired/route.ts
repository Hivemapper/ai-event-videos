import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent, AIEventType } from "@/types/events";

const IMPAIRMENT_TYPES: AIEventType[] = [
  "SWERVING",
  "HARSH_BRAKING",
  "STOP_SIGN_VIOLATION",
  "TRAFFIC_LIGHT_VIOLATION",
  "TAILGATING",
];

// Nighttime hours (local estimate): 22:00–04:00
function isNighttime(timestamp: string, lonDeg: number): boolean {
  const date = new Date(timestamp);
  // Estimate local hour from UTC + longitude offset (15 deg per hour)
  const utcHour = date.getUTCHours();
  const offsetHours = Math.round(lonDeg / 15);
  const localHour = ((utcHour + offsetHours) % 24 + 24) % 24;
  return localHour >= 22 || localHour < 4;
}

function haversineMeters(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const R = 6371000;
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export interface ImpairedCluster {
  id: string;
  events: {
    id: string;
    type: AIEventType;
    timestamp: string;
    lat: number;
    lon: number;
  }[];
  eventTypes: AIEventType[];
  center: { lat: number; lon: number };
  timeRange: { start: string; end: string };
  score: number;
  location: string;
}

function clusterEvents(
  events: AIEvent[]
): ImpairedCluster[] {
  // Sort by timestamp
  const sorted = [...events].sort(
    (a, b) => new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime()
  );

  const clusters: ImpairedCluster[] = [];
  const assigned = new Set<string>();

  for (let i = 0; i < sorted.length; i++) {
    if (assigned.has(sorted[i].id)) continue;

    const cluster: AIEvent[] = [sorted[i]];
    assigned.add(sorted[i].id);

    // Greedily expand: add events within 500m and 10min of any cluster member
    for (let j = i + 1; j < sorted.length; j++) {
      if (assigned.has(sorted[j].id)) continue;

      const candidate = sorted[j];
      const withinTimeAndSpace = cluster.some((member) => {
        const timeDiff = Math.abs(
          new Date(candidate.timestamp).getTime() -
            new Date(member.timestamp).getTime()
        );
        const dist = haversineMeters(
          member.location.lat,
          member.location.lon,
          candidate.location.lat,
          candidate.location.lon
        );
        return timeDiff <= 10 * 60 * 1000 && dist <= 500;
      });

      if (withinTimeAndSpace) {
        cluster.push(candidate);
        assigned.add(candidate.id);
      }
    }

    // Only keep clusters with 2+ events and 2+ distinct types
    const distinctTypes = [...new Set(cluster.map((e) => e.type))];
    if (cluster.length >= 2 && distinctTypes.length >= 2) {
      const lats = cluster.map((e) => e.location.lat);
      const lons = cluster.map((e) => e.location.lon);
      const centerLat = lats.reduce((a, b) => a + b, 0) / lats.length;
      const centerLon = lons.reduce((a, b) => a + b, 0) / lons.length;

      const timestamps = cluster.map((e) => new Date(e.timestamp).getTime());

      clusters.push({
        id: `cluster-${clusters.length}`,
        events: cluster.map((e) => ({
          id: e.id,
          type: e.type,
          timestamp: e.timestamp,
          lat: e.location.lat,
          lon: e.location.lon,
        })),
        eventTypes: distinctTypes,
        center: { lat: centerLat, lon: centerLon },
        timeRange: {
          start: new Date(Math.min(...timestamps)).toISOString(),
          end: new Date(Math.max(...timestamps)).toISOString(),
        },
        score: distinctTypes.length,
        location: "", // filled in by geocoding
      });
    }
  }

  // Sort by score descending, then by event count
  clusters.sort((a, b) => b.score - a.score || b.events.length - a.events.length);
  return clusters;
}

async function reverseGeocodeServer(
  lat: number,
  lon: number
): Promise<string> {
  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      { headers: { "User-Agent": "AI-Event-Videos-App" } }
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
    const apiKey =
      request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;
    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    // Query last 31 days
    const endDate = new Date();
    const startDate = new Date();
    startDate.setDate(startDate.getDate() - 31);

    const body = {
      startDate: startDate.toISOString(),
      endDate: endDate.toISOString(),
      types: IMPAIRMENT_TYPES,
      limit: 500,
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

    // Filter to nighttime events
    const nightEvents = events.filter((e) =>
      isNighttime(e.timestamp, e.location.lon)
    );

    // Cluster
    const clusters = clusterEvents(nightEvents);
    const topClusters = clusters.slice(0, 10);

    // Reverse-geocode cluster centers
    for (let i = 0; i < topClusters.length; i++) {
      if (i > 0) await delay(1100);
      topClusters[i].location = await reverseGeocodeServer(
        topClusters[i].center.lat,
        topClusters[i].center.lon
      );
    }

    return NextResponse.json(topClusters);
  } catch (error) {
    console.error("Impaired driving API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

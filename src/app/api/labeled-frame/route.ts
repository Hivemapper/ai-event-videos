import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { haversineDistance, createCirclePolygon } from "@/lib/geo-utils";
import { fetchWithRetry } from "@/lib/fetch-retry";

const MAP_API_BASE_URL = "https://beemaps.com/api/developer";

interface MapFeature {
  class: string;
  position: { lon: number; lat: number };
  properties?: Record<string, unknown>;
}

interface LabeledFeature {
  class: string;
  distance: number;
  position: { lat: number; lon: number };
  speedLimit?: number;
  unit?: string;
}

export async function GET(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization");
    const eventId = request.nextUrl.searchParams.get("eventId");
    const timestampParam = request.nextUrl.searchParams.get("timestamp") || "0";
    const widthParam = request.nextUrl.searchParams.get("width") || "1280";
    const radiusParam = request.nextUrl.searchParams.get("radius") || "100";

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    if (!eventId) {
      return NextResponse.json(
        { error: "eventId is required" },
        { status: 400 }
      );
    }

    const timestamp = parseFloat(timestampParam);
    const width = parseInt(widthParam, 10);
    const radius = parseInt(radiusParam, 10);

    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    // Fetch event details
    const eventResponse = await fetchWithRetry(`${API_BASE_URL}/${eventId}`, {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
    });

    if (!eventResponse.ok) {
      const errorText = await eventResponse.text();
      return NextResponse.json(
        { error: `Failed to fetch event: ${errorText}` },
        { status: eventResponse.status }
      );
    }

    const event = await eventResponse.json();

    // Query map features API for nearby signs
    const polygonCoords = createCirclePolygon(
      event.location.lat,
      event.location.lon,
      radius
    );

    const mapFeaturesResponse = await fetchWithRetry(`${MAP_API_BASE_URL}/map-data`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
      body: JSON.stringify({
        type: ["mapFeatures"],
        geometry: {
          type: "Polygon",
          coordinates: [polygonCoords],
        },
      }),
    });

    let features: LabeledFeature[] = [];

    if (mapFeaturesResponse.ok) {
      const mapData = await mapFeaturesResponse.json();
      const rawFeatures = (mapData.mapFeatureResults?.data ||
        []) as MapFeature[];

      // Transform features and calculate distances
      features = rawFeatures
        .filter((f) => f.class && f.position)
        .map((f) => {
          const distance = haversineDistance(
            event.location.lat,
            event.location.lon,
            f.position.lat,
            f.position.lon
          );

          const labeled: LabeledFeature = {
            class: f.class,
            distance: Math.round(distance),
            position: {
              lat: f.position.lat,
              lon: f.position.lon,
            },
          };

          // Include speed limit info for speed signs
          if (f.properties?.speedLimit) {
            labeled.speedLimit = f.properties.speedLimit as number;
            labeled.unit = (f.properties.unit as string) || "mph";
          }

          return labeled;
        })
        .sort((a, b) => a.distance - b.distance);
    }

    // Build frame URL
    const frameUrl = `/api/frames?url=${encodeURIComponent(
      event.videoUrl
    )}&timestamp=${timestamp}&width=${width}`;

    // Build response
    const response = {
      frame: {
        url: frameUrl,
        timestamp,
        width,
      },
      location: {
        lat: event.location.lat,
        lon: event.location.lon,
      },
      features,
      event: {
        id: event.id,
        type: event.type,
        timestamp: event.timestamp,
        videoUrl: event.videoUrl,
      },
    };

    return NextResponse.json(response);
  } catch (error) {
    console.error("Labeled frame API error:", error);
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

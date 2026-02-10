import { NextRequest, NextResponse } from "next/server";
import { createCirclePolygon, haversineDistance } from "@/lib/geo-utils";
import { fetchWithRetry } from "@/lib/fetch-retry";

const API_BASE_URL = "https://beemaps.com/api/developer";

interface RawMapFeature {
  class: string;
  position: { lon: number; lat: number };
  properties?: Record<string, unknown>;
}

export async function GET(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization");
    const lat = request.nextUrl.searchParams.get("lat");
    const lon = request.nextUrl.searchParams.get("lon");
    const radius = request.nextUrl.searchParams.get("radius") || "200";

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    if (!lat || !lon) {
      return NextResponse.json(
        { error: "lat and lon are required" },
        { status: 400 }
      );
    }

    const latitude = parseFloat(lat);
    const longitude = parseFloat(lon);
    const radiusMeters = parseFloat(radius);

    // Create a polygon approximating a circle
    const polygonCoords = createCirclePolygon(latitude, longitude, radiusMeters);

    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    const response = await fetchWithRetry(`${API_BASE_URL}/map-data`, {
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

    if (!response.ok) {
      const errorText = await response.text();
      console.error("Map features API error:", response.status, errorText);
      return NextResponse.json(
        { error: `API error: ${response.status}`, details: errorText },
        { status: response.status }
      );
    }

    const data = await response.json();

    // Transform response to expected format
    // The /map-data endpoint returns { mapFeatureResults: { data: [...] } }
    const rawFeatures = (data.mapFeatureResults?.data || []) as RawMapFeature[];

    // Transform raw features into labeled features with speed limit info extracted
    const features = rawFeatures
      .filter((f) => f.class && f.position)
      .map((f) => {
        const distance = haversineDistance(latitude, longitude, f.position.lat, f.position.lon);
        const labeled: Record<string, unknown> = {
          class: f.class,
          distance: Math.round(distance),
          position: { lat: f.position.lat, lon: f.position.lon },
        };
        if (f.properties?.speedLimit) {
          labeled.speedLimit = f.properties.speedLimit;
          labeled.unit = (f.properties.unit as string) || "mph";
        }
        return labeled;
      })
      .sort((a, b) => (a.distance as number) - (b.distance as number));

    return NextResponse.json({ features, raw: data });
  } catch (error) {
    console.error("Map features proxy error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

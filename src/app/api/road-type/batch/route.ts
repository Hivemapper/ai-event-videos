import { NextRequest, NextResponse } from "next/server";

const ROAD_CLASS_LABELS: Record<string, string> = {
  motorway: "Highway",
  motorway_link: "Highway Ramp",
  trunk: "Major Road",
  trunk_link: "Major Road Ramp",
  primary: "Primary Road",
  primary_link: "Primary Road Ramp",
  secondary: "Secondary Road",
  secondary_link: "Secondary Road Ramp",
  tertiary: "Local Road",
  tertiary_link: "Local Road Ramp",
  street: "Residential",
  street_limited: "Residential",
  service: "Service Road",
  path: "Path/Trail",
  pedestrian: "Pedestrian",
  track: "Track",
};

const ROAD_CLASS_RANK: Record<string, number> = {
  motorway: 10,
  motorway_link: 9,
  trunk: 8,
  trunk_link: 7,
  primary: 6,
  primary_link: 5,
  secondary: 4,
  secondary_link: 3,
  tertiary: 2,
  tertiary_link: 1,
  street: 0,
  street_limited: 0,
  service: -1,
  path: -2,
  pedestrian: -3,
  track: -4,
};

interface BatchPoint {
  key: string;
  lat: number;
  lon: number;
}

interface BatchResult {
  class: string | null;
  label: string | null;
}

async function queryRoadAt(
  lon: number,
  lat: number,
  token: string
): Promise<{ class: string | null; structure: string | null }> {
  const url = `https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${lon},${lat}.json?layers=road&radius=10&limit=3&access_token=${token}`;
  const response = await fetch(url);
  if (!response.ok) return { class: null, structure: null };

  const data = await response.json();
  const features = data.features || [];

  let bestClass: string | null = null;
  let bestRank = -Infinity;
  let bestStructure: string | null = null;

  for (const feature of features) {
    const props = feature.properties || {};
    const cls = props.class;
    if (!cls) continue;
    const rank = ROAD_CLASS_RANK[cls] ?? -5;
    if (rank > bestRank) {
      bestRank = rank;
      bestClass = cls;
      bestStructure = props.structure || null;
    }
  }

  return { class: bestClass, structure: bestStructure };
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const points: BatchPoint[] = body.points;
    const token =
      body.token || process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

    if (!token) {
      return NextResponse.json(
        { error: "Mapbox token not configured" },
        { status: 500 }
      );
    }

    if (!Array.isArray(points) || points.length === 0) {
      return NextResponse.json(
        { error: "points array is required" },
        { status: 400 }
      );
    }

    if (points.length > 50) {
      return NextResponse.json(
        { error: "Maximum 50 points per batch" },
        { status: 400 }
      );
    }

    // Query all points in parallel with concurrency limit
    const CONCURRENCY = 10;
    const results: Record<string, BatchResult> = {};
    const queue = [...points];

    const worker = async () => {
      while (queue.length > 0) {
        const point = queue.shift();
        if (!point) break;

        const result = await queryRoadAt(point.lon, point.lat, token);
        results[point.key] = {
          class: result.class,
          label: result.class
            ? ROAD_CLASS_LABELS[result.class] || result.class
            : null,
        };
      }
    };

    await Promise.all(
      Array.from({ length: Math.min(CONCURRENCY, points.length) }, () =>
        worker()
      )
    );

    return NextResponse.json(
      { results },
      {
        headers: {
          "Cache-Control": "public, max-age=86400",
        },
      }
    );
  } catch (error) {
    console.error("Batch road type API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

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

/** Higher number = higher road class priority */
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

export interface RoadTypeResponse {
  class: string | null;
  classLabel: string | null;
  name: string | null;
  structure: string | null;
  toll: boolean;
}

/** Road classes that vehicles don't drive on — ignore when a drivable class exists */
const NON_DRIVABLE = new Set(["path", "pedestrian", "track"]);

async function queryRoadAt(
  lon: number,
  lat: number,
  token: string
): Promise<{ class: string | null; name: string | null; structure: string | null; toll: boolean }> {
  // Fetch multiple nearby features so we can skip non-drivable ones (paths/sidewalks)
  const url = `https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${lon},${lat}.json?layers=road&radius=15&limit=5&access_token=${token}`;
  const response = await fetch(url);
  if (!response.ok) return { class: null, name: null, structure: null, toll: false };

  const data = await response.json();
  const features = data.features || [];
  if (features.length === 0) return { class: null, name: null, structure: null, toll: false };

  // Prefer the first (closest) drivable road; fall back to first feature
  const drivable = features.find(
    (f: Record<string, unknown>) => {
      const cls = ((f.properties || {}) as Record<string, unknown>).class as string | undefined;
      return cls && !NON_DRIVABLE.has(cls);
    }
  );
  const feature = drivable || features[0];

  const props = (feature.properties || {}) as Record<string, unknown>;
  return {
    class: (props.class as string) || null,
    name: (props.name as string) || (props.ref as string) || null,
    structure: (props.structure as string) || null,
    toll: props.toll === true,
  };
}

export async function GET(request: NextRequest) {
  try {
    const mapboxToken =
      request.nextUrl.searchParams.get("token") ||
      process.env.NEXT_PUBLIC_MAPBOX_TOKEN;
    if (!mapboxToken) {
      return NextResponse.json(
        { error: "Mapbox token not configured" },
        { status: 500 }
      );
    }

    // Support multi-point sampling via `points` param: [[lon,lat], ...]
    const pointsParam = request.nextUrl.searchParams.get("points");
    const lat = request.nextUrl.searchParams.get("lat");
    const lon = request.nextUrl.searchParams.get("lon");

    let coords: [number, number][];

    if (pointsParam) {
      try {
        coords = JSON.parse(pointsParam);
        if (!Array.isArray(coords) || coords.length === 0) throw new Error();
      } catch {
        return NextResponse.json(
          { error: "Invalid points parameter" },
          { status: 400 }
        );
      }
    } else if (lat && lon) {
      const latitude = parseFloat(lat);
      const longitude = parseFloat(lon);
      if (isNaN(latitude) || isNaN(longitude)) {
        return NextResponse.json(
          { error: "Invalid lat/lon values" },
          { status: 400 }
        );
      }
      coords = [[longitude, latitude]];
    } else {
      return NextResponse.json(
        { error: "lat/lon or points are required" },
        { status: 400 }
      );
    }

    // Query all points in parallel
    const results = await Promise.all(
      coords.map(([lo, la]) => queryRoadAt(lo, la, mapboxToken))
    );

    // Pick the result with the highest road class rank
    let bestResult = results[0];
    let bestRank = -Infinity;

    for (const result of results) {
      if (!result.class) continue;
      const rank = ROAD_CLASS_RANK[result.class] ?? -5;
      if (rank > bestRank) {
        bestRank = rank;
        bestResult = result;
      }
    }

    const roadClass = bestResult.class;
    const classLabel = roadClass
      ? ROAD_CLASS_LABELS[roadClass] || roadClass
      : null;

    // Collect the most common road name across samples
    const roadName = results
      .map((r) => r.name)
      .filter((n): n is string => n !== null)[0] ?? null;

    return NextResponse.json({
      class: roadClass,
      classLabel,
      name: roadName,
      structure: bestResult.structure,
      toll: bestResult.toll,
    } as RoadTypeResponse, {
      headers: {
        "Cache-Control": "public, max-age=86400",
      },
    });
  } catch (error) {
    console.error("Road type API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

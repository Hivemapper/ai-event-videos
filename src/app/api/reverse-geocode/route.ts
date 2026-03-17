import { NextRequest, NextResponse } from "next/server";

const SUCCESS_TTL_MS = 30 * 24 * 60 * 60 * 1000;
const FAILURE_TTL_MS = 60 * 60 * 1000;

interface GeocodeResult {
  name: string;
  country?: string;
}

const geocodeCache = new Map<string, { value: GeocodeResult; expiresAt: number }>();

function getCacheKey(lat: number, lon: number): string {
  return `${lat.toFixed(2)},${lon.toFixed(2)}`;
}

function fallbackResult(lat: number, lon: number): GeocodeResult {
  return { name: `${lat.toFixed(2)}, ${lon.toFixed(2)}` };
}

function getCachedResult(cacheKey: string): GeocodeResult | null {
  const cached = geocodeCache.get(cacheKey);
  if (!cached) return null;
  if (cached.expiresAt <= Date.now()) {
    geocodeCache.delete(cacheKey);
    return null;
  }
  return cached.value;
}

function setCachedResult(cacheKey: string, value: GeocodeResult, ttlMs: number): void {
  geocodeCache.set(cacheKey, {
    value,
    expiresAt: Date.now() + ttlMs,
  });
}

async function reverseGeocodeWithMapbox(
  lat: number,
  lon: number,
  token: string
): Promise<GeocodeResult | null> {
  try {
    const response = await fetch(
      `https://api.mapbox.com/geocoding/v5/mapbox.places/${lon},${lat}.json?types=place,locality,region,country&limit=1&access_token=${token}`
    );

    if (!response.ok) return null;

    const data = await response.json();
    const feature = data.features?.[0];

    if (!feature) return null;

    const context = Array.isArray(feature.context) ? feature.context : [];
    const country =
      context.find((item: { id?: string; text?: string }) => item.id?.startsWith("country."))
        ?.text ??
      (Array.isArray(feature.place_type) && feature.place_type.includes("country")
        ? feature.text
        : undefined);

    const name =
      context.find((item: { id?: string; text?: string }) => item.id?.startsWith("place."))
        ?.text ??
      context.find((item: { id?: string; text?: string }) => item.id?.startsWith("locality."))
        ?.text ??
      context.find((item: { id?: string; text?: string }) => item.id?.startsWith("region."))
        ?.text ??
      feature.text;

    if (!name) return null;

    return { name, country };
  } catch {
    return null;
  }
}

async function reverseGeocodeWithNominatim(
  lat: number,
  lon: number
): Promise<GeocodeResult | null> {
  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      {
        headers: {
          "User-Agent": "AI-Event-Videos-App",
        },
      }
    );

    if (!response.ok) return null;

    const data = await response.json();
    const address = data.address;
    const name =
      address?.city ||
      address?.town ||
      address?.village ||
      address?.county ||
      address?.state;

    if (!name) return null;

    return {
      name,
      country: address?.country,
    };
  } catch {
    return null;
  }
}

export async function GET(request: NextRequest) {
  const latParam = request.nextUrl.searchParams.get("lat");
  const lonParam = request.nextUrl.searchParams.get("lon");

  const lat = Number(latParam);
  const lon = Number(lonParam);

  if (!Number.isFinite(lat) || !Number.isFinite(lon)) {
    return NextResponse.json(
      { error: "lat and lon query parameters are required" },
      { status: 400 }
    );
  }

  const cacheKey = getCacheKey(lat, lon);
  const cached = getCachedResult(cacheKey);

  if (cached) {
    return NextResponse.json(cached, {
      headers: {
        "Cache-Control": "public, max-age=3600",
      },
    });
  }

  const mapboxToken =
    request.headers.get("X-Mapbox-Token") ||
    process.env.MAPBOX_TOKEN ||
    process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

  const resolved =
    (mapboxToken
      ? await reverseGeocodeWithMapbox(lat, lon, mapboxToken)
      : null) ?? (await reverseGeocodeWithNominatim(lat, lon));

  if (resolved) {
    setCachedResult(cacheKey, resolved, SUCCESS_TTL_MS);
    return NextResponse.json(resolved, {
      headers: {
        "Cache-Control": "public, max-age=2592000",
      },
    });
  }

  const fallback = fallbackResult(lat, lon);
  setCachedResult(cacheKey, fallback, FAILURE_TTL_MS);
  return NextResponse.json(fallback, {
    headers: {
      "Cache-Control": "public, max-age=3600",
    },
  });
}

import { AIEvent, Region } from "@/types/events";

const GRID_SIZE = 0.1; // ~10km grid cells
const GEOCODE_CACHE_KEY = "geocode-cache";

interface GeocodeResult {
  name: string;
  country?: string;
}

interface GeocodeCache {
  [key: string]: {
    name: string;
    country?: string;
    timestamp: number;
  };
}

function getGeocodeCache(): GeocodeCache {
  if (typeof window === "undefined") return {};
  try {
    const cached = localStorage.getItem(GEOCODE_CACHE_KEY);
    return cached ? JSON.parse(cached) : {};
  } catch {
    return {};
  }
}

function setGeocodeCache(cache: GeocodeCache): void {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(GEOCODE_CACHE_KEY, JSON.stringify(cache));
  } catch {
    // localStorage might be full or unavailable
  }
}

function getCacheKey(lat: number, lon: number): string {
  return `${lat.toFixed(2)},${lon.toFixed(2)}`;
}

export async function reverseGeocode(
  latitude: number,
  longitude: number
): Promise<GeocodeResult> {
  const cacheKey = getCacheKey(latitude, longitude);
  const cache = getGeocodeCache();

  // Check cache (valid for 7 days)
  if (cache[cacheKey] && Date.now() - cache[cacheKey].timestamp < 7 * 24 * 60 * 60 * 1000) {
    return { name: cache[cacheKey].name, country: cache[cacheKey].country };
  }

  try {
    const response = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${latitude}&lon=${longitude}&zoom=10`,
      {
        headers: {
          "User-Agent": "AI-Event-Videos-App",
        },
      }
    );

    if (!response.ok) {
      throw new Error("Geocoding failed");
    }

    const data = await response.json();
    const address = data.address;

    // Try to get a meaningful location name
    const name =
      address?.city ||
      address?.town ||
      address?.village ||
      address?.county ||
      address?.state ||
      `${latitude.toFixed(2)}, ${longitude.toFixed(2)}`;

    const country = address?.country;

    // Update cache
    cache[cacheKey] = { name, country, timestamp: Date.now() };
    setGeocodeCache(cache);

    return { name, country };
  } catch {
    // Return coordinates as fallback
    return { name: `${latitude.toFixed(2)}, ${longitude.toFixed(2)}` };
  }
}

interface Cluster {
  events: AIEvent[];
  centerLat: number;
  centerLon: number;
}

function getGridCell(lat: number, lon: number): string {
  const gridLat = Math.floor(lat / GRID_SIZE) * GRID_SIZE;
  const gridLon = Math.floor(lon / GRID_SIZE) * GRID_SIZE;
  return `${gridLat.toFixed(1)},${gridLon.toFixed(1)}`;
}

export function clusterEventsByLocation(events: AIEvent[]): Cluster[] {
  const clusters: Map<string, Cluster> = new Map();

  for (const event of events) {
    const cellKey = getGridCell(event.location.lat, event.location.lon);

    if (!clusters.has(cellKey)) {
      clusters.set(cellKey, {
        events: [],
        centerLat: 0,
        centerLon: 0,
      });
    }

    const cluster = clusters.get(cellKey)!;
    cluster.events.push(event);
  }

  // Calculate cluster centers
  for (const cluster of clusters.values()) {
    const sumLat = cluster.events.reduce((sum, e) => sum + e.location.lat, 0);
    const sumLon = cluster.events.reduce((sum, e) => sum + e.location.lon, 0);
    cluster.centerLat = sumLat / cluster.events.length;
    cluster.centerLon = sumLon / cluster.events.length;
  }

  return Array.from(clusters.values()).sort((a, b) => b.events.length - a.events.length);
}

export async function getRegionsFromEvents(events: AIEvent[]): Promise<Region[]> {
  const clusters = clusterEventsByLocation(events);

  const regions: Region[] = await Promise.all(
    clusters.map(async (cluster, index) => {
      const { name, country } = await reverseGeocode(cluster.centerLat, cluster.centerLon);
      return {
        id: `region-${index}`,
        name,
        latitude: cluster.centerLat,
        longitude: cluster.centerLon,
        eventCount: cluster.events.length,
        country,
      };
    })
  );

  return regions;
}

export function getCountriesFromRegions(regions: Region[]): string[] {
  const countries = new Set<string>();
  for (const region of regions) {
    if (region.country) {
      countries.add(region.country);
    }
  }
  return Array.from(countries).sort();
}

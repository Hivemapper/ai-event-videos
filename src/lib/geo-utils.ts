const EARTH_RADIUS = 6371000; // meters

/**
 * Haversine distance between two lat/lon points in meters.
 */
export function haversineDistance(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return EARTH_RADIUS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

/**
 * Create a circular polygon from center point and radius.
 * Returns array of [lon, lat] coordinates (note: lon first per GeoJSON spec).
 */
export function createCirclePolygon(
  lat: number,
  lon: number,
  radiusMeters: number,
  numPoints = 32
): [number, number][] {
  const coords: [number, number][] = [];

  for (let i = 0; i <= numPoints; i++) {
    const angle = (i / numPoints) * 2 * Math.PI;
    const dLat = (radiusMeters / EARTH_RADIUS) * Math.cos(angle);
    const dLon =
      (radiusMeters / (EARTH_RADIUS * Math.cos((lat * Math.PI) / 180))) *
      Math.sin(angle);

    coords.push([
      lon + (dLon * 180) / Math.PI,
      lat + (dLat * 180) / Math.PI,
    ]);
  }

  return coords;
}

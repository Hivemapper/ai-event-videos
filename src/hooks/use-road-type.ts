import { useState, useEffect } from "react";
import { getMapboxToken } from "@/lib/api";

export interface RoadTypeData {
  class: string | null;
  classLabel: string | null;
  name: string | null;
  structure: string | null;
  toll: boolean;
}

interface PathPoint {
  lat: number;
  lon: number;
}

interface UseRoadTypeResult {
  roadType: RoadTypeData | null;
  isLoading: boolean;
  error: string | null;
}

const SAMPLE_COUNT = 20;

/** Pick up to SAMPLE_COUNT evenly-spaced points from the path. */
function samplePath(path: PathPoint[]): [number, number][] {
  if (path.length <= SAMPLE_COUNT) {
    return path.map((p) => [p.lon, p.lat]);
  }
  const points: [number, number][] = [];
  for (let i = 0; i < SAMPLE_COUNT; i++) {
    const idx = Math.round((i / (SAMPLE_COUNT - 1)) * (path.length - 1));
    points.push([path[idx].lon, path[idx].lat]);
  }
  return points;
}

export function useRoadType(
  lat: number | null,
  lon: number | null,
  gnssPath?: PathPoint[]
): UseRoadTypeResult {
  const [roadType, setRoadType] = useState<RoadTypeData | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Stable key for the gnss path so the effect doesn't re-run on every render
  const pathKey = gnssPath ? gnssPath.length : 0;

  useEffect(() => {
    if (lat === null || lon === null) {
      setRoadType(null);
      setError(null);
      return;
    }

    let cancelled = false;

    const fetchRoadType = async () => {
      setIsLoading(true);
      setError(null);

      try {
        const mapboxToken = getMapboxToken();
        if (!mapboxToken) {
          throw new Error("Mapbox token not configured");
        }

        let url: string;
        if (gnssPath && gnssPath.length >= 2) {
          const points = samplePath(gnssPath);
          url = `/api/road-type?points=${encodeURIComponent(JSON.stringify(points))}&token=${encodeURIComponent(mapboxToken)}`;
        } else {
          url = `/api/road-type?lat=${lat}&lon=${lon}&token=${encodeURIComponent(mapboxToken)}`;
        }

        const response = await fetch(url);
        if (cancelled) return;

        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.error || "Failed to fetch road type");
        }

        const data: RoadTypeData = await response.json();
        if (!cancelled) {
          setRoadType(data);
        }
      } catch (err) {
        if (!cancelled) {
          setError(err instanceof Error ? err.message : "Failed to fetch road type");
          setRoadType(null);
        }
      } finally {
        if (!cancelled) {
          setIsLoading(false);
        }
      }
    };

    fetchRoadType();

    return () => {
      cancelled = true;
    };
  }, [lat, lon, pathKey]);

  return { roadType, isLoading, error };
}

import { GnssDataPoint } from "@/types/events";
import { TimeOfDay } from "@/lib/sun";
import { Sun, Moon, Sunrise, Sunset, LucideIcon } from "lucide-react";
import { haversineDistance } from "@/lib/geo-utils";

export interface SpeedDataPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

export interface LabeledFeature {
  class: string;
  distance: number;
  position: { lat: number; lon: number };
  speedLimit?: number;
  unit?: string;
}

export function formatDateTime(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toLocaleString();
}

export function formatCoordinates(lat: number, lon: number): string {
  return `${lat.toFixed(6)}, ${lon.toFixed(6)}`;
}

export function formatSpeed(speedMs: number): string {
  const mph = speedMs * 2.237;
  return `${mph.toFixed(1)} mph`;
}

export function getTimeOfDayIcon(timeOfDay: TimeOfDay): LucideIcon {
  switch (timeOfDay) {
    case "Day":
      return Sun;
    case "Dawn":
      return Sunrise;
    case "Dusk":
      return Sunset;
    case "Night":
      return Moon;
  }
}

export function getProxyVideoUrl(url: string): string {
  return `/api/video?url=${encodeURIComponent(url)}`;
}

export function deriveSpeedFromGnss(gnssData: GnssDataPoint[]): SpeedDataPoint[] {
  if (gnssData.length < 2) return [];
  const result: SpeedDataPoint[] = [];
  for (let i = 1; i < gnssData.length; i++) {
    const prev = gnssData[i - 1];
    const curr = gnssData[i];
    const dt = (curr.timestamp - prev.timestamp) / 1000;
    if (dt <= 0) continue;
    const dist = haversineDistance(prev.lat, prev.lon, curr.lat, curr.lon);
    result.push({ AVG_SPEED_MS: dist / dt, TIMESTAMP: curr.timestamp });
  }
  return result;
}

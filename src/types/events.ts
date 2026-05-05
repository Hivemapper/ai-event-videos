export type AIEventType =
  | "HARSH_BRAKING"
  | "AGGRESSIVE_ACCELERATION"
  | "SWERVING"
  | "HIGH_SPEED"
  | "HIGH_G_FORCE"
  | "STOP_SIGN_VIOLATION"
  | "TRAFFIC_LIGHT_VIOLATION"
  | "TAILGATING"
  | "MANUAL_REQUEST"
  | "UNKNOWN";

export interface AIEventLocation {
  lat: number;
  lon: number;
}

export interface GnssDataPoint {
  lat: number;
  lon: number;
  alt: number;
  timestamp: number;
}

export interface ImuDataPoint {
  timestamp: number;
  accelerometer?: {
    x: number;
    y: number;
    z: number;
  };
  gyroscope?: {
    x: number;
    y: number;
    z: number;
  };
}

export interface EventEnrichment {
  nearMiss: { score: number; label?: string; distanceM?: number; approaching?: boolean } | null;
  vruDetections: Array<{
    label: string;
    segments: Array<{ startMs: number; endMs: number; maxConfidence: number }>;
  }>;
  weather: { value: string; confidence: number } | null;
  road: {
    type: string | null;
    label: string | null;
    name: string | null;
    speedLimit: { value: number; unit: string } | null;
  };
  summary: string | null;
  timeOfDay: string | null;
  location: { city: string | null; country: string | null } | null;
  timeline: Array<{ startSec: number; endSec: number; event: string; details: string }> | null;
}

export interface AIEvent {
  id: string;
  type: AIEventType;
  timestamp: string;
  location: AIEventLocation;
  metadata?: Record<string, unknown>;
  videoUrl: string;
  gnssData?: GnssDataPoint[];
  imuData?: ImuDataPoint[];
  enrichment?: EventEnrichment;
}

export interface AIEventsResponse {
  pagination: {
    total: number;
    limit: number;
    offset: number;
  };
  events: AIEvent[];
}

export interface AIEventsRequest {
  startDate: string;
  endDate: string;
  types?: AIEventType[];
  polygon?: [number, number][]; // Array of [lon, lat] coordinates forming a closed polygon
  vruLabels?: string[];
  limit?: number;
  offset?: number;
}

export interface Region {
  id: string;
  name: string;
  latitude: number;
  longitude: number;
  eventCount: number;
  country?: string;
}

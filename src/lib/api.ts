import { AIEventsRequest, AIEventsResponse } from "@/types/events";
import { fetchWithRetry } from "@/lib/fetch-retry";

const API_KEY_STORAGE_KEY = "beemaps-api-key";
const MAPBOX_TOKEN_STORAGE_KEY = "mapbox-token";
const ANTHROPIC_KEY_STORAGE_KEY = "anthropic-api-key";
const CAMERA_INTRINSICS_STORAGE_KEY = "camera-intrinsics";
const SPEED_UNIT_STORAGE_KEY = "speed-unit";

export type SpeedUnit = "mph" | "kmh";

export function getSpeedUnit(): SpeedUnit {
  if (typeof window === "undefined") return "mph";
  return (localStorage.getItem(SPEED_UNIT_STORAGE_KEY) as SpeedUnit) || "mph";
}

export function setSpeedUnit(unit: SpeedUnit): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(SPEED_UNIT_STORAGE_KEY, unit);
}

export function convertSpeed(kmh: number, unit: SpeedUnit): number {
  return unit === "mph" ? Math.round(kmh * 0.621371) : Math.round(kmh);
}

export function speedLabel(unit: SpeedUnit): string {
  return unit === "mph" ? "mph" : "km/h";
}

export interface CameraIntrinsics {
  focal: number;
  k1: number;
  k2: number;
  k3?: number;
  k4?: number;
  k5?: number;
  k6?: number;
  p1?: number;
  p2?: number;
  s1?: number;
  s2?: number;
  s3?: number;
  s4?: number;
  tauX?: number;
  tauY?: number;
}

export interface DevicesResponse {
  hdc: CameraIntrinsics;
  "hdc-s": CameraIntrinsics;
  bee: CameraIntrinsics;
}

// Bee camera horizontal FOV (degrees) — manufacturer spec.
// The normalized focal length from /devices is for the distortion model,
// not suitable for a simple pinhole FOV calculation.
export const BEE_HFOV = 142;

export function getApiKey(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(API_KEY_STORAGE_KEY);
}

export function setApiKey(key: string): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(API_KEY_STORAGE_KEY, key);
}

export function clearApiKey(): void {
  if (typeof window === "undefined") return;
  localStorage.removeItem(API_KEY_STORAGE_KEY);
}

export function getMapboxToken(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(MAPBOX_TOKEN_STORAGE_KEY) || process.env.NEXT_PUBLIC_MAPBOX_TOKEN || null;
}

export function setMapboxToken(token: string): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(MAPBOX_TOKEN_STORAGE_KEY, token);
}

export function clearMapboxToken(): void {
  if (typeof window === "undefined") return;
  localStorage.removeItem(MAPBOX_TOKEN_STORAGE_KEY);
}

export function getAnthropicKey(): string | null {
  if (typeof window === "undefined") return null;
  return localStorage.getItem(ANTHROPIC_KEY_STORAGE_KEY);
}

export function setAnthropicKey(key: string): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(ANTHROPIC_KEY_STORAGE_KEY, key);
}

export function clearAnthropicKey(): void {
  if (typeof window === "undefined") return;
  localStorage.removeItem(ANTHROPIC_KEY_STORAGE_KEY);
}

export function getCameraIntrinsics(): DevicesResponse | null {
  if (typeof window === "undefined") return null;
  const stored = localStorage.getItem(CAMERA_INTRINSICS_STORAGE_KEY);
  if (!stored) return null;
  try {
    return JSON.parse(stored);
  } catch {
    return null;
  }
}

export function setCameraIntrinsics(data: DevicesResponse): void {
  if (typeof window === "undefined") return;
  localStorage.setItem(CAMERA_INTRINSICS_STORAGE_KEY, JSON.stringify(data));
}

export function clearCameraIntrinsics(): void {
  if (typeof window === "undefined") return;
  localStorage.removeItem(CAMERA_INTRINSICS_STORAGE_KEY);
}

export async function fetchCameraIntrinsics(): Promise<DevicesResponse> {
  const response = await fetch("https://beemaps.com/api/developer/devices");
  if (!response.ok) {
    throw new Error(`Failed to fetch camera intrinsics: ${response.status}`);
  }
  return response.json();
}

export async function fetchEvents(
  request: AIEventsRequest,
  apiKey?: string
): Promise<AIEventsResponse> {
  const key = apiKey || getApiKey();

  if (!key) {
    throw new Error("API key is required. Please configure your API key in settings.");
  }

  const response = await fetchWithRetry("/api/events", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: key,
    },
    body: JSON.stringify(request),
  });

  if (!response.ok) {
    const error = await response.json().catch(() => ({ error: "Unknown error" }));
    throw new Error(error.error || `API error: ${response.status}`);
  }

  return response.json();
}

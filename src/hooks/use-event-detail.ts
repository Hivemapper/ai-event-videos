"use client";

import useSWR from "swr";
import { AIEvent } from "@/types/events";
import { getApiKey, getMapboxToken } from "@/lib/api";
import { haversineDistance } from "@/lib/geo-utils";
import { LabeledFeature } from "@/lib/event-helpers";

const SWR_OPTIONS = {
  dedupingInterval: 60000,
  revalidateOnFocus: false,
};

async function fetchEventDetail(id: string): Promise<AIEvent> {
  const apiKey = getApiKey();
  if (!apiKey) throw new Error("API key not configured");

  const response = await fetch(`/api/events/${id}?includeGnssData=true&includeImuData=true`, {
    headers: { Authorization: apiKey },
  });

  if (!response.ok) {
    const data = await response.json();
    throw new Error(data.error || "Failed to fetch event");
  }

  return response.json();
}

export function useEventDetail(id: string) {
  return useSWR<AIEvent>(
    id ? `event-detail-${id}` : null,
    () => fetchEventDetail(id),
    SWR_OPTIONS
  );
}

async function fetchCountryName([, lon, lat]: [string, number, number]): Promise<string | null> {
  const token = getMapboxToken();
  if (!token) return null;

  const response = await fetch(
    `https://api.mapbox.com/geocoding/v5/mapbox.places/${lon},${lat}.json?types=country&access_token=${token}`
  );
  if (!response.ok) return null;

  const data = await response.json();
  return data.features?.[0]?.text ?? null;
}

export function useCountryName(lat: number | null, lon: number | null) {
  return useSWR(
    lat !== null && lon !== null ? ["country", lon, lat] as [string, number, number] : null,
    fetchCountryName,
    SWR_OPTIONS
  );
}

async function fetchNearestSpeedLimit([, lat, lon]: [string, number, number]): Promise<{ limit: number; unit: string } | null> {
  const apiKey = getApiKey();
  if (!apiKey) return null;

  const response = await fetch(
    `/api/map-features?lat=${lat}&lon=${lon}&radius=200`,
    { headers: { Authorization: apiKey } }
  );

  if (!response.ok) return null;

  const data = await response.json();
  const features = data.features as LabeledFeature[] | undefined;
  if (!features || features.length === 0) return null;

  const speedSigns = features.filter(
    (f) => f.class === "speed-sign" && f.speedLimit !== undefined
  );
  if (speedSigns.length === 0) return null;

  let nearest = speedSigns[0];
  let minDistance = haversineDistance(lat, lon, nearest.position.lat, nearest.position.lon);

  for (const sign of speedSigns.slice(1)) {
    const distance = haversineDistance(lat, lon, sign.position.lat, sign.position.lon);
    if (distance < minDistance) {
      minDistance = distance;
      nearest = sign;
    }
  }

  return { limit: nearest.speedLimit!, unit: nearest.unit || "mph" };
}

export function useNearestSpeedLimit(lat: number | null, lon: number | null) {
  return useSWR(
    lat !== null && lon !== null ? ["speed-limit", lat, lon] as [string, number, number] : null,
    fetchNearestSpeedLimit,
    SWR_OPTIONS
  );
}

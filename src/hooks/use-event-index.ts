"use client";

import { useState, useEffect, useRef } from "react";
import { fetchEvents, getApiKey, getMapboxToken } from "@/lib/api";
import {
  EventIndexEntry,
  getAllIndexedEvents,
  putIndexedEvents,
} from "@/lib/event-index";
import {
  getCountryForCoordinateSync,
  preloadCountryData,
} from "@/lib/country-lookup";

export interface EventIndexProgress {
  phase:
    | "idle"
    | "loading-countries"
    | "discovering"
    | "road-types"
    | "complete";
  eventsDiscovered: number;
  totalEvents: number;
  countriesResolved: number;
  roadTypesResolved: number;
  roadTypesTotal: number;
}

export interface EventIndexResult {
  index: Map<string, EventIndexEntry>;
  countries: string[];
  roadTypes: string[];
  progress: EventIndexProgress;
}

/** Group raw Mapbox road classes into filter-friendly labels */
export const ROAD_CLASS_GROUPS: Record<string, string> = {
  motorway: "Highway",
  motorway_link: "Highway",
  trunk: "Major Road",
  trunk_link: "Major Road",
  primary: "Primary Road",
  primary_link: "Primary Road",
  secondary: "Secondary Road",
  secondary_link: "Secondary Road",
  tertiary: "Local Road",
  tertiary_link: "Local Road",
  street: "Residential",
  street_limited: "Residential",
  service: "Service Road",
  path: "Path/Trail",
  pedestrian: "Pedestrian",
  track: "Track",
};

export function getRoadTypeGroup(roadClass: string | null): string | null {
  if (!roadClass) return null;
  return ROAD_CLASS_GROUPS[roadClass] || roadClass;
}

/** All possible road type groups in display order */
export const ALL_ROAD_TYPES = [
  "Highway",
  "Major Road",
  "Primary Road",
  "Secondary Road",
  "Local Road",
  "Residential",
  "Service Road",
];

function getGridKey(lat: number, lon: number): string {
  return `${(Math.floor(lat / 0.01) * 0.01).toFixed(2)},${(Math.floor(lon / 0.01) * 0.01).toFixed(2)}`;
}

export function useEventIndex(
  startDate: string,
  endDate: string,
  enabled = true
): EventIndexResult {
  const [index, setIndex] = useState<Map<string, EventIndexEntry>>(new Map());
  const [countries, setCountries] = useState<string[]>([]);
  const [roadTypes, setRoadTypes] = useState<string[]>([]);
  const [progress, setProgress] = useState<EventIndexProgress>({
    phase: "idle",
    eventsDiscovered: 0,
    totalEvents: 0,
    countriesResolved: 0,
    roadTypesResolved: 0,
    roadTypesTotal: 0,
  });

  const cancelledRef = useRef(false);
  const runIdRef = useRef(0);

  useEffect(() => {
    if (!enabled) {
      cancelledRef.current = true;
      return;
    }

    cancelledRef.current = true; // cancel previous run
    const runId = ++runIdRef.current;
    cancelledRef.current = false;

    const isCancelled = () => cancelledRef.current || runIdRef.current !== runId;

    const run = async () => {
      const apiKey = getApiKey();
      if (!apiKey) return;

      // Load existing index from IndexedDB
      let existing: Map<string, EventIndexEntry>;
      try {
        existing = await getAllIndexedEvents();
      } catch {
        existing = new Map();
      }
      if (isCancelled()) return;
      setIndex(existing);

      // Phase 1: Load country boundary data
      setProgress((p) => ({ ...p, phase: "loading-countries" }));
      await preloadCountryData();
      if (isCancelled()) return;

      // Phase 2: Discover all events and resolve countries
      setProgress((p) => ({ ...p, phase: "discovering" }));
      const startDateTime = new Date(
        startDate + "T00:00:00.000Z"
      ).toISOString();
      const endDateTime = new Date(endDate + "T23:59:59.999Z").toISOString();

      let offset = 0;
      const pageSize = 500;
      let total = 0;
      const allEventIds = new Set<string>();
      let pendingEntries: EventIndexEntry[] = [];
      const countriesSet = new Set<string>();

      // Collect countries from existing entries
      for (const entry of existing.values()) {
        if (entry.country) countriesSet.add(entry.country);
      }

      do {
        if (isCancelled()) return;

        try {
          const response = await fetchEvents(
            {
              startDate: startDateTime,
              endDate: endDateTime,
              limit: pageSize,
              offset,
            },
            apiKey
          );

          total = response.pagination.total;

          for (const event of response.events) {
            allEventIds.add(event.id);

            const existingEntry = existing.get(event.id);
            if (existingEntry?.country) {
              // Already indexed with country
              countriesSet.add(existingEntry.country);
              // Update lat/lon if missing
              if (!existingEntry.lat) {
                existingEntry.lat = event.location.lat;
                existingEntry.lon = event.location.lon;
                pendingEntries.push(existingEntry);
              }
              continue;
            }

            const country = getCountryForCoordinateSync(
              event.location.lat,
              event.location.lon
            );
            const entry: EventIndexEntry = {
              eventId: event.id,
              lat: event.location.lat,
              lon: event.location.lon,
              country,
              roadClass: existingEntry?.roadClass ?? null,
              roadLabel: existingEntry?.roadLabel ?? null,
            };
            pendingEntries.push(entry);
            existing.set(event.id, entry);
            if (country) countriesSet.add(country);
          }

          offset += response.events.length;

          setProgress((p) => ({
            ...p,
            eventsDiscovered: offset,
            totalEvents: total,
            countriesResolved: countriesSet.size,
          }));

          // Flush to IndexedDB periodically
          if (pendingEntries.length >= 500) {
            await putIndexedEvents(pendingEntries);
            pendingEntries = [];
            setIndex(new Map(existing));
            setCountries(Array.from(countriesSet).sort());
          }
        } catch (err) {
          console.error("Event index discovery error:", err);
          break;
        }
      } while (offset < total);

      // Flush remaining entries
      if (pendingEntries.length > 0) {
        await putIndexedEvents(pendingEntries);
        pendingEntries = [];
      }

      if (isCancelled()) return;
      setIndex(new Map(existing));
      setCountries(Array.from(countriesSet).sort());

      // Phase 3: Road types
      setProgress((p) => ({ ...p, phase: "road-types" }));

      const mapboxToken = getMapboxToken();
      if (!mapboxToken) {
        setProgress((p) => ({ ...p, phase: "complete" }));
        return;
      }

      // Group events needing road type by grid cell
      const gridCells = new Map<
        string,
        { lat: number; lon: number; eventIds: string[] }
      >();

      for (const [eventId, entry] of existing) {
        if (!allEventIds.has(eventId)) continue; // not in current date range
        if (entry.roadClass !== null) continue; // already resolved

        const key = getGridKey(entry.lat, entry.lon);
        if (!gridCells.has(key)) {
          gridCells.set(key, { lat: entry.lat, lon: entry.lon, eventIds: [] });
        }
        gridCells.get(key)!.eventIds.push(eventId);
      }

      const cellsToQuery = Array.from(gridCells.entries());
      const roadTypesTotal = cellsToQuery.length;

      setProgress((p) => ({ ...p, roadTypesTotal }));

      if (roadTypesTotal === 0) {
        // Collect existing road types
        const rtSet = new Set<string>();
        for (const [eventId, entry] of existing) {
          if (!allEventIds.has(eventId)) continue;
          const group = getRoadTypeGroup(entry.roadClass);
          if (group) rtSet.add(group);
        }
        setRoadTypes(Array.from(rtSet).sort());
        setProgress((p) => ({ ...p, phase: "complete" }));
        return;
      }

      // Batch query road types — 20 points per batch
      const BATCH_SIZE = 20;
      let resolved = 0;

      for (let i = 0; i < cellsToQuery.length; i += BATCH_SIZE) {
        if (isCancelled()) return;

        const batch = cellsToQuery.slice(i, i + BATCH_SIZE);
        const points = batch.map(([key, cell]) => ({
          key,
          lat: cell.lat,
          lon: cell.lon,
        }));

        try {
          const response = await fetch("/api/road-type/batch", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ points, token: mapboxToken }),
          });

          if (response.ok) {
            const data = await response.json();
            const batchEntries: EventIndexEntry[] = [];

            for (const [key, cell] of batch) {
              const result = data.results?.[key];
              const roadClass = result?.class ?? null;
              const roadLabel = result?.label ?? null;

              for (const eventId of cell.eventIds) {
                const entry = existing.get(eventId);
                if (entry) {
                  entry.roadClass = roadClass;
                  entry.roadLabel = roadLabel;
                  batchEntries.push(entry);
                }
              }
            }

            await putIndexedEvents(batchEntries);
          }
        } catch (err) {
          console.error("Road type batch error:", err);
        }

        resolved += batch.length;
        setProgress((p) => ({ ...p, roadTypesResolved: resolved }));

        // Update index periodically
        if (resolved % 100 === 0 || i + BATCH_SIZE >= cellsToQuery.length) {
          setIndex(new Map(existing));
        }
      }

      if (isCancelled()) return;

      // Collect all road types
      const rtSet = new Set<string>();
      for (const [eventId, entry] of existing) {
        if (!allEventIds.has(eventId)) continue;
        const group = getRoadTypeGroup(entry.roadClass);
        if (group) rtSet.add(group);
      }
      setRoadTypes(Array.from(rtSet).sort());
      setIndex(new Map(existing));
      setProgress((p) => ({ ...p, phase: "complete" }));
    };

    run();

    return () => {
      cancelledRef.current = true;
    };
  }, [startDate, endDate, enabled]);

  return { index, countries, roadTypes, progress };
}

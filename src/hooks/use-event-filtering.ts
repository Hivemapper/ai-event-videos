"use client";

import { useMemo, useRef, useEffect } from "react";
import { AIEvent, Region } from "@/types/events";
import { getTimeOfDay, TimeOfDay } from "@/lib/sun";
import { EventIndexEntry } from "@/lib/event-index";
import { getRoadTypeGroup } from "@/hooks/use-event-index";

interface Coordinates {
  lat: number;
  lon: number;
}

interface UseEventFilteringOptions {
  events: AIEvent[];
  regions: Region[];
  countries: string[];
  selectedTimeOfDay: TimeOfDay[];
  selectedCountries: string[];
  searchCoordinates?: Coordinates | null;
  eventIndex?: Map<string, EventIndexEntry>;
  indexCountries?: string[];
  selectedRoadTypes?: string[];
}

function getGridCellKey(lat: number, lon: number): string {
  return `${Math.floor(lat / 0.1) * 0.1},${Math.floor(lon / 0.1) * 0.1}`;
}

export function useEventFiltering({
  events,
  regions,
  countries,
  selectedTimeOfDay,
  selectedCountries,
  searchCoordinates,
  eventIndex,
  indexCountries,
  selectedRoadTypes,
}: UseEventFilteringOptions): AIEvent[] {
  // Cache time-of-day results per event ID, cleared when events change
  const todCacheRef = useRef<Map<string, TimeOfDay>>(new Map());
  const prevEventsRef = useRef<AIEvent[]>([]);

  useEffect(() => {
    if (events !== prevEventsRef.current) {
      todCacheRef.current = new Map();
      prevEventsRef.current = events;
    }
  }, [events]);

  // Build a grid-cell → region-id lookup map from regions (O(1) per event)
  const gridToRegionId = useMemo(() => {
    const map = new Map<string, string>();
    for (const r of regions) {
      const key = getGridCellKey(r.latitude, r.longitude);
      map.set(key, r.id);
    }
    return map;
  }, [regions]);

  // Determine whether to use index-based or region-based country filtering
  const useIndexForCountries = !!eventIndex && eventIndex.size > 0 && !!indexCountries && indexCountries.length > 0;

  // The effective country list for "all selected" check
  const effectiveCountries = useIndexForCountries ? indexCountries! : countries;

  // Build a set of region IDs in selected countries (fallback when no index)
  const regionIdsInSelectedCountries = useMemo(() => {
    if (useIndexForCountries) return null; // using index instead
    if (selectedCountries.length === 0 || selectedCountries.length >= countries.length) {
      return null; // no filtering needed
    }
    const selectedCountrySet = new Set(selectedCountries);
    const ids = new Set<string>();
    for (const r of regions) {
      if (r.country && selectedCountrySet.has(r.country)) {
        ids.add(r.id);
      }
    }
    return ids;
  }, [regions, countries, selectedCountries, useIndexForCountries]);

  return useMemo(() => {
    let filtered = events;

    // Filter by time of day if any selected
    if (selectedTimeOfDay.length > 0) {
      const cache = todCacheRef.current;
      const todSet = new Set(selectedTimeOfDay);

      filtered = filtered.filter((event) => {
        let tod = cache.get(event.id);
        if (tod === undefined) {
          tod = getTimeOfDay(
            event.timestamp,
            event.location.lat,
            event.location.lon
          ).timeOfDay;
          cache.set(event.id, tod);
        }
        return todSet.has(tod);
      });
    }

    // Skip country filtering when using coordinate search (server handles it)
    if (searchCoordinates) {
      // Still apply road type filtering even with coordinate search
      if (selectedRoadTypes && selectedRoadTypes.length > 0 && eventIndex) {
        const rtSet = new Set(selectedRoadTypes);
        filtered = filtered.filter((event) => {
          const entry = eventIndex.get(event.id);
          if (!entry || entry.roadClass === null) return true; // pass unindexed events
          const group = getRoadTypeGroup(entry.roadClass);
          return group !== null && rtSet.has(group);
        });
      }
      return filtered;
    }

    // Country filtering
    if (useIndexForCountries) {
      // Index-based country filtering
      const allSelected = selectedCountries.length === 0 || selectedCountries.length >= effectiveCountries.length;
      if (!allSelected && selectedCountries.length > 0) {
        const selectedSet = new Set(selectedCountries);
        filtered = filtered.filter((event) => {
          const entry = eventIndex!.get(event.id);
          if (!entry || !entry.country) return true; // pass unindexed events
          return selectedSet.has(entry.country);
        });
      }
    } else if (regionIdsInSelectedCountries) {
      // Fallback: region-based country filtering
      filtered = filtered.filter((event) => {
        const cellKey = getGridCellKey(event.location.lat, event.location.lon);
        const regionId = gridToRegionId.get(cellKey);
        return regionId !== undefined && regionIdsInSelectedCountries.has(regionId);
      });
    }

    // Road type filtering
    if (selectedRoadTypes && selectedRoadTypes.length > 0 && eventIndex) {
      const rtSet = new Set(selectedRoadTypes);
      filtered = filtered.filter((event) => {
        const entry = eventIndex.get(event.id);
        if (!entry || entry.roadClass === null) return true; // pass unindexed events
        const group = getRoadTypeGroup(entry.roadClass);
        return group !== null && rtSet.has(group);
      });
    }

    return filtered;
  }, [
    events,
    selectedTimeOfDay,
    searchCoordinates,
    regionIdsInSelectedCountries,
    gridToRegionId,
    useIndexForCountries,
    eventIndex,
    indexCountries,
    effectiveCountries,
    selectedCountries,
    selectedRoadTypes,
  ]);
}

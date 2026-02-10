"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { AIEvent, AIEventType, Region } from "@/types/events";
import { fetchEvents, getApiKey } from "@/lib/api";
import { getRegionsFromEvents, getCountriesFromRegions } from "@/lib/geo";
import { TimeOfDay } from "@/lib/sun";
import { createCirclePolygon } from "@/lib/geo-utils";
import { useEventPolling } from "@/hooks/use-event-polling";
import { useEventFiltering } from "@/hooks/use-event-filtering";

interface Coordinates {
  lat: number;
  lon: number;
}

interface UseEventsOptions {
  startDate: string;
  endDate: string;
  types?: AIEventType[];
  selectedTimeOfDay?: TimeOfDay[];
  selectedCountries?: string[];
  searchCoordinates?: Coordinates | null;
  searchRadius?: number;
  limit?: number;
}

interface UseEventsResult {
  events: AIEvent[];
  filteredEvents: AIEvent[];
  regions: Region[];
  countries: string[];
  isLoading: boolean;
  error: string | null;
  hasApiKey: boolean;
  totalCount: number;
  loadMore: () => void;
  hasMore: boolean;
  refresh: () => void;
  newEventsCount: number;
  showNewEvents: () => void;
}

export function useEvents(options: UseEventsOptions): UseEventsResult {
  const { startDate, endDate, types, selectedTimeOfDay = [], selectedCountries = [], searchCoordinates, searchRadius = 500, limit = 50 } = options;

  const [events, setEvents] = useState<AIEvent[]>([]);
  const [regions, setRegions] = useState<Region[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasApiKey, setHasApiKey] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const initialTotalRef = useRef(0);

  // Stabilize offset and events with refs to avoid recreating loadEvents
  const offsetRef = useRef(0);
  const eventsRef = useRef<AIEvent[]>([]);

  // Keep eventsRef in sync
  useEffect(() => {
    eventsRef.current = events;
  }, [events]);

  // Check for API key on mount
  useEffect(() => {
    setHasApiKey(!!getApiKey());
  }, []);

  const loadEvents = useCallback(
    async (reset = false) => {
      if (!getApiKey()) {
        setHasApiKey(false);
        setError("Please configure your API key in settings.");
        return;
      }

      setHasApiKey(true);
      setIsLoading(true);
      setError(null);

      const currentOffset = reset ? 0 : offsetRef.current;

      try {
        const startDateTime = new Date(startDate + "T00:00:00.000Z").toISOString();
        const endDateTime = new Date(endDate + "T23:59:59.999Z").toISOString();

        const polygon = searchCoordinates
          ? createCirclePolygon(searchCoordinates.lat, searchCoordinates.lon, searchRadius)
          : undefined;

        const response = await fetchEvents({
          startDate: startDateTime,
          endDate: endDateTime,
          types: types && types.length > 0 ? types : undefined,
          polygon,
          limit,
          offset: currentOffset,
        });

        const newEvents = reset
          ? response.events
          : [...eventsRef.current, ...response.events];

        // Show events immediately — no geocoding blocking here
        setEvents(newEvents);
        setTotalCount(response.pagination.total);
        offsetRef.current = currentOffset + response.events.length;

        if (reset) {
          initialTotalRef.current = response.pagination.total;
        }
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load events");
      } finally {
        setIsLoading(false);
      }
    },
    [startDate, endDate, types, searchCoordinates, searchRadius, limit]
  );

  // Load events when server-side filters change
  useEffect(() => {
    offsetRef.current = 0;
    setEvents([]);
    eventsRef.current = [];
    loadEvents(true);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [startDate, endDate, JSON.stringify(types), searchCoordinates?.lat, searchCoordinates?.lon, searchRadius]);

  // Geocode regions asynchronously after events load
  useEffect(() => {
    if (events.length === 0) {
      setRegions([]);
      setCountries([]);
      return;
    }

    let cancelled = false;

    getRegionsFromEvents(events).then((newRegions) => {
      if (cancelled) return;
      setRegions(newRegions);
      setCountries(getCountriesFromRegions(newRegions));
    });

    return () => {
      cancelled = true;
    };
  }, [events]);

  const loadMore = useCallback(() => {
    if (!isLoading && events.length < totalCount) {
      loadEvents(false);
    }
  }, [isLoading, events.length, totalCount, loadEvents]);

  // Polling for new events
  const fetchCount = useCallback(async () => {
    const startDateTime = new Date(startDate + "T00:00:00.000Z").toISOString();
    const endDateTime = new Date(endDate + "T23:59:59.999Z").toISOString();
    const polygon = searchCoordinates
      ? createCirclePolygon(searchCoordinates.lat, searchCoordinates.lon, searchRadius)
      : undefined;
    const response = await fetchEvents({
      startDate: startDateTime,
      endDate: endDateTime,
      types: types && types.length > 0 ? types : undefined,
      polygon,
      limit: 1,
      offset: 0,
    });
    return response.pagination.total;
  }, [startDate, endDate, types, searchCoordinates, searchRadius]);

  const { newEventsCount, showNewEvents: pollingShowNew } = useEventPolling({
    enabled: hasApiKey && !isLoading,
    fetchCount,
    initialTotal: initialTotalRef.current,
  });

  const refresh = useCallback(() => {
    offsetRef.current = 0;
    setEvents([]);
    eventsRef.current = [];
    loadEvents(true);
  }, [loadEvents]);

  const showNewEvents = useCallback(() => {
    pollingShowNew();
    offsetRef.current = 0;
    setEvents([]);
    eventsRef.current = [];
    loadEvents(true);
  }, [loadEvents, pollingShowNew]);

  // Client-side filtering
  const filteredEvents = useEventFiltering({
    events,
    regions,
    countries,
    selectedTimeOfDay,
    selectedCountries,
    searchCoordinates,
  });

  return {
    events,
    filteredEvents,
    regions,
    countries,
    isLoading,
    error,
    hasApiKey,
    totalCount,
    loadMore,
    hasMore: events.length < totalCount,
    refresh,
    newEventsCount,
    showNewEvents,
  };
}

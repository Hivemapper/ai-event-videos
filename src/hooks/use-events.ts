"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { AIEvent, AIEventType, Region } from "@/types/events";
import { fetchEvents, getApiKey } from "@/lib/api";
import { getRegionsFromEvents, getCountriesFromRegions } from "@/lib/geo";
import { TimeOfDay } from "@/lib/sun";
import { createCirclePolygon } from "@/lib/geo-utils";
import { useEventPolling } from "@/hooks/use-event-polling";
import { useEventFiltering } from "@/hooks/use-event-filtering";
import { EventIndexEntry } from "@/lib/event-index";

const MAX_GALLERY_CACHE_ENTRIES = 8;

function deduplicateEvents(events: AIEvent[]): AIEvent[] {
  const seen = new Set<string>();
  return events.filter((e) => {
    if (seen.has(e.id)) return false;
    seen.add(e.id);
    return true;
  });
}

interface CachedGalleryState {
  events: AIEvent[];
  totalCount: number;
  offset: number;
}

const galleryStateCache = new Map<string, CachedGalleryState>();

function getCachedGalleryState(key: string): CachedGalleryState | null {
  return galleryStateCache.get(key) ?? null;
}

function setCachedGalleryState(key: string, state: CachedGalleryState): void {
  galleryStateCache.delete(key);
  galleryStateCache.set(key, state);

  if (galleryStateCache.size <= MAX_GALLERY_CACHE_ENTRIES) return;
  const oldestKey = galleryStateCache.keys().next().value;
  if (oldestKey) galleryStateCache.delete(oldestKey);
}

function getGalleryCacheKey(options: {
  startDate: string;
  endDate: string;
  typesKey: string;
  vruLabelsKey: string;
  searchCoordinates?: Coordinates | null;
  searchRadius: number;
  limit: number;
}): string {
  return JSON.stringify({
    startDate: options.startDate,
    endDate: options.endDate,
    types: options.typesKey,
    vruLabels: options.vruLabelsKey,
    lat: options.searchCoordinates?.lat ?? null,
    lon: options.searchCoordinates?.lon ?? null,
    radius: options.searchRadius,
    limit: options.limit,
  });
}

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
  eventIndex?: Map<string, EventIndexEntry>;
  indexCountries?: string[];
  selectedRoadTypes?: string[];
  selectedVruLabels?: string[];
  resolveRegions?: boolean;
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
  isRefreshing: boolean;
  isLoadingNewEvents: boolean;
  isLoadingMore: boolean;
}

type LoadingMode = "initial" | "more" | "refresh" | "new-events";

export function useEvents(options: UseEventsOptions): UseEventsResult {
  const { startDate, endDate, types, selectedTimeOfDay = [], selectedCountries = [], searchCoordinates, searchRadius = 500, limit = 50, eventIndex, indexCountries, selectedRoadTypes, selectedVruLabels = [], resolveRegions = true } = options;
  const typesKey = JSON.stringify(types ?? []);
  const vruLabelsKey = JSON.stringify(selectedVruLabels);
  const cacheKey = getGalleryCacheKey({
    startDate,
    endDate,
    typesKey,
    vruLabelsKey,
    searchCoordinates,
    searchRadius,
    limit,
  });
  const cachedState = getCachedGalleryState(cacheKey);

  const [events, setEvents] = useState<AIEvent[]>(() => cachedState?.events ?? []);
  const [regions, setRegions] = useState<Region[]>([]);
  const [countries, setCountries] = useState<string[]>([]);
  const [loadingMode, setLoadingMode] = useState<LoadingMode | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [hasApiKey, setHasApiKey] = useState(false);
  const [totalCount, setTotalCount] = useState(() => cachedState?.totalCount ?? 0);
  const initialTotalRef = useRef(cachedState?.totalCount ?? 0);

  // Stabilize offset and events with refs to avoid recreating loadEvents
  const offsetRef = useRef(cachedState?.offset ?? cachedState?.events.length ?? 0);
  const eventsRef = useRef<AIEvent[]>(cachedState?.events ?? []);
  const activeRequestRef = useRef(0);

  // Keep eventsRef in sync
  useEffect(() => {
    eventsRef.current = events;
  }, [events]);

  // Check for API key on mount
  useEffect(() => {
    setHasApiKey(!!getApiKey());
  }, []);

  const commitEvents = useCallback(
    (nextEvents: AIEvent[], nextTotalCount: number, nextOffset: number) => {
      setEvents(nextEvents);
      setTotalCount(nextTotalCount);
      offsetRef.current = nextOffset;
      eventsRef.current = nextEvents;
      setCachedGalleryState(cacheKey, {
        events: nextEvents,
        totalCount: nextTotalCount,
        offset: nextOffset,
      });
    },
    [cacheKey]
  );

  const loadEvents = useCallback(
    async ({
      reset = false,
      mode,
      preserveCurrent = false,
    }: {
      reset?: boolean;
      mode: LoadingMode;
      preserveCurrent?: boolean;
    }): Promise<boolean> => {
      if (!getApiKey()) {
        setHasApiKey(false);
        setError("Please configure your API key in settings.");
        return false;
      }

      const requestId = activeRequestRef.current + 1;
      activeRequestRef.current = requestId;
      setHasApiKey(true);
      setLoadingMode(mode);
      setError(null);

      const currentOffset = reset ? 0 : offsetRef.current;
      if (reset && !preserveCurrent) {
        commitEvents([], 0, 0);
      }

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
          vruLabels: selectedVruLabels.length > 0 ? selectedVruLabels : undefined,
          polygon,
          limit,
          offset: currentOffset,
        });

        if (activeRequestRef.current !== requestId) return false;

        const newEvents = reset
          ? response.events
          : deduplicateEvents([...eventsRef.current, ...response.events]);
        const nextOffset = currentOffset + response.events.length;

        // Show events immediately — no geocoding blocking here
        commitEvents(newEvents, response.pagination.total, nextOffset);

        if (reset) {
          initialTotalRef.current = response.pagination.total;
        }

        return true;
      } catch (err) {
        if (activeRequestRef.current !== requestId) return false;
        setError(err instanceof Error ? err.message : "Failed to load events");
        return false;
      } finally {
        if (activeRequestRef.current === requestId) {
          setLoadingMode(null);
        }
      }
    },
    [startDate, endDate, types, selectedVruLabels, searchCoordinates, searchRadius, limit, commitEvents]
  );

  // Load events when server-side filters change
  useEffect(() => {
    activeRequestRef.current += 1;
    const cached = getCachedGalleryState(cacheKey);

    if (cached) {
      commitEvents(cached.events, cached.totalCount, cached.offset);
      initialTotalRef.current = cached.totalCount;
      setError(null);
      setLoadingMode(null);
    } else {
      offsetRef.current = 0;
      eventsRef.current = [];
      setEvents([]);
      setTotalCount(0);
      initialTotalRef.current = 0;
      void loadEvents({ reset: true, mode: "initial" });
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [cacheKey]);

  // Geocode regions asynchronously after events load
  useEffect(() => {
    if (!resolveRegions || events.length === 0) {
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
  }, [events, resolveRegions]);

  const isLoading = loadingMode !== null;
  const isRefreshing = loadingMode === "refresh" || loadingMode === "new-events";
  const isLoadingMore = loadingMode === "more";
  const isLoadingNewEvents = loadingMode === "new-events";

  const loadMore = useCallback(() => {
    if (!isLoading && events.length < totalCount) {
      void loadEvents({ reset: false, mode: "more" });
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
      vruLabels: selectedVruLabels.length > 0 ? selectedVruLabels : undefined,
      polygon,
      limit: 1,
      offset: 0,
    });
    return response.pagination.total;
  }, [startDate, endDate, types, selectedVruLabels, searchCoordinates, searchRadius]);

  const { newEventsCount, showNewEvents: pollingShowNew } = useEventPolling({
    enabled: hasApiKey && !isLoading,
    fetchCount,
    initialTotal: initialTotalRef.current,
  });

  const refresh = useCallback(() => {
    void loadEvents({
      reset: true,
      mode: eventsRef.current.length > 0 ? "refresh" : "initial",
      preserveCurrent: eventsRef.current.length > 0,
    });
  }, [loadEvents]);

  const showNewEvents = useCallback(() => {
    void loadEvents({
      reset: true,
      mode: "new-events",
      preserveCurrent: true,
    }).then((loaded) => {
      if (loaded) pollingShowNew();
    });
  }, [loadEvents, pollingShowNew]);

  // Client-side filtering
  const filteredEvents = useEventFiltering({
    events,
    regions,
    countries,
    selectedTimeOfDay,
    selectedCountries,
    searchCoordinates,
    eventIndex,
    indexCountries,
    selectedRoadTypes,
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
    isRefreshing,
    isLoadingNewEvents,
    isLoadingMore,
  };
}

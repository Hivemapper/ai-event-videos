"use client";

import { useState, useCallback, useEffect, useMemo, useRef, Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { RefreshCw, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import Link from "next/link";
import {
  EventGrid,
  FilterBar,
  NewEventsBanner,
} from "@/components/events";
import { Header } from "@/components/layout/header";
import type { Coordinates, FilterUrlOverrides } from "@/components/events/filter-bar";
import { useEvents } from "@/hooks/use-events";
import { useEventIndex } from "@/hooks/use-event-index";
import { AIEvent, AIEventType } from "@/types/events";
import { TimeOfDay } from "@/lib/sun";

// Default to last 7 days
function getDefaultDates() {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 7);

  return {
    startDate: start.toISOString().split("T")[0],
    endDate: end.toISOString().split("T")[0],
  };
}

function parseCoordinates(str: string | null): Coordinates | null {
  if (!str) return null;
  const [lat, lon] = str.split(",").map(Number);
  if (isNaN(lat) || isNaN(lon)) return null;
  return { lat, lon };
}

function GalleryView() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const defaultDates = useMemo(() => getDefaultDates(), []);

  // Initialize state from URL params or defaults
  const [startDate, setStartDate] = useState(
    searchParams.get("startDate") || defaultDates.startDate
  );
  const [endDate, setEndDate] = useState(
    searchParams.get("endDate") || defaultDates.endDate
  );
  const [selectedTypes, setSelectedTypes] = useState<AIEventType[]>(
    searchParams.get("types")?.split(",").filter(Boolean) as AIEventType[] || []
  );
  const [selectedCountries, setSelectedCountries] = useState<string[]>(
    searchParams.get("countries")?.split(",").filter(Boolean) || []
  );
  const [selectedTimeOfDay, setSelectedTimeOfDay] = useState<TimeOfDay[]>(
    searchParams.get("timeOfDay")?.split(",").filter(Boolean) as TimeOfDay[] || []
  );
  const [selectedRoadTypes, setSelectedRoadTypes] = useState<string[]>(
    searchParams.get("roadTypes")?.split(",").filter(Boolean) || []
  );
  const [selectedVruLabels, setSelectedVruLabels] = useState<string[]>(
    searchParams.get("vruLabels")?.split(",").filter(Boolean) || []
  );
  const [searchCoordinates, setSearchCoordinates] = useState<Coordinates | null>(
    parseCoordinates(searchParams.get("coords"))
  );
  const [searchRadius, setSearchRadius] = useState(
    parseInt(searchParams.get("radius") || "500")
  );
  const [eventIndexEnabled, setEventIndexEnabled] = useState(
    () => searchParams.has("countries") || searchParams.has("roadTypes")
  );


  // Background event index for country + road type discovery
  const { index: eventIndex, countries: indexCountries, roadTypes: indexRoadTypes, progress: indexProgress } = useEventIndex(startDate, endDate, eventIndexEnabled);

  // Update URL when filters change
  const updateUrl = useCallback((overrides: FilterUrlOverrides = {}) => {
    const hasOverride = (key: keyof FilterUrlOverrides) =>
      Object.prototype.hasOwnProperty.call(overrides, key);
    const nextStartDate = overrides.startDate ?? startDate;
    const nextEndDate = overrides.endDate ?? endDate;
    const nextSelectedTypes = overrides.selectedTypes ?? selectedTypes;
    const nextSelectedTimeOfDay = overrides.selectedTimeOfDay ?? selectedTimeOfDay;
    const nextSelectedCountries = overrides.selectedCountries ?? selectedCountries;
    const nextSelectedRoadTypes = overrides.selectedRoadTypes ?? selectedRoadTypes;
    const nextSelectedVruLabels = overrides.selectedVruLabels ?? selectedVruLabels;
    const nextSearchCoordinates = hasOverride("searchCoordinates")
      ? overrides.searchCoordinates ?? null
      : searchCoordinates;
    const nextSearchRadius = overrides.searchRadius ?? searchRadius;

    const params = new URLSearchParams();
    if (nextStartDate !== defaultDates.startDate) params.set("startDate", nextStartDate);
    if (nextEndDate !== defaultDates.endDate) params.set("endDate", nextEndDate);
    if (nextSelectedTypes.length > 0) params.set("types", nextSelectedTypes.join(","));
    if (nextSelectedTimeOfDay.length > 0) params.set("timeOfDay", nextSelectedTimeOfDay.join(","));
    if (nextSelectedCountries.length > 0) params.set("countries", nextSelectedCountries.join(","));
    if (nextSelectedRoadTypes.length > 0) params.set("roadTypes", nextSelectedRoadTypes.join(","));
    if (nextSelectedVruLabels.length > 0) params.set("vruLabels", nextSelectedVruLabels.join(","));
    if (nextSearchCoordinates) params.set("coords", `${nextSearchCoordinates.lat},${nextSearchCoordinates.lon}`);
    if (nextSearchRadius !== 500) params.set("radius", nextSearchRadius.toString());

    const queryString = params.toString();
    router.replace(queryString ? `?${queryString}` : "/", { scroll: false });
  }, [startDate, endDate, selectedTypes, selectedTimeOfDay, selectedCountries, selectedRoadTypes, selectedVruLabels, searchCoordinates, searchRadius, defaultDates, router]);

  const {
    filteredEvents,
    countries,
    isLoading,
    error,
    hasApiKey,
    totalCount,
    loadMore,
    hasMore,
    refresh,
    newEventsCount,
    showNewEvents,
    isRefreshing,
    isLoadingNewEvents,
    isLoadingMore,
  } = useEvents({
    startDate,
    endDate,
    types: selectedTypes,
    selectedTimeOfDay,
    selectedCountries,
    searchCoordinates,
    searchRadius,
    eventIndex,
    indexCountries,
    selectedRoadTypes,
    selectedVruLabels,
    resolveRegions: eventIndexEnabled,
  });

  // Use index countries when available, fallback to useEvents countries
  const effectiveCountries = indexCountries.length > 0 ? indexCountries : countries;

  // Initialize selectedCountries to all countries only when countries list first loads
  // Skip if countries were already set from URL params
  const [countriesInitialized, setCountriesInitialized] = useState(
    searchParams.get("countries") !== null
  );
  useEffect(() => {
    if (effectiveCountries.length > 0 && !countriesInitialized) {
      queueMicrotask(() => {
        setSelectedCountries(effectiveCountries);
        setCountriesInitialized(true);
      });
    }
  }, [effectiveCountries, countriesInitialized]);

  // When index discovers new countries, add them to selection if all were selected
  const prevIndexCountriesRef = useRef(indexCountries.length);
  useEffect(() => {
    if (countriesInitialized && indexCountries.length > prevIndexCountriesRef.current) {
      // If user had all countries selected, keep all selected
      if (selectedCountries.length >= prevIndexCountriesRef.current && prevIndexCountriesRef.current > 0) {
        queueMicrotask(() => setSelectedCountries(indexCountries));
      }
      prevIndexCountriesRef.current = indexCountries.length;
    }
  }, [indexCountries, countriesInitialized, selectedCountries.length]);

  const handleEventClick = useCallback(
    (event: AIEvent) => {
      // Update URL before navigating so state is preserved
      updateUrl();
      router.push(`/event/${event.id}`);
    },
    [router, updateUrl]
  );

  const handleApply = useCallback((overrides?: FilterUrlOverrides) => {
    updateUrl(overrides);
  }, [updateUrl]);

  return (
    <>
      <Header>
        <Button
          variant="ghost"
          size="icon"
          onClick={refresh}
          disabled={isLoading}
        >
          <RefreshCw
            className={`w-4 h-4 ${isLoading ? "animate-spin" : ""}`}
          />
        </Button>
      </Header>

      <main className="container mx-auto px-4 py-6 space-y-6">
        {/* Filter bar */}
        <FilterBar
          startDate={startDate}
          endDate={endDate}
          selectedTypes={selectedTypes}
          selectedTimeOfDay={selectedTimeOfDay}
          countries={effectiveCountries}
          selectedCountries={selectedCountries}
          searchCoordinates={searchCoordinates}
          searchRadius={searchRadius}
          roadTypes={indexRoadTypes}
          selectedRoadTypes={selectedRoadTypes}
          selectedVruLabels={selectedVruLabels}
          indexProgress={indexProgress}
          onStartDateChange={setStartDate}
          onEndDateChange={setEndDate}
          onTypesChange={setSelectedTypes}
          onTimeOfDayChange={setSelectedTimeOfDay}
          onCountriesChange={setSelectedCountries}
          onCoordinatesChange={setSearchCoordinates}
          onRadiusChange={setSearchRadius}
          onRoadTypesChange={setSelectedRoadTypes}
          onVruLabelsChange={setSelectedVruLabels}
          onApply={handleApply}
          onIndexingRequested={() => setEventIndexEnabled(true)}
        />

        {/* Error message */}
        {error && (
          <div className="flex items-center gap-2 p-4 bg-destructive/10 text-destructive rounded-lg">
            <AlertCircle className="w-5 h-5 shrink-0" />
            <span>{error}</span>
            {!hasApiKey && (
              <Link href="/settings" className="ml-auto text-sm font-medium underline hover:no-underline">
                Configure
              </Link>
            )}
          </div>
        )}

        {/* Results count */}
        {!error && filteredEvents.length > 0 && (
          <p className="flex items-center gap-2 text-sm text-muted-foreground">
            <span>
              Showing {filteredEvents.length.toLocaleString()} of {totalCount.toLocaleString()} events
            </span>
            {isRefreshing && (
              <span className="inline-flex items-center gap-1 text-xs">
                <RefreshCw className="h-3 w-3 animate-spin" />
                Updating
              </span>
            )}
          </p>
        )}

        {/* New events link */}
        <NewEventsBanner
          count={newEventsCount}
          isLoading={isLoadingNewEvents}
          onClick={showNewEvents}
        />

        <EventGrid
          events={filteredEvents}
          isLoading={(isLoading && filteredEvents.length === 0) || isLoadingMore}
          hasMore={hasMore}
          onLoadMore={loadMore}
          onEventClick={handleEventClick}
        />
      </main>
    </>
  );
}

function HomeContent() {
  return (
    <div className="min-h-screen bg-background">
      <GalleryView />
    </div>
  );
}

function HomeSkeleton() {
  return (
    <div className="min-h-screen bg-background">
      <div className="sticky top-0 z-50 w-full border-b bg-background/95 h-14" />
      <main className="container mx-auto px-4 py-6 space-y-6">
        <Skeleton className="h-10 w-32" />
        <div className="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-4">
          {Array.from({ length: 8 }).map((_, i) => (
            <Skeleton key={i} className="aspect-video rounded-lg" />
          ))}
        </div>
      </main>
    </div>
  );
}

export default function Home() {
  return (
    <Suspense fallback={<HomeSkeleton />}>
      <HomeContent />
    </Suspense>
  );
}

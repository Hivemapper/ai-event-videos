"use client";

import { useState, useCallback, useEffect, Suspense } from "react";
import { useRouter, useSearchParams } from "next/navigation";
import { RefreshCw, AlertCircle } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import {
  EventGrid,
  FilterBar,
  NewEventsBanner,
  SettingsDialog,
  AgentView,
} from "@/components/events";
import { Header } from "@/components/layout/header";
import { EventsMap } from "@/components/map/events-map";
import { Coordinates } from "@/components/events/filter-bar";
import { useEvents } from "@/hooks/use-events";
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

function HomeContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const defaultDates = getDefaultDates();

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
  const [searchCoordinates, setSearchCoordinates] = useState<Coordinates | null>(
    parseCoordinates(searchParams.get("coords"))
  );
  const [searchRadius, setSearchRadius] = useState(
    parseInt(searchParams.get("radius") || "500")
  );
  const [view, setView] = useState<"list" | "map">(
    (searchParams.get("view") as "list" | "map") || "list"
  );
  const agentOpen = searchParams.has("agent");

  // Update URL when filters change
  const updateUrl = useCallback(() => {
    const params = new URLSearchParams();
    if (startDate !== defaultDates.startDate) params.set("startDate", startDate);
    if (endDate !== defaultDates.endDate) params.set("endDate", endDate);
    if (selectedTypes.length > 0) params.set("types", selectedTypes.join(","));
    if (selectedTimeOfDay.length > 0) params.set("timeOfDay", selectedTimeOfDay.join(","));
    if (selectedCountries.length > 0) params.set("countries", selectedCountries.join(","));
    if (searchCoordinates) params.set("coords", `${searchCoordinates.lat},${searchCoordinates.lon}`);
    if (searchRadius !== 500) params.set("radius", searchRadius.toString());
    if (view !== "list") params.set("view", view);

    const queryString = params.toString();
    router.replace(queryString ? `?${queryString}` : "/", { scroll: false });
  }, [startDate, endDate, selectedTypes, selectedTimeOfDay, selectedCountries, searchCoordinates, searchRadius, view, defaultDates, router]);

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
  } = useEvents({
    startDate,
    endDate,
    types: selectedTypes,
    selectedTimeOfDay,
    selectedCountries,
    searchCoordinates,
    searchRadius,
  });

  // Initialize selectedCountries to all countries only when countries list first loads
  // Skip if countries were already set from URL params
  const [countriesInitialized, setCountriesInitialized] = useState(
    searchParams.get("countries") !== null
  );
  useEffect(() => {
    if (countries.length > 0 && !countriesInitialized) {
      setSelectedCountries(countries);
      setCountriesInitialized(true);
    }
  }, [countries, countriesInitialized]);

  const handleEventClick = useCallback(
    (event: AIEvent) => {
      // Update URL before navigating so state is preserved
      updateUrl();
      router.push(`/event/${event.id}`);
    },
    [router, updateUrl]
  );

  const handleApply = useCallback(() => {
    updateUrl();
    refresh();
  }, [updateUrl, refresh]);

  return (
    <div className="min-h-screen bg-background">
      <Header>
        {!agentOpen && (
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
        )}
        <SettingsDialog onApiKeyChange={refresh} filteredEvents={filteredEvents} />
      </Header>

      {agentOpen ? (
        <AgentView />
      ) : (
      <main className="container mx-auto px-4 py-6 space-y-6">
        {/* Filter bar */}
        <FilterBar
          startDate={startDate}
          endDate={endDate}
          selectedTypes={selectedTypes}
          selectedTimeOfDay={selectedTimeOfDay}
          countries={countries}
          selectedCountries={selectedCountries}
          searchCoordinates={searchCoordinates}
          searchRadius={searchRadius}
          view={view}
          onStartDateChange={setStartDate}
          onEndDateChange={setEndDate}
          onTypesChange={setSelectedTypes}
          onTimeOfDayChange={setSelectedTimeOfDay}
          onCountriesChange={setSelectedCountries}
          onCoordinatesChange={setSearchCoordinates}
          onRadiusChange={setSearchRadius}
          onViewChange={setView}
          onApply={handleApply}
        />

        {/* Error message */}
        {error && (
          <div className="flex items-center gap-2 p-4 bg-destructive/10 text-destructive rounded-lg">
            <AlertCircle className="w-5 h-5 shrink-0" />
            <span>{error}</span>
            {!hasApiKey && (
              <span className="ml-auto">
                <SettingsDialog onApiKeyChange={refresh} filteredEvents={filteredEvents} />
              </span>
            )}
          </div>
        )}

        {/* Results count */}
        {!error && !isLoading && filteredEvents.length > 0 && (
          <p className="text-sm text-muted-foreground">
            Showing {filteredEvents.length} of {totalCount} events
          </p>
        )}

        {/* New events link */}
        <NewEventsBanner count={newEventsCount} onClick={showNewEvents} />

        {/* Event grid or map */}
        {view === "list" ? (
          <EventGrid
            events={filteredEvents}
            isLoading={isLoading}
            hasMore={hasMore}
            onLoadMore={loadMore}
            onEventClick={handleEventClick}
          />
        ) : (
          <EventsMap
            events={filteredEvents}
            onEventClick={handleEventClick}
            className="h-[calc(100vh-200px)] rounded-xl"
          />
        )}
      </main>
      )}
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

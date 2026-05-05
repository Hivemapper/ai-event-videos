"use client";

import { useState, type FormEvent } from "react";
import {
  Calendar,
  Check,
  Filter,
  Globe,
  X,
  MapPin,
  SlidersHorizontal,
  Sun,
  Moon,
  Sunrise,
  Sunset,
  Route,
  Loader2,
  Search,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { AIEventType } from "@/types/events";
import { ALL_EVENT_TYPES, EVENT_TYPE_CONFIG } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { TimeOfDay, getTimeOfDayStyle } from "@/lib/sun";
import { EventIndexProgress } from "@/hooks/use-event-index";
import { getMapboxToken } from "@/lib/api";
import {
  VRU_OBJECT_FILTER_OPTIONS,
  getVruObjectFilterLabel,
} from "@/lib/vru-labels";

const TIME_OF_DAY_OPTIONS: { value: TimeOfDay; label: string; icon: typeof Sun }[] = [
  { value: "Day", label: "Day", icon: Sun },
  { value: "Dawn", label: "Dawn", icon: Sunrise },
  { value: "Dusk", label: "Dusk", icon: Sunset },
  { value: "Night", label: "Night", icon: Moon },
];

export interface Coordinates {
  lat: number;
  lon: number;
}

export const RADIUS_OPTIONS = [
  { value: 100, label: "100m" },
  { value: 250, label: "250m" },
  { value: 500, label: "500m" },
  { value: 1000, label: "1km" },
  { value: 2000, label: "2km" },
  { value: 5000, label: "5km" },
  { value: 10000, label: "10km" },
  { value: 25000, label: "25km" },
] as const;

export interface FilterUrlOverrides {
  startDate?: string;
  endDate?: string;
  selectedTypes?: AIEventType[];
  selectedTimeOfDay?: TimeOfDay[];
  selectedCountries?: string[];
  selectedRoadTypes?: string[];
  selectedVruLabels?: string[];
  searchCoordinates?: Coordinates | null;
  searchRadius?: number;
}

interface MapboxGeocodeFeature {
  bbox?: [number, number, number, number];
  center?: [number, number];
  place_name?: string;
  place_type?: string[];
}

interface MapboxGeocodeResponse {
  features?: MapboxGeocodeFeature[];
}

const COORDINATE_PATTERN = /^\s*\(?\s*(-?\d+(?:\.\d+)?)\s*,\s*(-?\d+(?:\.\d+)?)\s*\)?\s*$/;
const EARTH_RADIUS_METERS = 6371000;

function parseCoordinateInput(value: string): Coordinates | null {
  const match = value.match(COORDINATE_PATTERN);
  if (!match) return null;

  const lat = Number(match[1]);
  const lon = Number(match[2]);
  if (!Number.isFinite(lat) || !Number.isFinite(lon)) return null;
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;
  return { lat, lon };
}

function formatCoordinatesForInput(coords: Coordinates): string {
  return `${Number(coords.lat.toFixed(5))},${Number(coords.lon.toFixed(5))}`;
}

function distanceMeters(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const toRadians = (degrees: number) => (degrees * Math.PI) / 180;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLon / 2) ** 2;
  return EARTH_RADIUS_METERS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function nearestRadiusOption(radiusMeters: number): number {
  return RADIUS_OPTIONS.reduce((best, option) =>
    Math.abs(option.value - radiusMeters) < Math.abs(best.value - radiusMeters)
      ? option
      : best
  ).value;
}

function estimateRadiusFromFeature(feature: MapboxGeocodeFeature): number {
  if (feature.bbox && feature.bbox.length === 4) {
    const [west, south, east, north] = feature.bbox;
    const centerLat = feature.center?.[1] ?? (south + north) / 2;
    const centerLon = feature.center?.[0] ?? (west + east) / 2;
    const radius = Math.max(
      distanceMeters(centerLat, centerLon, south, west),
      distanceMeters(centerLat, centerLon, south, east),
      distanceMeters(centerLat, centerLon, north, west),
      distanceMeters(centerLat, centerLon, north, east)
    );
    return nearestRadiusOption(radius);
  }

  const types = feature.place_type ?? [];
  if (types.includes("country") || types.includes("region")) return 25000;
  if (types.includes("district") || types.includes("place")) return 10000;
  if (types.includes("locality") || types.includes("neighborhood")) return 2000;
  return 500;
}

interface FilterBarProps {
  startDate: string;
  endDate: string;
  selectedTypes: AIEventType[];
  selectedTimeOfDay: TimeOfDay[];
  countries: string[];
  selectedCountries: string[];
  searchCoordinates: Coordinates | null;
  searchRadius: number;
  roadTypes?: string[];
  selectedRoadTypes?: string[];
  selectedVruLabels?: string[];
  indexProgress?: EventIndexProgress;
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onTypesChange: (types: AIEventType[]) => void;
  onTimeOfDayChange: (times: TimeOfDay[]) => void;
  onCountriesChange: (countries: string[]) => void;
  onCoordinatesChange: (coords: Coordinates | null) => void;
  onRadiusChange: (radius: number) => void;
  onRoadTypesChange?: (types: string[]) => void;
  onVruLabelsChange?: (labels: string[]) => void;
  onApply?: (filters?: FilterUrlOverrides) => void;
  onIndexingRequested?: () => void;
}

export function FilterBar({
  startDate,
  endDate,
  selectedTypes,
  selectedTimeOfDay,
  countries,
  selectedCountries,
  searchCoordinates,
  searchRadius,
  roadTypes = [],
  selectedRoadTypes = [],
  selectedVruLabels = [],
  indexProgress,
  onStartDateChange,
  onEndDateChange,
  onTypesChange,
  onTimeOfDayChange,
  onCountriesChange,
  onCoordinatesChange,
  onRadiusChange,
  onRoadTypesChange,
  onVruLabelsChange,
  onApply,
  onIndexingRequested,
}: FilterBarProps) {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [showAllCountries, setShowAllCountries] = useState(false);
  const [locationSearch, setLocationSearch] = useState(
    searchCoordinates ? formatCoordinatesForInput(searchCoordinates) : ""
  );
  const [locationError, setLocationError] = useState<string | null>(null);
  const [isLocationSearching, setIsLocationSearching] = useState(false);

  // Draft state for modal - only applied when user clicks "Apply Filters"
  const [draftStartDate, setDraftStartDate] = useState(startDate);
  const [draftEndDate, setDraftEndDate] = useState(endDate);
  const [draftTypes, setDraftTypes] = useState<AIEventType[]>(selectedTypes);
  const [draftTimeOfDay, setDraftTimeOfDay] = useState<TimeOfDay[]>(selectedTimeOfDay);
  const [draftCountries, setDraftCountries] = useState<string[]>(selectedCountries);
  const [draftRoadTypes, setDraftRoadTypes] = useState<string[]>(selectedRoadTypes);
  const [draftVruLabels, setDraftVruLabels] = useState<string[]>(selectedVruLabels);

  const handleAdvancedOpenChange = (open: boolean) => {
    if (open) {
      onIndexingRequested?.();
      setDraftStartDate(startDate);
      setDraftEndDate(endDate);
      setDraftTypes([...selectedTypes]);
      setDraftTimeOfDay([...selectedTimeOfDay]);
      setDraftCountries([...selectedCountries]);
      setDraftRoadTypes([...selectedRoadTypes]);
      setDraftVruLabels([...selectedVruLabels]);
    }
    setAdvancedOpen(open);
  };

  const handleTypeToggle = (type: AIEventType) => {
    if (draftTypes.includes(type)) {
      setDraftTypes(draftTypes.filter((t) => t !== type));
    } else {
      setDraftTypes([...draftTypes, type]);
    }
  };

  const handleRemoveType = (type: AIEventType) => {
    const nextTypes = selectedTypes.filter((t) => t !== type);
    onTypesChange(nextTypes);
    onApply?.({ selectedTypes: nextTypes });
  };

  const handleClearAllTypes = () => {
    setDraftTypes([]);
  };

  const handleTimeOfDayToggle = (time: TimeOfDay) => {
    if (draftTimeOfDay.includes(time)) {
      setDraftTimeOfDay(draftTimeOfDay.filter((t) => t !== time));
    } else {
      setDraftTimeOfDay([...draftTimeOfDay, time]);
    }
  };

  const handleRemoveTimeOfDay = (time: TimeOfDay) => {
    const nextTimeOfDay = selectedTimeOfDay.filter((t) => t !== time);
    onTimeOfDayChange(nextTimeOfDay);
    onApply?.({ selectedTimeOfDay: nextTimeOfDay });
  };

  const handleClearAllTimeOfDay = () => {
    setDraftTimeOfDay([]);
  };

  const handleCountryToggle = (country: string) => {
    if (draftCountries.includes(country)) {
      setDraftCountries(draftCountries.filter((c) => c !== country));
    } else {
      setDraftCountries([...draftCountries, country]);
    }
  };

  const handleSelectAllCountries = () => {
    setDraftCountries([...countries]);
  };

  const handleClearAllCountries = () => {
    setDraftCountries([]);
  };

  const handleRoadTypeToggle = (type: string) => {
    if (draftRoadTypes.includes(type)) {
      setDraftRoadTypes(draftRoadTypes.filter((t) => t !== type));
    } else {
      setDraftRoadTypes([...draftRoadTypes, type]);
    }
  };

  const handleClearAllRoadTypes = () => {
    setDraftRoadTypes([]);
  };

  const handleVruLabelToggle = (label: string) => {
    if (draftVruLabels.includes(label)) {
      setDraftVruLabels(draftVruLabels.filter((item) => item !== label));
    } else {
      setDraftVruLabels([...draftVruLabels, label]);
    }
  };

  const handleRemoveVruLabel = (label: string) => {
    const nextLabels = selectedVruLabels.filter((item) => item !== label);
    onVruLabelsChange?.(nextLabels);
    onApply?.({ selectedVruLabels: nextLabels });
  };

  const handleClearAllVruLabels = () => {
    setDraftVruLabels([]);
  };

  const handleApplyFilters = () => {
    onStartDateChange(draftStartDate);
    onEndDateChange(draftEndDate);
    onTypesChange(draftTypes);
    onTimeOfDayChange(draftTimeOfDay);
    onCountriesChange(draftCountries);
    onRoadTypesChange?.(draftRoadTypes);
    onVruLabelsChange?.(draftVruLabels);
    onApply?.({
      startDate: draftStartDate,
      endDate: draftEndDate,
      selectedTypes: draftTypes,
      selectedTimeOfDay: draftTimeOfDay,
      selectedCountries: draftCountries,
      selectedRoadTypes: draftRoadTypes,
      selectedVruLabels: draftVruLabels,
    });
    setAdvancedOpen(false);
  };

  const handleClearAllFilters = () => {
    onTypesChange([]);
    onTimeOfDayChange([]);
    onCountriesChange([...countries]);
    onRoadTypesChange?.([]);
    onVruLabelsChange?.([]);
    onCoordinatesChange(null);
    setLocationSearch("");
    setLocationError(null);
    onApply?.({
      selectedTypes: [],
      selectedTimeOfDay: [],
      selectedCountries: [...countries],
      selectedRoadTypes: [],
      selectedVruLabels: [],
      searchCoordinates: null,
    });
  };

  const handleLocationSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    const query = locationSearch.trim();
    setLocationError(null);

    if (!query) {
      onCoordinatesChange(null);
      onApply?.({ searchCoordinates: null });
      return;
    }

    const parsedCoordinates = parseCoordinateInput(query);
    if (parsedCoordinates) {
      onCoordinatesChange(parsedCoordinates);
      onRadiusChange(searchRadius);
      onApply?.({
        searchCoordinates: parsedCoordinates,
        searchRadius,
      });
      return;
    }

    const token = getMapboxToken();
    if (!token) {
      setLocationError("Place search needs a Mapbox token in Settings.");
      return;
    }

    setIsLocationSearching(true);
    try {
      const response = await fetch(
        `https://api.mapbox.com/geocoding/v5/mapbox.places/${encodeURIComponent(query)}.json?` +
          new URLSearchParams({
            access_token: token,
            autocomplete: "false",
            limit: "1",
            types: "address,poi,neighborhood,locality,place,district,region,country",
          }).toString()
      );
      if (!response.ok) {
        throw new Error(`Mapbox geocoding failed: ${response.status}`);
      }

      const data = (await response.json()) as MapboxGeocodeResponse;
      const feature = data.features?.[0];
      if (!feature?.center || feature.center.length < 2) {
        throw new Error("No matching place found.");
      }

      const nextCoordinates = {
        lat: feature.center[1],
        lon: feature.center[0],
      };
      const nextRadius = estimateRadiusFromFeature(feature);
      onCoordinatesChange(nextCoordinates);
      onRadiusChange(nextRadius);
      setLocationSearch(feature.place_name ?? query);
      onApply?.({
        searchCoordinates: nextCoordinates,
        searchRadius: nextRadius,
      });
    } catch (error) {
      setLocationError(error instanceof Error ? error.message : "Unable to search that place.");
    } finally {
      setIsLocationSearching(false);
    }
  };

  const handleClearAppliedCoordinates = () => {
    onCoordinatesChange(null);
    setLocationSearch("");
    setLocationError(null);
    onApply?.({ searchCoordinates: null });
  };

  const allCountriesSelected =
    countries.length > 0 && selectedCountries.length === countries.length;

  const draftAllCountriesSelected =
    countries.length > 0 && draftCountries.length === countries.length;

  const isIndexing = indexProgress && indexProgress.phase !== "idle" && indexProgress.phase !== "complete";

  const activeFilterCount =
    (searchCoordinates ? 1 : 0) +
    (selectedTypes.length > 0 ? 1 : 0) +
    (selectedTimeOfDay.length > 0 ? 1 : 0) +
    (!allCountriesSelected && selectedCountries.length > 0 ? 1 : 0) +
    (selectedRoadTypes.length > 0 ? 1 : 0) +
    (selectedVruLabels.length > 0 ? 1 : 0);

  return (
    <div className="space-y-4">
      {/* Filter controls */}
      <div className="flex flex-col gap-3 lg:flex-row lg:items-center">
        <form
          onSubmit={handleLocationSubmit}
          className="flex min-w-0 flex-1 flex-col gap-2 sm:flex-row sm:items-center"
        >
          <div className="relative min-w-0 flex-1">
            <Search className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <Input
              type="search"
              value={locationSearch}
              onChange={(event) => {
                setLocationSearch(event.target.value);
                setLocationError(null);
              }}
              placeholder="Menlo Park, CA or 37.45,-122.18"
              className="h-10 pl-9"
            />
          </div>
          <Select
            value={searchRadius.toString()}
            onValueChange={(value) => {
              const nextRadius = parseInt(value, 10);
              onRadiusChange(nextRadius);
              if (searchCoordinates) {
                onApply?.({ searchRadius: nextRadius });
              }
            }}
          >
            <SelectTrigger className="h-10 w-full sm:w-28">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {RADIUS_OPTIONS.map((option) => (
                <SelectItem
                  key={option.value}
                  value={option.value.toString()}
                >
                  {option.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <Button
            type="submit"
            className="h-10 gap-2 sm:w-auto"
            disabled={isLocationSearching}
          >
            {isLocationSearching ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Search className="w-4 h-4" />
            )}
            Search
          </Button>
        </form>

        <div className="flex flex-wrap items-center gap-2">
          {/* Filters button */}
          <Dialog
            open={advancedOpen}
            onOpenChange={handleAdvancedOpenChange}
          >
            <DialogTrigger asChild>
              <Button
                variant="outline"
                className={cn(
                  "h-10 gap-2",
                  activeFilterCount > 0 && "border-primary text-primary"
                )}
              >
                <SlidersHorizontal className="w-4 h-4" />
                Filters
                {activeFilterCount > 0 && (
                  <Badge variant="secondary" className="ml-1 px-1.5 py-0 text-xs">
                    {activeFilterCount}
                  </Badge>
                )}
              </Button>
            </DialogTrigger>
            <DialogContent className="sm:max-w-6xl">
              <DialogHeader>
                <DialogTitle className="flex items-center gap-2">
                  Filters
                  {isIndexing && (
                    <span className="flex items-center gap-1.5 text-xs font-normal text-muted-foreground">
                      <Loader2 className="w-3 h-3 animate-spin" />
                      {indexProgress.phase === "loading-countries" && "Loading country data..."}
                      {indexProgress.phase === "discovering" && (
                        <>Indexing events ({indexProgress.eventsDiscovered.toLocaleString()}/{indexProgress.totalEvents.toLocaleString()})</>
                      )}
                      {indexProgress.phase === "road-types" && (
                        <>Road types ({indexProgress.roadTypesResolved}/{indexProgress.roadTypesTotal})</>
                      )}
                    </span>
                  )}
                </DialogTitle>
              </DialogHeader>

            <div className="grid grid-cols-1 gap-6 py-4 sm:grid-cols-2 lg:grid-cols-[1.3fr_1fr_1fr_1fr_1fr_1fr]">
              {/* Column 1: Date Range */}
              <div className="space-y-6">
                {/* Date range */}
                <div className="space-y-3">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Calendar className="w-4 h-4" />
                    Date Range
                  </label>
                  <div className="flex flex-wrap gap-1.5">
                    {[
                      { label: "24h", days: 1 },
                      { label: "7d", days: 7 },
                      { label: "30d", days: 30 },
                      { label: "60d", days: 60 },
                      { label: "90d", days: 90 },
                    ].map(({ label, days }) => {
                      const presetStart = new Date();
                      presetStart.setDate(presetStart.getDate() - days);
                      const presetStartStr = presetStart.toISOString().split("T")[0];
                      const todayStr = new Date().toISOString().split("T")[0];
                      const isActive = draftStartDate === presetStartStr && draftEndDate === todayStr;
                      return (
                        <Button
                          key={days}
                          type="button"
                          variant={isActive ? "secondary" : "ghost"}
                          size="sm"
                          className="h-7 px-2.5 text-xs"
                          onClick={() => {
                            setDraftStartDate(presetStartStr);
                            setDraftEndDate(todayStr);
                          }}
                        >
                          {label}
                        </Button>
                      );
                    })}
                  </div>
                  <div className="space-y-2">
                    <Input
                      type="date"
                      value={draftStartDate}
                      onChange={(e) => setDraftStartDate(e.target.value)}
                      className="w-full"
                    />
                    <Input
                      type="date"
                      value={draftEndDate}
                      onChange={(e) => setDraftEndDate(e.target.value)}
                      className="w-full"
                    />
                  </div>
                </div>

              </div>

              {/* Column 2: Event Types */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Filter className="w-4 h-4" />
                    Event Types
                  </label>
                  {draftTypes.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearAllTypes}
                      className="text-xs h-6 px-2"
                    >
                      Clear
                    </Button>
                  )}
                </div>
                <div className="space-y-1">
                  {ALL_EVENT_TYPES.map((type) => {
                    const config = EVENT_TYPE_CONFIG[type];
                    const isSelected = draftTypes.includes(type);
                    const Icon = config.icon;

                    return (
                      <button
                        key={type}
                        type="button"
                        onClick={() => handleTypeToggle(type)}
                        className={cn(
                          "w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm transition-colors text-left",
                          "hover:bg-accent",
                          isSelected && "bg-accent/50"
                        )}
                      >
                        <Icon className={cn("w-4 h-4 shrink-0", config.color)} />
                        <span className="text-xs flex-1">{config.label}</span>
                        {isSelected && <Check className="w-3 h-3 shrink-0 text-primary" />}
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Column 3: Time of Day */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Sun className="w-4 h-4" />
                    Time of Day
                  </label>
                  {draftTimeOfDay.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearAllTimeOfDay}
                      className="text-xs h-6 px-2"
                    >
                      Clear
                    </Button>
                  )}
                </div>
                <div className="space-y-1">
                  {TIME_OF_DAY_OPTIONS.map((option) => {
                    const isSelected = draftTimeOfDay.includes(option.value);
                    const Icon = option.icon;
                    const style = getTimeOfDayStyle(option.value);

                    return (
                      <button
                        key={option.value}
                        type="button"
                        onClick={() => handleTimeOfDayToggle(option.value)}
                        className={cn(
                          "w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm transition-colors text-left",
                          "hover:bg-accent",
                          isSelected && "bg-accent/50"
                        )}
                      >
                        <Icon className={cn("w-4 h-4 shrink-0", style.color)} />
                        <span className="text-xs flex-1">{option.label}</span>
                        {isSelected && <Check className="w-3 h-3 shrink-0 text-primary" />}
                      </button>
                    );
                  })}
                </div>
              </div>

              {/* Column 4: Country Filter */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Globe className="w-4 h-4" />
                    Country
                  </label>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => {
                      if (draftAllCountriesSelected) {
                        handleClearAllCountries();
                      } else {
                        handleSelectAllCountries();
                      }
                    }}
                    className="text-xs h-6 px-2"
                  >
                    {draftAllCountriesSelected ? "Clear" : "All"}
                  </Button>
                </div>

                {countries.length > 0 ? (
                  <div className="space-y-1 max-h-[240px] overflow-y-auto">
                    {(showAllCountries ? countries : countries.slice(0, 8)).map(
                      (country) => {
                        const isSelected = draftCountries.includes(country);
                        return (
                          <button
                            key={country}
                            type="button"
                            onClick={() => handleCountryToggle(country)}
                            className={cn(
                              "w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm transition-colors text-left",
                              "hover:bg-accent",
                              isSelected && "bg-accent/50"
                            )}
                          >
                            <span className="text-xs flex-1">{country}</span>
                            {isSelected && <Check className="w-3 h-3 shrink-0 text-primary" />}
                          </button>
                        );
                      }
                    )}

                    {countries.length > 8 && (
                      <Button
                        variant="ghost"
                        size="sm"
                        onClick={() => setShowAllCountries(!showAllCountries)}
                        className="w-full mt-1 text-xs text-muted-foreground h-7"
                      >
                        {showAllCountries
                          ? "Show less"
                          : `+${countries.length - 8} more`}
                      </Button>
                    )}
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground py-2">
                    {isIndexing ? "Discovering..." : "No countries available"}
                  </p>
                )}
              </div>

              {/* Column 5: Road Type Filter */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Route className="w-4 h-4" />
                    Road Type
                  </label>
                  {draftRoadTypes.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearAllRoadTypes}
                      className="text-xs h-6 px-2"
                    >
                      Clear
                    </Button>
                  )}
                </div>

                {roadTypes.length > 0 ? (
                  <div className="space-y-1">
                    {roadTypes.map((type) => {
                      const isSelected = draftRoadTypes.includes(type);
                      return (
                        <button
                          key={type}
                          type="button"
                          onClick={() => handleRoadTypeToggle(type)}
                          className={cn(
                            "w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm transition-colors text-left",
                            "hover:bg-accent",
                            isSelected && "bg-accent/50"
                          )}
                        >
                          <span className="text-xs flex-1">{type}</span>
                          {isSelected && <Check className="w-3 h-3 shrink-0 text-primary" />}
                        </button>
                      );
                    })}
                  </div>
                ) : (
                  <p className="text-xs text-muted-foreground py-2">
                    {indexProgress?.phase === "road-types"
                      ? "Resolving road types..."
                      : indexProgress?.phase === "discovering" || indexProgress?.phase === "loading-countries"
                        ? "Pending..."
                        : "No road types available"}
                  </p>
                )}
              </div>

              {/* Column 6: VRU/Object Filter */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Search className="w-4 h-4" />
                    VRU / Object
                  </label>
                  {draftVruLabels.length > 0 && (
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={handleClearAllVruLabels}
                      className="text-xs h-6 px-2"
                    >
                      Clear
                    </Button>
                  )}
                </div>

                <div className="space-y-1 max-h-[240px] overflow-y-auto">
                  {VRU_OBJECT_FILTER_OPTIONS.map((option) => {
                    const isSelected = draftVruLabels.includes(option.value);
                    return (
                      <button
                        key={option.value}
                        type="button"
                        onClick={() => handleVruLabelToggle(option.value)}
                        className={cn(
                          "w-full flex items-center gap-2 px-2 py-1.5 rounded-md text-sm transition-colors text-left",
                          "hover:bg-accent",
                          isSelected && "bg-accent/50"
                        )}
                      >
                        <span className="text-xs flex-1">{option.label}</span>
                        {isSelected && <Check className="w-3 h-3 shrink-0 text-primary" />}
                      </button>
                    );
                  })}
                </div>
              </div>
            </div>

            <DialogFooter>
              <Button
                onClick={handleApplyFilters}
                className="w-full sm:w-auto"
              >
                Apply Filters
              </Button>
            </DialogFooter>
          </DialogContent>
        </Dialog>

        {/* Clear all button */}
        {activeFilterCount > 0 && (
          <Button
            variant="ghost"
            size="sm"
            onClick={handleClearAllFilters}
            className="text-muted-foreground"
          >
            Clear filters
          </Button>
        )}

        </div>
      </div>

      {locationError && (
        <p className="text-sm text-destructive">{locationError}</p>
      )}

      {/* Selected filter chips */}
      {(searchCoordinates || selectedTypes.length > 0 || selectedTimeOfDay.length > 0 || selectedRoadTypes.length > 0 || selectedVruLabels.length > 0) && (
        <div className="flex flex-wrap gap-2">
          {searchCoordinates && (
            <Badge
              variant="secondary"
              className="pl-2 pr-1 py-1 flex items-center gap-1 border"
            >
              <MapPin className="w-3 h-3" />
              <span>{formatCoordinatesForInput(searchCoordinates)}</span>
              <button
                type="button"
                onClick={handleClearAppliedCoordinates}
                className="ml-1 p-0.5 rounded-full hover:bg-black/10"
              >
                <X className="w-3 h-3" />
              </button>
            </Badge>
          )}
          {selectedTypes.map((type) => {
            const config = EVENT_TYPE_CONFIG[type];
            const Icon = config.icon;

            return (
              <Badge
                key={type}
                variant="secondary"
                className={cn(
                  "pl-2 pr-1 py-1 flex items-center gap-1",
                  config.bgColor,
                  config.color,
                  config.borderColor,
                  "border"
                )}
              >
                <Icon className="w-3 h-3" />
                <span>{config.label}</span>
                <button
                  type="button"
                  onClick={() => handleRemoveType(type)}
                  className="ml-1 p-0.5 rounded-full hover:bg-black/10"
                >
                  <X className="w-3 h-3" />
                </button>
              </Badge>
            );
          })}
          {selectedTimeOfDay.map((time) => {
            const option = TIME_OF_DAY_OPTIONS.find((o) => o.value === time)!;
            const style = getTimeOfDayStyle(time);
            const Icon = option.icon;

            return (
              <Badge
                key={time}
                variant="secondary"
                className={cn(
                  "pl-2 pr-1 py-1 flex items-center gap-1",
                  style.bgColor,
                  style.color,
                  "border"
                )}
              >
                <Icon className="w-3 h-3" />
                <span>{option.label}</span>
                <button
                  type="button"
                  onClick={() => handleRemoveTimeOfDay(time)}
                  className="ml-1 p-0.5 rounded-full hover:bg-black/10"
                >
                  <X className="w-3 h-3" />
                </button>
              </Badge>
            );
          })}
          {selectedRoadTypes.map((type) => (
            <Badge
              key={type}
              variant="secondary"
              className="pl-2 pr-1 py-1 flex items-center gap-1 border"
            >
              <Route className="w-3 h-3" />
              <span>{type}</span>
              <button
                type="button"
                onClick={() => {
                  const nextRoadTypes = selectedRoadTypes.filter((t) => t !== type);
                  onRoadTypesChange?.(nextRoadTypes);
                  onApply?.({ selectedRoadTypes: nextRoadTypes });
                }}
                className="ml-1 p-0.5 rounded-full hover:bg-black/10"
              >
                <X className="w-3 h-3" />
              </button>
            </Badge>
          ))}
          {selectedVruLabels.map((label) => (
            <Badge
              key={label}
              variant="secondary"
              className="pl-2 pr-1 py-1 flex items-center gap-1 border"
            >
              <Search className="w-3 h-3" />
              <span>{getVruObjectFilterLabel(label)}</span>
              <button
                type="button"
                onClick={() => handleRemoveVruLabel(label)}
                className="ml-1 p-0.5 rounded-full hover:bg-black/10"
              >
                <X className="w-3 h-3" />
              </button>
            </Badge>
          ))}
        </div>
      )}
    </div>
  );
}

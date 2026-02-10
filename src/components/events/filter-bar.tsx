"use client";

import { useState, useEffect } from "react";
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
  LayoutGrid,
  Map,
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
] as const;

interface FilterBarProps {
  startDate: string;
  endDate: string;
  selectedTypes: AIEventType[];
  selectedTimeOfDay: TimeOfDay[];
  countries: string[];
  selectedCountries: string[];
  searchCoordinates: Coordinates | null;
  searchRadius: number;
  view: "list" | "map";
  onStartDateChange: (date: string) => void;
  onEndDateChange: (date: string) => void;
  onTypesChange: (types: AIEventType[]) => void;
  onTimeOfDayChange: (times: TimeOfDay[]) => void;
  onCountriesChange: (countries: string[]) => void;
  onCoordinatesChange: (coords: Coordinates | null) => void;
  onRadiusChange: (radius: number) => void;
  onViewChange: (view: "list" | "map") => void;
  onApply?: () => void;
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
  view,
  onStartDateChange,
  onEndDateChange,
  onTypesChange,
  onTimeOfDayChange,
  onCountriesChange,
  onCoordinatesChange,
  onRadiusChange,
  onViewChange,
  onApply,
}: FilterBarProps) {
  const [advancedOpen, setAdvancedOpen] = useState(false);
  const [showAllCountries, setShowAllCountries] = useState(false);

  // Draft state for modal - only applied when user clicks "Apply Filters"
  const [draftStartDate, setDraftStartDate] = useState(startDate);
  const [draftEndDate, setDraftEndDate] = useState(endDate);
  const [draftTypes, setDraftTypes] = useState<AIEventType[]>(selectedTypes);
  const [draftTimeOfDay, setDraftTimeOfDay] = useState<TimeOfDay[]>(selectedTimeOfDay);
  const [draftCountries, setDraftCountries] = useState<string[]>(selectedCountries);
  const [draftCoordinates, setDraftCoordinates] = useState<Coordinates | null>(searchCoordinates);
  const [draftRadius, setDraftRadius] = useState(searchRadius);
  const [draftCoordsInput, setDraftCoordsInput] = useState("");

  // Reset draft state when modal opens
  useEffect(() => {
    if (advancedOpen) {
      setDraftStartDate(startDate);
      setDraftEndDate(endDate);
      setDraftTypes([...selectedTypes]);
      setDraftTimeOfDay([...selectedTimeOfDay]);
      setDraftCountries([...selectedCountries]);
      setDraftCoordinates(searchCoordinates);
      setDraftRadius(searchRadius);
      setDraftCoordsInput(
        searchCoordinates ? `${searchCoordinates.lat},${searchCoordinates.lon}` : ""
      );
    }
  }, [advancedOpen, startDate, endDate, selectedTypes, selectedTimeOfDay, selectedCountries, searchCoordinates, searchRadius]);

  const handleTypeToggle = (type: AIEventType) => {
    if (draftTypes.includes(type)) {
      setDraftTypes(draftTypes.filter((t) => t !== type));
    } else {
      setDraftTypes([...draftTypes, type]);
    }
  };

  const handleRemoveType = (type: AIEventType) => {
    onTypesChange(selectedTypes.filter((t) => t !== type));
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
    onTimeOfDayChange(selectedTimeOfDay.filter((t) => t !== time));
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

  const handleCoordsInputChange = (value: string) => {
    setDraftCoordsInput(value);

    // Parse coordinates in format "lat,lon"
    const match = value.match(/^\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*$/);
    if (match) {
      const lat = parseFloat(match[1]);
      const lon = parseFloat(match[2]);
      if (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) {
        setDraftCoordinates({ lat, lon });
        return;
      }
    }

    // Clear coordinates if input is empty or invalid
    if (value.trim() === "") {
      setDraftCoordinates(null);
    }
  };

  const handleClearCoordinates = () => {
    setDraftCoordsInput("");
    setDraftCoordinates(null);
  };

  const handleApplyFilters = () => {
    onStartDateChange(draftStartDate);
    onEndDateChange(draftEndDate);
    onTypesChange(draftTypes);
    onTimeOfDayChange(draftTimeOfDay);
    onCountriesChange(draftCountries);
    onCoordinatesChange(draftCoordinates);
    onRadiusChange(draftRadius);
    onApply?.();
    setAdvancedOpen(false);
  };

  const handleClearAllFilters = () => {
    onTypesChange([]);
    onTimeOfDayChange([]);
    onCountriesChange([...countries]);
    onCoordinatesChange(null);
  };

  const allCountriesSelected =
    countries.length > 0 && selectedCountries.length === countries.length;

  const draftAllCountriesSelected =
    countries.length > 0 && draftCountries.length === countries.length;

  return (
    <div className="space-y-4">
      {/* Filter controls */}
      <div className="flex flex-wrap items-center gap-4">
        {/* Filters button */}
        <Dialog open={advancedOpen} onOpenChange={setAdvancedOpen}>
          <DialogTrigger asChild>
            <Button
              variant="outline"
              size="sm"
              className={cn(
                "gap-2",
                (searchCoordinates ||
                  selectedTypes.length > 0 ||
                  selectedTimeOfDay.length > 0 ||
                  (!allCountriesSelected && selectedCountries.length > 0)) &&
                  "border-primary text-primary"
              )}
            >
              <SlidersHorizontal className="w-4 h-4" />
              Filters
              {(searchCoordinates ||
                selectedTypes.length > 0 ||
                selectedTimeOfDay.length > 0 ||
                (!allCountriesSelected && selectedCountries.length > 0)) && (
                <Badge variant="secondary" className="ml-1 px-1.5 py-0 text-xs">
                  {(searchCoordinates ? 1 : 0) +
                    (selectedTypes.length > 0 ? 1 : 0) +
                    (selectedTimeOfDay.length > 0 ? 1 : 0) +
                    (!allCountriesSelected && selectedCountries.length > 0
                      ? 1
                      : 0)}
                </Badge>
              )}
            </Button>
          </DialogTrigger>
          <DialogContent className="sm:max-w-4xl">
            <DialogHeader>
              <DialogTitle>Filters</DialogTitle>
            </DialogHeader>

            <div className="grid grid-cols-2 md:grid-cols-[1.5fr_1fr_1fr_1fr] gap-8 py-4">
              {/* Column 1: Date Range + Coordinate Search */}
              <div className="space-y-6">
                {/* Date range */}
                <div className="space-y-3">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Calendar className="w-4 h-4" />
                    Date Range
                  </label>
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

                {/* Coordinate search */}
                <div className="space-y-3">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <MapPin className="w-4 h-4" />
                    Location
                  </label>
                  <div className="space-y-2">
                    <div className="relative">
                      <Input
                        type="text"
                        placeholder="lat,lon"
                        value={draftCoordsInput}
                        onChange={(e) => handleCoordsInputChange(e.target.value)}
                        className={cn(
                          draftCoordinates && "pr-8 border-primary"
                        )}
                      />
                      {draftCoordinates && (
                        <button
                          type="button"
                          onClick={handleClearCoordinates}
                          className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5 rounded-full hover:bg-muted"
                        >
                          <X className="w-4 h-4 text-muted-foreground" />
                        </button>
                      )}
                    </div>
                    <Select
                      value={draftRadius.toString()}
                      onValueChange={(value) => setDraftRadius(parseInt(value))}
                    >
                      <SelectTrigger className="w-full">
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

              {/* Column 4: Region Filter */}
              <div className="space-y-3">
                <div className="flex items-center justify-between">
                  <label className="text-sm font-medium flex items-center gap-2 text-muted-foreground uppercase tracking-wide">
                    <Globe className="w-4 h-4" />
                    Region
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
                    No regions available
                  </p>
                )}
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
        {(selectedTypes.length > 0 ||
          selectedTimeOfDay.length > 0 ||
          (countries.length > 0 && !allCountriesSelected) ||
          searchCoordinates) && (
          <Button
            variant="ghost"
            size="sm"
            onClick={handleClearAllFilters}
            className="text-muted-foreground"
          >
            Clear filters
          </Button>
        )}

        {/* View toggle */}
        <div className="flex gap-1 ml-auto">
          <Button
            variant={view === "list" ? "secondary" : "ghost"}
            size="icon"
            className="h-8 w-8"
            onClick={() => onViewChange("list")}
          >
            <LayoutGrid className="w-4 h-4" />
          </Button>
          <Button
            variant={view === "map" ? "secondary" : "ghost"}
            size="icon"
            className="h-8 w-8"
            onClick={() => onViewChange("map")}
          >
            <Map className="w-4 h-4" />
          </Button>
        </div>
      </div>

      {/* Selected filter chips */}
      {(selectedTypes.length > 0 || selectedTimeOfDay.length > 0) && (
        <div className="flex flex-wrap gap-2">
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
        </div>
      )}
    </div>
  );
}

"use client";

import { useState, useCallback } from "react";
import { SlidersHorizontal, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { AnalysisFilters } from "@/lib/analysis-store";
import { cn } from "@/lib/utils";

const WEATHER_OPTIONS = ["clear", "cloudy", "rainy", "foggy", "snowy", "overcast"];
const LIGHTING_OPTIONS = ["daylight", "dawn", "dusk", "night_lit", "night_dark"];
const VISIBILITY_OPTIONS = ["excellent", "good", "moderate", "poor"];
const SETTING_OPTIONS = ["urban", "suburban", "rural", "highway", "industrial", "parking"];
const SEVERITY_OPTIONS = ["none", "low", "moderate", "high", "critical"];
const SURFACE_OPTIONS = ["paved", "gravel", "dirt", "wet", "snow_covered"];
const ASSESSMENT_OPTIONS = ["normal", "cautious", "aggressive", "erratic", "emergency"];

function MultiSelect({
  label,
  options,
  selected,
  onChange,
}: {
  label: string;
  options: string[];
  selected: string[];
  onChange: (value: string[]) => void;
}) {
  const toggle = (option: string) => {
    if (selected.includes(option)) {
      onChange(selected.filter((s) => s !== option));
    } else {
      onChange([...selected, option]);
    }
  };

  return (
    <div className="space-y-1.5">
      <span className="text-xs font-medium text-muted-foreground">{label}</span>
      <div className="flex flex-wrap gap-1">
        {options.map((option) => (
          <button
            key={option}
            onClick={() => toggle(option)}
            className={cn(
              "px-2 py-0.5 rounded-full text-xs border transition-colors capitalize",
              selected.includes(option)
                ? "bg-primary text-primary-foreground border-primary"
                : "bg-background text-muted-foreground border-border hover:border-primary/50"
            )}
          >
            {option.replace("_", " ")}
          </button>
        ))}
      </div>
    </div>
  );
}

function ToggleSwitch({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (value: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 cursor-pointer">
      <div
        className={cn(
          "w-8 h-4 rounded-full transition-colors relative",
          checked ? "bg-primary" : "bg-muted"
        )}
        onClick={() => onChange(!checked)}
      >
        <div
          className={cn(
            "w-3 h-3 rounded-full bg-white absolute top-0.5 transition-transform",
            checked ? "translate-x-4.5" : "translate-x-0.5"
          )}
        />
      </div>
      <span className="text-xs">{label}</span>
    </label>
  );
}

interface AnalysisFiltersBarProps {
  filters: AnalysisFilters;
  onChange: (filters: AnalysisFilters) => void;
  analyzedCount: number;
  totalCount: number;
}

export function AnalysisFiltersBar({
  filters,
  onChange,
  analyzedCount,
  totalCount,
}: AnalysisFiltersBarProps) {
  const [isExpanded, setIsExpanded] = useState(false);

  const activeFilterCount = [
    filters.weather?.length,
    filters.lighting?.length,
    filters.visibility?.length,
    filters.setting?.length,
    filters.roadSurface?.length,
    filters.hazardSeverity?.length,
    filters.drivingAssessment?.length,
    filters.hasNearMiss ? 1 : 0,
    filters.hasPedestrians ? 1 : 0,
    filters.hasCyclists ? 1 : 0,
    filters.hasTrafficLight ? 1 : 0,
    filters.hasStopSign ? 1 : 0,
    filters.hasSpeedLimit ? 1 : 0,
    filters.hasVisibilityIssues ? 1 : 0,
  ].filter((v) => v && v > 0).length;

  const clearFilters = useCallback(() => {
    onChange({});
  }, [onChange]);

  if (analyzedCount === 0) return null;

  return (
    <div className="space-y-2">
      <div className="flex items-center gap-2">
        <Button
          variant="outline"
          size="sm"
          onClick={() => setIsExpanded(!isExpanded)}
          className="gap-2"
        >
          <SlidersHorizontal className="w-3.5 h-3.5" />
          Scene Filters
          {activeFilterCount > 0 && (
            <Badge variant="secondary" className="text-xs px-1.5">
              {activeFilterCount}
            </Badge>
          )}
        </Button>
        <span className="text-xs text-muted-foreground">
          {analyzedCount}/{totalCount} events analyzed
        </span>
        {activeFilterCount > 0 && (
          <Button
            variant="ghost"
            size="sm"
            onClick={clearFilters}
            className="h-7 px-2 text-xs"
          >
            <X className="w-3 h-3 mr-1" />
            Clear
          </Button>
        )}
      </div>

      {isExpanded && (
        <div className="p-4 border rounded-lg bg-muted/30 space-y-3">
          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
            <MultiSelect
              label="Weather"
              options={WEATHER_OPTIONS}
              selected={filters.weather || []}
              onChange={(weather) => onChange({ ...filters, weather })}
            />
            <MultiSelect
              label="Lighting"
              options={LIGHTING_OPTIONS}
              selected={filters.lighting || []}
              onChange={(lighting) => onChange({ ...filters, lighting })}
            />
            <MultiSelect
              label="Visibility"
              options={VISIBILITY_OPTIONS}
              selected={filters.visibility || []}
              onChange={(visibility) => onChange({ ...filters, visibility })}
            />
            <MultiSelect
              label="Setting"
              options={SETTING_OPTIONS}
              selected={filters.setting || []}
              onChange={(setting) => onChange({ ...filters, setting })}
            />
            <MultiSelect
              label="Road Surface"
              options={SURFACE_OPTIONS}
              selected={filters.roadSurface || []}
              onChange={(roadSurface) => onChange({ ...filters, roadSurface })}
            />
            <MultiSelect
              label="Hazard Severity"
              options={SEVERITY_OPTIONS}
              selected={filters.hazardSeverity || []}
              onChange={(hazardSeverity) =>
                onChange({ ...filters, hazardSeverity })
              }
            />
            <MultiSelect
              label="Driving Assessment"
              options={ASSESSMENT_OPTIONS}
              selected={filters.drivingAssessment || []}
              onChange={(drivingAssessment) =>
                onChange({ ...filters, drivingAssessment })
              }
            />
          </div>
          <div className="flex flex-wrap gap-x-4 gap-y-2 pt-2 border-t">
            <ToggleSwitch
              label="Has near-miss"
              checked={filters.hasNearMiss || false}
              onChange={(hasNearMiss) => onChange({ ...filters, hasNearMiss })}
            />
            <ToggleSwitch
              label="Pedestrians"
              checked={filters.hasPedestrians || false}
              onChange={(hasPedestrians) =>
                onChange({ ...filters, hasPedestrians })
              }
            />
            <ToggleSwitch
              label="Cyclists"
              checked={filters.hasCyclists || false}
              onChange={(hasCyclists) => onChange({ ...filters, hasCyclists })}
            />
            <ToggleSwitch
              label="Traffic light"
              checked={filters.hasTrafficLight || false}
              onChange={(hasTrafficLight) =>
                onChange({ ...filters, hasTrafficLight })
              }
            />
            <ToggleSwitch
              label="Stop sign"
              checked={filters.hasStopSign || false}
              onChange={(hasStopSign) => onChange({ ...filters, hasStopSign })}
            />
            <ToggleSwitch
              label="Speed limit sign"
              checked={filters.hasSpeedLimit || false}
              onChange={(hasSpeedLimit) =>
                onChange({ ...filters, hasSpeedLimit })
              }
            />
            <ToggleSwitch
              label="Visibility issues"
              checked={filters.hasVisibilityIssues || false}
              onChange={(hasVisibilityIssues) =>
                onChange({ ...filters, hasVisibilityIssues })
              }
            />
          </div>
        </div>
      )}
    </div>
  );
}

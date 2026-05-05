"use client";

import { Suspense, useState, useEffect, useCallback, useRef, useMemo } from "react";
import Link from "next/link";
import Image from "next/image";
import { usePathname } from "next/navigation";
import { List, LayoutGrid, Play, AlertTriangle, Trophy, X, ArrowUp, ArrowDown, ArrowUpDown } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { highlightSections, HighlightEvent, HighlightSection } from "@/lib/highlights";
import type { ImpairedCluster } from "@/app/api/highlights/impaired/route";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getApiKey, getSpeedUnit, convertSpeed, speedLabel, SpeedUnit } from "@/lib/api";
import {
  useTopHits,
  type TopHitPipelineStatus,
  type TopHitProductionStatus,
  type TopHitEventSummary,
  type TopHitVruStatus,
} from "@/lib/top-hits";
import { useThumbnail } from "@/hooks/use-thumbnail";
import { cn } from "@/lib/utils";


const countryFlags: Record<string, string> = {
  "United Kingdom": "🇬🇧",
  Germany: "🇩🇪",
  Japan: "🇯🇵",
  Australia: "🇦🇺",
  Canada: "🇨🇦",
  Brazil: "🇧🇷",
  USA: "🇺🇸",
  "United States": "🇺🇸",
  France: "🇫🇷",
  Italy: "🇮🇹",
  Spain: "🇪🇸",
  Mexico: "🇲🇽",
  Portugal: "🇵🇹",
  Poland: "🇵🇱",
  Slovenia: "🇸🇮",
  Taiwan: "🇹🇼",
  Austria: "🇦🇹",
  Belgium: "🇧🇪",
  Serbia: "🇷🇸",
  Romania: "🇷🇴",
  Hungary: "🇭🇺",
};

function getCountryFromLocation(location: string): string {
  const parts = location.split(", ");
  return parts[parts.length - 1];
}

function getCityFromLocation(location: string): string {
  return location.split(", ")[0];
}

function getFlagForLocation(location: string): string {
  const country = getCountryFromLocation(location);
  return countryFlags[country] || "🌍";
}

const NEW_BADGE_DURATION_MS = 24 * 60 * 60 * 1000; // 24 hours

const FPS_QC_CONFIG = {
  perfect: { label: "Perfect", color: "bg-emerald-50 text-emerald-700 border-emerald-200" },
  ok: { label: "OK", color: "bg-sky-50 text-sky-700 border-sky-200" },
  filter_out: { label: "Filter Out", color: "bg-rose-50 text-rose-700 border-rose-200" },
} as const;

function isRecentlyAdded(event: HighlightEvent): boolean {
  if (!event.addedAt) return false;
  return Date.now() - event.addedAt < NEW_BADGE_DURATION_MS;
}

type SortMode = "newest" | "extreme";

function parseEventDate(dateStr: string): number {
  const d = new Date(dateStr);
  return isNaN(d.getTime()) ? 0 : d.getTime();
}

function formatLateFramePct(value: number | null | undefined): string {
  if (value === null || value === undefined) return "-";
  const num = Number(value);
  return Number.isFinite(num) ? `${num.toFixed(2)}%` : "-";
}

function formatBitrateMbps(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value) || value <= 0) return "-";
  return (value / 1_000_000).toFixed(2);
}

function formatShortDate(value: string): string {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value.replace(/,?\s+\d{4}\b/, "");
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
  });
}

function getFpsQcConfig(value: string | null | undefined): { label: string; color: string } | null {
  if (!value) return null;
  if (value in FPS_QC_CONFIG) {
    return FPS_QC_CONFIG[value as keyof typeof FPS_QC_CONFIG];
  }
  return {
    label: value.replace(/_/g, " ").replace(/\b\w/g, (letter) => letter.toUpperCase()),
    color: "bg-muted text-muted-foreground border-muted-foreground/20",
  };
}

interface FrameTimingQcSummary {
  fpsQc: string | null;
  lateFramePct: number | null;
}

const EMPTY_FRAME_QC: Record<string, FrameTimingQcSummary> = {};

function useHighlightFrameQc(eventIds: string[]): Record<string, FrameTimingQcSummary> {
  const idsKey = Array.from(new Set(eventIds.filter(Boolean))).sort().join("|");
  const [frameQcById, setFrameQcById] = useState<Record<string, FrameTimingQcSummary>>({});

  useEffect(() => {
    if (!idsKey) {
      return;
    }

    let cancelled = false;
    const ids = idsKey.split("|");
    fetch("/api/highlights/frame-qc", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ ids }),
    })
      .then((res) => (res.ok ? res.json() : { frameTimingQcById: {} }))
      .then((data: { frameTimingQcById?: Record<string, FrameTimingQcSummary> }) => {
        if (!cancelled) setFrameQcById(data.frameTimingQcById ?? {});
      })
      .catch(() => {
        if (!cancelled) setFrameQcById({});
      });

    return () => {
      cancelled = true;
    };
  }, [idsKey]);

  return idsKey ? frameQcById : EMPTY_FRAME_QC;
}

function FpsQcCell({ value }: { value: string | null | undefined }) {
  const config = getFpsQcConfig(value);
  return config ? (
    <Badge variant="outline" className={cn("whitespace-nowrap", config.color)}>
      {config.label}
    </Badge>
  ) : (
    <span className="text-muted-foreground">-</span>
  );
}

/** Sort events by date (newest first) or by extremity (section-specific) */
function sortEvents(
  events: HighlightEvent[],
  mode: SortMode,
  sectionIndex: number
): HighlightEvent[] {
  const sorted = [...events];
  if (mode === "newest") {
    sorted.sort((a, b) => {
      const ta = a.addedAt || parseEventDate(a.date);
      const tb = b.addedAt || parseEventDate(b.date);
      return tb - ta;
    });
  } else {
    // "extreme" — sort by section-specific metric
    switch (sectionIndex) {
      case 0: // Extreme Braking: speed drop
        sorted.sort((a, b) => (b.maxSpeed - b.minSpeed) - (a.maxSpeed - a.minSpeed));
        break;
      case 1: // High Speed: top speed
        sorted.sort((a, b) => b.maxSpeed - a.maxSpeed);
        break;
      case 2: // G-Force: acceleration
      case 3: // Acceleration
      case 4: // Swerving
        sorted.sort((a, b) => b.acceleration - a.acceleration);
        break;
      case 6: { // VRU: confidence × acceleration (close-call severity)
        const vruScore = (e: HighlightEvent) =>
          (e.vruConfidence ?? 0) * (e.acceleration || 1);
        sorted.sort((a, b) => vruScore(b) - vruScore(a));
        break;
      }
      default: // International / other: composite
        sorted.sort((a, b) => {
          const sa = (a.maxSpeed - a.minSpeed) + a.acceleration * 30 + a.maxSpeed * 0.3;
          const sb = (b.maxSpeed - b.minSpeed) + b.acceleration * 30 + b.maxSpeed * 0.3;
          return sb - sa;
        });
    }
  }
  return sorted;
}

function SortToggle({ mode, onChange }: { mode: SortMode; onChange: (m: SortMode) => void }) {
  return (
    <div className="flex items-center gap-1 border rounded-lg p-0.5">
      <button
        onClick={() => onChange("newest")}
        className={cn(
          "px-2.5 py-1 rounded-md text-xs font-medium transition-colors",
          mode === "newest" ? "bg-foreground text-background" : "text-muted-foreground hover:text-foreground"
        )}
      >
        Newest
      </button>
      <button
        onClick={() => onChange("extreme")}
        className={cn(
          "px-2.5 py-1 rounded-md text-xs font-medium transition-colors",
          mode === "extreme" ? "bg-foreground text-background" : "text-muted-foreground hover:text-foreground"
        )}
      >
        Most Extreme
      </button>
    </div>
  );
}

function NewBadge() {
  return (
    <Badge variant="outline" className="bg-blue-100 text-blue-700 border-blue-300 text-[10px] px-1.5 py-0">
      NEW
    </Badge>
  );
}

interface TableProps {
  events: HighlightEvent[];
  discoveredEvents?: HighlightEvent[];
  unit: SpeedUnit;
}

function ExtremeBrakingTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const sl = speedLabel(unit);
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {drop} {sl}
                  <span className="text-muted-foreground font-normal ml-1 text-xs">
                    ({convertSpeed(event.maxSpeed, unit)} → {convertSpeed(event.minSpeed, unit)})
                  </span>
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function HighSpeedTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const sl = speedLabel(unit);
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Top Speed</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {convertSpeed(event.maxSpeed, unit)} {sl}
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function AggressiveAccelerationTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const sl = speedLabel(unit);
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Accel</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Range</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {event.acceleration.toFixed(2)} m/s²
                </td>
                <td className="px-4 py-2.5 text-left font-mono">
                  {convertSpeed(event.minSpeed, unit)} → {convertSpeed(event.maxSpeed, unit)} {sl}
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function HighestGForceTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const sl = speedLabel(unit);
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Accel</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {event.acceleration.toFixed(2)} m/s²
                </td>
                <td className="px-4 py-2.5 text-left font-mono">
                  {drop} {sl}
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function SwervingTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Accel</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {event.acceleration.toFixed(2)} m/s²
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function InternationalTable({ events, discoveredEvents = [], unit }: TableProps) {
  const existingCount = events.length;
  const sl = speedLabel(unit);
  const rows = [...events, ...discoveredEvents];
  const frameQcById = useHighlightFrameQc(rows.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">Flag</th>
            <th className="px-4 py-2.5 text-left font-medium">Type</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium">City</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {rows.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;

            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-lg">
                  {isNew ? <NewBadge /> : getFlagForLocation(event.location)}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Badge
                    className={cn(
                      config.bgColor,
                      config.color,
                      config.borderColor,
                      "border"
                    )}
                    variant="outline"
                  >
                    {config.label}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {drop} {sl}
                </td>
                <td className="px-4 py-2.5 text-left">
                  {getCityFromLocation(event.location)}
                </td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

const VRU_LABEL_STYLE: Record<string, string> = {
  stroller: "bg-red-50 text-red-700 border-red-200",
  wheelchair: "bg-red-50 text-red-700 border-red-200",
  child: "bg-orange-50 text-orange-700 border-orange-200",
  bicycle: "bg-blue-50 text-blue-700 border-blue-200",
  scooter: "bg-blue-50 text-blue-700 border-blue-200",
  skateboard: "bg-blue-50 text-blue-700 border-blue-200",
  motorcycle: "bg-indigo-50 text-indigo-700 border-indigo-200",
  person: "bg-emerald-50 text-emerald-700 border-emerald-200",
  pedestrian: "bg-emerald-50 text-emerald-700 border-emerald-200",
};

function VRUTable({ events, unit }: { events: HighlightEvent[]; unit: SpeedUnit }) {
  const sl = speedLabel(unit);
  const frameQcById = useHighlightFrameQc(events.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">VRU</th>
            <th className="px-4 py-2.5 text-left font-medium">Type</th>
            <th className="px-4 py-2.5 text-left font-medium">Accel</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event, index) => {
            const isNew = isRecentlyAdded(event);
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
            const vruLabel = event.vruLabel ?? "—";
            const vruStyle = VRU_LABEL_STYLE[vruLabel] || "bg-muted text-foreground border-border";
            const conf = event.vruConfidence != null ? Math.round(event.vruConfidence * 100) : null;
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-blue-50/50")}>
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {isNew ? <NewBadge /> : index + 1}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Badge variant="outline" className={cn(vruStyle, "border capitalize")}>
                    {vruLabel}
                    {conf != null && <span className="ml-1 opacity-70">{conf}%</span>}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Badge className={cn(config.bgColor, config.color, config.borderColor, "border")} variant="outline">
                    {config.label}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {event.acceleration.toFixed(2)} m/s²
                </td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.maxSpeed > 0 ? `${drop} ${sl}` : "—"}
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function TrendingTable({ events, unit }: { events: HighlightEvent[]; unit: SpeedUnit }) {
  const sl = speedLabel(unit);
  const frameQcById = useHighlightFrameQc(events.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Type</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">
              Accel
            </th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event, index) => {
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            const config =
              EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;

            return (
              <tr key={event.id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                  {index + 1}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Badge
                    className={cn(
                      config.bgColor,
                      config.color,
                      config.borderColor,
                      "border"
                    )}
                    variant="outline"
                  >
                    {config.label}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-left font-mono font-medium">
                  {drop > 0 ? (
                    <>
                      {drop} {sl}
                      <span className="text-muted-foreground font-normal ml-1 text-xs">
                        ({convertSpeed(event.maxSpeed, unit)} →{" "}
                        {convertSpeed(event.minSpeed, unit)})
                      </span>
                    </>
                  ) : (
                    <span className="text-muted-foreground">—</span>
                  )}
                </td>
                <td className="px-4 py-2.5 text-left font-mono hidden sm:table-cell">
                  {event.acceleration > 0
                    ? `${event.acceleration.toFixed(2)} m/s²`
                    : "—"}
                </td>
                <td className="px-4 py-2.5 text-left">{event.location}</td>
                <td className="px-4 py-2.5 text-left">
                  <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
                </td>
                <td className="px-4 py-2.5 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id.slice(0, 8)}…
                  </Link>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function TrendingSection({ unit, viewMode }: { unit: SpeedUnit; viewMode: "list" | "video" }) {
  const [events, setEvents] = useState<HighlightEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortMode, setSortMode] = useState<SortMode>("newest");

  useEffect(() => {
    const apiKey = getApiKey();
    if (!apiKey) {
      queueMicrotask(() => {
        setLoading(false);
        setError("Configure your Beemaps API key in settings to see trending events.");
      });
      return;
    }

    fetch("/api/highlights/trending", {
      headers: { Authorization: apiKey },
    })
      .then((res) => {
        if (!res.ok) throw new Error(`Failed to fetch trending events`);
        return res.json();
      })
      .then((data: HighlightEvent[]) => {
        setEvents(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  const sorted = useMemo(() => sortEvents(events, sortMode, -1), [events, sortMode]);

  if (error || events.length === 0) {
    if (!loading) return null;
  }

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Trending</CardTitle>
          <p className="text-sm text-muted-foreground">
            Most interesting events from the past 31 days, ranked by extremity.
          </p>
        </CardHeader>
        <CardContent className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  if (events.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-4">
          <div>
            <CardTitle className="text-lg">Trending</CardTitle>
            <p className="text-sm text-muted-foreground">
              Most interesting events from the past 31 days.
            </p>
          </div>
          {viewMode === "list" && (
            <SortToggle mode={sortMode} onChange={setSortMode} />
          )}
        </div>
      </CardHeader>
      <CardContent>
        {viewMode === "video" ? (
          <HighlightVideoGrid events={sorted} />
        ) : (
          <TrendingTable events={sorted} unit={unit} />
        )}
      </CardContent>
    </Card>
  );
}

function LowSpeedStopTable({ events, unit }: { events: HighlightEvent[]; unit: SpeedUnit }) {
  const sl = speedLabel(unit);
  const frameQcById = useHighlightFrameQc(events.map((event) => event.id));
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Peak Decel</th>
            <th className="px-4 py-2.5 text-left font-medium">From → To</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {events.map((event, index) => (
            <tr key={event.id} className="border-t hover:bg-muted/30">
              <td className="px-4 py-2.5 text-left text-muted-foreground font-medium">
                {index + 1}
              </td>
              <td className="px-4 py-2.5 text-left font-mono font-medium">
                {event.acceleration.toFixed(2)} m/s²
              </td>
              <td className="px-4 py-2.5 text-left font-mono">
                {convertSpeed(event.maxSpeed, unit)} → {convertSpeed(event.minSpeed, unit)} {sl}
              </td>
              <td className="px-4 py-2.5 text-left">{event.location}</td>
              <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                {event.date}
              </td>
              <td className="px-4 py-2.5 text-left">
                <FpsQcCell value={frameQcById[event.id]?.fpsQc} />
              </td>
              <td className="px-4 py-2.5 text-left">
                <Link
                  href={`/event/${event.id}`}
                  className="text-primary hover:underline font-mono text-xs"
                >
                  {event.id.slice(0, 8)}…
                </Link>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function LowSpeedStopSection({ unit, viewMode }: { unit: SpeedUnit; viewMode: "list" | "video" }) {
  const [events, setEvents] = useState<HighlightEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const apiKey = getApiKey();
    if (!apiKey) {
      queueMicrotask(() => {
        setLoading(false);
        setError("Configure your Beemaps API key in settings to see low-speed emergency stops.");
      });
      return;
    }

    fetch("/api/highlights/low-speed-stop", {
      headers: { Authorization: apiKey },
    })
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch low-speed stop events");
        return res.json();
      })
      .then((data: HighlightEvent[]) => {
        setEvents(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  const header = (
    <CardHeader>
      <CardTitle className="text-lg">Low-Speed Emergency Stop</CardTitle>
      <p className="text-sm text-muted-foreground">
        Harsh-braking events that went from ≤10 mph (16 km/h) to a near-stop very rapidly —
        the &quot;emergency stop at parking-lot speed&quot; pattern. Past 90 days, ranked by peak deceleration.
      </p>
    </CardHeader>
  );

  if (error && !loading) {
    return (
      <Card>
        {header}
        <CardContent>
          <p className="text-sm text-muted-foreground">{error}</p>
        </CardContent>
      </Card>
    );
  }

  if (loading) {
    return (
      <Card>
        {header}
        <CardContent className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  if (events.length === 0) {
    return (
      <Card>
        {header}
        <CardContent>
          <p className="text-sm text-muted-foreground py-4 text-center">
            No matching events in the past 31 days.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      {header}
      <CardContent>
        {viewMode === "video" ? (
          <HighlightVideoGrid events={events} />
        ) : (
          <LowSpeedStopTable events={events} unit={unit} />
        )}
      </CardContent>
    </Card>
  );
}

function HighlightVideoCard({ event, onRemove }: { event: HighlightEvent; onRemove?: () => void }) {
  const [videoUrl, setVideoUrl] = useState<string | null>(null);
  const [videoError, setVideoError] = useState(false);
  const [isVisible, setIsVisible] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const previewRef = useRef<HTMLDivElement | null>(null);
  const {
    thumbnailUrl,
    isLoading,
    error: thumbnailError,
    ref: thumbnailRef,
  } = useThumbnail(videoUrl ?? "");

  const setPreviewRef = useCallback(
    (node: HTMLDivElement | null) => {
      previewRef.current = node;
      thumbnailRef.current = node;
    },
    [thumbnailRef]
  );

  useEffect(() => {
    const el = previewRef.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          setIsVisible(true);
          observer.disconnect();
        }
      },
      { rootMargin: "200px", threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!isVisible || videoUrl || videoError) return;

    const apiKey = getApiKey();
    if (!apiKey) return;

    const controller = new AbortController();
    fetch(`/api/events/${event.id}`, {
      headers: { Authorization: apiKey },
      signal: controller.signal,
    })
      .then((res) => (res.ok ? res.json() : null))
      .then((data: { videoUrl?: unknown } | null) => {
        if (typeof data?.videoUrl === "string") {
          setVideoUrl(data.videoUrl);
        } else {
          setVideoError(true);
        }
      })
      .catch((error) => {
        if (!(error instanceof DOMException && error.name === "AbortError")) {
          setVideoError(true);
        }
      });

    return () => controller.abort();
  }, [event.id, isVisible, videoError, videoUrl]);

  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
  const IconComponent = config.icon;

  return (
    <Link
      href={`/event/${event.id}`}
      className="cursor-pointer group block"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <div ref={setPreviewRef} className="relative aspect-video bg-muted rounded-xl overflow-hidden">
        {thumbnailUrl ? (
          <Image
            src={thumbnailUrl}
            alt={`${config.label} event preview`}
            fill
            className="object-cover"
            unoptimized
          />
        ) : (
          <div
            className={cn(
              "w-full h-full flex items-center justify-center",
              `bg-gradient-to-br ${config.gradient}`
            )}
          >
            {isLoading && !thumbnailError ? (
              <div className="w-8 h-8 border-2 border-white/30 border-t-white/80 rounded-full animate-spin" />
            ) : (
              <IconComponent className="w-12 h-12 text-white/80" />
            )}
          </div>
        )}

        <div
          className={cn(
            "absolute inset-0 flex items-center justify-center",
            "bg-black/0 transition-all duration-200",
            isHovered && "bg-black/20"
          )}
        >
          <div
            className={cn(
              "w-12 h-12 rounded-full bg-black/80 flex items-center justify-center",
              "opacity-0 scale-90 transition-all duration-200",
              isHovered && "opacity-100 scale-100"
            )}
          >
            <Play className="w-5 h-5 text-white ml-0.5" fill="white" />
          </div>
        </div>

        <Badge
          className={cn(
            "absolute top-2 left-2",
            config.bgColor,
            config.color,
            config.borderColor,
            "border text-xs"
          )}
          variant="outline"
        >
          {config.label}
        </Badge>

        {onRemove && (
          <button
            type="button"
            onClick={(e) => {
              e.preventDefault();
              e.stopPropagation();
              onRemove();
            }}
            title="Remove from Top Hits"
            className="absolute top-2 right-2 w-7 h-7 rounded-full bg-black/70 hover:bg-black/90 text-white/90 hover:text-white flex items-center justify-center transition-colors"
          >
            <X className="w-4 h-4" />
          </button>
        )}
      </div>

      <div className="pt-2 px-1">
        <p className="text-sm font-medium truncate">{event.location}</p>
        <span className="text-xs text-muted-foreground">{event.date}</span>
      </div>
    </Link>
  );
}

function HighlightVideoGrid({ events, onRemove }: { events: HighlightEvent[]; onRemove?: (id: string) => void }) {
  return (
    <div
      className={cn(
        "grid gap-4",
        "grid-cols-1",
        "sm:grid-cols-2",
        "md:grid-cols-3",
        "lg:grid-cols-4",
        "xl:grid-cols-5"
      )}
    >
      {events.map((event) => (
        <HighlightVideoCard
          key={event.id}
          event={event}
          onRemove={onRemove ? () => onRemove(event.id) : undefined}
        />
      ))}
    </div>
  );
}

function ImpairedClusterTable({ clusters }: { clusters: ImpairedCluster[] }) {
  const frameQcById = useHighlightFrameQc(
    clusters.flatMap((cluster) => cluster.events.map((event) => event.id))
  );
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">Risk</th>
            <th className="px-4 py-2.5 text-left font-medium">Event Types</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Time Window</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-left font-medium">Events</th>
          </tr>
        </thead>
        <tbody>
          {clusters.map((cluster, index) => {
            const clusterQcValues = Array.from(
              new Set(
                cluster.events
                  .map((event) => frameQcById[event.id]?.fpsQc)
                  .filter((value): value is string => Boolean(value))
              )
            );
            const riskLevel =
              cluster.score >= 4
                ? "High"
                : cluster.score >= 3
                ? "Medium"
                : "Low";
            const riskColor =
              cluster.score >= 4
                ? "text-red-600 bg-red-100 border-red-200"
                : cluster.score >= 3
                ? "text-orange-600 bg-orange-100 border-orange-200"
                : "text-yellow-600 bg-yellow-100 border-yellow-200";

            const start = new Date(cluster.timeRange.start);
            const end = new Date(cluster.timeRange.end);
            const dateStr = start.toLocaleDateString("en-US", {
              month: "short",
              day: "2-digit",
            });
            const startTime = start.toLocaleTimeString("en-US", {
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
            });
            const endTime = end.toLocaleTimeString("en-US", {
              hour: "2-digit",
              minute: "2-digit",
              hour12: false,
            });

            return (
              <tr key={cluster.id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2.5 text-left">
                  <Badge
                    className={cn(riskColor, "border")}
                    variant="outline"
                  >
                    {riskLevel}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-left">
                  <div className="flex flex-wrap gap-1">
                    {cluster.eventTypes.map((type) => {
                      const config =
                        EVENT_TYPE_CONFIG[type] || EVENT_TYPE_CONFIG.UNKNOWN;
                      return (
                        <Badge
                          key={type}
                          className={cn(
                            config.bgColor,
                            config.color,
                            config.borderColor,
                            "border text-xs"
                          )}
                          variant="outline"
                        >
                          {config.label}
                        </Badge>
                      );
                    })}
                  </div>
                </td>
                <td className="px-4 py-2.5 text-left">{cluster.location}</td>
                <td className="px-4 py-2.5 text-left text-muted-foreground hidden sm:table-cell">
                  {dateStr} {startTime}–{endTime}
                </td>
                <td className="px-4 py-2.5 text-left">
                  {clusterQcValues.length > 0 ? (
                    <div className="flex flex-wrap gap-1">
                      {clusterQcValues.map((value) => (
                        <FpsQcCell key={value} value={value} />
                      ))}
                    </div>
                  ) : (
                    <FpsQcCell value={null} />
                  )}
                </td>
                <td className="px-4 py-2.5 text-left">
                  <div className="flex flex-wrap gap-1">
                    {cluster.events.map((event) => (
                      <Link
                        key={event.id}
                        href={`/event/${event.id}`}
                        className="text-primary hover:underline font-mono text-xs"
                      >
                        {event.id.slice(0, 8)}
                      </Link>
                    ))}
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function ImpairedDrivingSection({ viewMode }: { viewMode: "list" | "video" }) {
  const [clusters, setClusters] = useState<ImpairedCluster[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    const apiKey = getApiKey();
    if (!apiKey) {
      queueMicrotask(() => {
        setLoading(false);
        setError("Configure your Beemaps API key in settings to see impaired driving analysis.");
      });
      return;
    }

    fetch("/api/highlights/impaired", {
      headers: { Authorization: apiKey },
    })
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch impaired driving data");
        return res.json();
      })
      .then((data: ImpairedCluster[]) => {
        setClusters(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (!loading && error) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg flex items-center gap-2">
            <AlertTriangle className="w-5 h-5 text-orange-500" />
            Suspected Impaired Driving
          </CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">{error}</p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          <AlertTriangle className="w-5 h-5 text-orange-500" />
          Suspected Impaired Driving
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Clusters of co-occurring nighttime events (10pm–4am) within 500m and 10 minutes.
          More distinct event types in a cluster = higher risk score.
        </p>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="space-y-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : clusters.length === 0 ? (
          <p className="text-sm text-muted-foreground py-4 text-center">
            No impaired driving clusters detected in the past 31 days of nighttime events.
          </p>
        ) : (
          <ImpairedClusterTable clusters={clusters} />
        )}
      </CardContent>
    </Card>
  );
}

function InternationalSection({ unit, viewMode }: { unit: SpeedUnit; viewMode: "list" | "video" }) {
  const [events, setEvents] = useState<HighlightEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [sortMode, setSortMode] = useState<SortMode>("newest");

  const sorted = useMemo(() => sortEvents(events, sortMode, 5), [events, sortMode]);

  useEffect(() => {
    const apiKey = getApiKey();
    if (!apiKey) {
      queueMicrotask(() => {
        setLoading(false);
        setError("Configure your Beemaps API key in settings to see international events.");
      });
      return;
    }

    const mapboxToken = typeof window !== "undefined" ? localStorage.getItem("mapbox-token") || "" : "";
    const url = mapboxToken
      ? `/api/highlights/international?mapboxToken=${encodeURIComponent(mapboxToken)}`
      : "/api/highlights/international";

    fetch(url, {
      headers: { Authorization: apiKey },
    })
      .then((res) => {
        if (!res.ok) throw new Error("Failed to fetch international events");
        return res.json();
      })
      .then((data: HighlightEvent[]) => {
        setEvents(data);
        setLoading(false);
      })
      .catch((err) => {
        setError(err.message);
        setLoading(false);
      });
  }, []);

  if (error && !loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">International</CardTitle>
        </CardHeader>
        <CardContent>
          <p className="text-sm text-muted-foreground">{error}</p>
        </CardContent>
      </Card>
    );
  }

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">International</CardTitle>
          <p className="text-sm text-muted-foreground">
            Best events from across the globe — one per country, one per US state.
          </p>
        </CardHeader>
        <CardContent className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  if (events.length === 0) return null;

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-4">
          <div>
            <CardTitle className="text-lg">International</CardTitle>
            <p className="text-sm text-muted-foreground">
              Best events from across the globe — one per country, one per US state.
            </p>
          </div>
          {viewMode === "list" && (
            <SortToggle mode={sortMode} onChange={setSortMode} />
          )}
        </div>
      </CardHeader>
      <CardContent>
        {viewMode === "video" ? (
          <HighlightVideoGrid events={sorted} />
        ) : (
          <InternationalTable events={sorted} unit={unit} />
        )}
      </CardContent>
    </Card>
  );
}

type TopHitRow = HighlightEvent & {
  bitrateBps: number | null;
  fpsQc: string | null;
  lateFramePct: number | null;
  pipelineStatus: TopHitPipelineStatus;
};

const TOP_HIT_VRU_STATUS_CONFIG: Record<
  TopHitVruStatus,
  { label: string; className: string }
> = {
  not_run: {
    label: "Not Run",
    className: "bg-muted text-muted-foreground border-border",
  },
  queued: {
    label: "Queued",
    className: "bg-amber-50 text-amber-700 border-amber-200",
  },
  running: {
    label: "Running",
    className: "bg-blue-50 text-blue-700 border-blue-200",
  },
  completed: {
    label: "Complete",
    className: "bg-green-50 text-green-700 border-green-200",
  },
  failed: {
    label: "Failed",
    className: "bg-red-50 text-red-700 border-red-200",
  },
  cancelled: {
    label: "Cancelled",
    className: "bg-zinc-50 text-zinc-700 border-zinc-200",
  },
};

const TOP_HIT_PRODUCTION_STATUS_CONFIG: Record<
  TopHitProductionStatus,
  { label: string; className: string }
> = {
  not_queued: {
    label: "Not Queued",
    className: "bg-muted text-muted-foreground border-border",
  },
  queued: {
    label: "Queued",
    className: "bg-amber-50 text-amber-700 border-amber-200",
  },
  processing: {
    label: "Processing",
    className: "bg-blue-50 text-blue-700 border-blue-200",
  },
  completed: {
    label: "Complete",
    className: "bg-green-50 text-green-700 border-green-200",
  },
  failed: {
    label: "Failed",
    className: "bg-red-50 text-red-700 border-red-200",
  },
};

function TopHitVruCell({ event }: { event: TopHitRow }) {
  const status = event.pipelineStatus;
  const vru = TOP_HIT_VRU_STATUS_CONFIG[status.vruStatus];
  const confidence =
    event.vruConfidence != null ? Math.round(event.vruConfidence * 100) : null;

  return (
    <div className="flex items-center gap-2">
      <Badge
        variant="outline"
        className={cn("whitespace-nowrap", vru.className)}
      >
        {vru.label}
      </Badge>
      {event.vruLabel && (
        <span className="text-xs text-muted-foreground">
          {event.vruLabel}
          {confidence != null ? ` ${confidence}%` : ""}
        </span>
      )}
    </div>
  );
}

function TopHitProductionCell({
  status,
}: {
  status: TopHitPipelineStatus;
}) {
  const production =
    TOP_HIT_PRODUCTION_STATUS_CONFIG[status.productionStatus];
  return (
    <Badge
      variant="outline"
      className={cn("gap-1 whitespace-nowrap", production.className)}
    >
      {production.label}
      {status.productionStatus === "queued" &&
        status.productionPriority === 0 && (
          <span className="text-[10px] opacity-70">Priority</span>
        )}
    </Badge>
  );
}

function normalizeTopHitType(value: string | null): HighlightEvent["type"] {
  return value && value in EVENT_TYPE_CONFIG
    ? (value as HighlightEvent["type"])
    : "UNKNOWN";
}

function formatTopHitDate(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "2-digit",
    year: "numeric",
  });
}

function formatTopHitLocation(row: TopHitEventSummary): string {
  return row.lat !== null && row.lon !== null
    ? `${row.lat.toFixed(3)}, ${row.lon.toFixed(3)}`
    : "-";
}

function topHitSummaryToRow(row: TopHitEventSummary): TopHitRow {
  return {
    id: row.eventId,
    type: normalizeTopHitType(row.eventType),
    location: formatTopHitLocation(row),
    coords: {
      lat: row.lat ?? 0,
      lon: row.lon ?? 0,
    },
    date: formatTopHitDate(row.eventTimestamp),
    maxSpeed: 0,
    minSpeed: 0,
    acceleration: 0,
    vruLabel: row.vruLabel ?? undefined,
    vruConfidence: row.vruConfidence ?? undefined,
    bitrateBps: row.bitrateBps,
    fpsQc: row.fpsQc,
    lateFramePct: row.lateFramePct,
    pipelineStatus: row.pipelineStatus,
  };
}

function StaticBitrateCell({ value }: { value: number | null }) {
  return (
    <span className="font-mono text-xs text-muted-foreground">
      {formatBitrateMbps(value)}
    </span>
  );
}

function TopHitsTable({
  events,
  onRemove,
}: {
  events: TopHitRow[];
  onRemove: (id: string) => void;
}) {
  // Column sort: date or bitrate, mutually exclusive. Cycles none → desc → asc → none.
  type SortDir = "none" | "desc" | "asc";
  const [dateSort, setDateSort] = useState<SortDir>("none");
  const [bitrateSort, setBitrateSort] = useState<SortDir>("none");

  const sortedEvents = useMemo(() => {
    if (bitrateSort !== "none") {
      const arr = [...events];
      arr.sort((a, b) => {
        const ba = a.bitrateBps;
        const bb = b.bitrateBps;
        const aKnown = ba != null;
        const bKnown = bb != null;
        if (!aKnown && !bKnown) return 0;
        if (!aKnown) return 1;
        if (!bKnown) return -1;
        return bitrateSort === "desc" ? bb! - ba! : ba! - bb!;
      });
      return arr;
    }
    if (dateSort !== "none") {
      const arr = [...events];
      arr.sort((a, b) => {
        const ta = parseEventDate(a.date);
        const tb = parseEventDate(b.date);
        return dateSort === "desc" ? tb - ta : ta - tb;
      });
      return arr;
    }
    return events;
  }, [events, dateSort, bitrateSort]);

  const cycleDateSort = () => {
    setBitrateSort("none");
    setDateSort((s) => (s === "none" ? "desc" : s === "desc" ? "asc" : "none"));
  };
  const cycleBitrateSort = () => {
    setDateSort("none");
    setBitrateSort((s) => (s === "none" ? "desc" : s === "desc" ? "asc" : "none"));
  };
  const DateSortIcon = dateSort === "desc" ? ArrowDown : dateSort === "asc" ? ArrowUp : ArrowUpDown;
  const BitrateSortIcon = bitrateSort === "desc" ? ArrowDown : bitrateSort === "asc" ? ArrowUp : ArrowUpDown;

  return (
    <div className="overflow-x-auto rounded-lg border">
      <table className="w-full min-w-[1040px] text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="w-12 px-3 py-2.5 text-center font-medium">X</th>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
            <th className="px-4 py-2.5 text-left font-medium">Type</th>
            <th className="px-4 py-2.5 text-left font-medium">VRU</th>
            <th className="px-4 py-2.5 text-left font-medium">Production</th>
            <th className="px-4 py-2.5 text-left font-medium">FPS QC</th>
            <th className="px-4 py-2.5 text-right font-medium">Late %</th>
            <th className="px-4 py-2.5 text-right font-medium">
              <button
                type="button"
                onClick={cycleBitrateSort}
                className={cn(
                  "inline-flex items-center gap-1 rounded -mx-1 px-1 py-0.5 hover:bg-muted/80 transition-colors",
                  bitrateSort !== "none" && "text-foreground"
                )}
                title={
                  bitrateSort === "none"
                    ? "Sort by bitrate (highest first)"
                    : bitrateSort === "desc"
                      ? "Sort by bitrate (lowest first)"
                      : "Clear bitrate sort"
                  }
                >
                Bitrate
                <BitrateSortIcon className="w-3 h-3" />
              </button>
            </th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">
              <button
                type="button"
                onClick={cycleDateSort}
                className={cn(
                  "inline-flex items-center gap-1 rounded -mx-1 px-1 py-0.5 hover:bg-muted/80 transition-colors",
                  dateSort !== "none" && "text-foreground"
                )}
                title={
                  dateSort === "none"
                    ? "Sort by date (newest first)"
                    : dateSort === "desc"
                      ? "Sort by date (oldest first)"
                      : "Clear date sort"
                }
              >
                Date
                <DateSortIcon className="w-3 h-3" />
              </button>
            </th>
          </tr>
        </thead>
        <tbody>
          {sortedEvents.map((event, index) => {
            const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
            return (
              <tr key={event.id} className="border-t hover:bg-muted/30">
                <td className="px-3 py-2 text-center">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="h-7 w-7 text-muted-foreground hover:text-red-600"
                    title="Remove from Top Hits"
                    onClick={() => onRemove(event.id)}
                  >
                    <X className="w-4 h-4" />
                  </Button>
                </td>
                <td className="px-4 py-2 text-left text-muted-foreground font-medium">
                  {index + 1}
                </td>
                <td className="px-4 py-2 text-left">
                  <Link
                    href={`/event/${event.id}`}
                    className="text-primary hover:underline font-mono text-xs"
                  >
                    {event.id}
                  </Link>
                </td>
                <td className="px-4 py-2 text-left">
                  <Badge
                    className={cn(config.bgColor, config.color, config.borderColor, "border")}
                    variant="outline"
                  >
                    {config.label}
                  </Badge>
                </td>
                <td className="px-4 py-2 text-left">
                  <TopHitVruCell event={event} />
                </td>
                <td className="px-4 py-2 text-left">
                  <TopHitProductionCell status={event.pipelineStatus} />
                </td>
                <td className="px-4 py-2 text-left">
                  <FpsQcCell value={event.fpsQc} />
                </td>
                <td className="px-4 py-2 text-right font-mono text-xs text-muted-foreground whitespace-nowrap">
                  {formatLateFramePct(event.lateFramePct)}
                </td>
                <td className="px-4 py-2 text-right font-mono text-xs text-muted-foreground whitespace-nowrap">
                  <StaticBitrateCell value={event.bitrateBps} />
                </td>
                <td className="px-4 py-2 text-left text-muted-foreground hidden sm:table-cell">
                  {formatShortDate(event.date)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function TopHitsSection({ viewMode }: { viewMode: "list" | "video" }) {
  const {
    ids,
    rows: topHitRows,
    remove,
    isLoading: isLoadingIds,
  } = useTopHits();

  const events = useMemo(
    () => topHitRows.map(topHitSummaryToRow),
    [topHitRows]
  );

  const pipelineSummary = useMemo(() => {
    let vruCompleted = 0;
    let productionCompleted = 0;
    for (const event of events) {
      const status = event.pipelineStatus;
      if (status?.vruStatus === "completed") vruCompleted += 1;
      if (status?.productionStatus === "completed") productionCompleted += 1;
    }
    return { vruCompleted, productionCompleted };
  }, [events]);

  const header = (
    <CardHeader>
      <div className="flex items-center justify-between gap-4">
        <div>
          <CardTitle className="text-lg flex items-center gap-2">
            <Trophy className="w-5 h-5 text-amber-500" />
            Top Hits
          </CardTitle>
          <p className="text-sm text-muted-foreground">
            Your curated collection. Promote events from the event detail page.
          </p>
        </div>
        <div className="text-right text-xs text-muted-foreground font-mono">
          <div>{ids.length} {ids.length === 1 ? "event" : "events"}</div>
          {ids.length > 0 && (
            <div className="mt-0.5">
              VRU {pipelineSummary.vruCompleted}/{ids.length} · Prod{" "}
              {pipelineSummary.productionCompleted}/{ids.length}
            </div>
          )}
        </div>
      </div>
    </CardHeader>
  );

  if (isLoadingIds && ids.length === 0) {
    return (
      <Card>
        {header}
        <CardContent className="space-y-2">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </CardContent>
      </Card>
    );
  }

  if (ids.length === 0) {
    return (
      <Card>
        {header}
        <CardContent>
          <p className="text-sm text-muted-foreground py-4 text-center">
            No Top Hits yet. Open any event and click <strong>Top Hit</strong> to add it here.
          </p>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card>
      {header}
      <CardContent>
        {isLoadingIds && events.length === 0 ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }).map((_, i) => (
              <Skeleton key={i} className="h-10 w-full" />
            ))}
          </div>
        ) : viewMode === "video" ? (
          <HighlightVideoGrid events={events} onRemove={remove} />
        ) : (
          <TopHitsTable events={events} onRemove={remove} />
        )}
      </CardContent>
    </Card>
  );
}

function SectionTable({
  section,
  index,
  viewMode,
  unit,
}: {
  section: HighlightSection;
  index: number;
  viewMode: "list" | "video";
  unit: SpeedUnit;
}) {
  const [sortMode, setSortMode] = useState<SortMode>("newest");
  const sorted = useMemo(
    () => sortEvents(section.events, sortMode, index),
    [section.events, sortMode, index]
  );

  return (
    <Card>
      <CardHeader>
        <div className="flex items-center justify-between gap-4">
          <div>
            <CardTitle className="text-lg">{section.title}</CardTitle>
            <p className="text-sm text-muted-foreground">{section.description}</p>
          </div>
          {viewMode === "list" && (
            <SortToggle mode={sortMode} onChange={setSortMode} />
          )}
        </div>
      </CardHeader>
      <CardContent>
        {viewMode === "video" ? (
          <HighlightVideoGrid events={sorted} />
        ) : (
          <>
            {index === 0 && <ExtremeBrakingTable events={sorted} unit={unit} />}
            {index === 1 && <HighSpeedTable events={sorted} unit={unit} />}
            {index === 2 && <HighestGForceTable events={sorted} unit={unit} />}
            {index === 3 && <AggressiveAccelerationTable events={sorted} unit={unit} />}
            {index === 4 && <SwervingTable events={sorted} unit={unit} />}
            {index === 5 && <InternationalTable events={sorted} unit={unit} />}
            {index === 6 && <VRUTable events={sorted} unit={unit} />}
          </>
        )}
      </CardContent>
    </Card>
  );
}

type TabId = "topHits" | "trending" | "braking" | "lowSpeedStop" | "speed" | "gforce" | "acceleration" | "swerving" | "international" | "impaired" | "vru";

const TABS: { id: TabId; label: string; slug: string }[] = [
  { id: "topHits", slug: "top-hits", label: "Top Hits" },
  { id: "trending", slug: "trending", label: "Trending" },
  { id: "braking", slug: "extreme-braking", label: "Extreme Braking" },
  { id: "lowSpeedStop", slug: "low-speed-stop", label: "Low-Speed Emergency Stop" },
  { id: "speed", slug: "high-speed", label: "High Speed" },
  { id: "gforce", slug: "g-force", label: "G-Force" },
  { id: "acceleration", slug: "acceleration", label: "Acceleration" },
  { id: "swerving", slug: "swerving", label: "Swerving" },
  { id: "international", slug: "international", label: "International" },
  { id: "impaired", slug: "impaired", label: "Impaired" },
  { id: "vru", slug: "vru", label: "VRU" },
];

const SLUG_TO_TAB: Record<string, TabId> = Object.fromEntries(
  TABS.map((t) => [t.slug, t.id])
) as Record<string, TabId>;

const DEFAULT_SLUG = "extreme-braking";

/** Maps tab id to the index in highlightSections (international is dynamic, not static) */
const TAB_TO_SECTION_INDEX: Partial<Record<TabId, number>> = {
  braking: 0,
  speed: 1,
  gforce: 2,
  acceleration: 3,
  swerving: 4,
  vru: 6,
};

function HighlightsContent({ initialTab }: { initialTab?: TabId }) {
  const pathname = usePathname();
  const [viewMode, setViewMode] = useState<"list" | "video">("list");
  const [unit] = useState<SpeedUnit>(() => getSpeedUnit());
  const slug = pathname.split("/").filter(Boolean)[1];
  const activeTab = slug ? SLUG_TO_TAB[slug] ?? SLUG_TO_TAB[DEFAULT_SLUG] : initialTab || "braking";

  const sectionIndex = TAB_TO_SECTION_INDEX[activeTab];
  const activeSection = sectionIndex !== undefined ? highlightSections[sectionIndex] : null;

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container mx-auto px-4 py-6 space-y-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-2xl font-bold tracking-tight">Highlights</h2>
            <p className="text-muted-foreground mt-1">
              Curated collection of notable AI-detected driving events.
            </p>
          </div>
          <div className="flex items-center gap-1 border rounded-lg p-1 shrink-0">
            <Button
              variant={viewMode === "list" ? "secondary" : "ghost"}
              size="icon"
              className="h-8 w-8"
              onClick={() => setViewMode("list")}
            >
              <List className="w-4 h-4" />
            </Button>
            <Button
              variant={viewMode === "video" ? "secondary" : "ghost"}
              size="icon"
              className="h-8 w-8"
              onClick={() => setViewMode("video")}
            >
              <LayoutGrid className="w-4 h-4" />
            </Button>
          </div>
        </div>

        {/* Horizontal tabs */}
        <div className="flex gap-1 overflow-x-auto pb-1 -mb-1 scrollbar-none">
          {TABS.map((tab) => (
            <Link
              key={tab.id}
              href={`/highlights/${tab.slug}`}
              className={cn(
                "px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors",
                activeTab === tab.id
                  ? "bg-foreground text-background"
                  : "bg-muted text-muted-foreground hover:bg-muted/80"
              )}
            >
              {tab.label}
            </Link>
          ))}
        </div>

        {/* Tab content */}
        {activeTab === "topHits" && (
          <TopHitsSection viewMode={viewMode} />
        )}

        {activeTab === "trending" && (
          <TrendingSection unit={unit} viewMode={viewMode} />
        )}

        {activeTab === "lowSpeedStop" && (
          <LowSpeedStopSection unit={unit} viewMode={viewMode} />
        )}

        {activeTab === "impaired" && (
          <ImpairedDrivingSection viewMode={viewMode} />
        )}

        {activeTab === "international" && (
          <InternationalSection unit={unit} viewMode={viewMode} />
        )}

        {activeSection && sectionIndex !== undefined && (
          <SectionTable
            section={activeSection}
            index={sectionIndex}
            viewMode={viewMode}
            unit={unit}
          />
        )}
      </main>
    </div>
  );
}

function HighlightsSkeleton() {
  return (
    <div className="min-h-screen bg-background">
      <div className="sticky top-0 z-50 w-full border-b bg-background/95 h-14" />
      <main className="container mx-auto px-4 py-6 space-y-6">
        <div>
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-96 mt-2" />
        </div>
        {Array.from({ length: 3 }).map((_, i) => (
          <Skeleton key={i} className="h-64 rounded-lg" />
        ))}
      </main>
    </div>
  );
}

export default function HighlightsPage() {
  // Redirect to the default highlights tab
  return (
    <Suspense fallback={<HighlightsSkeleton />}>
      <HighlightsContent />
    </Suspense>
  );
}

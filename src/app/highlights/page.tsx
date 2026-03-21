"use client";

import { Suspense, useState, useEffect, useCallback, useRef, useMemo } from "react";
import Link from "next/link";
import Image from "next/image";
import { List, LayoutGrid, Play, AlertTriangle } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { highlightSections, HighlightEvent, HighlightSection } from "@/lib/highlights";
import type { ImpairedCluster } from "@/app/api/highlights/impaired/route";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getApiKey, getSpeedUnit, convertSpeed, speedLabel, SpeedUnit } from "@/lib/api";
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

function isRecentlyAdded(event: HighlightEvent): boolean {
  if (!event.addedAt) return false;
  return Date.now() - event.addedAt < NEW_BADGE_DURATION_MS;
}

type SortMode = "newest" | "extreme";

function parseEventDate(dateStr: string): number {
  const d = new Date(dateStr);
  return isNaN(d.getTime()) ? 0 : d.getTime();
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
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Speed Drop</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Top Speed</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">#</th>
            <th className="px-4 py-2.5 text-left font-medium">Accel</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Date</th>
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
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
      setLoading(false);
      setError("Configure your Beemaps API key in settings to see trending events.");
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

function HighlightVideoCard({ event }: { event: HighlightEvent }) {
  const [thumbnailUrl, setThumbnailUrl] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);
  const fetchedRef = useRef(false);

  const loadThumbnail = useCallback(async () => {
    if (fetchedRef.current) return;
    fetchedRef.current = true;
    const apiKey = getApiKey();
    if (!apiKey) return;

    setIsLoading(true);
    try {
      const res = await fetch(`/api/events/${event.id}`, {
        headers: { Authorization: apiKey },
      });
      if (!res.ok) return;
      const data = await res.json();
      if (!data.videoUrl) return;

      const thumbRes = await fetch(`/api/thumbnail?url=${encodeURIComponent(data.videoUrl)}`);
      if (!thumbRes.ok) return;
      const blob = await thumbRes.blob();
      setThumbnailUrl(URL.createObjectURL(blob));
    } catch {
      // Fallback to gradient
    } finally {
      setIsLoading(false);
    }
  }, [event.id]);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      (entries) => {
        if (entries[0].isIntersecting) {
          observer.disconnect();
          loadThumbnail();
        }
      },
      { rootMargin: "200px", threshold: 0 }
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, [loadThumbnail]);

  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
  const IconComponent = config.icon;

  return (
    <Link
      href={`/event/${event.id}`}
      className="cursor-pointer group block"
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      <div ref={ref} className="relative aspect-video bg-muted rounded-xl overflow-hidden">
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
            {isLoading ? (
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
      </div>

      <div className="pt-2 px-1">
        <p className="text-sm font-medium truncate">{event.location}</p>
        <span className="text-xs text-muted-foreground">{event.date}</span>
      </div>
    </Link>
  );
}

function HighlightVideoGrid({ events }: { events: HighlightEvent[] }) {
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
        <HighlightVideoCard key={event.id} event={event} />
      ))}
    </div>
  );
}

function ImpairedClusterTable({ clusters }: { clusters: ImpairedCluster[] }) {
  return (
    <div className="overflow-hidden rounded-lg border">
      <table className="w-full text-sm">
        <thead className="bg-muted/50">
          <tr>
            <th className="px-4 py-2.5 text-left font-medium w-12">Risk</th>
            <th className="px-4 py-2.5 text-left font-medium">Event Types</th>
            <th className="px-4 py-2.5 text-left font-medium">Location</th>
            <th className="px-4 py-2.5 text-left font-medium hidden sm:table-cell">Time Window</th>
            <th className="px-4 py-2.5 text-left font-medium">Events</th>
          </tr>
        </thead>
        <tbody>
          {clusters.map((cluster, index) => {
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
      setLoading(false);
      setError("Configure your Beemaps API key in settings to see impaired driving analysis.");
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
      setLoading(false);
      setError("Configure your Beemaps API key in settings to see international events.");
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
          </>
        )}
      </CardContent>
    </Card>
  );
}

type TabId = "trending" | "braking" | "speed" | "gforce" | "acceleration" | "swerving" | "international" | "impaired";

const TABS: { id: TabId; label: string }[] = [
  { id: "trending", label: "Trending" },
  { id: "braking", label: "Extreme Braking" },
  { id: "speed", label: "High Speed" },
  { id: "gforce", label: "G-Force" },
  { id: "acceleration", label: "Acceleration" },
  { id: "swerving", label: "Swerving" },
  { id: "international", label: "International" },
  { id: "impaired", label: "Impaired" },
];

/** Maps tab id to the index in highlightSections (international is dynamic, not static) */
const TAB_TO_SECTION_INDEX: Partial<Record<TabId, number>> = {
  braking: 0,
  speed: 1,
  gforce: 2,
  acceleration: 3,
  swerving: 4,
};

function HighlightsContent() {
  const [viewMode, setViewMode] = useState<"list" | "video">("list");
  const [unit, setUnit] = useState<SpeedUnit>("mph");
  const [activeTab, setActiveTab] = useState<TabId>("braking");

  useEffect(() => {
    setUnit(getSpeedUnit());
  }, []);

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
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={cn(
                "px-3 py-1.5 rounded-full text-sm font-medium whitespace-nowrap transition-colors",
                activeTab === tab.id
                  ? "bg-foreground text-background"
                  : "bg-muted text-muted-foreground hover:bg-muted/80"
              )}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab content */}
        {activeTab === "trending" && (
          <TrendingSection unit={unit} viewMode={viewMode} />
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
  return (
    <Suspense fallback={<HighlightsSkeleton />}>
      <HighlightsContent />
    </Suspense>
  );
}

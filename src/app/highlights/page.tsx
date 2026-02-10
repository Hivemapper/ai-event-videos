"use client";

import { Suspense, useState, useEffect, useCallback } from "react";
import Link from "next/link";
import { Search, Loader2 } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { highlightSections, HighlightEvent, HighlightSection } from "@/lib/highlights";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getApiKey, getMapboxToken, getSpeedUnit, convertSpeed, speedLabel, SpeedUnit } from "@/lib/api";
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

function NewBadge() {
  return (
    <Badge variant="outline" className="bg-green-100 text-green-700 border-green-300 text-[10px] px-1.5 py-0">
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
            const isNew = index >= existingCount;
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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
            const isNew = index >= existingCount;
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
            const isNew = index >= existingCount;
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
            const isNew = index >= existingCount;
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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
            const isNew = index >= existingCount;
            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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
            <th className="px-4 py-2.5 text-left font-medium">Event</th>
          </tr>
        </thead>
        <tbody>
          {[...events, ...discoveredEvents].map((event, index) => {
            const isNew = index >= existingCount;
            const drop = convertSpeed(event.maxSpeed - event.minSpeed, unit);
            const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;

            return (
              <tr key={event.id} className={cn("border-t hover:bg-muted/30", isNew && "bg-green-50/50")}>
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

function TrendingSection({ unit }: { unit: SpeedUnit }) {
  const [events, setEvents] = useState<HighlightEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

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

  if (error || events.length === 0) {
    if (!loading) return null;
  }

  if (loading) {
    return (
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">
            Trending
          </CardTitle>
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
        <CardTitle className="text-lg">
          Trending
        </CardTitle>
        <p className="text-sm text-muted-foreground">
          Most interesting events from the past 31 days, ranked by extremity.
        </p>
      </CardHeader>
      <CardContent>
        <TrendingTable events={events} unit={unit} />
      </CardContent>
    </Card>
  );
}

function SectionTable({
  section,
  index,
  discoveredEvents,
  unit,
}: {
  section: HighlightSection;
  index: number;
  discoveredEvents?: HighlightEvent[];
  unit: SpeedUnit;
}) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg">
          {section.title}
        </CardTitle>
        <p className="text-sm text-muted-foreground">{section.description}</p>
      </CardHeader>
      <CardContent>
        {index === 0 && <ExtremeBrakingTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
        {index === 1 && <HighSpeedTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
        {index === 2 && <HighestGForceTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
        {index === 3 && <AggressiveAccelerationTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
        {index === 4 && <SwervingTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
        {index === 5 && <InternationalTable events={section.events} discoveredEvents={discoveredEvents} unit={unit} />}
      </CardContent>
    </Card>
  );
}

function HighlightsContent() {
  const [discoveredEvents, setDiscoveredEvents] = useState<Record<number, HighlightEvent[]>>({});
  const [isDiscovering, setIsDiscovering] = useState(false);
  const [hasApiKey, setHasApiKey] = useState(false);
  const [unit, setUnit] = useState<SpeedUnit>("mph");

  useEffect(() => {
    setHasApiKey(!!getApiKey());
    setUnit(getSpeedUnit());
  }, []);

  const handleDiscover = useCallback(async () => {
    const apiKey = getApiKey();
    if (!apiKey) return;

    setIsDiscovering(true);
    try {
      const excludeIds = highlightSections.flatMap((s) => s.events.map((e) => e.id));
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
        Authorization: apiKey,
      };
      const mapboxToken = getMapboxToken();
      if (mapboxToken) {
        headers["X-Mapbox-Token"] = mapboxToken;
      }

      const res = await fetch("/api/highlights/discover", {
        method: "POST",
        headers,
        body: JSON.stringify({ excludeIds }),
      });

      if (!res.ok) throw new Error("Failed to discover events");
      const data: Record<number, HighlightEvent[]> = await res.json();
      setDiscoveredEvents(data);
    } catch (err) {
      console.error("Discover error:", err);
    } finally {
      setIsDiscovering(false);
    }
  }, []);

  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container mx-auto px-4 py-6 space-y-6">
        <div className="flex items-start justify-between gap-4">
          <div>
            <h2 className="text-2xl font-bold tracking-tight">Highlights</h2>
            <p className="text-muted-foreground mt-1">
              Curated collection of notable AI-detected driving events — extreme
              braking, high speed, aggressive acceleration, swerving, and
              international incidents.
            </p>
          </div>
          <Button
            variant="outline"
            size="sm"
            onClick={handleDiscover}
            disabled={isDiscovering || !hasApiKey}
            className="shrink-0"
          >
            {isDiscovering ? (
              <>
                <Loader2 className="w-4 h-4 mr-1.5 animate-spin" />
                Searching...
              </>
            ) : (
              <>
                <Search className="w-4 h-4 mr-1.5" />
                Discover New
              </>
            )}
          </Button>
        </div>

        <TrendingSection unit={unit} />

        {highlightSections.map((section, index) => (
          <SectionTable
            key={section.title}
            section={section}
            index={index}
            discoveredEvents={discoveredEvents[index]}
            unit={unit}
          />
        ))}
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

"use client";

import { Suspense } from "react";
import Link from "next/link";
import { Trophy, Zap, RotateCcw, Globe } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { highlightSections, HighlightEvent, HighlightSection } from "@/lib/highlights";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { cn } from "@/lib/utils";

const sectionIcons = [Trophy, Zap, RotateCcw, Globe];

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

function ExtremeBrakingTable({ events }: { events: HighlightEvent[] }) {
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
          {events.map((event, index) => {
            const drop = Math.round(event.maxSpeed - event.minSpeed);
            return (
              <tr key={event.id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2.5 text-muted-foreground font-medium">
                  {index + 1}
                </td>
                <td className="px-4 py-2.5 font-mono font-medium">
                  {drop} km/h
                  <span className="text-muted-foreground font-normal ml-1 text-xs">
                    ({Math.round(event.maxSpeed)} → {Math.round(event.minSpeed)})
                  </span>
                </td>
                <td className="px-4 py-2.5">{event.location}</td>
                <td className="px-4 py-2.5 text-muted-foreground hidden sm:table-cell">
                  {event.date}
                </td>
                <td className="px-4 py-2.5">
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

function HighestGForceTable({ events }: { events: HighlightEvent[] }) {
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
          {events.map((event, index) => {
            const drop = Math.round(event.maxSpeed - event.minSpeed);
            return (
              <tr key={event.id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2.5 text-muted-foreground font-medium">
                  {index + 1}
                </td>
                <td className="px-4 py-2.5 font-mono font-medium">
                  {event.acceleration.toFixed(2)} m/s²
                </td>
                <td className="px-4 py-2.5 font-mono">
                  {drop} km/h
                </td>
                <td className="px-4 py-2.5">{event.location}</td>
                <td className="px-4 py-2.5">
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

function SwervingTable({ events }: { events: HighlightEvent[] }) {
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
          {events.map((event, index) => (
            <tr key={event.id} className="border-t hover:bg-muted/30">
              <td className="px-4 py-2.5 text-muted-foreground font-medium">
                {index + 1}
              </td>
              <td className="px-4 py-2.5 font-mono font-medium">
                {event.acceleration.toFixed(2)} m/s²
              </td>
              <td className="px-4 py-2.5">{event.location}</td>
              <td className="px-4 py-2.5 text-muted-foreground hidden sm:table-cell">
                {event.date}
              </td>
              <td className="px-4 py-2.5">
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

function InternationalTable({ events }: { events: HighlightEvent[] }) {
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
          {events.map((event) => {
            const drop = Math.round(event.maxSpeed - event.minSpeed);
            const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
            const IconComponent = config.icon;
            return (
              <tr key={event.id} className="border-t hover:bg-muted/30">
                <td className="px-4 py-2.5 text-lg">
                  {getFlagForLocation(event.location)}
                </td>
                <td className="px-4 py-2.5">
                  <Badge
                    className={cn(
                      config.bgColor,
                      config.color,
                      config.borderColor,
                      "border"
                    )}
                    variant="outline"
                  >
                    <IconComponent className="w-3 h-3 mr-1" />
                    {config.label}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 font-mono font-medium">
                  {drop} km/h
                </td>
                <td className="px-4 py-2.5">
                  {getCityFromLocation(event.location)}
                </td>
                <td className="px-4 py-2.5">
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

function SectionTable({
  section,
  index,
}: {
  section: HighlightSection;
  index: number;
}) {
  const Icon = sectionIcons[index] || Trophy;

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          <Icon className="w-5 h-5" />
          {section.title}
        </CardTitle>
        <p className="text-sm text-muted-foreground">{section.description}</p>
      </CardHeader>
      <CardContent>
        {index === 0 && <ExtremeBrakingTable events={section.events} />}
        {index === 1 && <HighestGForceTable events={section.events} />}
        {index === 2 && <SwervingTable events={section.events} />}
        {index === 3 && <InternationalTable events={section.events} />}
      </CardContent>
    </Card>
  );
}

function HighlightsContent() {
  return (
    <div className="min-h-screen bg-background">
      <Header />

      <main className="container mx-auto px-4 py-6 space-y-6">
        <div>
          <h2 className="text-2xl font-bold tracking-tight">Highlights</h2>
          <p className="text-muted-foreground mt-1">
            Curated collection of notable AI-detected driving events — extreme
            braking, high G-force, and international incidents.
          </p>
        </div>

        {highlightSections.map((section, index) => (
          <SectionTable key={section.title} section={section} index={index} />
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

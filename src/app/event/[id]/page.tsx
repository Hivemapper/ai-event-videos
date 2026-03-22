"use client";

import { useEffect, useState, use, useRef, useCallback } from "react";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import {
  ArrowLeft,
  Check,
  Download,
  Loader2,
  ChevronRight,
  Gauge,
  Zap,
  Clock,
  CircleAlert,
  MapPin,
  Activity,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
  Collapsible,
  CollapsibleContent,
  CollapsibleTrigger,
} from "@/components/ui/collapsible";

import { Skeleton } from "@/components/ui/skeleton";
import dynamic from "next/dynamic";

const EventMap = dynamic(
  () => import("@/components/map/event-map").then((m) => m.EventMap),
  {
    ssr: false,
    loading: () => <Skeleton className="aspect-video" />,
  }
);
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import {
  getCameraIntrinsics,
  BEE_HFOV,
  DevicesResponse,
  getSpeedUnit,
  SpeedUnit,
} from "@/lib/api";
import { cn } from "@/lib/utils";
import { getTimeOfDay, getTimeOfDayStyle } from "@/lib/sun";
import { useRoadType } from "@/hooks/use-road-type";
import {
  useEventDetail,
  useCountryName,
  useNearestSpeedLimit,
} from "@/hooks/use-event-detail";
import { useVideoVru } from "@/hooks/use-video-vru";
import { SpeedProfileChart } from "@/components/events/speed-profile-chart";
import { MetadataTable } from "@/components/events/metadata-table";
const PositioningSection = dynamic(
  () =>
    import("@/components/events/positioning-section").then(
      (m) => m.PositioningSection
    ),
  { loading: () => <Skeleton className="h-32" /> }
);
const VideoVruPanel = dynamic(
  () =>
    import("@/components/events/video-vru-panel").then((m) => m.VideoVruPanel),
  { loading: () => <Skeleton className="h-16" /> }
);
import { ClipSummary } from "@/components/events/clip-summary";
import { SpeedOverlay } from "@/components/events/speed-overlay";
import { DetectionOverlay } from "@/components/events/detection-overlay";
import {
  SpeedDataPoint,
  formatDateTime,
  formatCoordinates,
  formatSpeed,
  getProxyVideoUrl,
  deriveSpeedFromGnss,
} from "@/lib/event-helpers";

const COUNTRY_FLAGS: Record<string, string> = {
  "United States": "\u{1F1FA}\u{1F1F8}",
  "United Kingdom": "\u{1F1EC}\u{1F1E7}",
  Canada: "\u{1F1E8}\u{1F1E6}",
  Mexico: "\u{1F1F2}\u{1F1FD}",
  Germany: "\u{1F1E9}\u{1F1EA}",
  France: "\u{1F1EB}\u{1F1F7}",
  Italy: "\u{1F1EE}\u{1F1F9}",
  Spain: "\u{1F1EA}\u{1F1F8}",
  Portugal: "\u{1F1F5}\u{1F1F9}",
  Brazil: "\u{1F1E7}\u{1F1F7}",
  Japan: "\u{1F1EF}\u{1F1F5}",
  Australia: "\u{1F1E6}\u{1F1FA}",
  Austria: "\u{1F1E6}\u{1F1F9}",
  Belgium: "\u{1F1E7}\u{1F1EA}",
  Poland: "\u{1F1F5}\u{1F1F1}",
  Slovenia: "\u{1F1F8}\u{1F1EE}",
  Serbia: "\u{1F1F7}\u{1F1F8}",
  Taiwan: "\u{1F1F9}\u{1F1FC}",
  India: "\u{1F1EE}\u{1F1F3}",
  China: "\u{1F1E8}\u{1F1F3}",
  Netherlands: "\u{1F1F3}\u{1F1F1}",
  Switzerland: "\u{1F1E8}\u{1F1ED}",
  Sweden: "\u{1F1F8}\u{1F1EA}",
  Norway: "\u{1F1F3}\u{1F1F4}",
  Denmark: "\u{1F1E9}\u{1F1F0}",
  Finland: "\u{1F1EB}\u{1F1EE}",
  Ireland: "\u{1F1EE}\u{1F1EA}",
  "South Korea": "\u{1F1F0}\u{1F1F7}",
  "New Zealand": "\u{1F1F3}\u{1F1FF}",
  Argentina: "\u{1F1E6}\u{1F1F7}",
  Colombia: "\u{1F1E8}\u{1F1F4}",
  Chile: "\u{1F1E8}\u{1F1F1}",
  Peru: "\u{1F1F5}\u{1F1EA}",
  "South Africa": "\u{1F1FF}\u{1F1E6}",
  Thailand: "\u{1F1F9}\u{1F1ED}",
  Indonesia: "\u{1F1EE}\u{1F1E9}",
  Philippines: "\u{1F1F5}\u{1F1ED}",
  Turkey: "\u{1F1F9}\u{1F1F7}",
  Greece: "\u{1F1EC}\u{1F1F7}",
  "Czech Republic": "\u{1F1E8}\u{1F1FF}",
  Czechia: "\u{1F1E8}\u{1F1FF}",
  Romania: "\u{1F1F7}\u{1F1F4}",
  Hungary: "\u{1F1ED}\u{1F1FA}",
  Israel: "\u{1F1EE}\u{1F1F1}",
  "United Arab Emirates": "\u{1F1E6}\u{1F1EA}",
  Singapore: "\u{1F1F8}\u{1F1EC}",
  Malaysia: "\u{1F1F2}\u{1F1FE}",
  Vietnam: "\u{1F1FB}\u{1F1F3}",
  Croatia: "\u{1F1ED}\u{1F1F7}",
};

function countryFlag(name: string): string {
  return COUNTRY_FLAGS[name] ?? "";
}

function CollapsibleSection({
  title,
  children,
  defaultOpen = false,
}: {
  title: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}) {
  const [open, setOpen] = useState(defaultOpen);
  return (
    <Collapsible open={open} onOpenChange={setOpen}>
      <CollapsibleTrigger className="flex items-center gap-2 w-full py-2 text-sm font-medium hover:text-foreground transition-colors text-muted-foreground">
        <ChevronRight
          className={cn(
            "w-4 h-4 transition-transform",
            open && "rotate-90"
          )}
        />
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent>{children}</CollapsibleContent>
    </Collapsible>
  );
}

/* ── Stat Card ── */

function StatCard({
  icon: Icon,
  label,
  value,
  tint,
}: {
  icon: React.ComponentType<{ className?: string }>;
  label: string;
  value: string | number;
  tint?: "red" | "amber" | "emerald";
}) {
  const tintStyles = {
    red: "border-red-500/20 bg-red-500/[0.03]",
    amber: "border-amber-500/20 bg-amber-500/[0.03]",
    emerald: "border-emerald-500/20 bg-emerald-500/[0.03]",
  };
  const tintText = {
    red: "text-red-600 dark:text-red-400",
    amber: "text-amber-600 dark:text-amber-400",
    emerald: "text-emerald-600 dark:text-emerald-400",
  };
  return (
    <Card className={cn("border-border/60", tint && tintStyles[tint])}>
      <CardContent className="pt-3 pb-3 px-3">
        <div
          className={cn(
            "flex items-center gap-1.5 text-[11px] font-medium uppercase tracking-wider mb-1",
            tint ? tintText[tint] : "text-muted-foreground"
          )}
        >
          <Icon className="w-3.5 h-3.5" />
          {label}
        </div>
        <div
          className={cn(
            "text-xl font-bold tabular-nums",
            tint ? tintText[tint] : ""
          )}
        >
          {value}
        </div>
      </CardContent>
    </Card>
  );
}

export default function EventDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const { data: event, error: eventError, isLoading } = useEventDetail(id);
  const error = eventError?.message ?? null;
  const { data: countryName = null } = useCountryName(
    event?.location.lat ?? null,
    event?.location.lon ?? null
  );
  const { data: nearestSpeedLimit = null } = useNearestSpeedLimit(
    event?.location.lat ?? null,
    event?.location.lon ?? null
  );
  const { data: vruData } = useVideoVru(id);
  const [copied, setCopied] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const [videoContainerHeight, setVideoContainerHeight] = useState<
    number | null
  >(null);
  const [cameraIntrinsics, setCameraIntrinsics] =
    useState<DevicesResponse | null>(null);
  const [videoCurrentTime, setVideoCurrentTime] = useState(0);
  const [videoDuration, setVideoDuration] = useState(0);
  const [isDownloading, setIsDownloading] = useState(false);
  const [speedUnit, setSpeedUnitState] = useState<SpeedUnit>("mph");

  useEffect(() => {
    setSpeedUnitState(getSpeedUnit());
  }, []);

  // Measure video container height to sync map height
  useEffect(() => {
    const el = videoContainerRef.current;
    if (!el) return;
    const observer = new ResizeObserver(([entry]) => {
      setVideoContainerHeight(entry.contentRect.height);
    });
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  // Track video playback time
  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    let lastUpdate = 0;
    const handleTimeUpdate = () => {
      const now = performance.now();
      if (now - lastUpdate < 200) return;
      lastUpdate = now;
      setVideoCurrentTime(video.currentTime);
    };

    const handleLoadedMetadata = () => {
      setVideoDuration(video.duration || 0);
    };

    video.addEventListener("timeupdate", handleTimeUpdate);
    video.addEventListener("loadedmetadata", handleLoadedMetadata);

    if (video.duration) {
      setVideoDuration(video.duration);
    }

    return () => {
      video.removeEventListener("timeupdate", handleTimeUpdate);
      video.removeEventListener("loadedmetadata", handleLoadedMetadata);
    };
  }, [event]);

  // Load camera intrinsics from localStorage
  useEffect(() => {
    const intrinsics = getCameraIntrinsics();
    setCameraIntrinsics(intrinsics);
  }, []);

  // Fetch road type
  const { roadType } = useRoadType(
    event?.location.lat ?? null,
    event?.location.lon ?? null,
    event?.gnssData
  );

  const copyCoordinates = async () => {
    if (!event) return;
    const coords = `${event.location.lat}, ${event.location.lon}`;
    await navigator.clipboard.writeText(coords);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const downloadVideo = async () => {
    if (!event?.videoUrl) return;
    setIsDownloading(true);
    try {
      const response = await fetch(getProxyVideoUrl(event.videoUrl));
      const blob = await response.blob();
      const url = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `event_${event.id}.mp4`;
      link.click();
      URL.revokeObjectURL(url);
    } catch {
      window.open(getProxyVideoUrl(event.videoUrl), "_blank");
    } finally {
      setIsDownloading(false);
    }
  };

  const seekTo = useCallback(
    (time: number) => {
      if (videoRef.current) videoRef.current.currentTime = time;
    },
    []
  );

  if (isLoading) {
    return <EventDetailSkeleton />;
  }

  if (error || !event) {
    return (
      <div className="min-h-screen bg-background">
        <Header>
          <Link href="/">
            <Button variant="ghost" size="sm">
              <ArrowLeft className="w-4 h-4 mr-2" />
              Back
            </Button>
          </Link>
        </Header>
        <main className="container mx-auto px-4 py-8">
          <div className="flex flex-col items-center justify-center py-16 text-muted-foreground">
            <p className="text-lg">{error || "Event not found"}</p>
            <Link href="/" className="mt-4">
              <Button>Return to Gallery</Button>
            </Link>
          </div>
        </main>
      </div>
    );
  }

  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
  const TypeIcon = config.icon;

  const speedData = event.metadata?.SPEED_ARRAY as
    | SpeedDataPoint[]
    | undefined;
  const overlaySpeedData =
    speedData && speedData.length > 0
      ? speedData
      : event.gnssData
        ? deriveSpeedFromGnss(event.gnssData)
        : [];
  const maxSpeedMs = speedData
    ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS))
    : null;
  const minSpeedMs = speedData
    ? Math.min(...speedData.map((s) => s.AVG_SPEED_MS))
    : null;
  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;
  const sunInfo = getTimeOfDay(
    event.timestamp,
    event.location.lat,
    event.location.lon
  );
  const todStyle = getTimeOfDayStyle(sunInfo.timeOfDay);

  const exceedsSpeedLimit =
    nearestSpeedLimit && maxSpeedMs
      ? maxSpeedMs * 2.237 > nearestSpeedLimit.limit
      : false;

  return (
    <div className="min-h-screen bg-background">
      <Header>
        <Link href="/">
          <Button variant="ghost" size="sm">
            <ArrowLeft className="w-4 h-4 mr-2" />
            Back to Gallery
          </Button>
        </Link>
      </Header>

      <main className="container mx-auto px-4 py-6 space-y-5">
        {/* ── Zone A: Hero ── */}

        {/* Event Header Bar */}
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <div className="flex items-center gap-3">
            <div
              className={cn(
                "w-10 h-10 rounded-xl flex items-center justify-center border",
                config.bgColor,
                config.borderColor
              )}
            >
              <TypeIcon className={cn("w-5 h-5", config.color)} />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h1 className="text-lg font-semibold">{config.label}</h1>
                {roadType?.classLabel && (
                  <span className="text-xs text-muted-foreground bg-muted px-2 py-0.5 rounded-full">
                    {roadType.classLabel}
                  </span>
                )}
                <Badge
                  variant="outline"
                  className={cn(
                    "text-xs",
                    todStyle.bgColor,
                    todStyle.color
                  )}
                >
                  {sunInfo.timeOfDay}
                </Badge>
              </div>
              <div className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground mt-0.5">
                <span>{formatDateTime(event.timestamp)}</span>
                <span className="text-border">|</span>
                <button
                  onClick={copyCoordinates}
                  className="font-mono hover:text-foreground transition-colors"
                  title="Copy coordinates"
                >
                  {formatCoordinates(event.location.lat, event.location.lon)}
                  {copied && (
                    <Check className="w-3 h-3 text-green-500 inline ml-1" />
                  )}
                </button>
                {countryName && (
                  <>
                    <span className="text-border">|</span>
                    <a
                      href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="hover:text-primary transition-colors"
                    >
                      {countryFlag(countryName)} {countryName}
                    </a>
                  </>
                )}
                {!countryName && (
                  <>
                    <span className="text-border">|</span>
                    <a
                      href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="hover:text-primary transition-colors"
                    >
                      Google Maps
                    </a>
                  </>
                )}
              </div>
            </div>
          </div>
          {event.videoUrl && (
            <Button
              variant="outline"
              size="sm"
              onClick={downloadVideo}
              disabled={isDownloading}
              className="gap-2 shrink-0"
            >
              {isDownloading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Download className="w-4 h-4" />
              )}
              {isDownloading ? "Downloading..." : "Download"}
            </Button>
          )}
        </div>

        {/* Video + Map */}
        <div className="grid grid-cols-1 lg:grid-cols-[3fr_2fr] gap-4">
          {/* Video */}
          <div ref={videoContainerRef} className="overflow-hidden rounded-xl border border-border/60">
            <div className="relative aspect-video bg-black">
              {event.videoUrl ? (
                <video
                  ref={videoRef}
                  src={getProxyVideoUrl(event.videoUrl)}
                  controls
                  autoPlay
                  className="w-full h-full"
                  controlsList="nodownload"
                >
                  Your browser does not support the video tag.
                </video>
              ) : (
                <div className="w-full h-full flex items-center justify-center text-muted-foreground">
                  No video available
                </div>
              )}
              {overlaySpeedData.length > 0 && (
                <SpeedOverlay
                  speedData={overlaySpeedData}
                  currentTime={videoCurrentTime}
                  duration={videoDuration}
                  unit={speedUnit}
                  speedLimit={nearestSpeedLimit}
                />
              )}
              {vruData?.boxes && vruData.boxes.length > 0 && (
                <DetectionOverlay
                  boxes={vruData.boxes}
                  currentTime={videoCurrentTime}
                />
              )}
            </div>
          </div>

          {/* Map */}
          <Card
            className="overflow-hidden py-0 flex flex-col border-border/60"
            style={
              videoContainerHeight
                ? { height: videoContainerHeight }
                : undefined
            }
          >
            <CardContent className="p-0 flex-1 min-h-0">
              <div className="overflow-hidden h-full">
                <EventMap
                  location={event.location}
                  path={event.gnssData}
                  currentTime={videoCurrentTime}
                  videoDuration={videoDuration}
                  className={
                    videoContainerHeight ? "h-full" : "aspect-video"
                  }
                  onSeek={seekTo}
                />
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Clip Summary */}
        <div className="max-w-4xl">
          <ClipSummary
            videoId={id}
            event={event}
            countryName={countryName ?? null}
            roadType={roadType?.classLabel ?? null}
            timeOfDay={sunInfo.timeOfDay}
            duration={videoDuration}
            vruLabels={vruData?.state?.labelsApplied}
            speedLimit={nearestSpeedLimit}
            exceedsSpeedLimit={exceedsSpeedLimit}
          />
        </div>

        {/* VRU Detection */}
        <div className="max-w-4xl">
          <VideoVruPanel
            videoId={id}
            videoUrl={event?.videoUrl ?? ""}
            currentTime={videoCurrentTime}
            duration={videoDuration}
            onSeek={seekTo}
          />
        </div>

        {/* Speed Profile */}
        <Card className="max-w-4xl">
          <CardHeader>
            <CardTitle className="text-lg flex items-center gap-2">
              Speed Profile
              {nearestSpeedLimit && (
                <Badge variant="secondary" className="text-xs">
                  Limit: {nearestSpeedLimit.limit} {nearestSpeedLimit.unit}
                </Badge>
              )}
            </CardTitle>
          </CardHeader>
          <CardContent>
            <SpeedProfileChart
              speedArray={speedData}
              gnssData={event.gnssData}
              imuData={event.imuData}
              currentTime={videoCurrentTime}
              duration={videoDuration}
              speedLimit={nearestSpeedLimit}
              unit={speedUnit}
              onSeek={seekTo}
            />
          </CardContent>
        </Card>

        {/* ── Zone C: Raw Data ── */}
        <PositioningSection eventId={id} gnssData={event.gnssData} />

        {/* Metadata */}
        {event.metadata && Object.keys(event.metadata).length > 0 && (
          <Card>
            <CardHeader>
              <CardTitle className="text-lg flex items-center gap-2">
                Metadata
                <Badge variant="secondary" className="text-xs font-mono">
                  {event.id}
                </Badge>
              </CardTitle>
            </CardHeader>
            <CardContent className="space-y-4">
              <MetadataTable metadata={event.metadata} />
            </CardContent>
          </Card>
        )}

        {/* Camera Info */}
        {cameraIntrinsics?.bee && (
          <Card className="border-border/60">
            <CardHeader>
              <CardTitle className="text-sm font-semibold">
                Bee Camera
              </CardTitle>
            </CardHeader>
            <CardContent>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-4 text-sm">
                <div>
                  <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                    Horizontal FOV
                  </p>
                  <p className="font-medium font-mono mt-0.5">
                    {BEE_HFOV}°
                  </p>
                </div>
                <div>
                  <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                    Focal Length
                  </p>
                  <p className="font-medium font-mono mt-0.5">
                    {cameraIntrinsics.bee.focal.toFixed(4)}
                  </p>
                </div>
                <div>
                  <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                    Distortion (k1)
                  </p>
                  <p className="font-medium font-mono mt-0.5">
                    {cameraIntrinsics.bee.k1.toFixed(4)}
                  </p>
                </div>
                <div>
                  <p className="text-[11px] font-medium uppercase tracking-wider text-muted-foreground">
                    Distortion (k2)
                  </p>
                  <p className="font-medium font-mono mt-0.5">
                    {cameraIntrinsics.bee.k2.toFixed(4)}
                  </p>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </main>
    </div>
  );
}

function EventDetailSkeleton() {
  return (
    <div className="min-h-screen bg-background">
      <div className="fixed top-0 left-0 right-0 z-[60] h-0.5 bg-muted overflow-hidden">
        <div className="h-full w-1/3 bg-primary/50 animate-[loading_1s_ease-in-out_infinite]" />
      </div>
      <Header>
        <Skeleton className="h-8 w-32" />
      </Header>
      <main className="container mx-auto px-4 py-6 space-y-5">
        {/* Header */}
        <Skeleton className="h-14 w-full rounded-lg" />
        {/* Video + Map */}
        <div className="grid grid-cols-1 lg:grid-cols-[3fr_2fr] gap-4">
          <Skeleton className="aspect-video rounded-xl" />
          <Skeleton className="aspect-video rounded-xl" />
        </div>
        {/* Stat cards */}
        <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-5 gap-3">
          {Array.from({ length: 5 }).map((_, i) => (
            <Skeleton key={i} className="h-[72px] rounded-lg" />
          ))}
        </div>
        {/* Tabs */}
        <Skeleton className="h-[320px] rounded-lg" />
      </main>
    </div>
  );
}

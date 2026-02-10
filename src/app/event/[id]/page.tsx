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
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
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
import { getCameraIntrinsics, BEE_HFOV, DevicesResponse, getSpeedUnit, SpeedUnit } from "@/lib/api";
import { cn } from "@/lib/utils";
import { getTimeOfDay, getTimeOfDayStyle } from "@/lib/sun";
import { useRoadType } from "@/hooks/use-road-type";
import { useActorDetection } from "@/hooks/use-actor-detection";
import { useActorTracking } from "@/hooks/use-actor-tracking";
import { useEventDetail, useCountryName, useNearestSpeedLimit } from "@/hooks/use-event-detail";
import { VideoAnalysisCard } from "@/components/events/video-analysis";
import { SpeedProfileChart } from "@/components/events/speed-profile-chart";
import { MetadataTable } from "@/components/events/metadata-table";
import { FrameLabeling } from "@/components/events/frame-labeling";
import { PositioningSection } from "@/components/events/positioning-section";
import { SpeedOverlay } from "@/components/events/speed-overlay";
import { ActorControls } from "@/components/events/actor-controls";
import { calculateBearing } from "@/lib/geo-projection";
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
        <ChevronRight className={cn("w-4 h-4 transition-transform", open && "rotate-90")} />
        {title}
      </CollapsibleTrigger>
      <CollapsibleContent>{children}</CollapsibleContent>
    </Collapsible>
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
  const [copied, setCopied] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const [videoContainerHeight, setVideoContainerHeight] = useState<number | null>(null);
  const [cameraIntrinsics, setCameraIntrinsics] = useState<DevicesResponse | null>(null);
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

    const handleTimeUpdate = () => {
      setVideoCurrentTime(video.currentTime);
    };

    const handleLoadedMetadata = () => {
      setVideoDuration(video.duration || 0);
    };

    video.addEventListener("timeupdate", handleTimeUpdate);
    video.addEventListener("loadedmetadata", handleLoadedMetadata);

    // Set initial duration if already loaded
    if (video.duration) {
      setVideoDuration(video.duration);
    }

    return () => {
      video.removeEventListener("timeupdate", handleTimeUpdate);
      video.removeEventListener("loadedmetadata", handleLoadedMetadata);
    };
  }, [event]); // Re-run when event loads (video element gets src)

  // Load camera intrinsics from localStorage
  useEffect(() => {
    const intrinsics = getCameraIntrinsics();
    setCameraIntrinsics(intrinsics);
  }, []);

  // Fetch road type from Mapbox (samples multiple GNSS points for accuracy)
  const { roadType, isLoading: roadTypeLoading } = useRoadType(
    event?.location.lat ?? null,
    event?.location.lon ?? null,
    event?.gnssData
  );

  // Actor detection
  const { actors: detectedActors, isDetecting, error: actorError, detect: detectActors, clear: clearActors } = useActorDetection();

  // Actor tracking
  const { trackingResult, isTracking, progress: trackingProgress, error: trackingError, track: trackActors, clear: clearTracking } = useActorTracking();

  // Reusable camera state interpolation from GNSS path
  const getCameraState = useCallback((timestamp: number): { lat: number; lon: number; bearing: number } => {
    if (!event?.gnssData || event.gnssData.length < 2 || !videoDuration || videoDuration <= 0) {
      return { lat: event?.location.lat ?? 0, lon: event?.location.lon ?? 0, bearing: 0 };
    }
    const gnss = event.gnssData;

    // Map video time (seconds) to GNSS time domain (milliseconds)
    const gnssStart = gnss[0].timestamp;
    const gnssEnd = gnss[gnss.length - 1].timestamp;
    const progress = Math.max(0, Math.min(1, timestamp / videoDuration));
    const gnssTime = gnssStart + progress * (gnssEnd - gnssStart);

    // Find bracketing GNSS points by actual timestamp
    let lowerIndex = 0;
    for (let i = 0; i < gnss.length - 1; i++) {
      if (gnss[i + 1].timestamp >= gnssTime) {
        lowerIndex = i;
        break;
      }
      lowerIndex = i;
    }
    const upperIndex = Math.min(lowerIndex + 1, gnss.length - 1);

    // Interpolate based on actual timestamps, not array index
    const segmentDuration = gnss[upperIndex].timestamp - gnss[lowerIndex].timestamp;
    const t = segmentDuration > 0 ? (gnssTime - gnss[lowerIndex].timestamp) / segmentDuration : 0;

    const p1 = gnss[lowerIndex];
    const p2 = gnss[upperIndex];
    const lat = p1.lat + (p2.lat - p1.lat) * t;
    const lon = p1.lon + (p2.lon - p1.lon) * t;

    const lookAhead = 3;
    const endIndex = Math.min(lowerIndex + lookAhead, gnss.length - 1);
    const bearing = endIndex > lowerIndex
      ? calculateBearing(gnss[lowerIndex].lat, gnss[lowerIndex].lon, gnss[endIndex].lat, gnss[endIndex].lon)
      : calculateBearing(gnss[Math.max(0, lowerIndex - 1)].lat, gnss[Math.max(0, lowerIndex - 1)].lon, gnss[lowerIndex].lat, gnss[lowerIndex].lon);

    return { lat, lon, bearing };
  }, [event, videoDuration]);

  const handleDetectActors = useCallback(() => {
    if (!event?.videoUrl) return;

    const camera = getCameraState(videoCurrentTime);
    const fovDegrees = cameraIntrinsics?.bee ? BEE_HFOV : 120;

    detectActors({
      eventId: id,
      videoUrl: event.videoUrl,
      timestamp: videoCurrentTime,
      cameraLat: camera.lat,
      cameraLon: camera.lon,
      cameraBearing: camera.bearing,
      fovDegrees,
      cameraIntrinsics: cameraIntrinsics?.bee
        ? { focal: cameraIntrinsics.bee.focal, k1: cameraIntrinsics.bee.k1, k2: cameraIntrinsics.bee.k2 }
        : undefined,
    });
  }, [event, videoCurrentTime, getCameraState, cameraIntrinsics, id, detectActors]);

  const handleTrackActors = useCallback(() => {
    if (!event?.videoUrl || !videoDuration) return;

    const fovDegrees = cameraIntrinsics?.bee ? BEE_HFOV : 120;

    trackActors({
      eventId: id,
      videoUrl: event.videoUrl,
      videoDuration,
      fovDegrees,
      cameraIntrinsics: cameraIntrinsics?.bee
        ? { focal: cameraIntrinsics.bee.focal, k1: cameraIntrinsics.bee.k1, k2: cameraIntrinsics.bee.k2 }
        : undefined,
      getCameraState,
    });
  }, [event, videoDuration, cameraIntrinsics, id, trackActors, getCameraState]);

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
      // Fallback: open in new tab
      window.open(getProxyVideoUrl(event.videoUrl), "_blank");
    } finally {
      setIsDownloading(false);
    }
  };

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

  const speedData = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const overlaySpeedData = speedData && speedData.length > 0
    ? speedData
    : event.gnssData ? deriveSpeedFromGnss(event.gnssData) : [];
  const maxSpeed = speedData
    ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS))
    : null;
  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;

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

      {/* Main content */}
      <main className="container mx-auto px-4 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Left column - Video */}
          <div className="space-y-4">
            {/* Video player */}
            <div ref={videoContainerRef} className="overflow-hidden rounded-xl">
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
              </div>
              {event.videoUrl && (
                <div className="flex justify-end px-3 py-1.5 bg-black/80">
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-white/70 hover:text-white hover:bg-white/10"
                    onClick={downloadVideo}
                    disabled={isDownloading}
                  >
                    {isDownloading ? (
                      <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                    ) : (
                      <Download className="w-4 h-4 mr-2" />
                    )}
                    {isDownloading ? "Downloading..." : "Download"}
                  </Button>
                </div>
              )}
            </div>

            {/* Metadata bar */}
            <div className="space-y-2 px-1">
              {/* Badges row */}
              <div className="flex flex-wrap items-center gap-2">
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
                {roadType?.classLabel && (
                  <Badge variant="outline">
                    {roadType.classLabel}
                  </Badge>
                )}
                {(() => {
                  const sunInfo = getTimeOfDay(
                    event.timestamp,
                    event.location.lat,
                    event.location.lon
                  );
                  const style = getTimeOfDayStyle(sunInfo.timeOfDay);
                  return (
                    <Badge variant="outline" className={cn(style.bgColor, style.color)}>
                      {sunInfo.timeOfDay}
                    </Badge>
                  );
                })()}
              </div>
              {/* Details row */}
              <div className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
                <span>{formatDateTime(event.timestamp)}</span>
                <span>·</span>
                <button
                  onClick={copyCoordinates}
                  className="font-mono hover:text-foreground transition-colors"
                  title="Copy coordinates"
                >
                  {formatCoordinates(event.location.lat, event.location.lon)}
                  {copied && <Check className="w-3 h-3 text-green-500 inline ml-1" />}
                </button>
                {maxSpeed !== null && (
                  <>
                    <span>·</span>
                    <span>
                      Max <span className="text-foreground font-mono">{formatSpeed(maxSpeed)}</span>
                    </span>
                  </>
                )}
                {acceleration !== undefined && (
                  <>
                    <span>·</span>
                    <span>
                      Accel <span className="text-foreground font-mono">{acceleration.toFixed(2)} m/s²</span>
                    </span>
                  </>
                )}
                {countryName && (
                  <>
                    <span>·</span>
                    <a
                      href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-foreground hover:text-primary transition-colors"
                    >
                      {countryFlag(countryName)} {countryName}
                    </a>
                  </>
                )}
                {!countryName && (
                  <>
                    <span>·</span>
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

            {/* Speed Profile */}
            <Card>
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  Speed Profile
                  {nearestSpeedLimit && (
                    <Badge variant="outline" className="text-xs">
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
                  onSeek={(time) => {
                    if (videoRef.current) videoRef.current.currentTime = time;
                  }}
                />
              </CardContent>
            </Card>

            {/* Camera Info */}
            {cameraIntrinsics?.bee && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">
                    Bee Camera
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-muted-foreground">Horizontal FOV</p>
                      <p className="font-medium font-mono">
                        {BEE_HFOV}°
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Focal Length</p>
                      <p className="font-medium font-mono">
                        {cameraIntrinsics.bee.focal.toFixed(4)}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Radial Distortion (k1)</p>
                      <p className="font-medium font-mono">
                        {cameraIntrinsics.bee.k1.toFixed(4)}
                      </p>
                    </div>
                    <div>
                      <p className="text-muted-foreground">Radial Distortion (k2)</p>
                      <p className="font-medium font-mono">
                        {cameraIntrinsics.bee.k2.toFixed(4)}
                      </p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Right column - Map and metadata */}
          <div className="space-y-4">
            {/* Map */}
            <Card
              className="overflow-hidden py-0 flex flex-col"
              style={videoContainerHeight ? { height: videoContainerHeight } : undefined}
            >
              <CardContent className="p-0 flex-1 min-h-0">
                <div className="rounded-lg shadow-inner overflow-hidden h-full">
                  <EventMap
                    location={event.location}
                    path={event.gnssData}
                    currentTime={videoCurrentTime}
                    videoDuration={videoDuration}
                    className={videoContainerHeight ? "h-full" : "aspect-video"}
                    detectedActors={trackingResult ? undefined : (detectedActors ?? undefined)}
                    actorTracks={trackingResult?.tracks}
                    onSeek={(time) => {
                      if (videoRef.current) videoRef.current.currentTime = time;
                    }}
                  />
                </div>
              </CardContent>
              {event.videoUrl && (
                <ActorControls
                  detectedActors={detectedActors}
                  isDetecting={isDetecting}
                  actorError={actorError}
                  onDetect={handleDetectActors}
                  onClearActors={clearActors}
                  trackingResult={trackingResult}
                  isTracking={isTracking}
                  trackingProgress={trackingProgress}
                  trackingError={trackingError}
                  onTrack={handleTrackActors}
                  onClearTracking={clearTracking}
                />
              )}
            </Card>

            {/* Scene Analysis */}
            {event.videoUrl && (
              <CollapsibleSection title="Scene Analysis">
                <VideoAnalysisCard eventId={id} />
              </CollapsibleSection>
            )}

            {/* Positioning section */}
            <PositioningSection eventId={id} gnssData={event.gnssData} />

            {/* Metadata table */}
            {event.metadata && Object.keys(event.metadata).length > 0 && (
              <CollapsibleSection title="Metadata">
                <MetadataTable metadata={event.metadata} />
              </CollapsibleSection>
            )}

            {/* Frame Labeling */}
            {event.videoUrl && (
              <CollapsibleSection title="Frame Labeling">
                <FrameLabeling event={event} videoRef={videoRef} />
              </CollapsibleSection>
            )}
          </div>
        </div>
      </main>
    </div>
  );
}

function EventDetailSkeleton() {
  return (
    <div className="min-h-screen bg-background">
      {/* Subtle loading indicator */}
      <div className="fixed top-0 left-0 right-0 z-[60] h-0.5 bg-muted overflow-hidden">
        <div className="h-full w-1/3 bg-primary/50 animate-[loading_1s_ease-in-out_infinite]" />
      </div>
      <Header>
        <Skeleton className="h-8 w-32" />
      </Header>
      <main className="container mx-auto px-4 py-6">
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          <div className="space-y-4">
            <Skeleton className="aspect-video rounded-xl" />
            <Skeleton className="h-48 rounded-lg" />
          </div>
          <div className="space-y-4">
            <Skeleton className="h-[400px] rounded-lg" />
            <Skeleton className="h-32 rounded-lg" />
          </div>
        </div>
      </main>
    </div>
  );
}

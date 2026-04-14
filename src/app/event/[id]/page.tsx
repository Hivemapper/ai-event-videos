"use client";

import { useEffect, useState, use, useRef, useCallback, useMemo } from "react";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import {
  ArrowLeft,
  Check,
  Download,
  FileQuestion,
  Ghost,
  Loader2,
  Maximize2,
  Route,
  Tag,
  VideoOff,
  X,
  ChevronRight,
  Zap,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/ui/collapsible";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
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
import { useTriageStatus, TriageCategory } from "@/hooks/use-triage-status";
import { useDetectionRuns } from "@/hooks/use-detection-runs";
import { useDetectionTimestamps } from "@/hooks/use-detection-timestamps";
import { useRunLogs } from "@/hooks/use-run-logs";
import { useEventDetail, useCountryName, useNearestSpeedLimit } from "@/hooks/use-event-detail";
import { SpeedProfileChart } from "@/components/events/speed-profile-chart";
import { MetadataTable } from "@/components/events/metadata-table";
const PositioningSection = dynamic(
  () => import("@/components/events/positioning-section").then((m) => m.PositioningSection),
  { loading: () => <Skeleton className="h-32" /> }
);
const VideoVruPanel = dynamic(
  () => import("@/components/events/video-vru-panel").then((m) => m.VideoVruPanel),
  { loading: () => <Skeleton className="h-16" /> }
);
import { ClipSummary } from "@/components/events/clip-summary";
import { summarizeDetections } from "@/lib/detection-summary";
import { DetectionOverlay } from "@/components/events/detection-overlay";
import { SpeedOverlay } from "@/components/events/speed-overlay";
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
  // name may be "City, Country" — try full string first, then country part
  if (COUNTRY_FLAGS[name]) return COUNTRY_FLAGS[name];
  const parts = name.split(", ");
  if (parts.length > 1) {
    return COUNTRY_FLAGS[parts[parts.length - 1]] ?? "";
  }
  return "";
}

const TRIAGE_OPTIONS: { value: TriageCategory; label: string; icon: typeof Ghost; color: string }[] = [
  { value: "missing_video", label: "Missing Video", icon: VideoOff, color: "text-blue-600" },
  { value: "missing_metadata", label: "Missing Metadata", icon: FileQuestion, color: "text-violet-600" },
  { value: "ghost", label: "Ghost", icon: Ghost, color: "text-red-600" },
  { value: "open_road", label: "Open Road", icon: Route, color: "text-amber-600" },
  { value: "signal", label: "Signal", icon: Zap, color: "text-green-600" },
];

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
  const { data: triageStatus, setTriage, removeTriage } = useTriageStatus(id, event?.type);
  const [copied, setCopied] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const [videoContainerHeight, setVideoContainerHeight] = useState<number | null>(null);
  const [cameraIntrinsics, setCameraIntrinsics] = useState<DevicesResponse | null>(null);
  const [videoCurrentTime, setVideoCurrentTime] = useState(0);
  const [videoDuration, setVideoDuration] = useState(0);
  const [isPlaying, setIsPlaying] = useState(false);
  const [isFullscreen, setIsFullscreen] = useState(false);
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

  // Fullscreen: escape key + body scroll lock
  useEffect(() => {
    if (!isFullscreen) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === "Escape") setIsFullscreen(false);
    };
    document.addEventListener("keydown", handler);
    document.body.style.overflow = "hidden";
    return () => {
      document.removeEventListener("keydown", handler);
      document.body.style.overflow = "";
    };
  }, [isFullscreen]);

  // Track video playback time
  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    let lastUpdate = 0;
    const handleTimeUpdate = () => {
      const now = performance.now();
      if (now - lastUpdate < 200) return; // Throttle to ~5Hz
      lastUpdate = now;
      setVideoCurrentTime(video.currentTime);
    };

    const handleLoadedMetadata = () => {
      setVideoDuration(video.duration || 0);
    };

    const handlePlay = () => setIsPlaying(true);
    const handlePause = () => setIsPlaying(false);

    video.addEventListener("timeupdate", handleTimeUpdate);
    video.addEventListener("loadedmetadata", handleLoadedMetadata);
    video.addEventListener("play", handlePlay);
    video.addEventListener("pause", handlePause);

    // Set initial duration if already loaded
    if (video.duration) {
      setVideoDuration(video.duration);
    }

    return () => {
      video.removeEventListener("timeupdate", handleTimeUpdate);
      video.removeEventListener("loadedmetadata", handleLoadedMetadata);
      video.removeEventListener("play", handlePlay);
      video.removeEventListener("pause", handlePause);
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

  // Detection runs
  const {
    runs: detectionRuns,
    activeRun: activeDetectionRun,
    mutate: mutateRuns,
  } = useDetectionRuns(id);

  // Detection timestamps for frame stepping and overlay
  const [selectedRunId, setSelectedRunId] = useState<string | undefined>(undefined);
  const [minConfidence, setMinConfidence] = useState(0.4);

  // Auto-select the latest completed run (switch when a new one finishes)
  useEffect(() => {
    const latestCompleted = detectionRuns?.find((r) => r.status === "completed");
    if (latestCompleted && latestCompleted.id !== selectedRunId) {
      setSelectedRunId(latestCompleted.id);
    }
  }, [detectionRuns, selectedRunId]);
  const { timestamps: detectionTimestamps, detectionsByFrame, segments: detectionSegments, sceneAttributes, timeline, mutate: mutateDetections } = useDetectionTimestamps(event?.videoUrl ? id : null, selectedRunId);

  // Summarize detections for clip summary
  const detectionSummary = useMemo(() => {
    if (!detectionsByFrame || detectionsByFrame.size === 0) return undefined;
    return summarizeDetections(detectionsByFrame, minConfidence);
  }, [detectionsByFrame, minConfidence]);

  // Time of day for clip summary
  const sunInfo = useMemo(() => {
    if (!event) return null;
    return getTimeOfDay(event.timestamp, event.location.lat, event.location.lon);
  }, [event]);

  // Log streaming for active or selected in-progress run
  const logRunId = activeDetectionRun?.id ?? null;
  const { logs } = useRunLogs(logRunId, id);

  // Auto-select the latest completed run when runs become available
  useEffect(() => {
    if (detectionRuns.length > 0 && selectedRunId === undefined) {
      const completedRun = detectionRuns.find((r) => r.status === "completed");
      if (completedRun) {
        setSelectedRunId(completedRun.id);
      }
    }
  }, [detectionRuns, selectedRunId]);

  const handleRunDetection = useCallback(
    async (modelName: string) => {
      await fetch(`/api/videos/${id}/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ modelName }),
      });
      mutateRuns();
    },
    [id, mutateRuns]
  );

  const handleCancelRun = useCallback(
    async (runId: string) => {
      await fetch(`/api/videos/${id}/runs/${runId}/cancel`, { method: "POST" });
      mutateRuns();
    },
    [id, mutateRuns]
  );

  // Auto-refresh detections when a detection run completes
  const prevActiveRunRef = useRef<string | null>(null);

  useEffect(() => {
    const prevId = prevActiveRunRef.current;
    const currentId = activeDetectionRun?.id ?? null;

    if (prevId && !currentId) {
      // A run just completed — switch to it and refresh detections
      setSelectedRunId(prevId);
      mutateDetections();
    }

    prevActiveRunRef.current = currentId;
  }, [activeDetectionRun?.id, mutateDetections]);

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
      const d = new Date(event.timestamp);
      const dateStr = `${(d.getMonth() + 1).toString().padStart(2, "0")}-${d.getDate().toString().padStart(2, "0")}-${d.getFullYear()}`;
      link.download = `event_${event.id}_${dateStr}.mp4`;
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

  const exceedsSpeedLimit = (() => {
    if (!nearestSpeedLimit || maxSpeed === null) return false;
    const maxMph = maxSpeed * 2.237;
    const limitMph = nearestSpeedLimit.unit === "km/h"
      ? nearestSpeedLimit.limit * 0.621371
      : nearestSpeedLimit.limit;
    return maxMph > limitMph;
  })();

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
        {triageStatus?.triage_result === "missing_video" && (
          <div className="mb-4 flex items-start gap-3 rounded-lg border border-blue-200 bg-blue-50 px-4 py-3 text-sm text-blue-800">
            <VideoOff className="w-5 h-5 mt-0.5 shrink-0 text-blue-500" />
            <div>
              <p className="font-medium">Missing Video</p>
              <p className="mt-0.5 text-blue-700/80">
                The video file for this event is missing or too small to be a valid recording.
                {triageStatus.rules_triggered && (
                  <span className="ml-1">
                    Rules: {JSON.parse(triageStatus.rules_triggered).join(", ")}.
                  </span>
                )}
              </p>
            </div>
          </div>
        )}
        {triageStatus?.triage_result === "missing_metadata" && (
          <div className="mb-4 flex items-start gap-3 rounded-lg border border-violet-200 bg-violet-50 px-4 py-3 text-sm text-violet-800">
            <FileQuestion className="w-5 h-5 mt-0.5 shrink-0 text-violet-500" />
            <div>
              <p className="font-medium">Missing Metadata</p>
              <p className="mt-0.5 text-violet-700/80">
                This event is missing GNSS and/or IMU telemetry data required for triage analysis.
                {triageStatus.rules_triggered && (
                  <span className="ml-1">
                    Rules: {JSON.parse(triageStatus.rules_triggered).join(", ")}.
                  </span>
                )}
              </p>
            </div>
          </div>
        )}
        {triageStatus?.triage_result === "ghost" && (
          <div className="mb-4 flex items-start gap-3 rounded-lg border border-red-200 bg-red-50 px-4 py-3 text-sm text-red-800">
            <Ghost className="w-5 h-5 mt-0.5 shrink-0 text-red-500" />
            <div>
              <p className="font-medium">Ghost Event — Likely False Positive</p>
              <p className="mt-0.5 text-red-700/80">
                Telemetry analysis indicates this event may not reflect a real driving incident.
                {triageStatus.rules_triggered && (
                  <span className="ml-1">
                    Rules: {JSON.parse(triageStatus.rules_triggered).join(", ")}.
                  </span>
                )}
              </p>
            </div>
          </div>
        )}
        {triageStatus?.triage_result === "open_road" && (
          <div className="mb-4 flex items-start gap-3 rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-800">
            <Route className="w-5 h-5 mt-0.5 shrink-0 text-amber-500" />
            <div>
              <p className="font-medium">Open Road — Low-Risk Context</p>
              <p className="mt-0.5 text-amber-700/80">
                This event occurred on an open road with stable, consistent driving at highway speeds.
                {triageStatus.rules_triggered && (
                  <span className="ml-1">
                    Rules: {JSON.parse(triageStatus.rules_triggered).join(", ")}.
                  </span>
                )}
              </p>
            </div>
          </div>
        )}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
          {/* Left column - Video */}
          <div className="space-y-4">
            {/* Video player */}
            <div ref={videoContainerRef} className={cn("overflow-hidden rounded-xl", isFullscreen && "fixed inset-0 z-50 bg-black flex items-center justify-center rounded-none")}>
              <div className={cn("relative aspect-video bg-black", isFullscreen && "w-full h-full aspect-auto")}>
                {isFullscreen && (
                  <Button
                    variant="ghost"
                    size="icon"
                    className="absolute top-4 right-4 z-10 text-white/70 hover:text-white hover:bg-white/20"
                    onClick={() => setIsFullscreen(false)}
                  >
                    <X className="w-5 h-5" />
                  </Button>
                )}
                {event.videoUrl ? (
                  <>
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
                    <DetectionOverlay
                      videoRef={videoRef}
                      isPlaying={isPlaying}
                      currentTime={videoCurrentTime}
                      timestamps={detectionTimestamps ?? []}
                      detectionsByFrame={detectionsByFrame ?? new Map()}
                      minConfidence={minConfidence}
                    />
                  </>
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
                <div className={cn(
                  "flex justify-end gap-1 px-3 py-1.5 bg-black/80",
                  isFullscreen && "absolute bottom-12 right-4 z-10 rounded-lg bg-black/60 backdrop-blur-sm"
                )}>
                  <Button
                    variant="ghost"
                    size="sm"
                    className="text-white/70 hover:text-white hover:bg-white/10"
                    onClick={() => setIsFullscreen((f) => !f)}
                  >
                    <Maximize2 className="w-4 h-4 mr-2" />
                    Fullscreen
                  </Button>
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
                <Select
                  value={triageStatus?.triage_result ?? "none"}
                  onValueChange={(val) => {
                    if (val === "none") {
                      removeTriage();
                    } else {
                      setTriage(val as TriageCategory);
                    }
                  }}
                >
                  <SelectTrigger
                    size="sm"
                    className={cn(
                      "rounded-full px-2.5 text-xs font-medium",
                      !triageStatus && "text-muted-foreground",
                      triageStatus?.triage_result === "missing_video" && "bg-blue-50 text-blue-700 border-blue-200",
                      triageStatus?.triage_result === "missing_metadata" && "bg-violet-50 text-violet-700 border-violet-200",
                      triageStatus?.triage_result === "ghost" && "bg-red-50 text-red-700 border-red-200",
                      triageStatus?.triage_result === "open_road" && "bg-amber-50 text-amber-700 border-amber-200",
                      triageStatus?.triage_result === "signal" && "bg-green-50 text-green-700 border-green-200",
                    )}
                  >
                    <SelectValue placeholder="Triage" />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="none">No Category</SelectItem>
                    {TRIAGE_OPTIONS.map((opt) => (
                      <SelectItem key={opt.value} value={opt.value}>
                        {opt.label}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
                {roadType?.classLabel && (
                  <Badge variant="outline">
                    {roadType.classLabel}
                  </Badge>
                )}
                {sunInfo && (() => {
                  const style = getTimeOfDayStyle(sunInfo.timeOfDay);
                  return (
                    <Badge variant="outline" className={cn(style.bgColor, style.color)}>
                      {sunInfo.timeOfDay}
                    </Badge>
                  );
                })()}
                {sceneAttributes?.weather && (
                  <Badge variant="outline" className="bg-sky-50 text-sky-700 border-sky-200">
                    {sceneAttributes.weather.value}
                    {sceneAttributes.weather.confidence !== null && (
                      <span className="ml-1 opacity-60">{Math.round(sceneAttributes.weather.confidence * 100)}%</span>
                    )}
                  </Badge>
                )}
              </div>
              {/* Details row */}
              <div className="flex flex-wrap items-center gap-x-1.5 text-xs text-muted-foreground">
                <span>{formatDateTime(event.timestamp, event.location.lon)}</span>
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

            <VideoVruPanel
              videoId={id}
              currentTime={videoCurrentTime}
              duration={videoDuration}
              isPlaying={isPlaying}
              detectionTimestamps={detectionTimestamps}
              detectionsByFrame={detectionsByFrame}
              minConfidence={minConfidence}
              onMinConfidenceChange={setMinConfidence}
              activeDetectionRun={activeDetectionRun}
              detectionRuns={detectionRuns}
              onRunDetection={handleRunDetection}
              selectedRunId={selectedRunId}
              onSelectRun={setSelectedRunId}
              onCancelRun={handleCancelRun}
              segments={detectionSegments}
              sceneAttributes={sceneAttributes}
              logs={logs}
              onRemoveSegment={async (label, startMs, endMs) => {
                const params = new URLSearchParams({ label, startMs: String(startMs), endMs: String(endMs) });
                if (selectedRunId) params.set("runId", selectedRunId);
                await fetch(`/api/videos/${id}/detections?${params}`, { method: "DELETE" });
                mutateDetections();
              }}
              onSeek={(time) => {
                if (videoRef.current) {
                  videoRef.current.currentTime = time;
                }
              }}
            />

            <ClipSummary
              videoId={id}
              event={event}
              countryName={countryName}
              roadType={roadType?.classLabel ?? null}
              roadName={roadType?.name ?? null}
              timeOfDay={sunInfo?.timeOfDay ?? "Day"}
              duration={videoDuration}
              detections={detectionSummary}
              speedLimit={nearestSpeedLimit}
              exceedsSpeedLimit={exceedsSpeedLimit}
              weather={sceneAttributes?.weather?.value ?? null}
              timeline={timeline}
            />

            {/* Speed Profile */}
            <div className="space-y-3 rounded-lg border bg-card px-4 py-3">
              <h3 className="text-lg font-semibold flex items-center gap-2">
                Speed Profile
                {nearestSpeedLimit && (
                  <Badge variant="outline" className="text-xs">
                    Limit: {nearestSpeedLimit.limit} {nearestSpeedLimit.unit}
                  </Badge>
                )}
              </h3>
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
            </div>

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
                    detectionSegments={detectionSegments}
                    className={videoContainerHeight ? "h-full" : "aspect-video"}
                    onSeek={(time) => {
                      if (videoRef.current) videoRef.current.currentTime = time;
                    }}
                  />
                </div>
              </CardContent>
            </Card>

            {/* Positioning section */}
            <PositioningSection eventId={id} gnssData={event.gnssData} />

            {/* Metadata table */}
            {event.metadata && Object.keys(event.metadata).length > 0 && (
              <CollapsibleSection title="Metadata">
                <MetadataTable metadata={event.metadata} eventId={id} />
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

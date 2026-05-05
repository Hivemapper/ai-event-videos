"use client";

import { memo, useEffect, useLayoutEffect, useState, use, useRef, useCallback, useMemo, useSyncExternalStore, type ComponentProps } from "react";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import {
  Activity,
  ArrowLeft,
  Check,
  Download,
  EyeOff,
  FileQuestion,
  Ghost,
  Loader2,
  Maximize2,
  Route,
  Scissors,
  Trophy,
  Upload,
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
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import dynamic from "next/dynamic";
import useSWR from "swr";

const EventMap = dynamic(
  () => import("@/components/map/event-map").then((m) => m.EventMap),
  {
    ssr: false,
    loading: () => <Skeleton className="aspect-video" />,
  }
);
import { ALL_EVENT_TYPES, EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getApiKey, getCameraIntrinsics, BEE_HFOV, DevicesResponse, getSpeedUnit, SpeedUnit } from "@/lib/api";
import { cn } from "@/lib/utils";
import { getTimeOfDay, getTimeOfDayStyle } from "@/lib/sun";
import { haversineDistance } from "@/lib/geo-utils";
import { useRoadType } from "@/hooks/use-road-type";
import { useTriageStatus, TriageCategory } from "@/hooks/use-triage-status";
import { useTopHits } from "@/lib/top-hits";
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
import {
  bucketLabel as frameTimingQcBucketLabel,
  FrameTimingQcPanel,
  probeFrameTimingQc,
  type FrameTimingQc,
} from "@/components/events/frame-timing-qc-panel";
import { VideoClipper } from "@/components/events/video-clipper";
import { summarizeDetections } from "@/lib/detection-summary";
import { DetectionOverlay } from "@/components/events/detection-overlay";
import { SpeedOverlay } from "@/components/events/speed-overlay";
import type { AIEventLocation, AIEventType, GnssDataPoint } from "@/types/events";
import type { FrameDetection, ProductionRun } from "@/types/pipeline";
import {
  SpeedDataPoint,
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

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
}

function formatBitrate(bytes: number, durationSec: number): string {
  const bps = (bytes * 8) / durationSec;
  if (bps >= 1_000_000) return `${(bps / 1_000_000).toFixed(2)} Mbps`;
  if (bps >= 1_000) return `${(bps / 1_000).toFixed(0)} kbps`;
  return `${bps.toFixed(0)} bps`;
}

function formatCompactDateTime(timestamp: string, lon?: number): string {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) return timestamp;

  let displayDate = date;
  let offsetLabel = "";

  if (lon !== undefined) {
    const offsetHours = Math.round(lon / 15);
    displayDate = new Date(date.getTime() + offsetHours * 60 * 60 * 1000);
    offsetLabel = ` UTC${offsetHours >= 0 ? "+" : ""}${offsetHours}`;
  }

  const datePart = displayDate.toLocaleDateString("en-US", {
    timeZone: "UTC",
    month: "numeric",
    day: "numeric",
    year: "2-digit",
  });
  const timePart = displayDate.toLocaleTimeString("en-US", {
    timeZone: "UTC",
    hour: "numeric",
    minute: "2-digit",
  });

  return `${datePart} ${timePart}${offsetLabel}`;
}

function parseFirmwareVersion(value: string | null): [number, number, number] | null {
  if (!value) return null;
  const match = value.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function isFrameQualityFirmwareEligible(value: string | null): boolean {
  const version = parseFirmwareVersion(value);
  if (!version) return false;
  const minimum = [7, 4, 3] as const;
  for (let i = 0; i < minimum.length; i += 1) {
    if (version[i] > minimum[i]) return true;
    if (version[i] < minimum[i]) return false;
  }
  return true;
}

function countryFlag(name: string): string {
  // name may be "City, Country" — try full string first, then country part
  if (COUNTRY_FLAGS[name]) return COUNTRY_FLAGS[name];
  const parts = name.split(", ");
  if (parts.length > 1) {
    return COUNTRY_FLAGS[parts[parts.length - 1]] ?? "";
  }
  return "";
}

function isAIEventType(value: string | null | undefined): value is AIEventType {
  return typeof value === "string" && (ALL_EVENT_TYPES as readonly string[]).includes(value);
}

function isFiniteCoordinate(location: AIEventLocation | null | undefined): location is AIEventLocation {
  return (
    Boolean(location) &&
    Number.isFinite(location?.lat) &&
    Number.isFinite(location?.lon)
  );
}

function clampPlaybackTime(value: number, duration: number): number {
  if (!Number.isFinite(value)) return 0;
  if (!Number.isFinite(duration) || duration <= 0) return Math.max(value, 0);
  return Math.min(Math.max(value, 0), duration);
}

function timeFromGnssPoint(point: GnssDataPoint, firstTimestamp: number): number | null {
  const timestamp = Number(point.timestamp);
  if (!Number.isFinite(timestamp) || !Number.isFinite(firstTimestamp)) return null;
  return (timestamp - firstTimestamp) / 1000;
}

function deriveIncidentPlaybackTime({
  location,
  timestamp,
  gnssData,
  duration,
}: {
  location: AIEventLocation;
  timestamp: string;
  gnssData?: GnssDataPoint[];
  duration: number;
}): number | null {
  if (!gnssData?.length) return null;

  const firstTimestamp = Number(gnssData[0]?.timestamp);
  if (!Number.isFinite(firstTimestamp)) return null;

  if (isFiniteCoordinate(location)) {
    let bestTime: number | null = null;
    let bestDistance = Number.POSITIVE_INFINITY;

    for (const point of gnssData) {
      if (!isFiniteCoordinate(point)) continue;
      const pointTime = timeFromGnssPoint(point, firstTimestamp);
      if (pointTime === null) continue;
      const distance = haversineDistance(location.lat, location.lon, point.lat, point.lon);
      if (distance < bestDistance) {
        bestDistance = distance;
        bestTime = pointTime;
      }
    }

    if (bestTime !== null) {
      return clampPlaybackTime(bestTime, duration);
    }
  }

  const eventTimestamp = Date.parse(timestamp);
  if (!Number.isNaN(eventTimestamp)) {
    return clampPlaybackTime((eventTimestamp - firstTimestamp) / 1000, duration);
  }

  return null;
}

function formatIncidentTime(seconds: number): string {
  const safeSeconds = Math.max(seconds, 0);
  const minutes = Math.floor(safeSeconds / 60);
  const remaining = safeSeconds - minutes * 60;
  return `${minutes}:${remaining.toFixed(1).padStart(4, "0")}`;
}

function IncidentVideoMarker({
  timeSeconds,
  durationSeconds,
  onSeek,
}: {
  timeSeconds: number | null;
  durationSeconds: number;
  onSeek: (time: number) => void;
}) {
  if (timeSeconds === null || !Number.isFinite(timeSeconds)) return null;
  const markerTime = clampPlaybackTime(timeSeconds, durationSeconds);
  const progress = durationSeconds > 0 ? Math.min(Math.max(markerTime / durationSeconds, 0), 1) : 0;

  return (
    <div className="pointer-events-none absolute bottom-6 left-14 right-8 z-20 h-4">
      <button
        type="button"
        className="pointer-events-auto absolute top-1/2 h-4 w-4 -translate-x-1/2 -translate-y-1/2 rounded-full border-2 border-white bg-red-500 shadow-[0_0_0_3px_rgba(239,68,68,0.25)] outline-none transition-transform hover:scale-110 focus-visible:ring-2 focus-visible:ring-white/80"
        style={{ left: `${progress * 100}%` }}
        onClick={() => onSeek(markerTime)}
        aria-label={`Seek to incident location at ${formatIncidentTime(markerTime)}`}
        title={`Incident location: ${formatIncidentTime(markerTime)}`}
      >
        <span className="sr-only">Incident location</span>
      </button>
    </div>
  );
}

const TRIAGE_OPTIONS: { value: TriageCategory; label: string; icon: typeof Ghost; color: string }[] = [
  { value: "missing_video", label: "Missing Video", icon: VideoOff, color: "text-blue-600" },
  { value: "missing_metadata", label: "Missing Metadata", icon: FileQuestion, color: "text-violet-600" },
  { value: "ghost", label: "Ghost", icon: Ghost, color: "text-red-600" },
  { value: "open_road", label: "Open Road", icon: Route, color: "text-amber-600" },
  { value: "signal", label: "Signal", icon: Zap, color: "text-green-600" },
  { value: "non_linear", label: "Non Linear", icon: Activity, color: "text-teal-600" },
  { value: "privacy", label: "Privacy", icon: EyeOff, color: "text-indigo-600" },
];

interface ProductionPipelineResponse {
  run: ProductionRun | null;
  created?: boolean;
  prioritized?: boolean;
  requeued?: boolean;
  error?: string;
}

interface PlaybackSnapshot {
  currentTime: number;
  duration: number;
  isPlaying: boolean;
}

type TelemetryTab = "map" | "positioning" | "fps";

function createPlaybackStore() {
  let snapshot: PlaybackSnapshot = {
    currentTime: 0,
    duration: 0,
    isPlaying: false,
  };
  const listeners = new Set<() => void>();

  return {
    getSnapshot: () => snapshot,
    subscribe: (listener: () => void) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    set: (patch: Partial<PlaybackSnapshot>) => {
      const next = { ...snapshot, ...patch };
      if (
        next.currentTime === snapshot.currentTime &&
        next.duration === snapshot.duration &&
        next.isPlaying === snapshot.isPlaying
      ) {
        return;
      }
      snapshot = next;
      listeners.forEach((listener) => listener());
    },
  };
}

type PlaybackStore = ReturnType<typeof createPlaybackStore>;

function usePlaybackSnapshot(store: PlaybackStore): PlaybackSnapshot {
  return useSyncExternalStore(store.subscribe, store.getSnapshot, store.getSnapshot);
}

const PlaybackDetectionOverlay = memo(function PlaybackDetectionOverlay({
  playbackStore,
  videoRef,
  timestamps,
  detectionsByFrame,
  minConfidence,
}: {
  playbackStore: PlaybackStore;
  videoRef: React.RefObject<HTMLVideoElement | null>;
  timestamps: number[];
  detectionsByFrame: Map<number, FrameDetection[]>;
  minConfidence: number;
}) {
  const { currentTime, isPlaying } = usePlaybackSnapshot(playbackStore);
  return (
    <DetectionOverlay
      videoRef={videoRef}
      isPlaying={isPlaying}
      currentTime={currentTime}
      timestamps={timestamps}
      detectionsByFrame={detectionsByFrame}
      minConfidence={minConfidence}
    />
  );
});

const PlaybackSpeedOverlay = memo(function PlaybackSpeedOverlay({
  playbackStore,
  speedData,
  duration,
  unit,
  speedLimit,
}: {
  playbackStore: PlaybackStore;
  speedData: SpeedDataPoint[];
  duration: number;
  unit: SpeedUnit;
  speedLimit?: { limit: number; unit: string } | null;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return (
    <SpeedOverlay
      speedData={speedData}
      currentTime={currentTime}
      duration={duration}
      unit={unit}
      speedLimit={speedLimit}
    />
  );
});

const PlaybackVideoVruPanel = memo(function PlaybackVideoVruPanel({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof VideoVruPanel>, "currentTime" | "isPlaying"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime, isPlaying } = usePlaybackSnapshot(playbackStore);
  return (
    <VideoVruPanel
      {...props}
      currentTime={currentTime}
      isPlaying={isPlaying}
    />
  );
});

const PlaybackSpeedProfileChart = memo(function PlaybackSpeedProfileChart({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof SpeedProfileChart>, "currentTime"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return <SpeedProfileChart {...props} currentTime={currentTime} />;
});

const PlaybackEventMap = memo(function PlaybackEventMap({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof EventMap>, "currentTime"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return <EventMap {...props} currentTime={currentTime} />;
});

const PlaybackPositioningSection = memo(function PlaybackPositioningSection({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof PositioningSection>, "currentTime"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return <PositioningSection {...props} currentTime={currentTime} />;
});

const PlaybackFrameTimingQcPanel = memo(function PlaybackFrameTimingQcPanel({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof FrameTimingQcPanel>, "currentTime"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return <FrameTimingQcPanel {...props} currentTime={currentTime} />;
});

const PlaybackVideoClipper = memo(function PlaybackVideoClipper({
  playbackStore,
  ...props
}: Omit<ComponentProps<typeof VideoClipper>, "currentTime"> & {
  playbackStore: PlaybackStore;
}) {
  const { currentTime } = usePlaybackSnapshot(playbackStore);
  return <VideoClipper {...props} currentTime={currentTime} />;
});

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
  const { data: triageStatus, setTriage, setEventType, removeTriage } = useTriageStatus(id, event?.type);
  const { has: hasTopHit, toggle: toggleTopHit } = useTopHits();
  const isTopHit = hasTopHit(id);
  const [manualEventType, setManualEventType] = useState<AIEventType | null>(null);
  const [copied, setCopied] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const videoContainerRef = useRef<HTMLDivElement>(null);
  const frameQualityRef = useRef<HTMLDivElement>(null);
  const [videoContainerHeight, setVideoContainerHeight] = useState<number | null>(null);
  const [cameraIntrinsics, setCameraIntrinsics] = useState<DevicesResponse | null>(null);
  const [videoDuration, setVideoDuration] = useState(0);
  const [isFullscreen, setIsFullscreen] = useState(false);
  const [isDownloading, setIsDownloading] = useState(false);
  const [showClipper, setShowClipper] = useState(false);
  const [activeTelemetryTab, setActiveTelemetryTab] = useState<TelemetryTab>("map");
  const [isFrameQualityProbing, setIsFrameQualityProbing] = useState(false);
  const [frameQualityProbeError, setFrameQualityProbeError] = useState<string | null>(null);
  const frameQualityProbeRef = useRef<Promise<void> | null>(null);
  const firmwareVersion = typeof event?.metadata?.FIRMWARE_VERSION === "string"
    ? event.metadata.FIRMWARE_VERSION
    : null;
  const canRunFrameQuality = isFrameQualityFirmwareEligible(firmwareVersion);

  const {
    data: frameTimingQcData,
    isLoading: isFrameTimingQcLoading,
    mutate: mutateFrameTimingQc,
  } = useSWR<{ qc: FrameTimingQc | null }>(
    `/api/videos/${id}/frame-timing-qc`,
    (url: string) => fetch(url).then((r) => r.json()),
    { revalidateOnFocus: false, revalidateOnReconnect: false }
  );
  const frameTimingQc = frameTimingQcData?.qc ?? null;
  const [speedUnit] = useState<SpeedUnit>(() => getSpeedUnit());
  const [videoBytes, setVideoBytes] = useState<number | null>(null);
  const playbackStore = useMemo(() => createPlaybackStore(), []);

  useEffect(() => {
    setManualEventType(null);
  }, [id, triageStatus?.event_type]);

  // Measure video container height to sync the telemetry card and next row.
  useLayoutEffect(() => {
    const el = videoContainerRef.current;
    if (!el) return;

    const measure = () => {
      setVideoContainerHeight(el.getBoundingClientRect().height);
    };

    measure();
    const frame = window.requestAnimationFrame(measure);
    const observer = new ResizeObserver(measure);
    observer.observe(el);
    window.addEventListener("resize", measure);

    return () => {
      window.cancelAnimationFrame(frame);
      observer.disconnect();
      window.removeEventListener("resize", measure);
    };
  }, [event?.videoUrl, canRunFrameQuality, showClipper]);

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
      playbackStore.set({ currentTime: video.currentTime });
    };

    const handleLoadedMetadata = () => {
      const duration = video.duration || 0;
      setVideoDuration(duration);
      playbackStore.set({ duration });
    };

    const handlePlay = () => playbackStore.set({ isPlaying: true });
    const handlePause = () => playbackStore.set({ isPlaying: false });

    video.addEventListener("timeupdate", handleTimeUpdate);
    video.addEventListener("loadedmetadata", handleLoadedMetadata);
    video.addEventListener("play", handlePlay);
    video.addEventListener("pause", handlePause);

    // Set initial duration if already loaded
    if (video.duration) {
      setVideoDuration(video.duration);
      playbackStore.set({ duration: video.duration });
    }

    return () => {
      video.removeEventListener("timeupdate", handleTimeUpdate);
      video.removeEventListener("loadedmetadata", handleLoadedMetadata);
      video.removeEventListener("play", handlePlay);
      video.removeEventListener("pause", handlePause);
    };
  }, [event, playbackStore]); // Re-run when event loads (video element gets src)

  // Load camera intrinsics from localStorage
  useEffect(() => {
    const intrinsics = getCameraIntrinsics();
    setCameraIntrinsics(intrinsics);
  }, []);

  // Fetch video file size for bitrate display
  useEffect(() => {
    setVideoBytes(null);
    const videoUrl = event?.videoUrl;
    if (!videoUrl) return;
    const controller = new AbortController();
    const proxyUrl = getProxyVideoUrl(videoUrl);
    (async () => {
      try {
        // Use a 1-byte Range GET — the video proxy returns Content-Range: bytes 0-0/<total>
        const res = await fetch(proxyUrl, {
          headers: { Range: "bytes=0-0" },
          signal: controller.signal,
        });
        // Drain/cancel body to avoid leaking a stream
        res.body?.cancel().catch(() => {});
        const cr = res.headers.get("content-range");
        if (cr) {
          const total = cr.split("/").pop();
          if (total && total !== "*") {
            const n = parseInt(total, 10);
            if (!isNaN(n) && n > 0) setVideoBytes(n);
          }
        } else {
          const len = res.headers.get("content-length");
          if (len) {
            const n = parseInt(len, 10);
            if (!isNaN(n) && n > 1024) setVideoBytes(n);
          }
        }
      } catch {
        // ignore (abort or network error)
      }
    })();
    return () => controller.abort();
  }, [event?.videoUrl]);

  // Fetch road type from Mapbox (samples multiple GNSS points for accuracy)
  const { roadType } = useRoadType(
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
  const emptyDetectionsByFrame = useMemo(() => new Map<number, FrameDetection[]>(), []);

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
  const completedDetectionRun = useMemo(
    () => detectionRuns.find((run) => run.status === "completed") ?? null,
    [detectionRuns]
  );
  const [productionRun, setProductionRun] = useState<ProductionRun | null>(null);
  const [productionLoading, setProductionLoading] = useState(true);
  const [productionActionPending, setProductionActionPending] = useState(false);
  const [productionNotice, setProductionNotice] = useState<{
    type: "success" | "error";
    message: string;
  } | null>(null);

  const refreshProductionStatus = useCallback(
    async ({ silent = false }: { silent?: boolean } = {}) => {
      if (!silent) setProductionLoading(true);
      try {
        const response = await fetch(`/api/videos/${id}/production-pipeline`, {
          cache: "no-store",
        });
        const result = (await response.json()) as ProductionPipelineResponse;
        if (!response.ok) {
          throw new Error(result.error ?? "Failed to load production status");
        }
        setProductionRun(result.run ?? null);
      } catch (error) {
        setProductionNotice({
          type: "error",
          message: error instanceof Error ? error.message : String(error),
        });
      } finally {
        if (!silent) setProductionLoading(false);
      }
    },
    [id]
  );

  useEffect(() => {
    refreshProductionStatus();
  }, [refreshProductionStatus]);

  useEffect(() => {
    if (productionRun?.status !== "queued" && productionRun?.status !== "processing") {
      return;
    }

    const interval = window.setInterval(() => {
      refreshProductionStatus({ silent: true });
    }, 5000);

    return () => window.clearInterval(interval);
  }, [productionRun?.status, refreshProductionStatus]);

  const handleRunProduction = useCallback(async () => {
    setProductionActionPending(true);
    setProductionNotice(null);

    try {
      const response = await fetch(`/api/videos/${id}/production-pipeline`, {
        method: "POST",
      });
      const result = (await response.json()) as ProductionPipelineResponse;
      if (!response.ok) {
        throw new Error(result.error ?? "Failed to run production");
      }

      setProductionRun(result.run ?? null);
      const status = result.run?.status;
      const message =
        status === "completed"
          ? "Production is already complete."
          : status === "processing"
            ? "Production is already processing."
            : result.requeued
              ? "Requeued as the next production item."
              : result.created
                ? "Queued as the next production item."
                : result.prioritized
                  ? "Moved to the next production slot."
                  : "Already in the priority production queue.";
      setProductionNotice({ type: "success", message });
    } catch (error) {
      setProductionNotice({
        type: "error",
        message: error instanceof Error ? error.message : String(error),
      });
    } finally {
      setProductionActionPending(false);
    }
  }, [id]);

  const productionCompleted = productionRun?.status === "completed";
  const productionProcessing = productionRun?.status === "processing";
  const visibleProductionNotice =
    productionNotice &&
    !(
      productionNotice.type === "success" &&
      (productionCompleted || productionProcessing || productionRun?.status === "failed")
    )
      ? productionNotice
      : null;
  const hasCompletedVru = Boolean(completedDetectionRun);
  const showProductionPanel = hasCompletedVru || Boolean(productionRun);
  const canRunProduction =
    hasCompletedVru && !productionProcessing && !productionCompleted;
  const productionButtonDisabled =
    productionLoading || productionActionPending || !canRunProduction;
  const productionButtonTitle = !hasCompletedVru
    ? "Run VRU detections before production"
    : productionCompleted
      ? "Production has completed for this event"
      : productionProcessing
        ? "Production is already running for this event"
        : productionRun?.status === "queued"
          ? "Move this event to the front of the production queue"
          : "Push this event into the production pipeline";

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
      const response = await fetch(`/api/videos/${id}/runs`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ modelName }),
      });
      const result = await response.json().catch(() => ({}));
      if (!response.ok) {
        throw new Error(
          typeof result.error === "string"
            ? result.error
            : "Failed to start detection run"
        );
      }
      await mutateRuns();
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

  const copyCoordinates = async () => {
    if (!event) return;
    const coords = `${event.location.lat}, ${event.location.lon}`;
    await navigator.clipboard.writeText(coords);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  const triggerDownload = (blob: Blob, filename: string) => {
    const url = URL.createObjectURL(blob);
    const link = document.createElement("a");
    link.href = url;
    link.download = filename;
    link.click();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
  };

  const getDownloadDate = () => {
    if (!event) return "";
    const d = new Date(event.timestamp);
    return `${(d.getMonth() + 1).toString().padStart(2, "0")}-${d.getDate().toString().padStart(2, "0")}-${d.getFullYear()}`;
  };

  const downloadVideo = async () => {
    if (!event?.videoUrl) return;
    setIsDownloading(true);
    const dateStr = getDownloadDate();
    try {
      const response = await fetch(getProxyVideoUrl(event.videoUrl));
      if (!response.ok) throw new Error(`Video download failed: ${response.status}`);
      const blob = await response.blob();
      triggerDownload(blob, `event_${event.id}_${dateStr}.mp4`);
    } catch {
      // Fallback: open in new tab
      window.open(getProxyVideoUrl(event.videoUrl), "_blank");
      setIsDownloading(false);
      return;
    }

    try {
      const headers: Record<string, string> = {};
      const apiKey = getApiKey();
      if (apiKey) headers.Authorization = apiKey;

      const metadataResponse = await fetch(
        `/api/events/${event.id}/production-metadata`,
        { headers }
      );
      if (!metadataResponse.ok) {
        const errorBody = await metadataResponse.json().catch(() => null);
        throw new Error(errorBody?.error ?? `Metadata download failed: ${metadataResponse.status}`);
      }
      const metadataBlob = await metadataResponse.blob();
      triggerDownload(metadataBlob, `event_${event.id}_${dateStr}_metadata.json`);
    } catch (error) {
      console.error("Metadata download failed:", error);
    } finally {
      setIsDownloading(false);
    }
  };

  const showFrameQualityDetails = useCallback(
    (scroll = true) => {
      setActiveTelemetryTab("fps");
      if (isFullscreen) {
        setIsFullscreen(false);
      }
      if (scroll) {
        window.setTimeout(() => {
          frameQualityRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
        }, 0);
      }
    },
    [isFullscreen]
  );

  const runFrameQuality = useCallback(
    async ({
      reveal = true,
      scroll = reveal,
    }: {
      reveal?: boolean;
      scroll?: boolean;
    } = {}) => {
      if (reveal) {
        showFrameQualityDetails(scroll);
      }

      if (!event?.videoUrl || !canRunFrameQuality || frameTimingQc) {
        return;
      }

      if (frameQualityProbeRef.current) {
        return frameQualityProbeRef.current;
      }

      setIsFrameQualityProbing(true);
      setFrameQualityProbeError(null);
      const probePromise = probeFrameTimingQc(id, event.videoUrl, firmwareVersion)
        .then((result) => {
          mutateFrameTimingQc(result, false);
        })
        .catch((error) => {
          setFrameQualityProbeError(error instanceof Error ? error.message : String(error));
        })
        .finally(() => {
          frameQualityProbeRef.current = null;
          setIsFrameQualityProbing(false);
        });

      frameQualityProbeRef.current = probePromise;
      return probePromise;
    },
    [
      canRunFrameQuality,
      event?.videoUrl,
      firmwareVersion,
      frameTimingQc,
      id,
      mutateFrameTimingQc,
      showFrameQualityDetails,
    ]
  );

  const frameQualityButtonLabel =
    frameTimingQc?.probe_status === "failed"
      ? "Failed"
      : frameTimingQc
        ? frameTimingQcBucketLabel(frameTimingQc.bucket)
        : frameQualityProbeError
          ? "Failed"
          : isFrameQualityProbing || isFrameTimingQcLoading
            ? "Checking"
            : "Check";
  const frameQualityButtonTitle = event?.videoUrl
    ? frameTimingQc
      ? `Frame quality QC: ${frameQualityButtonLabel}`
      : frameQualityProbeError
        ? `Frame quality QC failed: ${frameQualityProbeError}`
        : isFrameQualityProbing || isFrameTimingQcLoading
          ? "Checking frame quality QC"
          : "Run frame quality QC"
    : "Video required for frame quality QC";
  const rawSpeedData = event?.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const overlaySpeedData = useMemo(() => {
    const metadataSpeedData = Array.isArray(rawSpeedData) ? rawSpeedData : [];
    if (metadataSpeedData.length > 0) return metadataSpeedData;
    return event?.gnssData ? deriveSpeedFromGnss(event.gnssData) : [];
  }, [event?.gnssData, rawSpeedData]);
  const incidentTimeSeconds = useMemo(
    () => (
      event
        ? deriveIncidentPlaybackTime({
            location: event.location,
            timestamp: event.timestamp,
            gnssData: event.gnssData,
            duration: videoDuration,
          })
        : null
    ),
    [event, videoDuration]
  );
  const seekToIncident = useCallback(() => {
    if (incidentTimeSeconds === null) return;
    if (videoRef.current) {
      videoRef.current.currentTime = incidentTimeSeconds;
    }
    playbackStore.set({ currentTime: incidentTimeSeconds });
  }, [incidentTimeSeconds, playbackStore]);
  const constrainTelemetryHeight = true;

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

  const selectedEventType = manualEventType
    ?? (isAIEventType(triageStatus?.event_type) ? triageStatus.event_type : event.type);
  const config = EVENT_TYPE_CONFIG[selectedEventType] || EVENT_TYPE_CONFIG.UNKNOWN;

  const speedData = Array.isArray(rawSpeedData) ? rawSpeedData : undefined;
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
                    <PlaybackDetectionOverlay
                      playbackStore={playbackStore}
                      videoRef={videoRef}
                      timestamps={detectionTimestamps ?? []}
                      detectionsByFrame={detectionsByFrame ?? emptyDetectionsByFrame}
                      minConfidence={minConfidence}
                    />
                  </>
                ) : (
                  <div className="w-full h-full flex items-center justify-center text-muted-foreground">
                    No video available
                  </div>
                )}
                {overlaySpeedData.length > 0 && (
                  <PlaybackSpeedOverlay
                    playbackStore={playbackStore}
                    speedData={overlaySpeedData}
                    duration={videoDuration}
                    unit={speedUnit}
                    speedLimit={nearestSpeedLimit}
                  />
                )}
                {event.videoUrl && (
                  <IncidentVideoMarker
                    timeSeconds={incidentTimeSeconds}
                    durationSeconds={videoDuration}
                    onSeek={seekToIncident}
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
                    size="icon-sm"
                    className={cn(
                      "h-9 w-9 text-white/70 hover:text-white hover:bg-white/10",
                      isTopHit && "text-amber-400 hover:text-amber-300"
                    )}
                    onClick={() => toggleTopHit(id)}
                    aria-label={isTopHit ? "Remove from Top Hits" : "Add to Top Hits"}
                    title={isTopHit ? "Remove from Top Hits" : "Add to Top Hits"}
                  >
                    <Trophy className={cn("w-4 h-4", isTopHit && "fill-current")} />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className={cn(
                      "h-9 w-9 text-white/70 hover:text-white hover:bg-white/10",
                      showClipper && "text-white bg-white/10"
                    )}
                    onClick={() => setShowClipper((visible) => !visible)}
                    aria-label={showClipper ? "Hide clipper" : "Clip video and metadata"}
                    title={showClipper ? "Hide clipper" : "Clip video and metadata"}
                  >
                    <Scissors className="w-4 h-4" />
                  </Button>
                  {canRunFrameQuality && (
                    <Button
                      variant="ghost"
                      size="icon-sm"
                      className="h-9 w-9 text-white/70 hover:text-white hover:bg-white/10"
                      onClick={() => void runFrameQuality()}
                      disabled={!event.videoUrl}
                      aria-label={frameQualityButtonTitle}
                      title={frameQualityButtonTitle}
                    >
                      {isFrameQualityProbing || isFrameTimingQcLoading ? (
                        <Loader2 className="w-4 h-4 animate-spin" />
                      ) : (
                        <Activity className="w-4 h-4" />
                      )}
                    </Button>
                  )}
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className={cn(
                      "h-9 w-9 text-white/70 hover:text-white hover:bg-white/10",
                      productionRun?.status === "queued" && "text-amber-300 hover:text-amber-200",
                      productionCompleted && "text-green-300 hover:text-green-200"
                    )}
                    onClick={handleRunProduction}
                    disabled={productionButtonDisabled}
                    aria-label={productionButtonTitle}
                    title={productionButtonTitle}
                  >
                    {productionLoading || productionActionPending || productionProcessing ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : productionCompleted ? (
                      <Check className="w-4 h-4" />
                    ) : (
                      <Upload className="w-4 h-4" />
                    )}
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className="h-9 w-9 text-white/70 hover:text-white hover:bg-white/10"
                    onClick={() => setIsFullscreen((f) => !f)}
                    aria-label="Expand video"
                    title="Expand video"
                  >
                    <Maximize2 className="w-4 h-4" />
                  </Button>
                  <Button
                    variant="ghost"
                    size="icon-sm"
                    className="h-9 w-9 text-white/70 hover:text-white hover:bg-white/10"
                    onClick={downloadVideo}
                    disabled={isDownloading}
                    aria-label={isDownloading ? "Downloading video" : "Download video"}
                    title={isDownloading ? "Downloading video" : "Download video"}
                  >
                    {isDownloading ? (
                      <Loader2 className="w-4 h-4 animate-spin" />
                    ) : (
                      <Download className="w-4 h-4" />
                    )}
                  </Button>
                </div>
              )}
            </div>

            {showClipper && event.videoUrl && (
              <PlaybackVideoClipper
                playbackStore={playbackStore}
                eventId={id}
                duration={videoDuration}
              />
            )}

            <div className="space-y-4 rounded-xl border bg-card px-4 py-3 shadow-sm">
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <Select
                  value={selectedEventType}
                  onValueChange={(val) => {
                    const nextType = val as AIEventType;
                    setManualEventType(nextType);
                    void setEventType(nextType);
                  }}
                >
                  <SelectTrigger
                    size="sm"
                    className={cn(
                      "w-auto rounded-full border px-2.5 text-xs font-medium",
                      config.bgColor,
                      config.color,
                      config.borderColor
                    )}
                    title="Event type"
                  >
                    <SelectValue placeholder="Type" />
                  </SelectTrigger>
                  <SelectContent>
                    {ALL_EVENT_TYPES.map((type) => {
                      const typeConfig = EVENT_TYPE_CONFIG[type];
                      const TypeIcon = typeConfig.icon;

                      return (
                        <SelectItem key={type} value={type} textValue={typeConfig.label}>
                          <TypeIcon className={cn("w-4 h-4", typeConfig.color)} />
                          {typeConfig.label}
                        </SelectItem>
                      );
                    })}
                  </SelectContent>
                </Select>

                <Select
                  value={triageStatus?.triage_result ?? "none"}
                  onValueChange={(val) => {
                    if (val === "none") {
                      void removeTriage();
                    } else {
                      void setTriage(val as TriageCategory, selectedEventType);
                    }
                  }}
                >
                  <SelectTrigger
                    size="sm"
                    className={cn(
                      "w-auto rounded-full border px-2.5 text-xs font-medium",
                      !triageStatus && "text-muted-foreground",
                      triageStatus?.triage_result === "missing_video" && "bg-blue-50 text-blue-700 border-blue-200",
                      triageStatus?.triage_result === "missing_metadata" && "bg-violet-50 text-violet-700 border-violet-200",
                      triageStatus?.triage_result === "ghost" && "bg-red-50 text-red-700 border-red-200",
                      triageStatus?.triage_result === "open_road" && "bg-amber-50 text-amber-700 border-amber-200",
                      triageStatus?.triage_result === "signal" && "bg-green-50 text-green-700 border-green-200",
                      triageStatus?.triage_result === "non_linear" && "bg-teal-50 text-teal-700 border-teal-200",
                      triageStatus?.triage_result === "privacy" && "bg-indigo-50 text-indigo-700 border-indigo-200",
                    )}
                    title="Triage category"
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
                  <Badge variant="outline" className="bg-sky-50 text-sky-700 border-sky-200">
                    {roadType.classLabel}
                  </Badge>
                )}
                {sunInfo && (
                  <Badge
                    variant="outline"
                    className={cn(getTimeOfDayStyle(sunInfo.timeOfDay).bgColor, getTimeOfDayStyle(sunInfo.timeOfDay).color)}
                  >
                    {sunInfo.timeOfDay}
                  </Badge>
                )}
                {sceneAttributes?.weather && (
                  <Badge variant="outline" className="bg-sky-50 text-sky-700 border-sky-200">
                    {sceneAttributes.weather.confidence !== null
                      ? `${sceneAttributes.weather.value} ${Math.round(sceneAttributes.weather.confidence * 100)}%`
                      : sceneAttributes.weather.value}
                  </Badge>
                )}
                <a
                  href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="inline-flex min-w-0 max-w-full items-center gap-1 rounded-full border bg-background px-2.5 py-1 text-xs font-medium text-muted-foreground shadow-sm transition-colors hover:text-primary"
                  title={countryName ?? "Google Maps"}
                >
                  <span className="truncate">
                    {countryName ? `${countryFlag(countryName)} ${countryName}` : "Google Maps"}
                  </span>
                </a>
              </div>

              <div className="grid grid-cols-2 gap-x-5 gap-y-4 text-sm sm:grid-cols-3 xl:grid-cols-4">
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Date</p>
                  <p
                    className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight"
                    title={formatCompactDateTime(event.timestamp, event.location.lon)}
                  >
                    {formatCompactDateTime(event.timestamp, event.location.lon)}
                  </p>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Coordinates</p>
                  <button
                    onClick={copyCoordinates}
                    className="block max-w-full truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight hover:text-primary"
                    title={`Copy ${formatCoordinates(event.location.lat, event.location.lon)}`}
                  >
                    {event.location.lat.toFixed(4)},{event.location.lon.toFixed(4)}
                    {copied && <Check className="ml-1 inline h-3 w-3 text-green-500" />}
                  </button>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Video Size</p>
                  <p className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight">
                    {videoBytes === null ? "-" : formatBytes(videoBytes)}
                  </p>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Bitrate</p>
                  <p className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight">
                    {videoBytes !== null && videoDuration > 0 ? formatBitrate(videoBytes, videoDuration) : "-"}
                  </p>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Max</p>
                  <p className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight">
                    {maxSpeed === null ? "-" : formatSpeed(maxSpeed)}
                  </p>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Acceleration</p>
                  <p className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight">
                    {acceleration === undefined ? "-" : `${acceleration.toFixed(2)} m/s²`}
                  </p>
                </div>
                <div className="min-w-0">
                  <p className="text-xs font-medium text-muted-foreground">Firmware</p>
                  <p className="truncate whitespace-nowrap font-mono text-[13px] leading-tight tracking-tight">
                    {firmwareVersion ?? "-"}
                  </p>
                </div>
              </div>
            </div>

            <PlaybackVideoVruPanel
              playbackStore={playbackStore}
              videoId={id}
              duration={videoDuration}
              detectionTimestamps={detectionTimestamps}
              detectionsByFrame={detectionsByFrame}
              minConfidence={minConfidence}
              onMinConfidenceChange={setMinConfidence}
              activeDetectionRun={activeDetectionRun}
              detectionRuns={detectionRuns}
              runDeviceLabel="AWS GPU"
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

            {showProductionPanel && (
              <div className="rounded-lg border bg-card px-4 py-3">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                  <div className="space-y-1">
                    <div className="flex flex-wrap items-center gap-2">
                      <h3 className="text-sm font-semibold">Production Pipeline</h3>
                      {productionLoading ? (
                        <Badge variant="outline" className="gap-1">
                          <Loader2 className="w-3 h-3 animate-spin" />
                          Checking
                        </Badge>
                      ) : productionProcessing ? (
                        <Badge
                          variant="outline"
                          className="bg-blue-50 text-blue-700 border-blue-200 gap-1"
                        >
                          <Loader2 className="w-3 h-3 animate-spin" />
                          Processing
                        </Badge>
                      ) : productionRun?.status === "queued" ? (
                        <Badge
                          variant="outline"
                          className="bg-amber-50 text-amber-700 border-amber-200"
                        >
                          {productionRun.priority === 0 ? "Priority Queue" : "Queued"}
                        </Badge>
                      ) : productionCompleted ? (
                        <Badge
                          variant="outline"
                          className="bg-green-50 text-green-700 border-green-200"
                        >
                          Complete
                        </Badge>
                      ) : productionRun?.status === "failed" ? (
                        <Badge
                          variant="outline"
                          className="bg-red-50 text-red-700 border-red-200"
                        >
                          Failed
                        </Badge>
                      ) : (
                        <Badge
                          variant="outline"
                          className="bg-green-50 text-green-700 border-green-200"
                        >
                          VRU Complete
                        </Badge>
                      )}
                    </div>
                    <p className="text-sm text-muted-foreground">
                      {productionCompleted
                        ? "Production has completed for this event."
                        : productionProcessing
                          ? "Production is already running for this event."
                          : !hasCompletedVru
                            ? "Run VRU detections before sending this event through production."
                            : "VRU detections are complete. Run Production moves this event to the front of the production queue."}
                    </p>
                    {visibleProductionNotice && (
                      <p
                        className={cn(
                          "text-sm",
                          visibleProductionNotice.type === "success"
                            ? "text-green-700"
                            : "text-destructive"
                        )}
                      >
                        {visibleProductionNotice.message}
                      </p>
                    )}
                    {productionRun?.lastError && productionRun.status === "failed" && (
                      <p className="text-sm text-destructive">
                        {productionRun.lastError}
                      </p>
                    )}
                  </div>
                  {canRunProduction && (
                    <Button
                      size="sm"
                      onClick={handleRunProduction}
                      disabled={productionLoading || productionActionPending}
                      className="sm:self-start"
                    >
                      {productionActionPending || productionLoading ? (
                        <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                      ) : (
                        <Upload className="w-4 h-4 mr-2" />
                      )}
                      {productionLoading ? "Checking..." : "Run Production"}
                    </Button>
                  )}
                </div>
              </div>
            )}

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

            {/* Camera Info */}
            {cameraIntrinsics?.bee && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-sm">Bee Camera</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-3 text-sm">
                    <div>
                      <p className="text-xs text-muted-foreground">Horizontal FOV</p>
                      <p className="font-mono">{BEE_HFOV}°</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Focal Length</p>
                      <p className="font-mono">{cameraIntrinsics.bee.focal.toFixed(4)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Radial k1</p>
                      <p className="font-mono">{cameraIntrinsics.bee.k1.toFixed(4)}</p>
                    </div>
                    <div>
                      <p className="text-xs text-muted-foreground">Radial k2</p>
                      <p className="font-mono">{cameraIntrinsics.bee.k2.toFixed(4)}</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            )}
          </div>

          {/* Right column - Map and metadata */}
          <div className="space-y-4">
            {/* Map, positioning, and FPS quality */}
            <Card
              ref={frameQualityRef}
              className={cn(
                "overflow-hidden rounded-xl py-0 flex flex-col bg-card",
                constrainTelemetryHeight && !videoContainerHeight && "aspect-[16/10]"
              )}
              style={constrainTelemetryHeight && videoContainerHeight ? { height: videoContainerHeight } : undefined}
            >
              <CardContent
                className={cn(
                  "flex flex-col p-0",
                  constrainTelemetryHeight && "min-h-0 flex-1"
                )}
              >
                <Tabs
                  value={activeTelemetryTab}
                  onValueChange={(value) => setActiveTelemetryTab(value as TelemetryTab)}
                  className={cn(
                    "flex flex-col gap-0",
                    constrainTelemetryHeight && "min-h-0 flex-1"
                  )}
                >
                  <div className="shrink-0 px-4 pb-3 pt-4">
                    <TabsList className="grid h-9 w-full grid-cols-3 gap-1 bg-transparent p-0">
                      <TabsTrigger
                        value="map"
                        className="h-9 w-full rounded-md border-0 bg-transparent px-3 py-1.5 text-sm font-medium text-muted-foreground shadow-none transition-colors hover:bg-muted hover:text-foreground data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:shadow-none"
                      >
                        Map
                      </TabsTrigger>
                      <TabsTrigger
                        value="positioning"
                        className="h-9 w-full rounded-md border-0 bg-transparent px-3 py-1.5 text-sm font-medium text-muted-foreground shadow-none transition-colors hover:bg-muted hover:text-foreground data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:shadow-none"
                      >
                        Positioning
                      </TabsTrigger>
                      <TabsTrigger
                        value="fps"
                        className="h-9 w-full rounded-md border-0 bg-transparent px-3 py-1.5 text-sm font-medium text-muted-foreground shadow-none transition-colors hover:bg-muted hover:text-foreground data-[state=active]:bg-primary/10 data-[state=active]:text-primary data-[state=active]:shadow-none"
                      >
                        FPS
                      </TabsTrigger>
                    </TabsList>
                  </div>
                  <div
                    className={cn(
                      "flex flex-col px-4 pb-4",
                      constrainTelemetryHeight && "min-h-0 flex-1"
                    )}
                  >
                    <TabsContent
                      value="map"
                      className="m-0 h-full min-h-0 flex-1 overflow-hidden"
                    >
                      <div className="h-full min-h-0 w-full overflow-hidden rounded-lg bg-muted">
                        <PlaybackEventMap
                          playbackStore={playbackStore}
                          location={event.location}
                          path={event.gnssData}
                          speedData={overlaySpeedData}
                          videoDuration={videoDuration}
                          detectionSegments={detectionSegments}
                          className="h-full rounded-lg"
                          onSeek={(time) => {
                            if (videoRef.current) videoRef.current.currentTime = time;
                          }}
                        />
                      </div>
                    </TabsContent>
                    <TabsContent
                      value="positioning"
                      className="m-0 h-full min-h-0 flex-1 overflow-auto"
                    >
                      <div className="h-full overflow-auto rounded-lg border bg-background p-3">
                        <PlaybackPositioningSection
                          playbackStore={playbackStore}
                          eventId={id}
                          gnssData={event.gnssData}
                          videoDuration={videoDuration}
                          embedded
                        />
                      </div>
                    </TabsContent>
                    <TabsContent
                      value="fps"
                      className="m-0 h-full min-h-0 flex-1 overflow-auto"
                    >
                      <div className="h-full overflow-auto rounded-lg border bg-background p-3">
                        <PlaybackFrameTimingQcPanel
                          playbackStore={playbackStore}
                          videoId={id}
                          videoUrl={event.videoUrl}
                          firmwareVersion={firmwareVersion}
                          isProbingExternal={isFrameQualityProbing}
                          incidentTimeSeconds={incidentTimeSeconds}
                          embedded
                          title={null}
                        />
                      </div>
                    </TabsContent>
                  </div>
                </Tabs>
              </CardContent>
            </Card>

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
              <PlaybackSpeedProfileChart
                playbackStore={playbackStore}
                speedArray={speedData}
                gnssData={event.gnssData}
                imuData={event.imuData}
                duration={videoDuration}
                speedLimit={nearestSpeedLimit}
                unit={speedUnit}
                onSeek={(time) => {
                  if (videoRef.current) videoRef.current.currentTime = time;
                }}
              />
            </div>

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

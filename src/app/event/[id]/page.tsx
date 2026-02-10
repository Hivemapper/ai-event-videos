"use client";

import { useEffect, useState, use, useMemo, useRef, useCallback } from "react";
import Link from "next/link";
import { Header } from "@/components/layout/header";
import {
  ArrowLeft,
  MapPin,
  Clock,
  Activity,
  Gauge,
  ExternalLink,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  Copy,
  Check,
  Camera,
  Download,
  Loader2,
  Octagon,
  CircleGauge,
  Tag,
  Sun,
  Moon,
  Sunrise,
  Sunset,
  Route,
  Satellite,
  ChevronDown,
  ChevronUp,
  Navigation,
  RotateCw,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { EventMap } from "@/components/map/event-map";
import { AIEvent, GnssDataPoint, ImuDataPoint } from "@/types/events";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getMapboxToken } from "@/lib/api";
import { getApiKey } from "@/lib/api";
import { getCameraIntrinsics, calculateFOV, DevicesResponse, getSpeedUnit, SpeedUnit, speedLabel as getSpeedLabel } from "@/lib/api";
import { cn } from "@/lib/utils";
import { getTimeOfDay, getTimeOfDayStyle, TimeOfDay } from "@/lib/sun";
import { useRoadType } from "@/hooks/use-road-type";
import { VideoAnalysisCard } from "@/components/events/video-analysis";
import { SpeedProfileChart } from "@/components/events/speed-profile-chart";

function calculateDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371000;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 +
    Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
    Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

interface SpeedDataPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

interface LabeledFeature {
  class: string;
  distance: number;
  position: { lat: number; lon: number };
  speedLimit?: number;
  unit?: string;
}

interface LabeledFrameData {
  frame: {
    url: string;
    timestamp: number;
    width: number;
  };
  location: {
    lat: number;
    lon: number;
  };
  features: LabeledFeature[];
  event: {
    id: string;
    type: string;
    timestamp: string;
    videoUrl: string;
  };
}

function formatDateTime(timestamp: string): string {
  const date = new Date(timestamp);
  return date.toLocaleString();
}

function formatCoordinates(lat: number, lon: number): string {
  return `${lat.toFixed(6)}, ${lon.toFixed(6)}`;
}

function formatSpeed(speedMs: number): string {
  const mph = speedMs * 2.237;
  return `${mph.toFixed(1)} mph`;
}

function formatTimestamp(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

function getTimeOfDayIcon(timeOfDay: TimeOfDay) {
  switch (timeOfDay) {
    case "Day":
      return Sun;
    case "Dawn":
      return Sunrise;
    case "Dusk":
      return Sunset;
    case "Night":
      return Moon;
  }
}

type SortDirection = "asc" | "desc" | null;
type SortColumn = "speed" | "timestamp" | null;

interface MetadataTableProps {
  metadata: Record<string, unknown>;
}

function MetadataTable({ metadata }: MetadataTableProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>(null);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);

  const speedData = metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const acceleration = metadata?.ACCELERATION_MS2 as number | undefined;

  const sortedSpeedData = useMemo(() => {
    if (!speedData) return [];
    if (!sortColumn || !sortDirection) return speedData;

    return [...speedData].sort((a, b) => {
      let comparison = 0;
      if (sortColumn === "speed") {
        comparison = a.AVG_SPEED_MS - b.AVG_SPEED_MS;
      } else if (sortColumn === "timestamp") {
        comparison = a.TIMESTAMP - b.TIMESTAMP;
      }
      return sortDirection === "asc" ? comparison : -comparison;
    });
  }, [speedData, sortColumn, sortDirection]);

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      if (sortDirection === "asc") {
        setSortDirection("desc");
      } else if (sortDirection === "desc") {
        setSortColumn(null);
        setSortDirection(null);
      }
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
  };

  const SortIcon = ({ column }: { column: SortColumn }) => {
    if (sortColumn !== column) {
      return <ArrowUpDown className="w-3 h-3 ml-1 opacity-50" />;
    }
    if (sortDirection === "asc") {
      return <ArrowUp className="w-3 h-3 ml-1" />;
    }
    return <ArrowDown className="w-3 h-3 ml-1" />;
  };

  // Get other metadata fields (not SPEED_ARRAY)
  const otherFields = Object.entries(metadata).filter(
    ([key]) => key !== "SPEED_ARRAY"
  );

  return (
    <div className="space-y-4">
      {/* Other metadata fields */}
      {otherFields.length > 0 && (
        <div className="overflow-hidden rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Field</th>
                <th className="px-4 py-2 text-left font-medium">Value</th>
              </tr>
            </thead>
            <tbody>
              {otherFields.map(([key, value]) => (
                <tr key={key} className="border-t">
                  <td className="px-4 py-2 font-mono text-muted-foreground">
                    {key}
                  </td>
                  <td className="px-4 py-2">
                    {typeof value === "number"
                      ? key.includes("SPEED")
                        ? formatSpeed(value)
                        : key.includes("ACCELERATION")
                        ? `${value.toFixed(3)} m/s²`
                        : value.toFixed(4)
                      : String(value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Speed array table */}
      {sortedSpeedData.length > 0 && (
        <div>
          <h4 className="text-sm font-medium mb-2">Speed Data ({sortedSpeedData.length} points)</h4>
          <div className="overflow-hidden rounded-lg border max-h-64 overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 sticky top-0">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">#</th>
                  <th
                    className="px-4 py-2 text-left font-medium cursor-pointer hover:bg-muted/70 select-none"
                    onClick={() => handleSort("speed")}
                  >
                    <span className="flex items-center">
                      Speed
                      <SortIcon column="speed" />
                    </span>
                  </th>
                  <th
                    className="px-4 py-2 text-left font-medium cursor-pointer hover:bg-muted/70 select-none"
                    onClick={() => handleSort("timestamp")}
                  >
                    <span className="flex items-center">
                      Timestamp
                      <SortIcon column="timestamp" />
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedSpeedData.map((point, index) => (
                  <tr key={index} className="border-t hover:bg-muted/30">
                    <td className="px-4 py-2 text-muted-foreground">{index + 1}</td>
                    <td className="px-4 py-2 font-mono">
                      {formatSpeed(point.AVG_SPEED_MS)}
                    </td>
                    <td className="px-4 py-2 font-mono text-muted-foreground">
                      {formatTimestamp(point.TIMESTAMP)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function deriveSpeedFromGnss(gnssData: GnssDataPoint[]): SpeedDataPoint[] {
  if (gnssData.length < 2) return [];
  const result: SpeedDataPoint[] = [];
  for (let i = 1; i < gnssData.length; i++) {
    const prev = gnssData[i - 1];
    const curr = gnssData[i];
    const dt = (curr.timestamp - prev.timestamp) / 1000; // ms → s
    if (dt <= 0) continue;
    const dist = calculateDistance(prev.lat, prev.lon, curr.lat, curr.lon);
    result.push({ AVG_SPEED_MS: dist / dt, TIMESTAMP: curr.timestamp });
  }
  return result;
}

function SpeedOverlay({
  speedData,
  currentTime,
  duration,
  unit,
}: {
  speedData: SpeedDataPoint[];
  currentTime: number;
  duration: number;
  unit: SpeedUnit;
}) {
  if (!speedData.length || duration <= 0) return null;

  const progress = Math.min(Math.max(currentTime / duration, 0), 1);
  const exactIndex = progress * (speedData.length - 1);
  const lowIndex = Math.floor(exactIndex);
  const highIndex = Math.min(lowIndex + 1, speedData.length - 1);
  const fraction = exactIndex - lowIndex;

  const speedMs =
    speedData[lowIndex].AVG_SPEED_MS * (1 - fraction) +
    speedData[highIndex].AVG_SPEED_MS * fraction;
  const speedKmh = speedMs * 3.6;
  const displaySpeed = unit === "mph" ? Math.round(speedKmh * 0.621371) : Math.round(speedKmh);

  return (
    <div className="absolute top-4 left-4 bg-black/60 backdrop-blur-sm text-white rounded-lg px-3 py-1.5 flex items-baseline gap-1 pointer-events-none">
      <span className="font-mono text-lg font-bold">{displaySpeed}</span>
      <span className="text-xs text-white/70">{getSpeedLabel(unit)}</span>
    </div>
  );
}

function getProxyVideoUrl(url: string): string {
  return `/api/video?url=${encodeURIComponent(url)}`;
}

function formatVideoTimestamp(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = Math.floor(seconds % 60);
  const ms = Math.floor((seconds % 1) * 100);
  return `${mins}:${secs.toString().padStart(2, "0")}.${ms.toString().padStart(2, "0")}`;
}

function getFeatureIcon(className: string) {
  if (className.toLowerCase().includes("stop")) {
    return Octagon;
  }
  if (className.toLowerCase().includes("speed")) {
    return CircleGauge;
  }
  return Tag;
}

function getFeatureColor(className: string) {
  if (className.toLowerCase().includes("stop")) {
    return "text-red-600 bg-red-50 border-red-200";
  }
  if (className.toLowerCase().includes("speed")) {
    return "text-blue-600 bg-blue-50 border-blue-200";
  }
  return "text-gray-600 bg-gray-50 border-gray-200";
}

interface FrameLabelingProps {
  event: AIEvent;
  videoRef: React.RefObject<HTMLVideoElement | null>;
}

function FrameLabeling({ event, videoRef }: FrameLabelingProps) {
  const [timestamp, setTimestamp] = useState(0);
  const [videoDuration, setVideoDuration] = useState(10);
  const [isExtracting, setIsExtracting] = useState(false);
  const [labeledData, setLabeledData] = useState<LabeledFrameData | null>(null);
  const [frameUrl, setFrameUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Sync timestamp with video
  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;

    const handleTimeUpdate = () => {
      setTimestamp(video.currentTime);
    };

    const handleLoadedMetadata = () => {
      setVideoDuration(video.duration || 10);
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
  }, [videoRef]);

  const handleSliderChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      const newTime = parseFloat(e.target.value);
      setTimestamp(newTime);
      if (videoRef.current) {
        videoRef.current.currentTime = newTime;
      }
    },
    [videoRef]
  );

  const extractFrame = async () => {
    setIsExtracting(true);
    setError(null);

    const apiKey = getApiKey();
    if (!apiKey) {
      setError("API key not configured");
      setIsExtracting(false);
      return;
    }

    try {
      const response = await fetch(
        `/api/labeled-frame?eventId=${event.id}&timestamp=${timestamp}&width=1280&radius=100`,
        {
          headers: {
            Authorization: apiKey,
          },
        }
      );

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to extract labeled frame");
      }

      const data: LabeledFrameData = await response.json();
      setLabeledData(data);
      setFrameUrl(data.frame.url);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to extract frame");
    } finally {
      setIsExtracting(false);
    }
  };

  const exportLabeledPair = async () => {
    if (!labeledData || !frameUrl) return;

    // Create labels.json
    const labels = {
      image: "frame.jpg",
      features: labeledData.features.map((f) => ({
        class: f.class,
        distance_m: f.distance,
        ...(f.speedLimit && { speed_limit: f.speedLimit }),
      })),
    };

    // Create metadata.json
    const metadata = {
      event_id: labeledData.event.id,
      event_type: labeledData.event.type,
      event_timestamp: labeledData.event.timestamp,
      frame_timestamp: labeledData.frame.timestamp,
      location: labeledData.location,
      exported_at: new Date().toISOString(),
    };

    // Download labels.json
    const labelsBlob = new Blob([JSON.stringify(labels, null, 2)], {
      type: "application/json",
    });
    const labelsUrl = URL.createObjectURL(labelsBlob);
    const labelsLink = document.createElement("a");
    labelsLink.href = labelsUrl;
    labelsLink.download = `event_${event.id}_t${timestamp.toFixed(1)}_labels.json`;
    labelsLink.click();
    URL.revokeObjectURL(labelsUrl);

    // Download metadata.json
    const metadataBlob = new Blob([JSON.stringify(metadata, null, 2)], {
      type: "application/json",
    });
    const metadataUrl = URL.createObjectURL(metadataBlob);
    const metadataLink = document.createElement("a");
    metadataLink.href = metadataUrl;
    metadataLink.download = `event_${event.id}_t${timestamp.toFixed(1)}_metadata.json`;
    metadataLink.click();
    URL.revokeObjectURL(metadataUrl);

    // Download frame image
    const frameLink = document.createElement("a");
    frameLink.href = frameUrl;
    frameLink.download = `event_${event.id}_t${timestamp.toFixed(1)}_frame.jpg`;
    frameLink.click();
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          <Camera className="w-5 h-5" />
          Frame Labeling
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {/* Timestamp slider */}
        <div className="space-y-2">
          <div className="flex items-center justify-between text-sm">
            <span className="text-muted-foreground">Timestamp</span>
            <span className="font-mono">{formatVideoTimestamp(timestamp)}</span>
          </div>
          <input
            type="range"
            min="0"
            max={videoDuration}
            step="0.1"
            value={timestamp}
            onChange={handleSliderChange}
            className="w-full h-2 bg-muted rounded-lg appearance-none cursor-pointer accent-primary"
          />
          <div className="flex justify-between text-xs text-muted-foreground">
            <span>0:00</span>
            <span>{formatVideoTimestamp(videoDuration)}</span>
          </div>
        </div>

        {/* Extract button */}
        <Button
          onClick={extractFrame}
          disabled={isExtracting}
          className="w-full"
        >
          {isExtracting ? (
            <>
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              Extracting...
            </>
          ) : (
            <>
              <Camera className="w-4 h-4 mr-2" />
              Extract Frame & Get Labels
            </>
          )}
        </Button>

        {error && (
          <div className="text-sm text-red-600 bg-red-50 p-3 rounded-lg">
            {error}
          </div>
        )}

        {/* Extracted frame display */}
        {frameUrl && (
          <div className="space-y-3">
            <div className="relative rounded-lg overflow-hidden border bg-black">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img
                src={frameUrl}
                alt={`Frame at ${formatVideoTimestamp(timestamp)}`}
                className="w-full"
              />
            </div>

            {/* Labels display */}
            {labeledData && (
              <div className="space-y-2">
                <h4 className="text-sm font-medium">
                  Nearby Map Features ({labeledData.features.length})
                </h4>
                {labeledData.features.length === 0 ? (
                  <p className="text-sm text-muted-foreground">
                    No map features found within 100m
                  </p>
                ) : (
                  <div className="space-y-2">
                    {labeledData.features.map((feature, index) => {
                      const Icon = getFeatureIcon(feature.class);
                      const colorClass = getFeatureColor(feature.class);
                      return (
                        <div
                          key={index}
                          className={cn(
                            "flex items-center justify-between p-2 rounded-lg border",
                            colorClass
                          )}
                        >
                          <div className="flex items-center gap-2">
                            <Icon className="w-4 h-4" />
                            <span className="font-medium">{feature.class}</span>
                            {feature.speedLimit && (
                              <Badge variant="secondary" className="text-xs">
                                {feature.speedLimit} {feature.unit || "mph"}
                              </Badge>
                            )}
                          </div>
                          <span className="text-sm">{feature.distance}m</span>
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            )}

            {/* Export button */}
            <Button
              variant="outline"
              onClick={exportLabeledPair}
              className="w-full"
            >
              <Download className="w-4 h-4 mr-2" />
              Export Labeled Pair
            </Button>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

interface PositioningSectionProps {
  eventId: string;
  gnssData?: GnssDataPoint[];
}

function PositioningSection({ eventId, gnssData }: PositioningSectionProps) {
  const [isExpanded, setIsExpanded] = useState(false);
  const [imuData, setImuData] = useState<ImuDataPoint[] | null>(null);
  const [isLoadingImu, setIsLoadingImu] = useState(false);
  const [imuError, setImuError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"gnss" | "imu">("gnss");

  const fetchImuData = async () => {
    if (imuData) return; // Already loaded

    setIsLoadingImu(true);
    setImuError(null);

    const apiKey = getApiKey();
    if (!apiKey) {
      setImuError("API key not configured");
      setIsLoadingImu(false);
      return;
    }

    try {
      const response = await fetch(
        `/api/events/${eventId}?includeImuData=true`,
        {
          headers: {
            Authorization: apiKey,
          },
        }
      );

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || "Failed to fetch IMU data");
      }

      const data = await response.json();
      setImuData(data.imuData || []);
    } catch (err) {
      setImuError(err instanceof Error ? err.message : "Failed to load IMU data");
    } finally {
      setIsLoadingImu(false);
    }
  };

  const handleExpand = () => {
    setIsExpanded(!isExpanded);
  };

  const handleTabChange = (tab: "gnss" | "imu") => {
    setActiveTab(tab);
    if (tab === "imu" && !imuData && !isLoadingImu) {
      fetchImuData();
    }
  };

  const formatAltitude = (alt: number) => `${alt.toFixed(1)}m`;
  const formatAccel = (val: number) => `${val.toFixed(3)} m/s²`;
  const formatGyro = (val: number) => `${val.toFixed(4)} rad/s`;

  return (
    <Card>
      <CardHeader
        className="cursor-pointer select-none"
        onClick={handleExpand}
      >
        <CardTitle className="text-lg flex items-center justify-between">
          <div className="flex items-center gap-2">
            <Satellite className="w-5 h-5" />
            Positioning
            {gnssData && (
              <Badge variant="secondary" className="text-xs">
                {gnssData.length} GNSS points
              </Badge>
            )}
          </div>
          {isExpanded ? (
            <ChevronUp className="w-5 h-5 text-muted-foreground" />
          ) : (
            <ChevronDown className="w-5 h-5 text-muted-foreground" />
          )}
        </CardTitle>
      </CardHeader>

      {isExpanded && (
        <CardContent className="space-y-4">
          {/* Tab buttons */}
          <div className="flex gap-2">
            <Button
              variant={activeTab === "gnss" ? "default" : "outline"}
              size="sm"
              onClick={() => handleTabChange("gnss")}
              className="flex items-center gap-2"
            >
              <Navigation className="w-4 h-4" />
              GNSS Data
            </Button>
            <Button
              variant={activeTab === "imu" ? "default" : "outline"}
              size="sm"
              onClick={() => handleTabChange("imu")}
              className="flex items-center gap-2"
            >
              <RotateCw className="w-4 h-4" />
              IMU Data
              {isLoadingImu && <Loader2 className="w-3 h-3 animate-spin" />}
            </Button>
          </div>

          {/* GNSS Tab */}
          {activeTab === "gnss" && (
            <div>
              {gnssData && gnssData.length > 0 ? (
                <div className="overflow-hidden rounded-lg border max-h-80 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        <th className="px-3 py-2 text-left font-medium">#</th>
                        <th className="px-3 py-2 text-left font-medium">Latitude</th>
                        <th className="px-3 py-2 text-left font-medium">Longitude</th>
                        <th className="px-3 py-2 text-left font-medium">Altitude</th>
                        <th className="px-3 py-2 text-left font-medium">Time</th>
                      </tr>
                    </thead>
                    <tbody>
                      {gnssData.map((point, index) => (
                        <tr key={index} className="border-t hover:bg-muted/30">
                          <td className="px-3 py-2 text-muted-foreground">{index + 1}</td>
                          <td className="px-3 py-2 font-mono text-xs">{point.lat.toFixed(6)}</td>
                          <td className="px-3 py-2 font-mono text-xs">{point.lon.toFixed(6)}</td>
                          <td className="px-3 py-2 font-mono text-xs">{formatAltitude(point.alt)}</td>
                          <td className="px-3 py-2 font-mono text-xs text-muted-foreground">
                            {formatTimestamp(point.timestamp)}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              ) : (
                <p className="text-sm text-muted-foreground py-4 text-center">
                  No GNSS data available for this event
                </p>
              )}
            </div>
          )}

          {/* IMU Tab */}
          {activeTab === "imu" && (
            <div>
              {imuError && (
                <div className="text-sm text-red-600 bg-red-50 p-3 rounded-lg mb-4">
                  {imuError}
                </div>
              )}

              {isLoadingImu && (
                <div className="flex items-center justify-center py-8">
                  <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
                  <span className="ml-2 text-muted-foreground">Loading IMU data...</span>
                </div>
              )}

              {!isLoadingImu && imuData && imuData.length > 0 && (
                <div className="overflow-hidden rounded-lg border max-h-80 overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-muted/50 sticky top-0">
                      <tr>
                        <th className="px-2 py-2 text-left font-medium">#</th>
                        <th className="px-2 py-2 text-left font-medium" colSpan={3}>
                          Accelerometer (m/s²)
                        </th>
                        <th className="px-2 py-2 text-left font-medium" colSpan={3}>
                          Gyroscope (rad/s)
                        </th>
                        <th className="px-2 py-2 text-left font-medium">Time</th>
                      </tr>
                      <tr className="bg-muted/30">
                        <th className="px-2 py-1"></th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">X</th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">Y</th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">Z</th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">X</th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">Y</th>
                        <th className="px-2 py-1 text-xs font-normal text-muted-foreground">Z</th>
                        <th className="px-2 py-1"></th>
                      </tr>
                    </thead>
                    <tbody>
                      {imuData.map((point, index) => (
                        <tr key={index} className="border-t hover:bg-muted/30">
                          <td className="px-2 py-2 text-muted-foreground">{index + 1}</td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.accelerometer ? formatAccel(point.accelerometer.x) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.accelerometer ? formatAccel(point.accelerometer.y) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.accelerometer ? formatAccel(point.accelerometer.z) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.gyroscope ? formatGyro(point.gyroscope.x) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.gyroscope ? formatGyro(point.gyroscope.y) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs">
                            {point.gyroscope ? formatGyro(point.gyroscope.z) : "-"}
                          </td>
                          <td className="px-2 py-2 font-mono text-xs text-muted-foreground">
                            {point.timestamp ? formatTimestamp(point.timestamp) : "-"}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}

              {!isLoadingImu && imuData && imuData.length === 0 && (
                <p className="text-sm text-muted-foreground py-4 text-center">
                  No IMU data available for this event
                </p>
              )}

              {!isLoadingImu && !imuData && !imuError && (
                <p className="text-sm text-muted-foreground py-4 text-center">
                  Click to load IMU data
                </p>
              )}
            </div>
          )}
        </CardContent>
      )}
    </Card>
  );
}

export default function EventDetailPage({
  params,
}: {
  params: Promise<{ id: string }>;
}) {
  const { id } = use(params);
  const [event, setEvent] = useState<AIEvent | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [countryName, setCountryName] = useState<string | null>(null);
  const [copied, setCopied] = useState(false);
  const videoRef = useRef<HTMLVideoElement>(null);
  const [cameraIntrinsics, setCameraIntrinsics] = useState<DevicesResponse | null>(null);
  const [nearestSpeedLimit, setNearestSpeedLimit] = useState<{
    limit: number;
    unit: string;
  } | null>(null);
  const [videoCurrentTime, setVideoCurrentTime] = useState(0);
  const [videoDuration, setVideoDuration] = useState(0);
  const [isDownloading, setIsDownloading] = useState(false);
  const [speedUnit, setSpeedUnitState] = useState<SpeedUnit>("mph");

  useEffect(() => {
    setSpeedUnitState(getSpeedUnit());
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

  useEffect(() => {
    async function fetchEvent() {
      const apiKey = getApiKey();
      if (!apiKey) {
        setError("API key not configured");
        setIsLoading(false);
        return;
      }

      try {
        const response = await fetch(`/api/events/${id}?includeGnssData=true&includeImuData=true`, {
          headers: {
            Authorization: apiKey,
          },
        });

        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.error || "Failed to fetch event");
        }

        const data = await response.json();
        setEvent(data);
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load event");
      } finally {
        setIsLoading(false);
      }
    }

    fetchEvent();
  }, [id]);

  // Load camera intrinsics from localStorage
  useEffect(() => {
    const intrinsics = getCameraIntrinsics();
    setCameraIntrinsics(intrinsics);
  }, []);

  // Fetch road type from Mapbox
  const { roadType, isLoading: roadTypeLoading } = useRoadType(
    event?.location.lat ?? null,
    event?.location.lon ?? null
  );

  // Fetch country name using Mapbox geocoding
  useEffect(() => {
    if (!event) return;

    const fetchCountry = async () => {
      const token = getMapboxToken();
      if (!token) return;

      try {
        const response = await fetch(
          `https://api.mapbox.com/geocoding/v5/mapbox.places/${event.location.lon},${event.location.lat}.json?types=country&access_token=${token}`
        );
        if (response.ok) {
          const data = await response.json();
          if (data.features?.[0]?.text) {
            setCountryName(data.features[0].text);
          }
        }
      } catch {
        // Silently fail
      }
    };

    fetchCountry();
  }, [event]);

  // Fetch nearest speed limit from BeeMaps
  useEffect(() => {
    if (!event) return;

    const fetchSpeedLimit = async () => {
      const apiKey = getApiKey();
      if (!apiKey) return;

      try {
        const response = await fetch(
          `/api/map-features?lat=${event.location.lat}&lon=${event.location.lon}&radius=200`,
          {
            headers: {
              Authorization: apiKey,
            },
          }
        );

        if (!response.ok) return;

        const data = await response.json();
        const features = data.features as LabeledFeature[] | undefined;

        if (!features || features.length === 0) return;

        // Filter for speed signs with speed limits
        const speedSigns = features.filter(
          (f) => f.class === "speed-sign" && f.speedLimit !== undefined
        );

        if (speedSigns.length === 0) return;

        // Find nearest speed sign
        let nearest = speedSigns[0];
        let minDistance = calculateDistance(
          event.location.lat,
          event.location.lon,
          nearest.position.lat,
          nearest.position.lon
        );

        for (const sign of speedSigns.slice(1)) {
          const distance = calculateDistance(
            event.location.lat,
            event.location.lon,
            sign.position.lat,
            sign.position.lon
          );
          if (distance < minDistance) {
            minDistance = distance;
            nearest = sign;
          }
        }

        setNearestSpeedLimit({
          limit: nearest.speedLimit!,
          unit: nearest.unit || "mph",
        });
      } catch {
        // Silently fail
      }
    };

    fetchSpeedLimit();
  }, [event]);

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
  const IconComponent = config.icon;
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
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Left column - Video */}
          <div className="space-y-6">
            {/* Video player */}
            <Card className="overflow-hidden py-0">
              <CardContent className="p-0">
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
                    />
                  )}
                </div>
                {event.videoUrl && (
                  <div className="flex justify-end px-3 py-2 border-t">
                    <Button
                      variant="ghost"
                      size="sm"
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
              </CardContent>
            </Card>

            {/* Event info */}
            <Card>
              <CardHeader className="flex flex-row items-center justify-between space-y-0">
                <CardTitle className="text-lg">Event Details</CardTitle>
                <div className="flex items-center gap-2">
                  {roadType?.classLabel && (
                    <Badge variant="outline">
                      <Route className="w-3 h-3 mr-1" />
                      {roadType.classLabel}
                    </Badge>
                  )}
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
                </div>
              </CardHeader>
              <CardContent className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="flex items-center gap-2 text-sm">
                    <Clock className="w-4 h-4 text-muted-foreground" />
                    <div>
                      <p className="text-muted-foreground">Timestamp</p>
                      <p className="font-medium flex items-center gap-2">
                        {formatDateTime(event.timestamp)}
                        {(() => {
                          const sunInfo = getTimeOfDay(
                            event.timestamp,
                            event.location.lat,
                            event.location.lon
                          );
                          const TimeIcon = getTimeOfDayIcon(sunInfo.timeOfDay);
                          const style = getTimeOfDayStyle(sunInfo.timeOfDay);
                          return (
                            <span
                              className={cn(
                                "inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium",
                                style.bgColor,
                                style.color
                              )}
                            >
                              <TimeIcon className="w-3 h-3" />
                              {sunInfo.timeOfDay}
                            </span>
                          );
                        })()}
                      </p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2 text-sm">
                    <MapPin className="w-4 h-4 text-muted-foreground" />
                    <div>
                      <p className="text-muted-foreground">Coordinates</p>
                      <p className="font-medium flex items-center gap-1">
                        {formatCoordinates(event.location.lat, event.location.lon)}
                        <button
                          onClick={copyCoordinates}
                          className="p-1 hover:bg-muted rounded transition-colors"
                          title="Copy coordinates"
                        >
                          {copied ? (
                            <Check className="w-3 h-3 text-green-500" />
                          ) : (
                            <Copy className="w-3 h-3 text-muted-foreground" />
                          )}
                        </button>
                      </p>
                    </div>
                  </div>
                  {maxSpeed !== null && (
                    <div className="flex items-center gap-2 text-sm">
                      <Gauge className="w-4 h-4 text-muted-foreground" />
                      <div>
                        <p className="text-muted-foreground">Max Speed</p>
                        <p className="font-medium">{formatSpeed(maxSpeed)}</p>
                      </div>
                    </div>
                  )}
                  {acceleration !== undefined && (
                    <div className="flex items-center gap-2 text-sm">
                      <Activity className="w-4 h-4 text-muted-foreground" />
                      <div>
                        <p className="text-muted-foreground">Acceleration</p>
                        <p className="font-medium">
                          {acceleration.toFixed(2)} m/s²
                        </p>
                      </div>
                    </div>
                  )}
                </div>

                {countryName && (
                  <div className="pt-2 flex items-center gap-2 text-sm">
                    <span className="font-medium">{countryName}</span>
                    <span className="text-muted-foreground">·</span>
                    <a
                      href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-primary hover:underline flex items-center gap-1"
                    >
                      <ExternalLink className="w-3 h-3" />
                      Open in Google Maps
                    </a>
                  </div>
                )}
                {!countryName && (
                  <div className="pt-2">
                    <a
                      href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="text-sm text-primary hover:underline flex items-center gap-1"
                    >
                      <ExternalLink className="w-3 h-3" />
                      Open in Google Maps
                    </a>
                  </div>
                )}
              </CardContent>
            </Card>

            {/* Speed Profile */}
            <Card>
              <CardHeader>
                <CardTitle className="text-lg flex items-center gap-2">
                  <Gauge className="w-5 h-5" />
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
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Camera className="w-5 h-5" />
                    Bee Camera
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <p className="text-muted-foreground">Horizontal FOV</p>
                      <p className="font-medium font-mono">
                        {calculateFOV(cameraIntrinsics.bee.focal).toFixed(1)}°
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
          <div className="space-y-6">
            {/* Map */}
            <Card className="overflow-hidden py-0">
              <CardContent className="p-0">
                <EventMap
                  location={event.location}
                  path={event.gnssData}
                  currentTime={videoCurrentTime}
                  videoDuration={videoDuration}
                  className="aspect-video"
                />
              </CardContent>
            </Card>

            {/* Scene Analysis */}
            {event.videoUrl && <VideoAnalysisCard eventId={id} />}

            {/* Positioning section */}
            <PositioningSection eventId={id} gnssData={event.gnssData} />

            {/* Metadata table */}
            {event.metadata && Object.keys(event.metadata).length > 0 && (
              <Card>
                <CardHeader>
                  <CardTitle className="text-lg">Metadata</CardTitle>
                </CardHeader>
                <CardContent>
                  <MetadataTable metadata={event.metadata} />
                </CardContent>
              </Card>
            )}

            {/* Frame Labeling */}
            {event.videoUrl && (
              <FrameLabeling event={event} videoRef={videoRef} />
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
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          <div className="space-y-6">
            <Skeleton className="aspect-video rounded-lg" />
            <Skeleton className="h-48 rounded-lg" />
          </div>
          <div className="space-y-6">
            <Skeleton className="h-[400px] rounded-lg" />
            <Skeleton className="h-32 rounded-lg" />
          </div>
        </div>
      </main>
    </div>
  );
}

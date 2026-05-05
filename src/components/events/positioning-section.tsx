"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  Loader2,
  ChevronDown,
  ChevronUp,
  Download,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { GnssDataPoint, ImuDataPoint } from "@/types/events";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";

function formatTimestamp(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

interface PositioningSectionProps {
  eventId: string;
  gnssData?: GnssDataPoint[];
  embedded?: boolean;
  className?: string;
  currentTime?: number;
  videoDuration?: number;
}

function nearestGnssIndex(
  gnssData: GnssDataPoint[] | undefined,
  currentTime: number | undefined,
  videoDuration: number | undefined
): number | null {
  if (!gnssData?.length || typeof currentTime !== "number" || !Number.isFinite(currentTime)) {
    return null;
  }

  if (!videoDuration || !Number.isFinite(videoDuration) || videoDuration <= 0) {
    return null;
  }

  const start = gnssData[0]?.timestamp;
  const end = gnssData[gnssData.length - 1]?.timestamp;
  if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start) {
    return Math.min(Math.max(Math.round((currentTime / videoDuration) * (gnssData.length - 1)), 0), gnssData.length - 1);
  }

  const progress = Math.min(Math.max(currentTime / videoDuration, 0), 1);
  const targetTimestamp = start + progress * (end - start);
  let low = 0;
  let high = gnssData.length - 1;

  while (low < high) {
    const mid = Math.floor((low + high) / 2);
    if (gnssData[mid].timestamp < targetTimestamp) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  const previous = Math.max(low - 1, 0);
  return Math.abs(gnssData[previous].timestamp - targetTimestamp) <= Math.abs(gnssData[low].timestamp - targetTimestamp)
    ? previous
    : low;
}

export function PositioningSection({
  eventId,
  gnssData,
  embedded = false,
  className,
  currentTime,
  videoDuration,
}: PositioningSectionProps) {
  const [isExpanded, setIsExpanded] = useState(true);
  const [imuData, setImuData] = useState<ImuDataPoint[] | null>(null);
  const [isLoadingImu, setIsLoadingImu] = useState(false);
  const [imuError, setImuError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<"gnss" | "imu">("gnss");
  const tableViewportRef = useRef<HTMLDivElement>(null);
  const highlightedRowRef = useRef<HTMLTableRowElement | null>(null);
  const highlightedGnssIndex = useMemo(
    () => nearestGnssIndex(gnssData, currentTime, videoDuration),
    [currentTime, gnssData, videoDuration]
  );

  useEffect(() => {
    if (activeTab !== "gnss" || highlightedGnssIndex === null) return;

    const viewport = tableViewportRef.current;
    const row = highlightedRowRef.current;
    if (!viewport || !row) return;

    const rowTop = row.offsetTop;
    const rowBottom = rowTop + row.offsetHeight;
    const viewportTop = viewport.scrollTop;
    const viewportBottom = viewportTop + viewport.clientHeight;

    if (rowTop < viewportTop || rowBottom > viewportBottom) {
      viewport.scrollTo({
        top: Math.max(rowTop - viewport.clientHeight / 2 + row.offsetHeight / 2, 0),
        behavior: "smooth",
      });
    }
  }, [activeTab, highlightedGnssIndex]);

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

  const downloadJson = (data: unknown, filename: string) => {
    const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    a.click();
    URL.revokeObjectURL(url);
  };

  const content = (
    <div className="space-y-4">
      {/* Tab buttons */}
      <div className="flex flex-wrap items-center gap-2">
        <Button
          variant={activeTab === "gnss" ? "default" : "outline"}
          size="sm"
          onClick={() => handleTabChange("gnss")}
        >
          GNSS Data
        </Button>
        <Button
          variant={activeTab === "imu" ? "default" : "outline"}
          size="sm"
          onClick={() => handleTabChange("imu")}
        >
          IMU Data
          {isLoadingImu && <Loader2 className="w-3 h-3 animate-spin" />}
        </Button>
        <div className="ml-auto">
          {activeTab === "gnss" && gnssData && gnssData.length > 0 && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => downloadJson(gnssData, `gnss-${eventId}.json`)}
            >
              <Download className="w-3.5 h-3.5 mr-1.5" />
              Download JSON
            </Button>
          )}
          {activeTab === "imu" && imuData && imuData.length > 0 && (
            <Button
              variant="outline"
              size="sm"
              onClick={() => downloadJson(imuData, `imu-${eventId}.json`)}
            >
              <Download className="w-3.5 h-3.5 mr-1.5" />
              Download JSON
            </Button>
          )}
        </div>
      </div>

      {/* GNSS Tab */}
      {activeTab === "gnss" && (
        <div>
          {gnssData && gnssData.length > 0 ? (
            <div ref={tableViewportRef} className="overflow-hidden rounded-lg border max-h-80 overflow-y-auto">
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
                  {gnssData.map((point, index) => {
                    const isCurrentRow = index === highlightedGnssIndex;
                    return (
                      <tr
                        key={index}
                        ref={isCurrentRow ? highlightedRowRef : undefined}
                        aria-current={isCurrentRow ? "location" : undefined}
                        className={cn(
                          "border-t transition-colors hover:bg-muted/30",
                          isCurrentRow && "bg-blue-50 ring-1 ring-inset ring-blue-200 hover:bg-blue-50"
                        )}
                      >
                        <td className={cn("px-3 py-2 text-muted-foreground", isCurrentRow && "font-semibold text-blue-700")}>
                          {index + 1}
                        </td>
                        <td className={cn("px-3 py-2 font-mono text-xs", isCurrentRow && "font-semibold text-blue-700")}>
                          {point.lat.toFixed(6)}
                        </td>
                        <td className={cn("px-3 py-2 font-mono text-xs", isCurrentRow && "font-semibold text-blue-700")}>
                          {point.lon.toFixed(6)}
                        </td>
                        <td className={cn("px-3 py-2 font-mono text-xs", isCurrentRow && "font-semibold text-blue-700")}>
                          {formatAltitude(point.alt)}
                        </td>
                        <td className={cn("px-3 py-2 font-mono text-xs text-muted-foreground", isCurrentRow && "font-semibold text-blue-700")}>
                          {formatTimestamp(point.timestamp)}
                        </td>
                      </tr>
                    );
                  })}
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
    </div>
  );

  if (embedded) {
    return (
      <div className={cn("space-y-3", className)}>
        {gnssData && (
          <Badge variant="secondary" className="text-xs">
            {gnssData.length} GNSS points
          </Badge>
        )}
        {content}
      </div>
    );
  }

  return (
    <div className={cn("space-y-3 rounded-lg border bg-card px-4 py-3", className)}>
      <div
        className="cursor-pointer select-none flex items-center justify-between"
        onClick={handleExpand}
      >
        <div className="flex items-center gap-2">
          <h3 className="text-lg font-semibold">Positioning</h3>
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
      </div>

      {isExpanded && content}
    </div>
  );
}

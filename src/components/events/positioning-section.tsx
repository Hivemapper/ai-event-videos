"use client";

import { useState } from "react";
import {
  Loader2,
  ChevronDown,
  ChevronUp,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { GnssDataPoint, ImuDataPoint } from "@/types/events";
import { getApiKey } from "@/lib/api";

function formatTimestamp(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

interface PositioningSectionProps {
  eventId: string;
  gnssData?: GnssDataPoint[];
}

export function PositioningSection({ eventId, gnssData }: PositioningSectionProps) {
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

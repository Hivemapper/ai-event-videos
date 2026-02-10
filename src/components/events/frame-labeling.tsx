"use client";

import { useEffect, useState, useCallback } from "react";
import {
  Camera,
  Download,
  Loader2,
  Octagon,
  CircleGauge,
  Tag,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AIEvent } from "@/types/events";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";

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

export function FrameLabeling({ event, videoRef }: FrameLabelingProps) {
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

"use client";

import { RefObject } from "react";
import { Download, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { SpeedOverlay } from "@/components/events/speed-overlay";
import { SpeedDataPoint, getProxyVideoUrl } from "@/lib/event-helpers";
import { SpeedUnit } from "@/lib/api";

interface VideoPlayerProps {
  videoUrl: string | undefined;
  videoRef: RefObject<HTMLVideoElement | null>;
  overlaySpeedData: SpeedDataPoint[];
  videoCurrentTime: number;
  videoDuration: number;
  speedUnit: SpeedUnit;
  nearestSpeedLimit: { limit: number; unit: string } | null;
  isDownloading: boolean;
  onDownload: () => void;
}

export function VideoPlayer({
  videoUrl,
  videoRef,
  overlaySpeedData,
  videoCurrentTime,
  videoDuration,
  speedUnit,
  nearestSpeedLimit,
  isDownloading,
  onDownload,
}: VideoPlayerProps) {
  return (
    <Card className="overflow-hidden py-0">
      <CardContent className="p-0">
        <div className="relative aspect-video bg-black">
          {videoUrl ? (
            <video
              ref={videoRef}
              src={getProxyVideoUrl(videoUrl)}
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
        {videoUrl && (
          <div className="flex justify-end px-3 py-2 border-t">
            <Button
              variant="ghost"
              size="sm"
              onClick={onDownload}
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
  );
}

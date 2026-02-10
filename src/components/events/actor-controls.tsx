"use client";

import { Loader2, Scan, Route, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { DetectedActor, ActorTrackingResult } from "@/types/actors";

interface ActorControlsProps {
  detectedActors: DetectedActor[] | null;
  isDetecting: boolean;
  actorError: string | null;
  onDetect: () => void;
  onClearActors: () => void;
  trackingResult: ActorTrackingResult | null;
  isTracking: boolean;
  trackingProgress: { currentFrame: number; totalFrames: number; message?: string } | null;
  trackingError: string | null;
  onTrack: () => void;
  onClearTracking: () => void;
}

export function ActorControls({
  detectedActors,
  isDetecting,
  actorError,
  onDetect,
  onClearActors,
  trackingResult,
  isTracking,
  trackingProgress,
  trackingError,
  onTrack,
  onClearTracking,
}: ActorControlsProps) {
  return (
    <div className="px-4 py-3 border-t space-y-2">
      <div className="flex items-center gap-2 flex-wrap">
        <Button
          variant={detectedActors ? "outline" : "default"}
          size="sm"
          onClick={onDetect}
          disabled={isDetecting || isTracking}
        >
          {isDetecting ? (
            <>
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              Detecting...
            </>
          ) : (
            <>
              <Scan className="w-4 h-4 mr-2" />
              Detect Actors
            </>
          )}
        </Button>
        <Button
          variant={trackingResult ? "outline" : "default"}
          size="sm"
          onClick={onTrack}
          disabled={isDetecting || isTracking}
        >
          {isTracking ? (
            <>
              <Loader2 className="w-4 h-4 mr-2 animate-spin" />
              {trackingProgress?.message ?? "Tracking..."}
            </>
          ) : (
            <>
              <Route className="w-4 h-4 mr-2" />
              Track Actors
            </>
          )}
        </Button>
        {detectedActors && !trackingResult && (
          <>
            <Badge variant="secondary">
              {detectedActors.length} actor{detectedActors.length !== 1 ? "s" : ""}
            </Badge>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClearActors}
            >
              <X className="w-4 h-4 mr-1" />
              Clear
            </Button>
          </>
        )}
        {trackingResult && (
          <>
            <Badge variant="secondary">
              {trackingResult.tracks.length} tracked actor{trackingResult.tracks.length !== 1 ? "s" : ""}
            </Badge>
            <Badge variant="outline">
              {trackingResult.keyframeTimestamps.length} frames
            </Badge>
            <Button
              variant="ghost"
              size="sm"
              onClick={onClearTracking}
            >
              <X className="w-4 h-4 mr-1" />
              Clear Tracks
            </Button>
          </>
        )}
        {(actorError || trackingError) && (
          <span className="text-sm text-red-600">{actorError || trackingError}</span>
        )}
      </div>
      {isTracking && trackingProgress && (
        <div className="w-full bg-muted rounded-full h-1.5 overflow-hidden">
          <div
            className="bg-primary h-full transition-all duration-300"
            style={{ width: `${Math.round((trackingProgress.currentFrame / trackingProgress.totalFrames) * 100)}%` }}
          />
        </div>
      )}
    </div>
  );
}

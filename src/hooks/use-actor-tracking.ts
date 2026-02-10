"use client";

import { useState, useCallback, useRef } from "react";
import {
  ActorDetectionResult,
  ActorTrackingResult,
  TrackingProgress,
} from "@/types/actors";
import { getAnthropicKey } from "@/lib/api";
import { buildTracks } from "@/lib/actor-matching";

const TRACKING_CACHE_PREFIX = "actor-tracking-";
const DETECTION_CACHE_PREFIX = "actor-detection-";
const KEYFRAME_INTERVAL = 1; // seconds between keyframes
const KEYFRAME_START = 0.5; // first keyframe timestamp

interface TrackParams {
  eventId: string;
  videoUrl: string;
  videoDuration: number;
  fovDegrees: number;
  cameraIntrinsics?: { focal: number; k1: number; k2: number };
  getCameraState: (timestamp: number) => { lat: number; lon: number; bearing: number };
}

interface UseActorTracking {
  trackingResult: ActorTrackingResult | null;
  isTracking: boolean;
  progress: TrackingProgress | null;
  error: string | null;
  track: (params: TrackParams) => void;
  clear: () => void;
}

function computeKeyframes(duration: number): number[] {
  const timestamps: number[] = [];
  for (let t = KEYFRAME_START; t < duration - 0.5; t += KEYFRAME_INTERVAL) {
    timestamps.push(Math.round(t * 10) / 10);
  }
  // Add a frame near the end
  const endFrame = Math.max(duration - 1, KEYFRAME_START);
  if (timestamps.length === 0 || timestamps[timestamps.length - 1] < endFrame - 1) {
    timestamps.push(Math.round(endFrame * 10) / 10);
  }
  return timestamps;
}

export function useActorTracking(): UseActorTracking {
  const [trackingResult, setTrackingResult] = useState<ActorTrackingResult | null>(null);
  const [isTracking, setIsTracking] = useState(false);
  const [progress, setProgress] = useState<TrackingProgress | null>(null);
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef(false);
  const lastEventIdRef = useRef<string | null>(null);

  const track = useCallback(async (params: TrackParams) => {
    const { eventId, videoUrl, videoDuration, fovDegrees, cameraIntrinsics, getCameraState } = params;

    setIsTracking(true);
    setError(null);
    setProgress(null);
    abortRef.current = false;
    lastEventIdRef.current = eventId;

    const keyframes = computeKeyframes(videoDuration);

    // Check full tracking cache — only use if keyframe count matches current settings
    const trackingCacheKey = `${TRACKING_CACHE_PREFIX}${eventId}`;
    try {
      const stored = localStorage.getItem(trackingCacheKey);
      if (stored) {
        const cached: ActorTrackingResult = JSON.parse(stored);
        if (cached.keyframeTimestamps.length === keyframes.length) {
          setTrackingResult(cached);
          setProgress({
            currentFrame: cached.keyframeTimestamps.length,
            totalFrames: cached.keyframeTimestamps.length,
            status: "done",
            message: "Loaded from cache",
          });
          setIsTracking(false);
          return;
        }
        // Stale cache (different keyframe count) — remove it
        localStorage.removeItem(trackingCacheKey);
      }
    } catch {
      // Ignore parse errors
    }

    const totalFrames = keyframes.length;
    const frameResults: ActorDetectionResult[] = [];

    try {
      for (let i = 0; i < keyframes.length; i++) {
        if (abortRef.current) {
          setIsTracking(false);
          setProgress(null);
          return;
        }

        const t = keyframes[i];
        setProgress({
          currentFrame: i + 1,
          totalFrames,
          status: "detecting",
          message: `Analyzing frame ${i + 1}/${totalFrames}...`,
        });

        // Check per-frame cache
        const frameCacheKey = `${DETECTION_CACHE_PREFIX}${eventId}-${t.toFixed(1)}`;
        let frameResult: ActorDetectionResult | null = null;

        try {
          const stored = localStorage.getItem(frameCacheKey);
          if (stored) {
            frameResult = JSON.parse(stored);
          }
        } catch {
          // Ignore
        }

        if (!frameResult) {
          const camera = getCameraState(t);
          const response = await fetch("/api/detect-actors", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              eventId,
              videoUrl,
              timestamp: t,
              cameraLat: camera.lat,
              cameraLon: camera.lon,
              cameraBearing: camera.bearing,
              fovDegrees,
              cameraIntrinsics,
              anthropicApiKey: getAnthropicKey(),
            }),
          });

          if (!response.ok) {
            const data = await response.json();
            throw new Error(data.error || `Detection failed at ${t}s: ${response.status}`);
          }

          frameResult = await response.json();

          // Cache per-frame result
          try {
            localStorage.setItem(frameCacheKey, JSON.stringify(frameResult));
          } catch {
            // localStorage full — non-fatal
          }
        }

        if (frameResult) {
          frameResults.push(frameResult);
        }
      }

      if (abortRef.current) {
        setIsTracking(false);
        setProgress(null);
        return;
      }

      // Match across frames
      setProgress({
        currentFrame: totalFrames,
        totalFrames,
        status: "matching",
        message: "Matching actors across frames...",
      });

      const result = buildTracks(frameResults, eventId);
      setTrackingResult(result);

      // Cache full result
      try {
        localStorage.setItem(trackingCacheKey, JSON.stringify(result));
      } catch {
        // localStorage full — non-fatal
      }

      setProgress({
        currentFrame: totalFrames,
        totalFrames,
        status: "done",
        message: `Tracked ${result.tracks.length} actor${result.tracks.length !== 1 ? "s" : ""} across ${totalFrames} frames`,
      });
    } catch (err) {
      setError(err instanceof Error ? err.message : "Tracking failed");
      setProgress({
        currentFrame: frameResults.length,
        totalFrames,
        status: "error",
        message: err instanceof Error ? err.message : "Tracking failed",
      });
    } finally {
      setIsTracking(false);
    }
  }, []);

  const clear = useCallback(() => {
    abortRef.current = true;
    // Clear localStorage caches for this event
    if (lastEventIdRef.current) {
      try {
        localStorage.removeItem(`${TRACKING_CACHE_PREFIX}${lastEventIdRef.current}`);
      } catch {
        // Non-fatal
      }
    }
    setTrackingResult(null);
    setProgress(null);
    setError(null);
  }, []);

  return { trackingResult, isTracking, progress, error, track, clear };
}

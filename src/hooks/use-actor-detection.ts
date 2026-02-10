"use client";

import { useState, useCallback } from "react";
import { DetectedActor, ActorDetectionResult } from "@/types/actors";
import { getAnthropicKey } from "@/lib/api";

const STORAGE_PREFIX = "actor-detection-";

interface DetectParams {
  eventId: string;
  videoUrl: string;
  timestamp: number;
  cameraLat: number;
  cameraLon: number;
  cameraBearing: number;
  fovDegrees: number;
  cameraIntrinsics?: { focal: number; k1: number; k2: number };
}

interface UseActorDetection {
  actors: DetectedActor[] | null;
  isDetecting: boolean;
  error: string | null;
  detect: (params: DetectParams) => void;
  clear: () => void;
}

export function useActorDetection(): UseActorDetection {
  const [actors, setActors] = useState<DetectedActor[] | null>(null);
  const [isDetecting, setIsDetecting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const detect = useCallback(async (params: DetectParams) => {
    setIsDetecting(true);
    setError(null);

    // Check localStorage cache
    const cacheKey = `${STORAGE_PREFIX}${params.eventId}-${params.timestamp.toFixed(1)}`;
    try {
      const stored = localStorage.getItem(cacheKey);
      if (stored) {
        const cached: ActorDetectionResult = JSON.parse(stored);
        setActors(cached.actors);
        setIsDetecting(false);
        return;
      }
    } catch {
      // Ignore parse errors
    }

    try {
      const response = await fetch("/api/detect-actors", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...params,
          anthropicApiKey: getAnthropicKey(),
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || `Detection failed: ${response.status}`);
      }

      const result: ActorDetectionResult = await response.json();
      setActors(result.actors);

      // Cache in localStorage
      try {
        localStorage.setItem(cacheKey, JSON.stringify(result));
      } catch {
        // localStorage full — non-fatal
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Detection failed");
    } finally {
      setIsDetecting(false);
    }
  }, []);

  const clear = useCallback(() => {
    setActors(null);
    setError(null);
  }, []);

  return { actors, isDetecting, error, detect, clear };
}

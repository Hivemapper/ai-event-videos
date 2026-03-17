import { useState } from "react";
import { getAnthropicKey, getMapboxToken } from "@/lib/api";

interface VideoScanEvent {
  eventId: string;
  lat: number;
  lon: number;
  eventType?: string;
}

interface VideoScanParams {
  query: string;
  events: VideoScanEvent[];
  model: "sonnet" | "haiku";
}

export interface ScanMatch {
  eventId: string;
  match: boolean;
  confidence: "high" | "medium" | "low";
  reason: string;
}

export interface VideoScanResult {
  matches: ScanMatch[];
  query: string;
  model: string;
  eventsScanned: number;
}

export function useVideoScan() {
  const [result, setResult] = useState<VideoScanResult | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = async (
    params: VideoScanParams
  ): Promise<VideoScanResult | null> => {
    setIsRunning(true);
    setError(null);
    setResult(null);

    try {
      const res = await fetch("/api/vision-scan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...params,
          anthropicApiKey: getAnthropicKey() || undefined,
          mapboxToken: getMapboxToken() || undefined,
        }),
      });

      const data = await res.json();

      if (!res.ok) {
        if (data.error === "NO_API_KEY") {
          setError("Anthropic API key required. Set it in Settings.");
        } else {
          setError(data.error || `Error: ${res.status}`);
        }
        return null;
      }

      const scanResult = data as VideoScanResult;
      setResult(scanResult);
      return scanResult;
    } catch (err) {
      setError(
        err instanceof Error ? err.message : "Vision scan request failed"
      );
      return null;
    } finally {
      setIsRunning(false);
    }
  };

  const clear = () => {
    setResult(null);
    setError(null);
  };

  return { result, isRunning, error, run, clear };
}

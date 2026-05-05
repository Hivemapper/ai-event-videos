"use client";

import { useEffect, useMemo, useState } from "react";
import { Check, ExternalLink, Loader2, Scissors } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";

interface ClipResponse {
  id: string;
  eventUrl: string;
  videoUrl: string;
  stats?: {
    clipDurationSeconds?: number;
    gnssSamples?: number;
    imuSamples?: number;
    frameDetections?: number;
    detectionSegments?: number;
  };
  error?: string;
}

interface VideoClipperProps {
  eventId: string;
  currentTime: number;
  duration: number;
  className?: string;
}

function formatSeconds(value: number): string {
  if (!Number.isFinite(value)) return "0";
  return value.toFixed(2).replace(/\.?0+$/, "");
}

function parseSeconds(value: string): number | null {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function VideoClipper({
  eventId,
  currentTime,
  duration,
  className,
}: VideoClipperProps) {
  const [startSeconds, setStartSeconds] = useState("0");
  const [endSeconds, setEndSeconds] = useState("");
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<ClipResponse | null>(null);

  useEffect(() => {
    if (duration > 0) setEndSeconds(formatSeconds(duration));
  }, [duration, eventId]);

  const parsedStart = parseSeconds(startSeconds);
  const parsedEnd = parseSeconds(endSeconds);
  const clipDuration = useMemo(() => {
    if (parsedStart === null || parsedEnd === null) return null;
    return parsedEnd - parsedStart;
  }, [parsedStart, parsedEnd]);
  const hasValidRange =
    parsedStart !== null &&
    parsedEnd !== null &&
    parsedStart >= 0 &&
    parsedEnd > parsedStart &&
    (duration <= 0 || parsedEnd <= duration + 0.25);

  const setCurrentAsStart = () => {
    const nextStart = Math.max(0, Math.min(currentTime, Math.max(0, duration - 0.25)));
    setStartSeconds(formatSeconds(nextStart));
    if (parsedEnd !== null && parsedEnd <= nextStart && duration > nextStart) {
      setEndSeconds(formatSeconds(duration));
    }
  };

  const setCurrentAsEnd = () => {
    const nextEnd = Math.max(0, Math.min(currentTime, duration || currentTime));
    setEndSeconds(formatSeconds(nextEnd));
  };

  const createClip = async () => {
    if (!hasValidRange || parsedStart === null || parsedEnd === null) return;
    setIsPending(true);
    setError(null);
    setResult(null);

    try {
      const headers: Record<string, string> = {
        "Content-Type": "application/json",
      };
      const apiKey = getApiKey();
      if (apiKey) headers.Authorization = apiKey;

      const response = await fetch(`/api/events/${eventId}/clip`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          startSeconds: parsedStart,
          endSeconds: parsedEnd,
        }),
      });
      const body = (await response.json()) as ClipResponse;
      if (!response.ok) {
        throw new Error(body.error ?? `Clip failed: ${response.status}`);
      }
      setResult(body);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setIsPending(false);
    }
  };

  return (
    <div className={cn("rounded-lg border bg-card px-4 py-3", className)}>
      <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold">
          <Scissors className="h-4 w-4" />
          Clip
        </h3>
        <Badge variant="outline" className="font-mono text-xs">
          {clipDuration !== null && clipDuration > 0 ? `${formatSeconds(clipDuration)}s` : "--"}
        </Badge>
      </div>

      <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
        <label className="space-y-1.5">
          <span className="text-xs font-medium text-muted-foreground">Start</span>
          <div className="flex gap-2">
            <Input
              type="number"
              min={0}
              step={0.1}
              value={startSeconds}
              onChange={(event) => setStartSeconds(event.target.value)}
              className="font-mono"
            />
            <Button type="button" variant="outline" size="sm" onClick={setCurrentAsStart}>
              Current
            </Button>
          </div>
        </label>

        <label className="space-y-1.5">
          <span className="text-xs font-medium text-muted-foreground">End</span>
          <div className="flex gap-2">
            <Input
              type="number"
              min={0}
              step={0.1}
              value={endSeconds}
              onChange={(event) => setEndSeconds(event.target.value)}
              className="font-mono"
            />
            <Button type="button" variant="outline" size="sm" onClick={setCurrentAsEnd}>
              Current
            </Button>
          </div>
        </label>
      </div>

      <div className="mt-3 flex flex-wrap items-center justify-between gap-2">
        <Button
          type="button"
          size="sm"
          onClick={createClip}
          disabled={!hasValidRange || isPending}
        >
          {isPending ? (
            <Loader2 className="mr-2 h-4 w-4 animate-spin" />
          ) : (
            <Scissors className="mr-2 h-4 w-4" />
          )}
          {isPending ? "Clipping..." : "Create Clip"}
        </Button>
        <span className="font-mono text-xs text-muted-foreground">
          {formatSeconds(Math.max(0, currentTime))}s / {duration > 0 ? `${formatSeconds(duration)}s` : "--"}
        </span>
      </div>

      {error && (
        <p className="mt-3 text-sm text-destructive">{error}</p>
      )}

      {result && (
        <div className="mt-3 flex flex-wrap items-center gap-2 text-sm">
          <span className="inline-flex items-center gap-1 text-green-700">
            <Check className="h-4 w-4" />
            {result.id}
          </span>
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={() => window.open(result.eventUrl, "_blank", "noopener,noreferrer")}
          >
            <ExternalLink className="mr-2 h-4 w-4" />
            Open
          </Button>
          {result.stats && (
            <span className="text-xs text-muted-foreground">
              {result.stats.gnssSamples ?? 0} GNSS, {result.stats.frameDetections ?? 0} detections
            </span>
          )}
        </div>
      )}
    </div>
  );
}

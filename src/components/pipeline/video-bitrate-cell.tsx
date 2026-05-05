"use client";

import { useCallback, useState } from "react";
import { Loader2 } from "lucide-react";
import { getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";
import { useVideoBitrate } from "@/hooks/use-video-bitrate";

function formatMbps(bps: number): string {
  return (bps / 1_000_000).toFixed(2);
}

interface VideoBitrateCellProps {
  eventId?: string;
  videoUrl?: string | null;
  className?: string;
}

export function VideoBitrateCell({
  eventId,
  videoUrl,
  className,
}: VideoBitrateCellProps) {
  const [requested, setRequested] = useState(Boolean(videoUrl));
  const [resolvedVideoUrl, setResolvedVideoUrl] = useState<string | null>(videoUrl ?? null);
  const [isResolving, setIsResolving] = useState(false);
  const [eventError, setEventError] = useState(false);
  const activeVideoUrl = videoUrl ?? resolvedVideoUrl;
  const {
    bitrate,
    isLoading: bitrateLoading,
    error: bitrateError,
  } = useVideoBitrate(activeVideoUrl, {
    eager: requested && Boolean(activeVideoUrl),
  });

  const handleProbe = useCallback(async () => {
    setRequested(true);
    if (activeVideoUrl || !eventId || isResolving) return;

    const apiKey = getApiKey();
    const headers: Record<string, string> = {};
    if (apiKey) headers.Authorization = apiKey;

    setEventError(false);
    setIsResolving(true);
    try {
      const response = await fetch(`/api/events/${eventId}`, { headers });
      const data = (await response.json()) as { videoUrl?: unknown };
      if (!response.ok || typeof data.videoUrl !== "string") {
        setEventError(true);
        return;
      }
      setResolvedVideoUrl(data.videoUrl);
    } catch {
      setEventError(true);
    } finally {
      setIsResolving(false);
    }
  }, [activeVideoUrl, eventId, isResolving]);

  if (!requested) {
    return (
      <button
        type="button"
        onClick={handleProbe}
        className={cn(
          "font-mono text-xs text-muted-foreground underline-offset-2 hover:text-foreground hover:underline",
          className
        )}
        title="Probe video bitrate"
      >
        Probe
      </button>
    );
  }

  return (
    <span className={cn("font-mono text-xs text-muted-foreground tabular-nums", className)}>
      {eventError || bitrateError ? (
        "—"
      ) : bitrate ? (
        formatMbps(bitrate.bps)
      ) : isResolving || bitrateLoading ? (
        <Loader2 className="inline h-3 w-3 animate-spin" />
      ) : (
        "—"
      )}
    </span>
  );
}

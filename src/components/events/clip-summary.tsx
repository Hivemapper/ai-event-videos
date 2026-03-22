"use client";

import { useState, useEffect, useRef } from "react";
import { Pencil, Check, X, Sparkles, Loader2, Gauge, Zap, Clock, CircleAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AIEvent, AIEventType } from "@/types/events";
import { SpeedDataPoint } from "@/lib/event-helpers";
import { getApiKey } from "@/lib/api";

interface ClipSummaryProps {
  videoId: string;
  event: AIEvent;
  countryName: string | null;
  roadType: string | null;
  timeOfDay: string;
  duration: number;
  vruLabels?: string[];
  speedLimit?: { limit: number; unit: string } | null;
  exceedsSpeedLimit?: boolean;
}

function speedMsToMph(speedMs: number): number {
  return Math.round(speedMs * 2.237);
}

function generateDefaultSummary({
  event,
  countryName,
  roadType,
  timeOfDay,
  duration,
  vruLabels,
}: Omit<ClipSummaryProps, "videoId">): string {
  const speedData = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;
  const maxSpeedMs = speedData ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS)) : null;
  const minSpeedMs = speedData ? Math.min(...speedData.map((s) => s.AVG_SPEED_MS)) : null;

  // Location string
  const location = countryName ?? "unknown location";
  const road = roadType ? `on a ${roadType.toLowerCase()}` : "";
  const tod = timeOfDay ? `during the ${timeOfDay.toLowerCase()}` : "";

  const maxMph = maxSpeedMs !== null ? speedMsToMph(maxSpeedMs) : null;
  const minMph = minSpeedMs !== null ? speedMsToMph(minSpeedMs) : null;

  const vruNote =
    vruLabels && vruLabels.length > 0
      ? ` ${vruLabels.join(", ")} detected nearby.`
      : "";

  const parts: string[] = [];

  switch (event.type) {
    case "HARSH_BRAKING":
      if (maxMph !== null && minMph !== null) {
        parts.push(
          `Driver in ${location} braked hard from ${maxMph} mph to ${minMph} mph${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      } else {
        parts.push(
          `Driver in ${location} braked hard${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      }
      break;

    case "HIGH_SPEED":
      if (maxMph !== null) {
        parts.push(
          `Driver in ${location} hit max speed of ${maxMph} mph${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      } else {
        parts.push(
          `Driver in ${location} exceeded speed threshold${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      }
      break;

    case "AGGRESSIVE_ACCELERATION":
      if (acceleration !== undefined && maxMph !== null) {
        parts.push(
          `Driver in ${location} accelerated aggressively (${acceleration.toFixed(1)} m/s\u00B2) reaching ${maxMph} mph${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      } else {
        parts.push(
          `Driver in ${location} accelerated aggressively${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
        );
      }
      break;

    case "SWERVING":
      parts.push(
        `Driver in ${location} swerved${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    case "HIGH_G_FORCE":
      parts.push(
        `Driver in ${location} experienced high g-force${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    case "STOP_SIGN_VIOLATION":
      parts.push(
        `Driver in ${location} ran a stop sign${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    case "TRAFFIC_LIGHT_VIOLATION":
      parts.push(
        `Driver in ${location} ran a red light${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    case "TAILGATING":
      parts.push(
        `Driver in ${location} was tailgating${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    case "MANUAL_REQUEST":
      parts.push(
        `Manual recording captured in ${location}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
      break;

    default:
      parts.push(
        `Driving event recorded in ${location}${road ? ` ${road}` : ""}${tod ? ` ${tod}` : ""}.`
      );
  }

  if (vruNote) {
    parts.push(vruNote);
  }

  return parts.join(" ").replace(/\s+/g, " ").trim();
}

export function ClipSummary({
  videoId,
  event,
  countryName,
  roadType,
  timeOfDay,
  duration,
  vruLabels,
  speedLimit,
  exceedsSpeedLimit,
}: ClipSummaryProps) {
  const [summary, setSummary] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [improving, setImproving] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const defaultSummary = generateDefaultSummary({
    event,
    countryName,
    roadType,
    timeOfDay,
    duration,
    vruLabels,
  });

  // Load saved summary
  useEffect(() => {
    fetch(`/api/videos/${videoId}/clip-summary`)
      .then((r) => r.json())
      .then((d) => {
        setSummary(d.summary);
        setLoaded(true);
      })
      .catch(() => setLoaded(true));
  }, [videoId]);

  // Auto-save default summary if none exists
  useEffect(() => {
    if (loaded && summary === null && defaultSummary) {
      setSummary(defaultSummary);
      fetch(`/api/videos/${videoId}/clip-summary`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ summary: defaultSummary }),
      }).catch(() => {});
    }
  }, [loaded, summary, defaultSummary, videoId]);

  const displaySummary = summary ?? defaultSummary;

  const startEditing = () => {
    setDraft(displaySummary);
    setEditing(true);
    setTimeout(() => inputRef.current?.focus(), 0);
  };

  const cancel = () => {
    setEditing(false);
    setDraft("");
  };

  const save = async (text?: string) => {
    const value = text ?? draft;
    setSaving(true);
    try {
      const res = await fetch(`/api/videos/${videoId}/clip-summary`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ summary: value }),
      });
      const data = await res.json();
      setSummary(data.summary);
      setEditing(false);
    } finally {
      setSaving(false);
    }
  };

  const improve = async () => {
    const apiKey = getApiKey();
    if (!apiKey) return;

    setImproving(true);
    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          message: `Improve this driving event clip summary. Keep it to 1-2 sentences, factual and concise. Include any VRU (vulnerable road user) detections if relevant.

Current summary: "${displaySummary}"

Event type: ${event.type}
Location: ${countryName ?? "unknown"}
Road type: ${roadType ?? "unknown"}
Time of day: ${timeOfDay}
Duration: ${duration}s
VRU labels detected: ${vruLabels?.length ? vruLabels.join(", ") : "none"}
${event.metadata?.ACCELERATION_MS2 ? `Acceleration: ${(event.metadata.ACCELERATION_MS2 as number).toFixed(2)} m/s²` : ""}
${event.metadata?.SPEED_ARRAY ? `Speed data available (max ${speedMsToMph(Math.max(...(event.metadata.SPEED_ARRAY as SpeedDataPoint[]).map((s) => s.AVG_SPEED_MS)))} mph)` : ""}

Return ONLY the improved summary text, nothing else.`,
        }),
      });
      const data = await res.json();
      const improved = data.response?.trim() || data.text?.trim();
      if (improved) {
        await save(improved);
      }
    } catch {
      // silently fail
    } finally {
      setImproving(false);
    }
  };

  // Stats
  const speedData = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;
  const maxSpeedMs = speedData ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS)) : null;
  const minSpeedMs = speedData ? Math.min(...speedData.map((s) => s.AVG_SPEED_MS)) : null;

  if (editing) {
    return (
      <Card className="gap-4">
        <CardHeader>
          <CardTitle className="text-lg">Clip Summary</CardTitle>
        </CardHeader>
        <CardContent className="space-y-3">
          <textarea
            ref={inputRef}
            value={draft}
            onChange={(e) => setDraft(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && !e.shiftKey) {
                e.preventDefault();
                save();
              }
              if (e.key === "Escape") cancel();
            }}
            rows={3}
            className="w-full rounded-lg border border-border bg-muted/30 px-3 py-2.5 text-sm leading-relaxed focus:outline-none focus:ring-2 focus:ring-ring resize-none"
          />
          <div className="flex items-center gap-2">
            <Button size="sm" onClick={() => save()} disabled={saving} className="gap-1.5">
              <Check className="w-3.5 h-3.5" />
              Save
            </Button>
            <Button size="sm" variant="ghost" onClick={cancel}>
              Cancel
            </Button>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="gap-4">
      <CardHeader>
        <CardTitle className="text-lg">Clip Summary</CardTitle>
      </CardHeader>
      <CardContent className="space-y-3">
        {/* Stats row */}
        <div className="flex flex-wrap gap-x-5 gap-y-2 text-sm">
          {duration > 0 && (
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Clock className="w-3.5 h-3.5" />
              <span><strong className="font-semibold text-foreground">{Math.round(duration)}s</strong></span>
            </div>
          )}
          {acceleration !== undefined && (
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Zap className="w-3.5 h-3.5" />
              <span><strong className="font-semibold text-foreground">{acceleration.toFixed(2)} m/s&sup2;</strong> accel</span>
            </div>
          )}
          {maxSpeedMs !== null && (
            <div className={`flex items-center gap-1.5 ${exceedsSpeedLimit ? "text-red-500" : "text-muted-foreground"}`}>
              <Gauge className="w-3.5 h-3.5" />
              <span><strong className={`font-semibold ${exceedsSpeedLimit ? "text-red-600" : "text-foreground"}`}>{speedMsToMph(maxSpeedMs)} mph</strong> max</span>
            </div>
          )}
          {minSpeedMs !== null && maxSpeedMs !== minSpeedMs && (
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Gauge className="w-3.5 h-3.5" />
              <span><strong className="font-semibold text-foreground">{speedMsToMph(minSpeedMs)} mph</strong> min</span>
            </div>
          )}
          {speedLimit && (
            <div className={`flex items-center gap-1.5 ${exceedsSpeedLimit ? "text-red-500" : "text-muted-foreground"}`}>
              <CircleAlert className="w-3.5 h-3.5" />
              <span><strong className={`font-semibold ${exceedsSpeedLimit ? "text-red-600" : "text-foreground"}`}>{speedLimit.limit} {speedLimit.unit}</strong> limit</span>
            </div>
          )}
        </div>

        {/* Summary text */}
        <div className="rounded-lg bg-muted/40 px-4 py-3">
          <p className="text-sm leading-relaxed">{displaySummary}</p>
        </div>

        {/* Actions */}
        <div className="flex items-center justify-between">
          <button
            onClick={startEditing}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
          >
            <Pencil className="w-3 h-3" />
            Edit
          </button>
          <button
            onClick={improve}
            disabled={improving}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
          >
            {improving ? (
              <Loader2 className="w-3 h-3 animate-spin" />
            ) : (
              <Sparkles className="w-3 h-3" />
            )}
            {improving ? "Improving..." : "Improve"}
          </button>
        </div>
      </CardContent>
    </Card>
  );
}

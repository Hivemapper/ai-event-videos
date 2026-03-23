"use client";

import { useState, useEffect, useRef } from "react";
import { Pencil, Check, Gauge, Clock, CircleAlert } from "lucide-react";
import { Button } from "@/components/ui/button";
import { AIEvent, AIEventType } from "@/types/events";
import { SpeedDataPoint, deriveSpeedFromGnss } from "@/lib/event-helpers";
import { formatDetectionSentence, type DetectionSummary } from "@/lib/detection-summary";

interface ClipSummaryProps {
  videoId: string;
  event: AIEvent;
  countryName: string | null;
  roadType: string | null;
  roadName: string | null;
  timeOfDay: string;
  duration: number;
  detections?: DetectionSummary;
  speedLimit?: { limit: number; unit: string } | null;
  exceedsSpeedLimit?: boolean;
  weather?: string | null;
  timeline?: Array<{ startSec: number; endSec: number; event: string; details: string }> | null;
}

function speedMsToMph(speedMs: number): number {
  return Math.round(speedMs * 2.237);
}

/** Map timeOfDay + local hour to a natural-language period.
 *  SunCalc gives us Dawn/Day/Dusk/Night based on solar position.
 *  We refine "Day" using the local hour for more natural phrasing. */
function timeOfDayLabel(timeOfDay: string, localHour?: number): string {
  if (timeOfDay === "Dawn") return "at dawn";
  if (timeOfDay === "Dusk") return "at dusk";
  if (timeOfDay === "Night") return "at night";
  // "Day" — refine by local hour
  if (localHour !== undefined) {
    if (localHour < 9) return "in the early morning";
    if (localHour < 12) return "in the morning";
    if (localHour < 17) return "in the afternoon";
    return "in the evening";
  }
  return "in the daytime";
}

/** Format time as "at 6:14 AM in the early morning" using longitude-estimated local time */
function formatTimeContext(timeOfDay: string, timestamp?: string, lon?: number): string {
  if (timestamp) {
    const date = new Date(timestamp);
    const offsetHours = lon !== undefined ? Math.round(lon / 15) : 0;
    const local = new Date(date.getTime() + offsetHours * 60 * 60 * 1000);
    const hours = local.getUTCHours();
    const minutes = local.getUTCMinutes();
    const period = timeOfDayLabel(timeOfDay, hours);
    const ampm = hours >= 12 ? "PM" : "AM";
    const h = hours % 12 || 12;
    const m = minutes.toString().padStart(2, "0");
    return `at ${h}:${m} ${ampm}${period ? ` ${period}` : ""}`;
  }
  return timeOfDayLabel(timeOfDay) || "";
}

/** Compute braking duration from speed array timestamps */
function computeBrakingDurationSec(speedData: SpeedDataPoint[]): number | null {
  if (speedData.length < 2) return null;
  // Find the peak speed point and the min speed point after it
  let peakIdx = 0;
  for (let i = 1; i < speedData.length; i++) {
    if (speedData[i].AVG_SPEED_MS > speedData[peakIdx].AVG_SPEED_MS) peakIdx = i;
  }
  let minIdx = peakIdx;
  for (let i = peakIdx + 1; i < speedData.length; i++) {
    if (speedData[i].AVG_SPEED_MS < speedData[minIdx].AVG_SPEED_MS) minIdx = i;
  }
  if (minIdx <= peakIdx) return null;
  const deltaMs = speedData[minIdx].TIMESTAMP - speedData[peakIdx].TIMESTAMP;
  return deltaMs > 0 ? deltaMs / 1000 : null;
}

const WEATHER_PHRASES: Record<string, string> = {
  rain: "in rainy conditions",
  snow: "in snowy conditions",
  fog: "in foggy conditions",
  overcast: "under overcast skies",
};

function generateDefaultSummary({
  event,
  countryName,
  roadType,
  roadName,
  timeOfDay,
  duration,
  detections,
  speedLimit,
  exceedsSpeedLimit,
  weather,
}: Omit<ClipSummaryProps, "videoId">): string {
  const rawSpeedData = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const speedData = (rawSpeedData && rawSpeedData.length > 0)
    ? rawSpeedData
    : event.gnssData ? deriveSpeedFromGnss(event.gnssData) : undefined;
  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;
  const maxSpeedMs = speedData && speedData.length > 0 ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS)) : null;
  const minSpeedMs = speedData && speedData.length > 0 ? Math.min(...speedData.map((s) => s.AVG_SPEED_MS)) : null;

  // Location string: "City, Country" → "City (Country)"
  const location = (() => {
    if (!countryName) return "unknown location";
    const parts = countryName.split(", ");
    if (parts.length >= 2) {
      return `${parts.slice(0, -1).join(", ")} (${parts[parts.length - 1]})`;
    }
    return countryName;
  })();
  const road = (() => {
    if (roadName && roadType) return `on ${roadName} (${roadType.toLowerCase()})`;
    if (roadName) return `on ${roadName}`;
    if (roadType) return `on a ${roadType.toLowerCase()}`;
    return "";
  })();
  const tod = formatTimeContext(timeOfDay, event.timestamp, event.location?.lon);

  const maxMph = maxSpeedMs !== null ? speedMsToMph(maxSpeedMs) : null;
  const minMph = minSpeedMs !== null ? speedMsToMph(minSpeedMs) : null;

  const detectionNote =
    detections && Object.keys(detections.counts).length > 0
      ? ` ${formatDetectionSentence(detections)}`
      : "";

  // Speed limit exceedance fragment
  const speedLimitNote = (() => {
    if (!exceedsSpeedLimit || !speedLimit || maxMph === null) return "";
    const limitMph = speedLimit.unit === "km/h"
      ? Math.round(speedLimit.limit * 0.621371)
      : speedLimit.limit;
    const over = maxMph - limitMph;
    if (over <= 0) return "";
    return ` Exceeded ${speedLimit.limit} ${speedLimit.unit} limit by ${over} mph.`;
  })();

  const parts: string[] = [];

  switch (event.type) {
    case "HARSH_BRAKING": {
      const brakingDur = speedData ? computeBrakingDurationSec(speedData) : null;
      const durPhrase = brakingDur !== null ? ` over ${brakingDur.toFixed(1)}s` : "";
      const speedPhrase = (maxMph !== null && minMph !== null)
        ? ` causing speed to drop from ${maxMph} to ${minMph} mph${durPhrase}`
        : "";

      parts.push(
        `Driver in ${location} braked hard${speedPhrase}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "HIGH_SPEED": {
      parts.push(
        `Driver in ${location} reached ${maxMph ?? "high"} mph${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      // Sustained speed: how long above 80% of max
      if (speedData && maxSpeedMs !== null && speedData.length >= 2) {
        const threshold = maxSpeedMs * 0.8;
        const above = speedData.filter((s) => s.AVG_SPEED_MS >= threshold);
        if (above.length >= 2) {
          const sustainedSec = (above[above.length - 1].TIMESTAMP - above[0].TIMESTAMP) / 1000;
          if (sustainedSec > 1) {
            parts.push(`Sustained above ${speedMsToMph(threshold)} mph for ${sustainedSec.toFixed(1)}s.`);
          }
        }
      }
      if (minMph !== null && maxMph !== null && maxMph - minMph > 5) {
        parts.push(`Speed ranged from ${minMph} to ${maxMph} mph.`);
      }
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "AGGRESSIVE_ACCELERATION": {
      parts.push(
        `Driver in ${location} accelerated aggressively${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (minMph !== null && maxMph !== null) {
        // Compute acceleration duration from speed array (min→max)
        let accelDur: string | null = null;
        if (speedData && speedData.length >= 2) {
          let minIdx = 0;
          for (let i = 1; i < speedData.length; i++) {
            if (speedData[i].AVG_SPEED_MS < speedData[minIdx].AVG_SPEED_MS) minIdx = i;
          }
          let maxIdx = minIdx;
          for (let i = minIdx + 1; i < speedData.length; i++) {
            if (speedData[i].AVG_SPEED_MS > speedData[maxIdx].AVG_SPEED_MS) maxIdx = i;
          }
          if (maxIdx > minIdx) {
            const sec = (speedData[maxIdx].TIMESTAMP - speedData[minIdx].TIMESTAMP) / 1000;
            if (sec > 0) accelDur = sec.toFixed(1);
          }
        }
        parts.push(`Speed increased from ${minMph} to ${maxMph} mph${accelDur ? ` over ${accelDur}s` : ""}.`);
      }
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "SWERVING": {
      parts.push(
        `Driver in ${location} swerved${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "HIGH_G_FORCE": {
      const gPhrase = acceleration !== undefined ? ` (${Math.abs(acceleration).toFixed(1)} m/s\u00B2)` : "";
      parts.push(
        `Driver in ${location} experienced high g-force${gPhrase}${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "STOP_SIGN_VIOLATION": {
      parts.push(
        `Driver in ${location} ran a stop sign${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (minMph !== null) {
        if (minMph === 0) {
          parts.push("Vehicle came to a full stop.");
        } else {
          parts.push(`Minimum speed was ${minMph} mph (rolling stop).`);
        }
      }
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "TRAFFIC_LIGHT_VIOLATION": {
      parts.push(
        `Driver in ${location} ran a red light${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (minMph !== null && maxMph !== null) {
        if (maxMph - minMph < 3) {
          parts.push("No significant deceleration detected.");
        } else {
          parts.push(`Speed dropped from ${maxMph} to ${minMph} mph.`);
        }
      }
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "TAILGATING": {
      parts.push(
        `Driver in ${location} was tailgating${maxMph ? ` at ${maxMph} mph` : ""}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      if (minMph !== null && maxMph !== null && maxMph - minMph > 3) {
        parts.push(`Speed ranged from ${minMph} to ${maxMph} mph.`);
      }
      if (speedLimitNote) parts.push(speedLimitNote);
      break;
    }

    case "MANUAL_REQUEST":
      parts.push(
        `Manual recording captured in ${location}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
      break;

    default:
      parts.push(
        `Driving event recorded in ${location}${road ? ` ${road}` : ""} ${tod}.`.replace(/\s+/g, " ")
      );
  }

  if (detectionNote) {
    parts.push(detectionNote);
  }

  // Add weather context (skip "clear skies" — that's the uninteresting default)
  const weatherPhrase = weather ? WEATHER_PHRASES[weather] : null;
  if (weatherPhrase) {
    // Append to the first sentence (before the period)
    if (parts.length > 0) {
      parts[0] = parts[0].replace(/\.\s*$/, ` ${weatherPhrase}.`);
    }
  }

  return parts.join(" ").replace(/\s+/g, " ").trim();
}

export function ClipSummary({
  videoId,
  event,
  countryName,
  roadType,
  roadName,
  timeOfDay,
  duration,
  detections,
  speedLimit,
  exceedsSpeedLimit,
  weather,
  timeline,
}: ClipSummaryProps) {
  const [summary, setSummary] = useState<string | null>(null);
  const [loaded, setLoaded] = useState(false);
  const [draft, setDraft] = useState("");
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const inputRef = useRef<HTMLTextAreaElement>(null);

  const defaultSummary = generateDefaultSummary({
    event,
    countryName,
    roadType,
    roadName,
    timeOfDay,
    duration,
    detections,
    speedLimit,
    exceedsSpeedLimit,
    weather,
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

  // Auto-save/refresh summary once location data is available.
  // Skip saving if countryName hasn't loaded yet to avoid persisting "unknown location".
  useEffect(() => {
    if (!loaded || !defaultSummary || !countryName) return;
    if (summary === null) {
      setSummary(defaultSummary);
      fetch(`/api/videos/${videoId}/clip-summary`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ summary: defaultSummary }),
      }).catch(() => {});
    } else if (summary.startsWith("Driver in ") && defaultSummary.startsWith("Driver in ") && summary !== defaultSummary) {
      // Saved summary looks auto-generated but is stale — refresh it
      setSummary(defaultSummary);
      fetch(`/api/videos/${videoId}/clip-summary`, {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ summary: defaultSummary }),
      }).catch(() => {});
    }
  }, [loaded, summary, defaultSummary, videoId, countryName]);

  // Always show the live-computed default for auto-generated summaries.
  // Only show the saved summary if it was manually edited (doesn't start with "Driver in").
  const displaySummary = (summary && !summary.startsWith("Driver in "))
    ? summary
    : defaultSummary;

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

  // Stats
  const speedData = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  const maxSpeedMs = speedData ? Math.max(...speedData.map((s) => s.AVG_SPEED_MS)) : null;
  const minSpeedMs = speedData ? Math.min(...speedData.map((s) => s.AVG_SPEED_MS)) : null;

  if (editing) {
    return (
      <div className="space-y-3 rounded-lg border bg-card px-4 py-3">
        <h3 className="text-lg font-semibold">Clip Summary</h3>
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
      </div>
    );
  }

  return (
    <div className="space-y-3 rounded-lg border bg-card px-4 py-3">
        <h3 className="text-lg font-semibold">Clip Summary</h3>
        {/* Stats row */}
        <div className="flex flex-wrap gap-x-5 gap-y-2 text-sm">
          {duration > 0 && (
            <div className="flex items-center gap-1.5 text-muted-foreground">
              <Clock className="w-3.5 h-3.5" />
              <span><strong className="font-semibold text-foreground">{Math.round(duration)}s</strong></span>
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
        <div className="flex items-center">
          <button
            onClick={startEditing}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-muted-foreground hover:text-foreground transition-colors"
          >
            <Pencil className="w-3 h-3" />
            Edit
          </button>
        </div>

        {/* Timeline */}
        {timeline && timeline.length > 0 && (
          <div className="mt-3 border-t pt-3">
            <table className="w-full text-sm">
              <thead>
                <tr className="text-left text-muted-foreground">
                  <th className="pb-2 pr-4 font-medium w-24">Time (s)</th>
                  <th className="pb-2 pr-4 font-medium w-44">Event</th>
                  <th className="pb-2 font-medium">Details</th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {timeline.map((seg, i) => (
                  <tr key={i} className="align-top">
                    <td className="py-2.5 pr-4 tabular-nums text-muted-foreground">
                      {seg.startSec}&ndash;{seg.endSec}
                    </td>
                    <td className="py-2.5 pr-4 font-medium">{seg.event}</td>
                    <td className="py-2.5 text-muted-foreground">{seg.details}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
    </div>
  );
}

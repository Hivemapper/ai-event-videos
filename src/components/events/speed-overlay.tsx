"use client";

import { SpeedUnit, speedLabel as getSpeedLabel } from "@/lib/api";
import { SpeedDataPoint } from "@/lib/event-helpers";
import { cn } from "@/lib/utils";

interface SpeedOverlayProps {
  speedData: SpeedDataPoint[];
  currentTime: number;
  duration: number;
  unit: SpeedUnit;
  speedLimit?: { limit: number; unit: string } | null;
}

export function SpeedOverlay({
  speedData,
  currentTime,
  duration,
  unit,
  speedLimit,
}: SpeedOverlayProps) {
  if (!speedData.length || duration <= 0) return null;

  const progress = Math.min(Math.max(currentTime / duration, 0), 1);
  const exactIndex = progress * (speedData.length - 1);
  const lowIndex = Math.floor(exactIndex);
  const highIndex = Math.min(lowIndex + 1, speedData.length - 1);
  const fraction = exactIndex - lowIndex;

  const speedMs =
    speedData[lowIndex].AVG_SPEED_MS * (1 - fraction) +
    speedData[highIndex].AVG_SPEED_MS * fraction;
  const speedKmh = speedMs * 3.6;
  const displaySpeed = unit === "mph" ? Math.round(speedKmh * 0.621371) : Math.round(speedKmh);

  let exceeding = false;
  if (speedLimit) {
    const limitInDisplayUnit =
      speedLimit.unit === unit
        ? speedLimit.limit
        : unit === "mph"
          ? speedLimit.limit * 0.621371
          : speedLimit.limit * 1.60934;
    exceeding = displaySpeed > limitInDisplayUnit;
  }

  return (
    <div
      key={exceeding ? "exceeding" : "normal"}
      className={cn(
        "absolute top-4 left-4 backdrop-blur-sm rounded-lg px-3 py-1.5 flex items-baseline gap-1 pointer-events-none transition-colors border",
        exceeding
          ? "bg-red-600 text-white border-red-400/50 animate-[speed-pulse_0.6s_ease-out]"
          : "bg-black/60 text-white border-white/20"
      )}
    >
      <span className="font-mono text-xl font-bold">{displaySpeed}</span>
      <span className={cn("text-sm", exceeding ? "text-white/90" : "text-white/70")}>{getSpeedLabel(unit)}</span>
    </div>
  );
}

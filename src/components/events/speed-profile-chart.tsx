"use client";

import { useRef, useState, useCallback, useMemo } from "react";
import { GnssDataPoint, ImuDataPoint } from "@/types/events";
import { SpeedUnit } from "@/lib/api";
import { generateSpeedProfile } from "@/lib/speed-profile";

interface SpeedArrayPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

interface SpeedProfileChartProps {
  speedArray?: SpeedArrayPoint[];
  gnssData?: GnssDataPoint[];
  imuData?: ImuDataPoint[];
  currentTime: number;
  duration: number;
  speedLimit?: { limit: number; unit: string } | null;
  unit: SpeedUnit;
  onSeek: (time: number) => void;
}

const PADDING = { top: 12, right: 12, bottom: 24, left: 44 };
const HEIGHT = 200;

function speedMsToDisplay(speedMs: number, unit: SpeedUnit): number {
  return unit === "mph" ? speedMs * 2.237 : speedMs * 3.6;
}

function speedLimitToMs(limit: number, limitUnit: string): number {
  if (limitUnit === "km/h" || limitUnit === "kph") return limit / 3.6;
  return limit / 2.237; // mph
}

function pointsToPath(points: { x: number; y: number }[]): string {
  if (points.length === 0) return "";
  return points
    .map((p, i) => `${i === 0 ? "M" : "L"}${p.x.toFixed(1)},${p.y.toFixed(1)}`)
    .join(" ");
}

function pointsToAreaPath(
  points: { x: number; y: number }[],
  baseline: number
): string {
  if (points.length === 0) return "";
  const linePath = pointsToPath(points);
  const last = points[points.length - 1];
  const first = points[0];
  return `${linePath} L${last.x.toFixed(1)},${baseline.toFixed(1)} L${first.x.toFixed(1)},${baseline.toFixed(1)} Z`;
}

function niceAxisMax(value: number): number {
  if (value <= 0) return 10;
  const magnitude = Math.pow(10, Math.floor(Math.log10(value)));
  const normalized = value / magnitude;
  if (normalized <= 1.5) return 1.5 * magnitude;
  if (normalized <= 2) return 2 * magnitude;
  if (normalized <= 3) return 3 * magnitude;
  if (normalized <= 5) return 5 * magnitude;
  if (normalized <= 7.5) return 7.5 * magnitude;
  return 10 * magnitude;
}

export function SpeedProfileChart({
  speedArray,
  gnssData,
  imuData,
  currentTime,
  duration,
  speedLimit,
  unit,
  onSeek,
}: SpeedProfileChartProps) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [hoverInfo, setHoverInfo] = useState<{
    x: number;
    y: number;
    svgWidth: number;
    time: number;
    speed: number | null;
    accel: number | null;
  } | null>(null);

  const profile = useMemo(
    () => generateSpeedProfile(gnssData, imuData, speedArray),
    [gnssData, imuData, speedArray]
  );

  const { speedPoints, accelPoints, maxSpeed } = profile;

  // Determine Y-axis max in display units
  const maxDisplaySpeed = speedMsToDisplay(maxSpeed, unit);
  const speedLimitMs = speedLimit
    ? speedLimitToMs(speedLimit.limit, speedLimit.unit)
    : null;
  const speedLimitDisplay = speedLimitMs
    ? speedMsToDisplay(speedLimitMs, unit)
    : null;

  const yMax = niceAxisMax(
    Math.max(
      maxDisplaySpeed,
      speedLimitDisplay ?? 0,
      10
    )
  );

  const getChartX = useCallback(
    (t: number, width: number) => {
      const plotWidth = width - PADDING.left - PADDING.right;
      return PADDING.left + t * plotWidth;
    },
    []
  );

  const getSpeedY = useCallback(
    (speedMs: number) => {
      const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
      const displaySpeed = speedMsToDisplay(speedMs, unit);
      const ratio = Math.min(displaySpeed / yMax, 1);
      return PADDING.top + plotHeight * (1 - ratio);
    },
    [unit, yMax]
  );

  const handleMouseMove = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = svgRef.current;
      if (!svg || duration <= 0) return;
      const rect = svg.getBoundingClientRect();
      const svgWidth = rect.width;
      const mouseX = e.clientX - rect.left;
      const plotWidth = svgWidth - PADDING.left - PADDING.right;
      const t = Math.min(
        Math.max((mouseX - PADDING.left) / plotWidth, 0),
        1
      );
      const time = t * duration;

      // Find nearest speed point
      let speed: number | null = null;
      if (speedPoints.length > 0) {
        speed = interpolateAt(speedPoints, t, "speed");
      }

      // Find nearest accel point
      let accel: number | null = null;
      if (accelPoints.length > 0) {
        accel = interpolateAt(accelPoints, t, "accel");
      }

      setHoverInfo({ x: mouseX, y: e.clientY - rect.top, svgWidth, time, speed, accel });
    },
    [duration, speedPoints, accelPoints]
  );

  const handleMouseLeave = useCallback(() => setHoverInfo(null), []);

  const handleClick = useCallback(
    (e: React.MouseEvent<SVGSVGElement>) => {
      const svg = svgRef.current;
      if (!svg || duration <= 0) return;
      const rect = svg.getBoundingClientRect();
      const plotWidth = rect.width - PADDING.left - PADDING.right;
      const t = Math.min(
        Math.max((e.clientX - rect.left - PADDING.left) / plotWidth, 0),
        1
      );
      onSeek(t * duration);
    },
    [duration, onSeek]
  );

  if (speedPoints.length === 0 && accelPoints.length === 0) {
    return (
      <p className="text-sm text-muted-foreground py-4 text-center">
        No speed data available for this event
      </p>
    );
  }

  // Generate Y-axis ticks
  const tickCount = 4;
  const yTicks: number[] = [];
  for (let i = 0; i <= tickCount; i++) {
    yTicks.push((yMax / tickCount) * i);
  }

  // X-axis time labels
  const xLabelCount = duration >= 10 ? 5 : Math.max(2, Math.ceil(duration));
  const xLabels: { t: number; label: string }[] = [];
  for (let i = 0; i <= xLabelCount; i++) {
    const frac = i / xLabelCount;
    const secs = frac * duration;
    xLabels.push({ t: frac, label: `${secs.toFixed(0)}s` });
  }

  const progress = duration > 0 ? Math.min(currentTime / duration, 1) : 0;
  const displayUnit = unit === "mph" ? "mph" : "km/h";

  return (
    <div className="relative">
      <svg
        ref={svgRef}
        width="100%"
        height={HEIGHT}
        className="cursor-crosshair select-none"
        viewBox={`0 0 400 ${HEIGHT}`}
        preserveAspectRatio="none"
        onMouseMove={handleMouseMove}
        onMouseLeave={handleMouseLeave}
        onClick={handleClick}
      >
        {/* Gradient definition */}
        <defs>
          <linearGradient id="speedGradient" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0%" stopColor="rgb(59, 130, 246)" stopOpacity={0.25} />
            <stop offset="100%" stopColor="rgb(59, 130, 246)" stopOpacity={0} />
          </linearGradient>
        </defs>

        {/* Grid lines */}
        {yTicks.map((tick) => {
          const y =
            PADDING.top +
            (HEIGHT - PADDING.top - PADDING.bottom) * (1 - tick / yMax);
          return (
            <line
              key={tick}
              x1={PADDING.left}
              y1={y}
              x2={400 - PADDING.right}
              y2={y}
              stroke="currentColor"
              className="text-muted-foreground/10"
              strokeWidth={0.5}
              strokeDasharray="2 4"
            />
          );
        })}

        {/* Y-axis labels */}
        {yTicks.map((tick) => {
          const y =
            PADDING.top +
            (HEIGHT - PADDING.top - PADDING.bottom) * (1 - tick / yMax);
          return (
            <text
              key={`label-${tick}`}
              x={PADDING.left - 4}
              y={y + 3}
              textAnchor="end"
              className="fill-muted-foreground"
              fontSize={9}
              fontFamily="monospace"
            >
              {Math.round(tick)}
            </text>
          );
        })}

        {/* X-axis labels */}
        {xLabels.map(({ t, label }) => (
          <text
            key={t}
            x={getChartX(t, 400)}
            y={HEIGHT - 4}
            textAnchor="middle"
            className="fill-muted-foreground"
            fontSize={9}
            fontFamily="monospace"
          >
            {label}
          </text>
        ))}

        {/* Acceleration area (behind speed line) */}
        {accelPoints.length > 0 && (() => {
          // Scale accel to fit in chart area — use a separate visual scale
          const maxAccelDisplay = Math.max(
            ...accelPoints.map((p) => p.accel),
            1
          );
          const plotHeight = HEIGHT - PADDING.top - PADDING.bottom;
          const baseline = PADDING.top + plotHeight;
          const accelScreenPoints = accelPoints.map((p) => ({
            x: getChartX(p.t, 400),
            y: baseline - (p.accel / maxAccelDisplay) * plotHeight * 0.5,
          }));
          return (
            <path
              d={pointsToAreaPath(accelScreenPoints, baseline)}
              fill="rgb(249, 115, 22)"
              fillOpacity={0.15}
              stroke="rgb(249, 115, 22)"
              strokeWidth={0.5}
              strokeOpacity={0.4}
            />
          );
        })()}

        {/* Speed limit reference line */}
        {speedLimitMs && (() => {
          const y = getSpeedY(speedLimitMs);
          return (
            <line
              x1={PADDING.left}
              y1={y}
              x2={400 - PADDING.right}
              y2={y}
              stroke="rgb(239, 68, 68)"
              strokeWidth={1}
              strokeDasharray="4 3"
              strokeOpacity={0.7}
            />
          );
        })()}

        {/* Speed area fill */}
        {speedPoints.length > 1 && (
          <path
            d={pointsToAreaPath(
              speedPoints.map((p) => ({
                x: getChartX(p.t, 400),
                y: getSpeedY(p.speed),
              })),
              HEIGHT - PADDING.bottom
            )}
            fill="url(#speedGradient)"
            stroke="none"
          />
        )}

        {/* Speed line */}
        {speedPoints.length > 1 && (
          <path
            d={pointsToPath(
              speedPoints.map((p) => ({
                x: getChartX(p.t, 400),
                y: getSpeedY(p.speed),
              }))
            )}
            fill="none"
            stroke="rgb(59, 130, 246)"
            strokeWidth={1.5}
            strokeLinejoin="round"
            strokeLinecap="round"
            vectorEffect="non-scaling-stroke"
          />
        )}

        {/* Playback cursor */}
        {duration > 0 && (
          <line
            x1={getChartX(progress, 400)}
            y1={PADDING.top}
            x2={getChartX(progress, 400)}
            y2={HEIGHT - PADDING.bottom}
            stroke="currentColor"
            className="text-foreground"
            strokeWidth={1}
            strokeOpacity={0.5}
            vectorEffect="non-scaling-stroke"
          />
        )}

        {/* Hover cursor */}
        {hoverInfo && (
          <line
            x1={hoverInfo.x * (400 / (hoverInfo.svgWidth || 400))}
            y1={PADDING.top}
            x2={hoverInfo.x * (400 / (hoverInfo.svgWidth || 400))}
            y2={HEIGHT - PADDING.bottom}
            stroke="currentColor"
            className="text-foreground/30"
            strokeWidth={0.5}
            strokeDasharray="2 2"
          />
        )}
      </svg>

      {/* Hover tooltip */}
      {hoverInfo && (
        <div
          className="absolute pointer-events-none bg-popover border shadow-md rounded-md px-2 py-1 text-xs z-10"
          style={{
            left: Math.min(hoverInfo.x + 8, (hoverInfo.svgWidth || 400) - 120),
            top: 4,
          }}
        >
          <div className="font-mono text-muted-foreground">
            {hoverInfo.time.toFixed(1)}s
          </div>
          {hoverInfo.speed !== null && (
            <div className="text-blue-500 font-medium">
              {speedMsToDisplay(hoverInfo.speed, unit).toFixed(1)} {displayUnit}
            </div>
          )}
          {hoverInfo.accel !== null && (
            <div className="text-orange-500">
              {hoverInfo.accel.toFixed(2)} m/s²
            </div>
          )}
        </div>
      )}

      {/* Legend */}
      <div className="flex items-center gap-3 mt-1.5 text-xs text-muted-foreground justify-end">
        {speedPoints.length > 0 && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-0.5 bg-blue-500 inline-block rounded" />
            Speed ({displayUnit})
          </span>
        )}
        {accelPoints.length > 0 && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-2 bg-orange-500/20 border border-orange-500/40 inline-block rounded-sm" />
            Accel
          </span>
        )}
        {speedLimitMs && (
          <span className="flex items-center gap-1">
            <span className="w-3 h-0 border-t border-dashed border-red-500 inline-block" />
            Limit
          </span>
        )}
      </div>
    </div>
  );
}

/** Linear interpolation at normalized time t within a sorted points array */
function interpolateAt<T extends { t: number }>(
  points: T[],
  t: number,
  key: keyof Omit<T, "t">
): number {
  if (points.length === 0) return 0;
  if (points.length === 1) return points[0][key] as number;
  if (t <= points[0].t) return points[0][key] as number;
  if (t >= points[points.length - 1].t)
    return points[points.length - 1][key] as number;

  // Binary search for bracket
  let lo = 0;
  let hi = points.length - 1;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (points[mid].t <= t) lo = mid;
    else hi = mid;
  }
  const frac =
    points[hi].t - points[lo].t > 0
      ? (t - points[lo].t) / (points[hi].t - points[lo].t)
      : 0;
  return (
    (points[lo][key] as number) * (1 - frac) +
    (points[hi][key] as number) * frac
  );
}

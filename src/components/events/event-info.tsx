"use client";

import { useState } from "react";
import {
  MapPin,
  Clock,
  Activity,
  Gauge,
  ExternalLink,
  Copy,
  Check,
  Route,
} from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { AIEvent } from "@/types/events";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { getTimeOfDay, getTimeOfDayStyle } from "@/lib/sun";
import { RoadTypeData } from "@/hooks/use-road-type";
import { formatDateTime, formatCoordinates, formatSpeed, getTimeOfDayIcon } from "@/lib/event-helpers";

interface EventInfoProps {
  event: AIEvent;
  roadType: RoadTypeData | null;
  countryName: string | null;
  maxSpeed: number | null;
  acceleration: number | undefined;
}

export function EventInfo({
  event,
  roadType,
  countryName,
  maxSpeed,
  acceleration,
}: EventInfoProps) {
  const [copied, setCopied] = useState(false);
  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
  const IconComponent = config.icon;

  const copyCoordinates = async () => {
    const coords = `${event.location.lat}, ${event.location.lon}`;
    await navigator.clipboard.writeText(coords);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <Card>
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <CardTitle className="text-lg">Event Details</CardTitle>
        <div className="flex items-center gap-2">
          {roadType?.classLabel && (
            <Badge variant="outline">
              <Route className="w-3 h-3 mr-1" />
              {roadType.classLabel}
            </Badge>
          )}
          <Badge
            className={cn(
              config.bgColor,
              config.color,
              config.borderColor,
              "border"
            )}
            variant="outline"
          >
            <IconComponent className="w-3 h-3 mr-1" />
            {config.label}
          </Badge>
        </div>
      </CardHeader>
      <CardContent className="space-y-4">
        <div className="grid grid-cols-2 gap-4">
          <div className="flex items-center gap-2 text-sm">
            <Clock className="w-4 h-4 text-muted-foreground" />
            <div>
              <p className="text-muted-foreground">Timestamp</p>
              <p className="font-medium flex items-center gap-2">
                {formatDateTime(event.timestamp)}
                {(() => {
                  const sunInfo = getTimeOfDay(
                    event.timestamp,
                    event.location.lat,
                    event.location.lon
                  );
                  const TimeIcon = getTimeOfDayIcon(sunInfo.timeOfDay);
                  const style = getTimeOfDayStyle(sunInfo.timeOfDay);
                  return (
                    <span
                      className={cn(
                        "inline-flex items-center gap-1 px-2 py-0.5 rounded text-xs font-medium",
                        style.bgColor,
                        style.color
                      )}
                    >
                      <TimeIcon className="w-3 h-3" />
                      {sunInfo.timeOfDay}
                    </span>
                  );
                })()}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2 text-sm">
            <MapPin className="w-4 h-4 text-muted-foreground" />
            <div>
              <p className="text-muted-foreground">Coordinates</p>
              <p className="font-medium flex items-center gap-1">
                {formatCoordinates(event.location.lat, event.location.lon)}
                <button
                  onClick={copyCoordinates}
                  className="p-1 hover:bg-muted rounded transition-colors"
                  title="Copy coordinates"
                >
                  {copied ? (
                    <Check className="w-3 h-3 text-green-500" />
                  ) : (
                    <Copy className="w-3 h-3 text-muted-foreground" />
                  )}
                </button>
              </p>
            </div>
          </div>
          {maxSpeed !== null && (
            <div className="flex items-center gap-2 text-sm">
              <Gauge className="w-4 h-4 text-muted-foreground" />
              <div>
                <p className="text-muted-foreground">Max Speed</p>
                <p className="font-medium">{formatSpeed(maxSpeed)}</p>
              </div>
            </div>
          )}
          {acceleration !== undefined && (
            <div className="flex items-center gap-2 text-sm">
              <Activity className="w-4 h-4 text-muted-foreground" />
              <div>
                <p className="text-muted-foreground">Acceleration</p>
                <p className="font-medium">
                  {acceleration.toFixed(2)} m/s²
                </p>
              </div>
            </div>
          )}
        </div>

        {countryName && (
          <div className="pt-2 flex items-center gap-2 text-sm">
            <span className="font-medium">{countryName}</span>
            <span className="text-muted-foreground">·</span>
            <a
              href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-primary hover:underline flex items-center gap-1"
            >
              <ExternalLink className="w-3 h-3" />
              Open in Google Maps
            </a>
          </div>
        )}
        {!countryName && (
          <div className="pt-2">
            <a
              href={`https://www.google.com/maps?q=${event.location.lat},${event.location.lon}`}
              target="_blank"
              rel="noopener noreferrer"
              className="text-sm text-primary hover:underline flex items-center gap-1"
            >
              <ExternalLink className="w-3 h-3" />
              Open in Google Maps
            </a>
          </div>
        )}
      </CardContent>
    </Card>
  );
}

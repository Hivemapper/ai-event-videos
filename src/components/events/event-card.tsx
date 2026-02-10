"use client";

import { useState, memo } from "react";
import Link from "next/link";
import Image from "next/image";
import { Play } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Skeleton } from "@/components/ui/skeleton";
import { AIEvent } from "@/types/events";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { cn } from "@/lib/utils";
import { useThumbnail } from "@/hooks/use-thumbnail";

interface EventCardProps {
  event: AIEvent;
  onClick: () => void;
}

function formatRelativeTime(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffSecs = Math.floor(diffMs / 1000);
  const diffMins = Math.floor(diffSecs / 60);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);

  if (diffDays > 30) {
    return date.toLocaleDateString();
  } else if (diffDays > 0) {
    return `${diffDays}d ago`;
  } else if (diffHours > 0) {
    return `${diffHours}h ago`;
  } else if (diffMins > 0) {
    return `${diffMins}m ago`;
  } else {
    return "Just now";
  }
}

export const EventCard = memo(function EventCard({ event, onClick }: EventCardProps) {
  const [isHovered, setIsHovered] = useState(false);
  const { thumbnailUrl, isLoading, error, ref } = useThumbnail(event.videoUrl);

  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;
  const IconComponent = config.icon;

  const showThumbnail = thumbnailUrl && !error;
  const showGradient = !thumbnailUrl || error;

  return (
    <Link
      href={`/event/${event.id}`}
      className="cursor-pointer group block"
      onClick={(e) => {
        e.preventDefault();
        onClick();
      }}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* Thumbnail - YouTube style: rounded corners, no card border */}
      <div
        ref={ref}
        className="relative aspect-video bg-muted rounded-xl overflow-hidden"
      >
        {/* Video thumbnail when loaded */}
        {showThumbnail && (
          <Image
            src={thumbnailUrl}
            alt={`${config.label} event preview`}
            fill
            className="object-cover"
            unoptimized
          />
        )}

        {/* Gradient fallback/loading state */}
        {showGradient && (
          <div
            className={cn(
              "w-full h-full flex items-center justify-center",
              `bg-gradient-to-br ${config.gradient}`
            )}
          >
            {isLoading ? (
              <div className="w-8 h-8 border-2 border-white/30 border-t-white/80 rounded-full animate-spin" />
            ) : (
              <IconComponent className="w-12 h-12 text-white/80" />
            )}
          </div>
        )}

        {/* Play button overlay */}
        <div
          className={cn(
            "absolute inset-0 flex items-center justify-center",
            "bg-black/0 transition-all duration-200",
            isHovered && "bg-black/20"
          )}
        >
          <div
            className={cn(
              "w-12 h-12 rounded-full bg-black/80 flex items-center justify-center",
              "opacity-0 scale-90 transition-all duration-200",
              isHovered && "opacity-100 scale-100"
            )}
          >
            <Play className="w-5 h-5 text-white ml-0.5" fill="white" />
          </div>
        </div>

        {/* Event type badge */}
        <Badge
          className={cn(
            "absolute top-2 left-2",
            config.bgColor,
            config.color,
            config.borderColor,
            "border text-xs"
          )}
          variant="outline"
        >
          {config.label}
        </Badge>
      </div>

      {/* Metadata below thumbnail - YouTube style */}
      <div className="pt-2 px-1">
        <span className="text-sm text-muted-foreground">
          {formatRelativeTime(event.timestamp)}
        </span>
      </div>
    </Link>
  );
}, (prev, next) => prev.event.id === next.event.id);

export function EventCardSkeleton() {
  return (
    <div>
      <Skeleton className="aspect-video rounded-xl" />
      <div className="pt-2 px-1">
        <Skeleton className="h-4 w-16" />
      </div>
    </div>
  );
}

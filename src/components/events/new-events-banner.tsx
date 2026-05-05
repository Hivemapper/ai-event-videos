"use client";

import { Loader2 } from "lucide-react";

interface NewEventsBannerProps {
  count: number;
  isLoading?: boolean;
  onClick: () => void;
}

export function NewEventsBanner({ count, isLoading = false, onClick }: NewEventsBannerProps) {
  if (count <= 0) return null;

  return (
    <button
      onClick={onClick}
      disabled={isLoading}
      className="inline-flex items-center gap-2 rounded-full border border-primary/30 px-3 py-1.5 text-sm text-primary transition-colors hover:bg-primary/10 disabled:cursor-wait disabled:opacity-70"
    >
      {isLoading && <Loader2 className="h-3.5 w-3.5 animate-spin" />}
      {isLoading ? "Loading" : "Show"} {count} new video{count !== 1 ? "s" : ""}
    </button>
  );
}

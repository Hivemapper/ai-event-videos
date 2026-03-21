"use client";

import { useState, useEffect, useRef, useCallback } from "react";

const POLL_INTERVAL = 30000;

interface UseEventPollingOptions {
  enabled: boolean;
  fetchCount: () => Promise<number>;
  initialTotal: number;
}

interface UseEventPollingResult {
  newEventsCount: number;
  showNewEvents: () => void;
}

export function useEventPolling({
  enabled,
  fetchCount,
  initialTotal,
}: UseEventPollingOptions): UseEventPollingResult {
  const [newEventsCount, setNewEventsCount] = useState(0);
  const lastKnownTotalRef = useRef<number>(initialTotal);
  const isPollingRef = useRef(false);

  // Sync initial total when it changes (e.g. after a fresh load)
  useEffect(() => {
    lastKnownTotalRef.current = initialTotal;
    setNewEventsCount(0);
  }, [initialTotal]);

  useEffect(() => {
    if (!enabled) return;

    const checkForNewEvents = async () => {
      if (isPollingRef.current || document.hidden) return;
      isPollingRef.current = true;

      try {
        const currentTotal = await fetchCount();
        if (lastKnownTotalRef.current > 0 && currentTotal > lastKnownTotalRef.current) {
          setNewEventsCount(currentTotal - lastKnownTotalRef.current);
        }
      } catch {
        // Silently fail polling errors
      } finally {
        isPollingRef.current = false;
      }
    };

    const intervalId = setInterval(checkForNewEvents, POLL_INTERVAL);

    // Pause polling when tab is hidden
    const handleVisibilityChange = () => {
      if (document.hidden) return;
      // Tab became visible — check now
      checkForNewEvents();
    };
    document.addEventListener("visibilitychange", handleVisibilityChange);

    return () => {
      clearInterval(intervalId);
      document.removeEventListener("visibilitychange", handleVisibilityChange);
    };
  }, [enabled, fetchCount]);

  const showNewEvents = useCallback(() => {
    setNewEventsCount(0);
  }, []);

  return { newEventsCount, showNewEvents };
}

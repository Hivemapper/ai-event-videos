"use client";

import useSWR from "swr";
import { useCallback } from "react";
import type { AIEventType } from "@/types/events";

export type TriageCategory = "missing_video" | "missing_metadata" | "ghost" | "open_road" | "signal" | "duplicate" | "non_linear" | "privacy" | "skipped_firmware";

export interface TriageResult {
  id: string;
  event_type: string;
  triage_result: TriageCategory;
  rules_triggered: string;
  speed_min: number | null;
  speed_max: number | null;
  speed_mean: number | null;
  speed_stddev: number | null;
  gnss_displacement_m: number | null;
  event_timestamp: string | null;
  created_at: string;
}

async function fetchTriageStatus(id: string): Promise<TriageResult | null> {
  const response = await fetch(`/api/triage/${id}`);
  if (!response.ok) return null;
  const data = await response.json();
  return data.triage ?? null;
}

export function useTriageStatus(eventId: string, eventType?: string) {
  const { data, mutate, ...rest } = useSWR<TriageResult | null>(
    `triage-${eventId}`,
    () => fetchTriageStatus(eventId),
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      dedupingInterval: 60000,
    }
  );

  const setTriage = useCallback(
    async (category: TriageCategory, nextEventType?: string) => {
      try {
        const resp = await fetch(`/api/triage/${eventId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            triage_result: category,
            event_type: nextEventType ?? eventType ?? "UNKNOWN",
          }),
        });
        if (!resp.ok) return;
        const json = await resp.json();
        mutate(json.triage ?? null);
      } catch {
        // DB may be locked or table missing
      }
    },
    [eventId, eventType, mutate]
  );

  const setEventType = useCallback(
    async (nextEventType: AIEventType) => {
      try {
        const resp = await fetch(`/api/triage/${eventId}`, {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ event_type: nextEventType }),
        });
        if (!resp.ok) return;
        const json = await resp.json();
        mutate(json.triage ?? null);
      } catch {
        // DB may be locked or table missing
      }
    },
    [eventId, mutate]
  );

  const removeTriage = useCallback(async () => {
    try {
      await fetch(`/api/triage/${eventId}`, { method: "DELETE" });
      mutate(null);
    } catch {
      // DB may be locked or table missing
    }
  }, [eventId, mutate]);

  return { data, mutate, setTriage, setEventType, removeTriage, ...rest };
}

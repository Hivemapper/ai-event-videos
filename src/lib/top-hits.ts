"use client";

import { useCallback } from "react";
import useSWR from "swr";
import type { TopHitEventSummary } from "@/lib/top-hits-store";

export { TOP_HITS_SEED } from "@/lib/top-hits-seed";
export type { TopHitEventSummary };

const API_URL = "/api/top-hits";
const EMPTY_IDS: string[] = [];
const EMPTY_ROWS: TopHitEventSummary[] = [];
const EMPTY_FRAME_QC: Record<string, TopHitFrameTimingQc> = {};
const EMPTY_PIPELINE_STATUS: Record<string, TopHitPipelineStatus> = {};

export interface TopHitFrameTimingQc {
  fpsQc: string | null;
  lateFramePct: number | null;
}

export type TopHitVruStatus =
  | "not_run"
  | "queued"
  | "running"
  | "completed"
  | "failed"
  | "cancelled";

export type TopHitProductionStatus =
  | "not_queued"
  | "queued"
  | "processing"
  | "completed"
  | "failed";

export interface TopHitPipelineStatus {
  vruStatus: TopHitVruStatus;
  productionStatus: TopHitProductionStatus;
  productionPriority: number | null;
}

interface TopHitsResponse {
  ids: string[];
  rows: TopHitEventSummary[];
  frameTimingQcById: Record<string, TopHitFrameTimingQc>;
  pipelineStatusById: Record<string, TopHitPipelineStatus>;
}

async function fetchTopHits(): Promise<TopHitsResponse> {
  const res = await fetch(API_URL);
  if (!res.ok) throw new Error(`Failed to load Top Hits: ${res.status}`);
  return res.json();
}

/**
 * Reactive hook backed by `/api/top-hits` (Turso). Shares a single SWR cache
 * key across the app so the highlights page and the event detail button stay
 * in sync. Mutations apply optimistic updates for snappy toggles.
 */
export function useTopHits(): {
  ids: string[];
  rows: TopHitEventSummary[];
  frameTimingQcById: Record<string, TopHitFrameTimingQc>;
  pipelineStatusById: Record<string, TopHitPipelineStatus>;
  add: (id: string) => void;
  remove: (id: string) => void;
  toggle: (id: string) => void;
  has: (id: string) => boolean;
  isLoading: boolean;
} {
  const { data, mutate, isLoading } = useSWR<TopHitsResponse>(
    API_URL,
    fetchTopHits,
    {
      revalidateOnFocus: false,
      revalidateOnReconnect: false,
      dedupingInterval: 5000,
    }
  );
  const ids = data?.ids ?? EMPTY_IDS;
  const rows = data?.rows ?? EMPTY_ROWS;
  const frameTimingQcById = data?.frameTimingQcById ?? EMPTY_FRAME_QC;
  const pipelineStatusById = data?.pipelineStatusById ?? EMPTY_PIPELINE_STATUS;

  const add = useCallback(
    (id: string) => {
      mutate(
        async () => {
          const res = await fetch(API_URL, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ id }),
          });
          if (!res.ok) throw new Error("add failed");
          return res.json();
        },
        {
          optimisticData: (current) => ({
            ids: [id, ...(current?.ids ?? []).filter((x) => x !== id)],
            rows: [
              {
                eventId: id,
                eventType: null,
                eventTimestamp: null,
                lat: null,
                lon: null,
                bitrateBps: null,
                fpsQc: null,
                lateFramePct: null,
                vruLabel: null,
                vruConfidence: null,
                pipelineStatus: {
                  vruStatus: "not_run",
                  productionStatus: "not_queued",
                  productionPriority: null,
                },
              },
              ...(current?.rows ?? []).filter((row) => row.eventId !== id),
            ],
            frameTimingQcById: current?.frameTimingQcById ?? {},
            pipelineStatusById: current?.pipelineStatusById ?? {},
          }),
          rollbackOnError: true,
          revalidate: false,
        }
      );
    },
    [mutate]
  );

  const remove = useCallback(
    (id: string) => {
      mutate(
        async () => {
          const res = await fetch(`${API_URL}/${encodeURIComponent(id)}`, {
            method: "DELETE",
          });
          if (!res.ok) throw new Error("remove failed");
          return res.json();
        },
        {
          optimisticData: (current) => {
            const frameTimingQcById = { ...(current?.frameTimingQcById ?? {}) };
            const pipelineStatusById = { ...(current?.pipelineStatusById ?? {}) };
            delete frameTimingQcById[id];
            delete pipelineStatusById[id];
            return {
              ids: (current?.ids ?? []).filter((x) => x !== id),
              rows: (current?.rows ?? []).filter((row) => row.eventId !== id),
              frameTimingQcById,
              pipelineStatusById,
            };
          },
          rollbackOnError: true,
          revalidate: false,
        }
      );
    },
    [mutate]
  );

  const toggle = useCallback(
    (id: string) => {
      if (ids.includes(id)) remove(id);
      else add(id);
    },
    [ids, add, remove]
  );

  const has = useCallback((id: string) => ids.includes(id), [ids]);

  return {
    ids,
    rows,
    frameTimingQcById,
    pipelineStatusById,
    add,
    remove,
    toggle,
    has,
    isLoading,
  };
}

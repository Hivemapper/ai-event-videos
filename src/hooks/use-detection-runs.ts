import useSWR from "swr";
import type { DetectionRun } from "@/types/pipeline";

const fetcher = (url: string) => fetch(url).then((r) => r.json());

export function useDetectionRuns(videoId: string | null) {
  const { data, error, isLoading, mutate } = useSWR<{ runs: DetectionRun[] }>(
    videoId ? `/api/videos/${videoId}/runs` : null,
    fetcher,
    {
      // Poll every 2s while there's an active run
      refreshInterval: (data) => {
        const hasActive = data?.runs?.some(
          (r: DetectionRun) => r.status === "queued" || r.status === "running"
        );
        return hasActive ? 2000 : 0;
      },
    }
  );

  const activeRun =
    data?.runs?.find(
      (r) => r.status === "queued" || r.status === "running"
    ) ?? null;

  return {
    runs: data?.runs ?? [],
    activeRun,
    isLoading,
    error,
    mutate,
  };
}

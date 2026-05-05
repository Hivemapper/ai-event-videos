import { useEffect, useRef, useState } from "react";

export function useRunLogs(runId: string | null, videoId: string | null) {
  const [logState, setLogState] = useState<{ key: string | null; logs: string }>({
    key: null,
    logs: "",
  });
  const eventSourceRef = useRef<EventSource | null>(null);

  useEffect(() => {
    if (!runId || !videoId) {
      // Don't clear logs when runId becomes null — keep final output visible
      return;
    }

    const key = `${videoId}:${runId}`;

    const es = new EventSource(
      `/api/videos/${videoId}/runs/${runId}/logs`
    );
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      try {
        const text = JSON.parse(event.data) as string;
        setLogState((prev) => ({
          key,
          logs: prev.key === key ? prev.logs + text : text,
        }));
      } catch {
        // Ignore malformed SSE data
      }
    };

    es.onerror = () => {
      es.close();
    };

    return () => {
      es.close();
      eventSourceRef.current = null;
    };
  }, [runId, videoId]);

  return { logs: logState.logs };
}

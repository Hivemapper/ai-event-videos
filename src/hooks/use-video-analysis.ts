"use client";

import { useState, useEffect, useCallback } from "react";
import { VideoAnalysis, ChatMessage } from "@/types/analysis";
import { getApiKey, getAnthropicKey, getMapboxToken } from "@/lib/api";

const STORAGE_PREFIX = "video-analysis-";

interface CachedAnalysis {
  analysis: VideoAnalysis;
  eventId: string;
  analyzedAt: string;
  frameTimestamps: number[];
}

interface UseVideoAnalysisResult {
  analysis: VideoAnalysis | null;
  analyzedAt: string | null;
  frameTimestamps: number[];
  isLoading: boolean;
  error: string | null;
  analyze: () => void;
  chatHistory: ChatMessage[];
  askFollowUp: (question: string) => Promise<string>;
  isChatLoading: boolean;
}

export function useVideoAnalysis(eventId: string): UseVideoAnalysisResult {
  const [analysis, setAnalysis] = useState<VideoAnalysis | null>(null);
  const [analyzedAt, setAnalyzedAt] = useState<string | null>(null);
  const [frameTimestamps, setFrameTimestamps] = useState<number[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [chatHistory, setChatHistory] = useState<ChatMessage[]>([]);
  const [isChatLoading, setIsChatLoading] = useState(false);

  // Check localStorage on mount
  useEffect(() => {
    try {
      const stored = localStorage.getItem(`${STORAGE_PREFIX}${eventId}`);
      if (stored) {
        const cached: CachedAnalysis = JSON.parse(stored);
        setAnalysis(cached.analysis);
        setAnalyzedAt(cached.analyzedAt);
        setFrameTimestamps(cached.frameTimestamps || []);
      }
    } catch {
      // Ignore parse errors
    }
  }, [eventId]);

  const analyze = useCallback(async () => {
    setIsLoading(true);
    setError(null);

    try {
      const response = await fetch("/api/analyze", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          eventId,
          anthropicApiKey: getAnthropicKey(),
          beemapsApiKey: getApiKey(),
          mapboxToken: getMapboxToken(),
        }),
      });

      if (!response.ok) {
        const data = await response.json();
        throw new Error(data.error || `Analysis failed: ${response.status}`);
      }

      const data: CachedAnalysis = await response.json();
      setAnalysis(data.analysis);
      setAnalyzedAt(data.analyzedAt);
      setFrameTimestamps(data.frameTimestamps || []);

      // Cache in localStorage
      try {
        localStorage.setItem(
          `${STORAGE_PREFIX}${eventId}`,
          JSON.stringify(data)
        );
      } catch {
        // localStorage full — non-fatal
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Analysis failed");
    } finally {
      setIsLoading(false);
    }
  }, [eventId]);

  const askFollowUp = useCallback(
    async (question: string): Promise<string> => {
      setIsChatLoading(true);

      // Add user message immediately
      const userMsg: ChatMessage = { role: "user", content: question };
      setChatHistory((prev) => [...prev, userMsg]);

      try {
        const response = await fetch("/api/analyze/chat", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            eventId,
            question,
            previousAnalysis: analysis,
            anthropicApiKey: getAnthropicKey(),
            beemapsApiKey: getApiKey(),
            mapboxToken: getMapboxToken(),
          }),
        });

        if (!response.ok) {
          const data = await response.json();
          throw new Error(data.error || "Follow-up failed");
        }

        const data = await response.json();
        const answer = data.answer as string;

        const assistantMsg: ChatMessage = {
          role: "assistant",
          content: answer,
        };
        setChatHistory((prev) => [...prev, assistantMsg]);

        return answer;
      } catch (err) {
        const errorMsg =
          err instanceof Error ? err.message : "Follow-up failed";
        // Remove the user message on error
        setChatHistory((prev) => prev.slice(0, -1));
        throw new Error(errorMsg);
      } finally {
        setIsChatLoading(false);
      }
    },
    [eventId, analysis]
  );

  return {
    analysis,
    analyzedAt,
    frameTimestamps,
    isLoading,
    error,
    analyze,
    chatHistory,
    askFollowUp,
    isChatLoading,
  };
}

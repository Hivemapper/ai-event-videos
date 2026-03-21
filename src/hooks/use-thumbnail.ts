"use client";

import { useState, useEffect, useRef, useCallback } from "react";

const LRU_MAX = 50;
const lruCache = new Map<string, string>(); // videoUrl → blob URL

function lruGet(key: string): string | undefined {
  const value = lruCache.get(key);
  if (value !== undefined) {
    // Move to end (most recently used)
    lruCache.delete(key);
    lruCache.set(key, value);
  }
  return value;
}

function lruSet(key: string, value: string): void {
  if (lruCache.has(key)) {
    lruCache.delete(key);
  } else if (lruCache.size >= LRU_MAX) {
    // Evict oldest entry (first key in Map)
    const oldest = lruCache.keys().next().value;
    if (oldest !== undefined) {
      const oldUrl = lruCache.get(oldest);
      if (oldUrl) URL.revokeObjectURL(oldUrl);
      lruCache.delete(oldest);
    }
  }
  lruCache.set(key, value);
}

// --- Concurrent request queue ---
const MAX_CONCURRENT = 4;
let activeCount = 0;
const pendingQueue: Array<{ resolve: () => void; priority: number }> = [];

function enqueue(priority: number): Promise<void> {
  if (activeCount < MAX_CONCURRENT) {
    activeCount++;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    pendingQueue.push({ resolve, priority });
    // Sort so higher priority (lower number = closer to top) goes first
    pendingQueue.sort((a, b) => a.priority - b.priority);
  });
}

function dequeue(): void {
  activeCount--;
  if (pendingQueue.length > 0) {
    activeCount++;
    const next = pendingQueue.shift()!;
    next.resolve();
  }
}

// Track permanently failed URLs to avoid retrying endlessly
const failedUrls = new Set<string>();

async function fetchThumbnail(
  videoUrl: string,
  signal: AbortSignal
): Promise<Blob> {
  const encodedUrl = encodeURIComponent(videoUrl);
  const response = await fetch(`/api/thumbnail?url=${encodedUrl}`, { signal });
  if (!response.ok) {
    throw new Error("Failed to load thumbnail");
  }
  return response.blob();
}

interface UseThumbnailResult {
  thumbnailUrl: string | null;
  isLoading: boolean;
  error: boolean;
  ref: React.RefObject<HTMLDivElement | null>;
}

export function useThumbnail(videoUrl: string): UseThumbnailResult {
  const [thumbnailUrl, setThumbnailUrl] = useState<string | null>(() => lruGet(videoUrl) ?? null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(() => failedUrls.has(videoUrl));
  const [isVisible, setIsVisible] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);
  const hasAttempted = useRef(!!lruGet(videoUrl) || failedUrls.has(videoUrl));
  const abortRef = useRef<AbortController | null>(null);

  // IntersectionObserver for lazy loading
  useEffect(() => {
    const element = ref.current;
    if (!element) return;

    const observer = new IntersectionObserver(
      (entries) => {
        entries.forEach((entry) => {
          if (entry.isIntersecting) {
            setIsVisible(true);
            observer.disconnect();
          }
        });
      },
      {
        rootMargin: "200px",
        threshold: 0,
      }
    );

    observer.observe(element);

    return () => observer.disconnect();
  }, []);

  // Load thumbnail when visible
  const loadThumbnail = useCallback(async () => {
    if (hasAttempted.current || !videoUrl) return;
    hasAttempted.current = true;

    // Check LRU cache first
    const cached = lruGet(videoUrl);
    if (cached) {
      setThumbnailUrl(cached);
      return;
    }

    setIsLoading(true);
    setError(false);

    // Get priority based on element's vertical position
    const rect = ref.current?.getBoundingClientRect();
    const priority = rect ? Math.max(0, rect.top) : 9999;

    // Wait for a queue slot
    await enqueue(priority);

    const controller = new AbortController();
    abortRef.current = controller;

    try {
      // Try up to 2 times (initial + 1 retry)
      let lastError: unknown;
      for (let attempt = 0; attempt < 2; attempt++) {
        try {
          const blob = await fetchThumbnail(videoUrl, controller.signal);
          const objectUrl = URL.createObjectURL(blob);
          lruSet(videoUrl, objectUrl);
          setThumbnailUrl(objectUrl);
          return; // success
        } catch (err) {
          if (err instanceof DOMException && err.name === "AbortError") throw err;
          lastError = err;
          // Brief pause before retry
          if (attempt === 0) {
            await new Promise((r) => setTimeout(r, 1000));
          }
        }
      }
      // Both attempts failed
      failedUrls.add(videoUrl);
      throw lastError;
    } catch (err) {
      if (err instanceof DOMException && err.name === "AbortError") return;
      setError(true);
    } finally {
      abortRef.current = null;
      dequeue();
      setIsLoading(false);
    }
  }, [videoUrl]);

  useEffect(() => {
    if (isVisible && !hasAttempted.current) {
      loadThumbnail();
    }
  }, [isVisible, loadThumbnail]);

  // Abort in-flight request on unmount
  useEffect(() => {
    return () => {
      abortRef.current?.abort();
    };
  }, []);

  return {
    thumbnailUrl,
    isLoading,
    error,
    ref,
  };
}

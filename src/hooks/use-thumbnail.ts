"use client";

import { useState, useEffect, useRef, useCallback } from "react";

const LRU_MAX = 30;
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

interface UseThumbnailResult {
  thumbnailUrl: string | null;
  isLoading: boolean;
  error: boolean;
  ref: React.RefObject<HTMLDivElement | null>;
}

export function useThumbnail(videoUrl: string): UseThumbnailResult {
  const [thumbnailUrl, setThumbnailUrl] = useState<string | null>(() => lruGet(videoUrl) ?? null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(false);
  const [isVisible, setIsVisible] = useState(false);
  const ref = useRef<HTMLDivElement | null>(null);
  const hasAttempted = useRef(!!lruGet(videoUrl));

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

    try {
      const encodedUrl = encodeURIComponent(videoUrl);
      const response = await fetch(`/api/thumbnail?url=${encodedUrl}`);

      if (!response.ok) {
        throw new Error("Failed to load thumbnail");
      }

      const blob = await response.blob();
      const objectUrl = URL.createObjectURL(blob);
      lruSet(videoUrl, objectUrl);
      setThumbnailUrl(objectUrl);
    } catch {
      setError(true);
    } finally {
      setIsLoading(false);
    }
  }, [videoUrl]);

  useEffect(() => {
    if (isVisible && !hasAttempted.current) {
      loadThumbnail();
    }
  }, [isVisible, loadThumbnail]);

  // No per-instance cleanup — LRU cache owns blob URL lifecycle

  return {
    thumbnailUrl,
    isLoading,
    error,
    ref,
  };
}

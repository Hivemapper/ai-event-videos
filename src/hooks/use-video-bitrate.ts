"use client";

import { useCallback, useEffect, useRef, useState } from "react";

/**
 * Lazy-loaded video bitrate probe.
 *
 * Approach:
 *  1) Range GET bytes=0-0 against the video proxy → read `Content-Range` for total bytes.
 *  2) Create an invisible <video> element with preload="metadata" → listen for
 *     `loadedmetadata` to get duration.
 *  3) bitrate_bps = bytes * 8 / duration.
 *
 * Probing is lazy (IntersectionObserver) and memoized in a module-level cache.
 */

interface BitrateResult {
  bytes: number;
  durationSec: number;
  bps: number;
}

const cache = new Map<string, BitrateResult | "error">();

function getProxyVideoUrl(url: string): string {
  return `/api/video?url=${encodeURIComponent(url)}`;
}

async function probeBytes(proxyUrl: string, signal: AbortSignal): Promise<number | null> {
  const res = await fetch(proxyUrl, { headers: { Range: "bytes=0-0" }, signal });
  res.body?.cancel().catch(() => {});
  const cr = res.headers.get("content-range");
  if (cr) {
    const total = cr.split("/").pop();
    if (total && total !== "*") {
      const n = parseInt(total, 10);
      if (!isNaN(n) && n > 0) return n;
    }
  }
  const len = res.headers.get("content-length");
  if (len) {
    const n = parseInt(len, 10);
    if (!isNaN(n) && n > 1024) return n;
  }
  return null;
}

function probeDuration(proxyUrl: string, signal: AbortSignal): Promise<number | null> {
  return new Promise((resolve) => {
    if (signal.aborted) return resolve(null);
    const video = document.createElement("video");
    video.preload = "metadata";
    video.muted = true;
    video.src = proxyUrl;
    const cleanup = () => {
      video.removeAttribute("src");
      video.load();
    };
    const onMeta = () => {
      const d = video.duration;
      cleanup();
      if (isFinite(d) && d > 0) resolve(d);
      else resolve(null);
    };
    const onError = () => {
      cleanup();
      resolve(null);
    };
    video.addEventListener("loadedmetadata", onMeta, { once: true });
    video.addEventListener("error", onError, { once: true });
    signal.addEventListener("abort", () => {
      cleanup();
      resolve(null);
    }, { once: true });
  });
}

export interface UseVideoBitrateResult {
  bitrate: BitrateResult | null;
  isLoading: boolean;
  error: boolean;
  ref: React.RefObject<HTMLElement | null>;
}

export function useVideoBitrate(
  videoUrl: string | null,
  opts?: { eager?: boolean }
): UseVideoBitrateResult {
  const eager = !!opts?.eager;
  const cached = videoUrl ? cache.get(videoUrl) : undefined;
  const [bitrate, setBitrate] = useState<BitrateResult | null>(
    cached && cached !== "error" ? cached : null
  );
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<boolean>(cached === "error");
  const [isVisible, setIsVisible] = useState(eager);
  const ref = useRef<HTMLElement | null>(null);
  const hasAttempted = useRef<boolean>(!!cached);
  const abortRef = useRef<AbortController | null>(null);

  useEffect(() => {
    if (eager) {
      setIsVisible(true);
      return;
    }
    const el = ref.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      (entries) => {
        for (const e of entries) {
          if (e.isIntersecting) {
            setIsVisible(true);
            obs.disconnect();
          }
        }
      },
      { rootMargin: "200px", threshold: 0 }
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [eager]);

  const probe = useCallback(async () => {
    if (!videoUrl || hasAttempted.current) return;
    hasAttempted.current = true;
    const existing = cache.get(videoUrl);
    if (existing) {
      if (existing === "error") setError(true);
      else setBitrate(existing);
      return;
    }
    setIsLoading(true);
    const proxyUrl = getProxyVideoUrl(videoUrl);
    const controller = new AbortController();
    abortRef.current = controller;
    try {
      const [bytes, durationSec] = await Promise.all([
        probeBytes(proxyUrl, controller.signal),
        probeDuration(proxyUrl, controller.signal),
      ]);
      if (bytes == null || durationSec == null) {
        cache.set(videoUrl, "error");
        setError(true);
        return;
      }
      const result: BitrateResult = { bytes, durationSec, bps: (bytes * 8) / durationSec };
      cache.set(videoUrl, result);
      setBitrate(result);
    } catch {
      cache.set(videoUrl, "error");
      setError(true);
    } finally {
      abortRef.current = null;
      setIsLoading(false);
    }
  }, [videoUrl]);

  useEffect(() => {
    if (isVisible && !hasAttempted.current) probe();
  }, [isVisible, probe]);

  useEffect(() => {
    return () => abortRef.current?.abort();
  }, []);

  return { bitrate, isLoading, error, ref };
}

export function formatBitrate(bps: number): string {
  if (bps >= 1_000_000) return `${(bps / 1_000_000).toFixed(2)} Mbps`;
  if (bps >= 1_000) return `${(bps / 1_000).toFixed(0)} kbps`;
  return `${bps.toFixed(0)} bps`;
}

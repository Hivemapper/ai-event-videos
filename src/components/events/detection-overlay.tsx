"use client";

import { useCallback, useEffect, useRef } from "react";
import type { FrameDetection } from "@/types/pipeline";
import {
  DETECTION_FRAME_TOLERANCE_MS,
  VRU_LABEL_COLOR_MAP,
} from "@/lib/pipeline-config";

interface DetectionOverlayProps {
  videoRef: React.RefObject<HTMLVideoElement | null>;
  isPlaying: boolean;
  currentTime: number;
  timestamps: number[];
  detectionsByFrame: Map<number, FrameDetection[]>;
  minConfidence?: number;
}

export function DetectionOverlay({
  videoRef,
  isPlaying,
  currentTime,
  timestamps,
  detectionsByFrame,
  minConfidence = 0,
}: DetectionOverlayProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  // Sync canvas to the parent container (not the video element, which may
  // include browser-native controls that skew clientHeight).
  const syncCanvasSize = useCallback(() => {
    const canvas = canvasRef.current;
    const container = canvas?.parentElement;
    if (!canvas || !container) return;
    const { clientWidth, clientHeight } = container;
    const dpr = window.devicePixelRatio || 1;
    const targetW = Math.round(clientWidth * dpr);
    const targetH = Math.round(clientHeight * dpr);
    if (canvas.width !== targetW || canvas.height !== targetH) {
      canvas.width = targetW;
      canvas.height = targetH;
      canvas.style.width = clientWidth + "px";
      canvas.style.height = clientHeight + "px";
    }
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    const container = canvas?.parentElement;
    if (!container) return;
    const observer = new ResizeObserver(() => {
      syncCanvasSize();
    });
    observer.observe(container);
    syncCanvasSize();
    return () => observer.disconnect();
  }, [syncCanvasSize]);

  // Draw detections on the canvas (synchronous — all data comes from props)
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    // Always clear
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // Don't draw while playing
    if (isPlaying) return;
    if (timestamps.length === 0) return;

    // Find nearest timestamp
    const currentMs = currentTime * 1000;
    let nearestMs = timestamps[0];
    let nearestDist = Math.abs(currentMs - nearestMs);
    for (const ts of timestamps) {
      const dist = Math.abs(currentMs - ts);
      if (dist < nearestDist) {
        nearestMs = ts;
        nearestDist = dist;
      }
    }

    if (nearestDist > DETECTION_FRAME_TOLERANCE_MS) return;

    const frameDetections = detectionsByFrame.get(nearestMs);
    if (!frameDetections || frameDetections.length === 0) return;

    syncCanvasSize();

    const video = videoRef.current;
    const container = canvas.parentElement;
    if (!video || !container) return;

    // Use the parent container dimensions for positioning (the canvas is
    // absolutely positioned relative to it). The video element's clientHeight
    // may include browser-native controls that shift the rendered area.
    const containerW = container.clientWidth;
    const containerH = container.clientHeight;
    const dpr = window.devicePixelRatio || 1;
    ctx.save();
    ctx.scale(dpr, dpr);

    // Calculate the actual rendered video area within the container
    // to account for letterboxing (black bars) when aspect ratios don't match.
    // Uses video.videoWidth/videoHeight for the natural aspect ratio.
    const videoNaturalW = video.videoWidth || 1280;
    const videoNaturalH = video.videoHeight || 720;
    const containerAspect = containerW / containerH;
    const videoAspect = videoNaturalW / videoNaturalH;

    let renderW: number, renderH: number, offsetX: number, offsetY: number;
    if (containerAspect > videoAspect) {
      // Black bars on sides (wider container than video)
      renderH = containerH;
      renderW = containerH * videoAspect;
      offsetX = (containerW - renderW) / 2;
      offsetY = 0;
    } else {
      // Black bars on top/bottom (taller container than video)
      renderW = containerW;
      renderH = containerW / videoAspect;
      offsetX = 0;
      offsetY = (containerH - renderH) / 2;
    }

    for (const det of frameDetections.filter(
      (d) => d.confidence >= minConfidence,
    )) {
      const scaleX = renderW / det.frameWidth;
      const scaleY = renderH / det.frameHeight;
      const x = det.xMin * scaleX + offsetX;
      const y = det.yMin * scaleY + offsetY;
      const w = (det.xMax - det.xMin) * scaleX;
      const h = (det.yMax - det.yMin) * scaleY;

      const color = VRU_LABEL_COLOR_MAP[det.label] ?? "#334155";

      // Draw bounding box
      ctx.strokeStyle = color;
      ctx.lineWidth = 2;
      ctx.strokeRect(x, y, w, h);

      // Draw label tag above the box
      const labelText = `${det.label} ${Math.round(det.confidence * 100)}%`;
      ctx.font = "bold 12px sans-serif";
      const textMetrics = ctx.measureText(labelText);
      const tagPadX = 4;
      const tagPadY = 2;
      const tagW = textMetrics.width + tagPadX * 2;
      const tagH = 16 + tagPadY * 2;
      const tagX = x;
      const tagY = Math.max(0, y - tagH);

      ctx.fillStyle = color;
      ctx.fillRect(tagX, tagY, tagW, tagH);

      ctx.fillStyle = "#ffffff";
      ctx.textBaseline = "top";
      ctx.fillText(labelText, tagX + tagPadX, tagY + tagPadY);
    }

    ctx.restore();
  }, [isPlaying, currentTime, timestamps, detectionsByFrame, syncCanvasSize, videoRef, minConfidence]);

  return (
    <canvas
      ref={canvasRef}
      className="absolute top-0 left-0 pointer-events-none"
    />
  );
}

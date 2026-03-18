"use client";

import { useCallback, useEffect, useRef } from "react";
import type { FrameDetection } from "@/types/pipeline";
import {
  DETECTION_FRAME_TOLERANCE_MS,
  DISPLAY_CONFIDENCE_THRESHOLD,
  VRU_LABEL_COLOR_MAP,
} from "@/lib/pipeline-config";

interface DetectionOverlayProps {
  videoRef: React.RefObject<HTMLVideoElement | null>;
  isPlaying: boolean;
  currentTime: number;
  timestamps: number[];
  detectionsByFrame: Map<number, FrameDetection[]>;
}

export function DetectionOverlay({
  videoRef,
  isPlaying,
  currentTime,
  timestamps,
  detectionsByFrame,
}: DetectionOverlayProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  // Keep canvas dimensions synced with video element via ResizeObserver
  const syncCanvasSize = useCallback(() => {
    const canvas = canvasRef.current;
    const video = videoRef.current;
    if (!canvas || !video) return;
    const { clientWidth, clientHeight } = video;
    const dpr = window.devicePixelRatio || 1;
    const targetW = Math.round(clientWidth * dpr);
    const targetH = Math.round(clientHeight * dpr);
    if (canvas.width !== targetW || canvas.height !== targetH) {
      canvas.width = targetW;
      canvas.height = targetH;
      canvas.style.width = clientWidth + "px";
      canvas.style.height = clientHeight + "px";
    }
  }, [videoRef]);

  useEffect(() => {
    const video = videoRef.current;
    if (!video) return;
    const observer = new ResizeObserver(() => {
      syncCanvasSize();
    });
    observer.observe(video);
    syncCanvasSize();
    return () => observer.disconnect();
  }, [videoRef, syncCanvasSize]);

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
    if (!video) return;
    const { clientWidth, clientHeight } = video;
    const dpr = window.devicePixelRatio || 1;
    ctx.save();
    ctx.scale(dpr, dpr);

    for (const det of frameDetections.filter(
      (d) => d.confidence >= DISPLAY_CONFIDENCE_THRESHOLD,
    )) {
      const scaleX = clientWidth / det.frameWidth;
      const scaleY = clientHeight / det.frameHeight;
      const x = det.xMin * scaleX;
      const y = det.yMin * scaleY;
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
  }, [isPlaying, currentTime, timestamps, detectionsByFrame, syncCanvasSize, videoRef]);

  return (
    <canvas
      ref={canvasRef}
      className="absolute top-0 left-0 pointer-events-none"
    />
  );
}

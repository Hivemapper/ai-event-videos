import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, unlinkSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import { tmpdir } from "os";

const FRAMES_DIR = join(tmpdir(), "video-frames");

export function ensureDir(dir: string): void {
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
}

export function formatFfmpegTimestamp(seconds: number): string {
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = seconds % 60;
  return `${hours.toString().padStart(2, "0")}:${minutes
    .toString()
    .padStart(2, "0")}:${secs.toFixed(3).padStart(6, "0")}`;
}

/**
 * Extract a single JPEG frame from a video URL at a given timestamp.
 * Uses a disk cache keyed on (url, timestamp, width).
 * Returns the JPEG buffer or null on failure.
 */
export function extractFrame(
  videoUrl: string,
  timestamp: number,
  width: number
): Buffer | null {
  ensureDir(FRAMES_DIR);

  const hash = createHash("md5")
    .update(`${videoUrl}-${timestamp}-${width}`)
    .digest("hex");
  const framePath = join(FRAMES_DIR, `${hash}.jpg`);

  if (existsSync(framePath)) {
    return readFileSync(framePath);
  }

  const timeFormatted = formatFfmpegTimestamp(timestamp);
  const cmd = `ffmpeg -ss ${timeFormatted} -i "${videoUrl}" -vframes 1 -q:v 2 -vf "scale=${width}:-1" -f image2 "${framePath}" -y 2>/dev/null`;

  try {
    execSync(cmd, { timeout: 30000 });
  } catch {
    return null;
  }

  if (!existsSync(framePath)) return null;

  // Opportunistic cleanup of old cached frames
  cleanupOldFiles(FRAMES_DIR, 24 * 60 * 60 * 1000);

  return readFileSync(framePath);
}

/**
 * Delete files older than `maxAgeMs` from a directory.
 * Runs opportunistically — errors are silently ignored.
 */
export function cleanupOldFiles(dir: string, maxAgeMs: number): void {
  try {
    const now = Date.now();
    for (const file of readdirSync(dir)) {
      const filePath = join(dir, file);
      try {
        const stat = statSync(filePath);
        if (stat.isFile() && now - stat.mtimeMs > maxAgeMs) {
          unlinkSync(filePath);
        }
      } catch {
        // Skip files we can't stat or delete
      }
    }
  } catch {
    // Directory doesn't exist or isn't readable — no-op
  }
}

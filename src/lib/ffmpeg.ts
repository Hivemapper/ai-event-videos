import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, readdirSync, statSync, unlinkSync } from "fs";
import { join } from "path";
import { execFile as execFileCb } from "child_process";
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

function runFFmpegFrame(args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = execFileCb("ffmpeg", args, { timeout: 30000 }, (error) => {
      if (error) reject(error);
      else resolve();
    });
    proc.stderr?.resume();
  });
}

/**
 * Extract a single JPEG frame from a video URL at a given timestamp.
 * Uses a disk cache keyed on (url, timestamp, width).
 * Returns the JPEG buffer or null on failure.
 */
export async function extractFrame(
  videoUrl: string,
  timestamp: number,
  width: number
): Promise<Buffer | null> {
  ensureDir(FRAMES_DIR);

  const hash = createHash("md5")
    .update(`${videoUrl}-${timestamp}-${width}`)
    .digest("hex");
  const framePath = join(FRAMES_DIR, `${hash}.jpg`);

  if (existsSync(framePath)) {
    return readFileSync(framePath);
  }

  const timeFormatted = formatFfmpegTimestamp(timestamp);

  try {
    await runFFmpegFrame([
      "-ss", timeFormatted,
      "-i", videoUrl,
      "-vframes", "1",
      "-q:v", "2",
      "-vf", `scale=${width}:-1`,
      "-f", "image2",
      framePath,
      "-y",
    ]);
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

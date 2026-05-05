import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";
import { execFile } from "child_process";
import { cleanupOldFiles } from "@/lib/ffmpeg";

const THUMBNAIL_DIR =
  process.env.THUMBNAIL_CACHE_DIR ??
  join(process.cwd(), "data", "thumbnail-cache");
const FRAME_TIME = "00:00:01"; // Extract frame at 1 second

// Limit concurrent FFmpeg processes
const MAX_FFMPEG = 2;
let activeFFmpeg = 0;
const ffmpegQueue: Array<{ resolve: () => void }> = [];

function acquireFFmpeg(): Promise<void> {
  if (activeFFmpeg < MAX_FFMPEG) {
    activeFFmpeg++;
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    ffmpegQueue.push({ resolve });
  });
}

function releaseFFmpeg(): void {
  activeFFmpeg--;
  if (ffmpegQueue.length > 0) {
    activeFFmpeg++;
    ffmpegQueue.shift()!.resolve();
  }
}

function runFFmpeg(args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    const proc = execFile("ffmpeg", args, { timeout: 60000 }, (error) => {
      if (error) reject(error);
      else resolve();
    });
    proc.stderr?.resume(); // drain stderr to prevent buffer stalls
  });
}

// Dedup: if the same hash is already being generated, wait for it
const inFlight = new Map<string, Promise<void>>();

function getUrlHash(url: string): string {
  return createHash("md5").update(url).digest("hex");
}

function ensureThumbnailDir(): void {
  if (!existsSync(THUMBNAIL_DIR)) {
    mkdirSync(THUMBNAIL_DIR, { recursive: true });
  }
}

export async function GET(request: NextRequest) {
  const url = request.nextUrl.searchParams.get("url");

  if (!url) {
    return NextResponse.json({ error: "URL is required" }, { status: 400 });
  }

  try {
    ensureThumbnailDir();

    const hash = getUrlHash(url);
    const thumbnailPath = join(THUMBNAIL_DIR, `${hash}.jpg`);

    // Check cache first
    if (existsSync(thumbnailPath)) {
      const imageBuffer = readFileSync(thumbnailPath);
      return new NextResponse(imageBuffer, {
        status: 200,
        headers: {
          "Content-Type": "image/jpeg",
          "Cache-Control": "public, max-age=31536000, immutable",
        },
      });
    }

    // Dedup concurrent requests for the same thumbnail
    let generation = inFlight.get(hash);
    if (!generation) {
      generation = (async () => {
        await acquireFFmpeg();
        try {
          await runFFmpeg([
            "-ss", FRAME_TIME,
            "-i", url,
            "-vframes", "1",
            "-q:v", "4",
            "-vf", "scale=320:-1",
            "-f", "image2",
            thumbnailPath,
            "-y",
          ]);
        } finally {
          releaseFFmpeg();
        }
      })();
      inFlight.set(hash, generation);
      generation.finally(() => inFlight.delete(hash));
    }

    try {
      await generation;
    } catch {
      return NextResponse.json(
        { error: "Failed to generate thumbnail" },
        { status: 404 }
      );
    }

    // Verify the file was created
    if (!existsSync(thumbnailPath)) {
      return NextResponse.json(
        { error: "Thumbnail generation failed" },
        { status: 500 }
      );
    }

    const imageBuffer = readFileSync(thumbnailPath);

    // Opportunistic cleanup of old thumbnails. Keep the cache warm across
    // server restarts, but avoid unbounded growth.
    cleanupOldFiles(THUMBNAIL_DIR, 7 * 24 * 60 * 60 * 1000);

    return new NextResponse(imageBuffer, {
      status: 200,
      headers: {
        "Content-Type": "image/jpeg",
        "Cache-Control": "public, max-age=31536000, immutable",
      },
    });
  } catch (error) {
    console.error("Thumbnail generation error:", error);
    return NextResponse.json(
      { error: "Failed to generate thumbnail" },
      { status: 500 }
    );
  }
}

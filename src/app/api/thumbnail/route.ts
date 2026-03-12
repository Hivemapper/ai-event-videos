import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, statSync, unlinkSync, writeFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import { tmpdir } from "os";
import { cleanupOldFiles } from "@/lib/ffmpeg";

const THUMBNAIL_DIR = join(tmpdir(), "thumbnails");
const FRAME_TIME = "00:00:01"; // Extract frame at 1 second
const FAILURE_CACHE_MS = 6 * 60 * 60 * 1000;

function getUrlHash(url: string): string {
  return createHash("md5").update(url).digest("hex");
}

function ensureThumbnailDir(): void {
  if (!existsSync(THUMBNAIL_DIR)) {
    mkdirSync(THUMBNAIL_DIR, { recursive: true });
  }
}

function createFallbackImageResponse(): NextResponse {
  const svg = `
    <svg xmlns="http://www.w3.org/2000/svg" width="320" height="180" viewBox="0 0 320 180" fill="none">
      <defs>
        <linearGradient id="g" x1="0" y1="0" x2="320" y2="180" gradientUnits="userSpaceOnUse">
          <stop stop-color="#F3F4F6"/>
          <stop offset="1" stop-color="#E5E7EB"/>
        </linearGradient>
      </defs>
      <rect width="320" height="180" rx="24" fill="url(#g)"/>
      <circle cx="160" cy="90" r="28" fill="rgba(15,23,42,0.08)"/>
      <path d="M151 75.5V104.5L174 90L151 75.5Z" fill="#94A3B8"/>
    </svg>
  `.trim();

  return new NextResponse(svg, {
    status: 200,
    headers: {
      "Content-Type": "image/svg+xml",
      "Cache-Control": "public, max-age=3600",
      "X-Thumbnail-Fallback": "1",
    },
  });
}

function hasRecentFailureMarker(markerPath: string): boolean {
  if (!existsSync(markerPath)) return false;

  try {
    return Date.now() - statSync(markerPath).mtimeMs < FAILURE_CACHE_MS;
  } catch {
    return false;
  }
}

function clearFailureMarker(markerPath: string): void {
  if (!existsSync(markerPath)) return;

  try {
    unlinkSync(markerPath);
  } catch {
    // Ignore cleanup failures
  }
}

function markFailure(markerPath: string): void {
  try {
    writeFileSync(markerPath, "");
  } catch {
    // Ignore marker write failures
  }
}

function tryGenerateThumbnail(inputUrl: string, thumbnailPath: string): boolean {
  const ffmpegCommand = `ffmpeg -ss ${FRAME_TIME} -i "${inputUrl}" -vframes 1 -q:v 2 -vf "scale=320:-1" -f image2 "${thumbnailPath}" -y 2>/dev/null`;

  try {
    execSync(ffmpegCommand, { timeout: 30000 });
  } catch {
    return false;
  }

  return existsSync(thumbnailPath);
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
    const failureMarkerPath = join(THUMBNAIL_DIR, `${hash}.missing`);

    // Check cache first
    if (existsSync(thumbnailPath)) {
      clearFailureMarker(failureMarkerPath);
      const imageBuffer = readFileSync(thumbnailPath);
      return new NextResponse(imageBuffer, {
        status: 200,
        headers: {
          "Content-Type": "image/jpeg",
          "Cache-Control": "public, max-age=31536000, immutable",
        },
      });
    }

    if (hasRecentFailureMarker(failureMarkerPath)) {
      return createFallbackImageResponse();
    }

    const proxiedUrl = `${request.nextUrl.origin}/api/video?url=${encodeURIComponent(url)}`;
    const generated =
      tryGenerateThumbnail(url, thumbnailPath) ||
      tryGenerateThumbnail(proxiedUrl, thumbnailPath);

    if (!generated) {
      markFailure(failureMarkerPath);
      cleanupOldFiles(THUMBNAIL_DIR, 24 * 60 * 60 * 1000);
      return createFallbackImageResponse();
    }

    clearFailureMarker(failureMarkerPath);
    const imageBuffer = readFileSync(thumbnailPath);

    // Opportunistic cleanup of old thumbnails (24h)
    cleanupOldFiles(THUMBNAIL_DIR, 24 * 60 * 60 * 1000);

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

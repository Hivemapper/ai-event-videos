import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import { tmpdir } from "os";
import { cleanupOldFiles } from "@/lib/ffmpeg";

const THUMBNAIL_DIR = join(tmpdir(), "thumbnails");
const FRAME_TIME = "00:00:01"; // Extract frame at 1 second

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

    // Generate thumbnail using FFmpeg
    // -ss: seek to position (before input for faster seeking)
    // -i: input URL
    // -vframes 1: extract only 1 frame
    // -q:v 2: JPEG quality (2-5 is good, lower is better quality)
    // -vf scale: resize to max 320px width, maintaining aspect ratio
    const ffmpegCommand = `ffmpeg -ss ${FRAME_TIME} -i "${url}" -vframes 1 -q:v 2 -vf "scale=320:-1" -f image2 "${thumbnailPath}" -y 2>/dev/null`;

    try {
      execSync(ffmpegCommand, { timeout: 30000 });
    } catch {
      // FFmpeg failed - return 404 so client can use fallback
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

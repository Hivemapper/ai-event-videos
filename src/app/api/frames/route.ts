import { NextRequest, NextResponse } from "next/server";
import { extractFrame } from "@/lib/ffmpeg";

export async function GET(request: NextRequest) {
  const url = request.nextUrl.searchParams.get("url");
  const timestampParam = request.nextUrl.searchParams.get("timestamp") || "0";
  const widthParam = request.nextUrl.searchParams.get("width") || "1280";

  if (!url) {
    return NextResponse.json({ error: "URL is required" }, { status: 400 });
  }

  const timestamp = parseFloat(timestampParam);
  const width = parseInt(widthParam, 10);

  if (isNaN(timestamp) || timestamp < 0) {
    return NextResponse.json(
      { error: "Invalid timestamp" },
      { status: 400 }
    );
  }

  if (isNaN(width) || width < 100 || width > 3840) {
    return NextResponse.json(
      { error: "Width must be between 100 and 3840" },
      { status: 400 }
    );
  }

  try {
    const imageBuffer = extractFrame(url, timestamp, width);

    if (!imageBuffer) {
      return NextResponse.json(
        { error: "Failed to extract frame" },
        { status: 500 }
      );
    }

    return new NextResponse(new Uint8Array(imageBuffer), {
      status: 200,
      headers: {
        "Content-Type": "image/jpeg",
        "Cache-Control": "public, max-age=3600",
        "X-Frame-Timestamp": timestamp.toString(),
        "X-Frame-Width": width.toString(),
      },
    });
  } catch (error) {
    console.error("Frame extraction error:", error);
    return NextResponse.json(
      { error: "Failed to extract frame" },
      { status: 500 }
    );
  }
}

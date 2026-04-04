import { NextRequest, NextResponse } from "next/server";

export async function GET(request: NextRequest) {
  const url = request.nextUrl.searchParams.get("url");

  if (!url) {
    return NextResponse.json({ error: "URL is required" }, { status: 400 });
  }

  try {
    const rangeHeader = request.headers.get("range");

    // Forward range header to origin for seeking support
    const fetchHeaders: HeadersInit = {};
    if (rangeHeader) {
      fetchHeaders["Range"] = rangeHeader;
    }

    // Retry on 403 (CDN rate limiting)
    let response = await fetch(url, { headers: fetchHeaders });
    if (response.status === 403) {
      await new Promise((r) => setTimeout(r, 2000));
      response = await fetch(url, { headers: fetchHeaders });
    }
    if (response.status === 403) {
      await new Promise((r) => setTimeout(r, 5000));
      response = await fetch(url, { headers: fetchHeaders });
    }

    if (!response.ok && response.status !== 206) {
      return NextResponse.json(
        { error: `Failed to fetch video: ${response.status}` },
        { status: response.status, headers: { "Cache-Control": "no-store" } }
      );
    }

    const contentType = response.headers.get("content-type") || "video/mp4";

    // Detect CDN challenge pages returning HTML instead of video
    if (contentType.includes("text/html")) {
      return NextResponse.json(
        { error: "Video temporarily unavailable (CDN challenge)" },
        { status: 503 }
      );
    }

    const contentLength = response.headers.get("content-length");
    const contentRange = response.headers.get("content-range");

    const headers = new Headers({
      "Content-Type": contentType,
      "Accept-Ranges": "bytes",
      "Cache-Control": "public, max-age=300",
    });

    if (contentLength) {
      headers.set("Content-Length", contentLength);
    }

    if (contentRange) {
      headers.set("Content-Range", contentRange);
    }

    return new NextResponse(response.body, {
      status: response.status === 206 ? 206 : 200,
      headers,
    });
  } catch (error) {
    console.error("Video proxy error:", error);
    return NextResponse.json(
      { error: "Failed to proxy video" },
      { status: 500 }
    );
  }
}

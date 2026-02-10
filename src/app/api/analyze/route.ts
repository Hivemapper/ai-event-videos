import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import Anthropic from "@anthropic-ai/sdk";
import { API_BASE_URL } from "@/lib/constants";
import { AIEvent } from "@/types/events";
import { VideoAnalysis } from "@/types/analysis";
import {
  ANALYZE_VIDEO_TOOL,
  getAnalysisSystemPrompt,
  selectFrameTimestamps,
  buildContextText,
} from "@/lib/analysis";
import { getTimeOfDay } from "@/lib/sun";
import { createCirclePolygon } from "@/lib/geo-utils";
import { extractFrame, ensureDir, cleanupOldFiles } from "@/lib/ffmpeg";
import { analyzeSchema } from "@/lib/schemas";

const ANALYSIS_CACHE_DIR = join(tmpdir(), "video-analysis");

function getCacheKey(eventId: string): string {
  return createHash("md5").update(eventId).digest("hex");
}

interface AnalyzeRequest {
  eventId: string;
  anthropicApiKey?: string;
  beemapsApiKey?: string;
  mapboxToken?: string;
  forceRefresh?: boolean;
}

export async function POST(request: NextRequest) {
  let body: AnalyzeRequest;
  try {
    const raw = await request.json();
    const parsed = analyzeSchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    body = parsed.data as AnalyzeRequest;
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  const { eventId, forceRefresh } = body;

  // Check cache
  ensureDir(ANALYSIS_CACHE_DIR);
  const cacheKey = getCacheKey(eventId);
  const cachePath = join(ANALYSIS_CACHE_DIR, `${cacheKey}.json`);

  if (!forceRefresh && existsSync(cachePath)) {
    try {
      const cached = JSON.parse(readFileSync(cachePath, "utf-8"));
      return NextResponse.json(cached);
    } catch {
      // Cache corrupted, continue with fresh analysis
    }
  }

  // Resolve API keys
  const anthropicKey =
    body.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  if (!anthropicKey) {
    return NextResponse.json(
      { error: "NO_API_KEY" },
      { status: 401 }
    );
  }

  const beemapsKey =
    body.beemapsApiKey || process.env.BEEMAPS_API_KEY;
  if (!beemapsKey) {
    return NextResponse.json(
      { error: "Bee Maps API key is required" },
      { status: 401 }
    );
  }

  const mapboxToken =
    body.mapboxToken || process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

  try {
    // 1. Fetch event with GNSS + IMU
    const authHeader = beemapsKey.startsWith("Basic ")
      ? beemapsKey
      : `Basic ${beemapsKey}`;

    const eventUrl = new URL(`${API_BASE_URL}/${eventId}`);
    eventUrl.searchParams.set("includeGnssData", "true");
    eventUrl.searchParams.set("includeImuData", "true");

    const eventResponse = await fetch(eventUrl.toString(), {
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
    });

    if (!eventResponse.ok) {
      return NextResponse.json(
        { error: `Failed to fetch event: ${eventResponse.status}` },
        { status: eventResponse.status }
      );
    }

    const event: AIEvent = await eventResponse.json();

    // 2. Parallel: road type + map features
    const [roadTypeResult, mapFeaturesResult] = await Promise.allSettled([
      mapboxToken
        ? fetch(
            `https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${event.location.lon},${event.location.lat}.json?layers=road&radius=10&limit=1&access_token=${mapboxToken}`
          ).then((r) => (r.ok ? r.json() : null))
        : Promise.resolve(null),
      fetch(`https://beemaps.com/api/developer/map-data`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Authorization: authHeader,
        },
        body: JSON.stringify({
          type: ["mapFeatures"],
          geometry: {
            type: "Polygon",
            coordinates: [
              createCirclePolygon(
                event.location.lat,
                event.location.lon,
                200
              ),
            ],
          },
        }),
      }).then((r) => (r.ok ? r.json() : null)),
    ]);

    const roadTypeData =
      roadTypeResult.status === "fulfilled" ? roadTypeResult.value : null;
    const roadTypeLabel =
      roadTypeData?.features?.[0]?.properties?.class || null;

    const mapFeaturesData =
      mapFeaturesResult.status === "fulfilled"
        ? mapFeaturesResult.value
        : null;
    const mapFeatures = mapFeaturesData?.mapFeatureResults?.data || [];

    // 3. Determine time of day
    const sunInfo = getTimeOfDay(
      event.timestamp,
      event.location.lat,
      event.location.lon
    );

    // 4. Select frame timestamps
    const speedArray = event.metadata?.SPEED_ARRAY as
      | Array<{ AVG_SPEED_MS: number; TIMESTAMP: number }>
      | undefined;

    // Estimate video duration from speed array timestamps or default to 10s
    let videoDuration = 10;
    if (speedArray && speedArray.length > 1) {
      const first = speedArray[0].TIMESTAMP;
      const last = speedArray[speedArray.length - 1].TIMESTAMP;
      const spanMs = last - first;
      if (spanMs > 0) {
        videoDuration = spanMs / 1000;
      }
    }

    const frameTimestamps = selectFrameTimestamps(
      videoDuration,
      speedArray,
      event.imuData
    );

    // 5. Extract frames via FFmpeg
    const frameBuffers: Array<{ buffer: Buffer; timestamp: number }> = [];
    for (const ts of frameTimestamps) {
      const buf = extractFrame(event.videoUrl, ts, 1280);
      if (buf) {
        frameBuffers.push({ buffer: buf, timestamp: ts });
      }
    }

    if (frameBuffers.length === 0) {
      return NextResponse.json(
        { error: "Failed to extract any frames from the video" },
        { status: 500 }
      );
    }

    // 6. Build Claude message with interleaved text + images
    const contextText = buildContextText(
      event,
      roadTypeLabel,
      mapFeatures,
      sunInfo.timeOfDay
    );

    // Build speed profile timeline for context
    let speedProfile = "";
    if (speedArray && speedArray.length > 0) {
      const samples = Math.min(speedArray.length, 10);
      const step = Math.max(1, Math.floor(speedArray.length / samples));
      const points: string[] = [];
      for (let i = 0; i < speedArray.length; i += step) {
        const t = (i / (speedArray.length - 1)) * videoDuration;
        const mph = (speedArray[i].AVG_SPEED_MS * 2.237).toFixed(0);
        points.push(`${t.toFixed(1)}s: ${mph} mph`);
      }
      speedProfile = `\nSpeed timeline: ${points.join(" → ")}`;
    }

    // Helper to get approximate speed at a video timestamp
    const getSpeedAtTime = (t: number): string => {
      if (!speedArray || speedArray.length === 0) return "";
      const idx = Math.min(
        Math.round((t / videoDuration) * (speedArray.length - 1)),
        speedArray.length - 1
      );
      const mph = (speedArray[idx].AVG_SPEED_MS * 2.237).toFixed(0);
      return ` — vehicle speed: ${mph} mph`;
    };

    const messageContent: Anthropic.Messages.ContentBlockParam[] = [
      {
        type: "text",
        text: `Analyze this video from a vehicle with a Bee camera. Here is the sensor and contextual data:\n\n${contextText}${speedProfile}\n\nI'm providing ${frameBuffers.length} frames from the video in chronological order:`,
      },
    ];

    for (let i = 0; i < frameBuffers.length; i++) {
      const { buffer, timestamp } = frameBuffers[i];
      messageContent.push({
        type: "text",
        text: `Frame ${i + 1} (at ${timestamp.toFixed(1)}s${getSpeedAtTime(timestamp)}):`,
      });
      messageContent.push({
        type: "image",
        source: {
          type: "base64",
          media_type: "image/jpeg",
          data: buffer.toString("base64"),
        },
      });
    }

    // 7. Call Claude for video analysis
    const client = new Anthropic({ apiKey: anthropicKey });

    const response = await client.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 4096,
      system: getAnalysisSystemPrompt(event.type),
      tools: [ANALYZE_VIDEO_TOOL],
      tool_choice: { type: "tool", name: "analyze_video" },
      messages: [{ role: "user", content: messageContent }],
    });

    const toolUse = response.content.find(
      (block): block is Anthropic.ToolUseBlock => block.type === "tool_use"
    );

    if (!toolUse) {
      return NextResponse.json(
        { error: "Claude did not return a structured analysis" },
        { status: 500 }
      );
    }

    const analysis = toolUse.input as VideoAnalysis;

    // 8. Cache result
    const cacheData = {
      analysis,
      eventId,
      analyzedAt: new Date().toISOString(),
      frameTimestamps: frameBuffers.map((f) => f.timestamp),
    };

    try {
      writeFileSync(cachePath, JSON.stringify(cacheData, null, 2));
    } catch {
      // Non-fatal: cache write failure
    }

    // Opportunistic cleanup of old analysis caches (24h)
    cleanupOldFiles(ANALYSIS_CACHE_DIR, 24 * 60 * 60 * 1000);

    return NextResponse.json(cacheData);
  } catch (error) {
    console.error("Analysis error:", error);
    const message =
      error instanceof Anthropic.APIError
        ? `Claude API error: ${error.message}`
        : error instanceof Error
          ? error.message
          : "An unexpected error occurred";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import Anthropic from "@anthropic-ai/sdk";
import { API_BASE_URL } from "@/lib/constants";
import { VideoAnalysis } from "@/types/analysis";
import { AIEvent } from "@/types/events";
import { getAnalysisSystemPrompt, buildContextText } from "@/lib/analysis";
import { getTimeOfDay } from "@/lib/sun";

const FRAMES_DIR = join(tmpdir(), "video-frames");

interface ChatRequest {
  eventId: string;
  question: string;
  previousAnalysis: VideoAnalysis;
  anthropicApiKey?: string;
  beemapsApiKey?: string;
  mapboxToken?: string;
}

export async function POST(request: NextRequest) {
  let body: ChatRequest;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  const { eventId, question, previousAnalysis } = body;
  if (!eventId || !question || !previousAnalysis) {
    return NextResponse.json(
      { error: "eventId, question, and previousAnalysis are required" },
      { status: 400 }
    );
  }

  const anthropicKey = body.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  if (!anthropicKey) {
    return NextResponse.json({ error: "NO_API_KEY" }, { status: 401 });
  }

  const beemapsKey = body.beemapsApiKey || process.env.BEEMAPS_API_KEY;
  if (!beemapsKey) {
    return NextResponse.json(
      { error: "Bee Maps API key is required" },
      { status: 401 }
    );
  }

  try {
    // Fetch event data for context
    const authHeader = beemapsKey.startsWith("Basic ")
      ? beemapsKey
      : `Basic ${beemapsKey}`;

    const eventUrl = new URL(`${API_BASE_URL}/${eventId}`);
    eventUrl.searchParams.set("includeGnssData", "true");

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

    // Try to load cached frames from Phase 1 analysis
    const speedArray = event.metadata?.SPEED_ARRAY as
      | Array<{ AVG_SPEED_MS: number; TIMESTAMP: number }>
      | undefined;

    let videoDuration = 10;
    if (speedArray && speedArray.length > 1) {
      const first = speedArray[0].TIMESTAMP;
      const last = speedArray[speedArray.length - 1].TIMESTAMP;
      const spanMs = last - first;
      if (spanMs > 0) videoDuration = spanMs / 1000;
    }

    // Try to reuse frames from the cached analysis
    const cacheKey = createHash("md5").update(eventId).digest("hex");
    const cachePath = join(tmpdir(), "video-analysis", `${cacheKey}.json`);

    let frameTimestamps: number[] = [];
    if (existsSync(cachePath)) {
      try {
        const cached = JSON.parse(readFileSync(cachePath, "utf-8"));
        frameTimestamps = cached.frameTimestamps || [];
      } catch {
        // Ignore
      }
    }

    // Build message content with frames if available
    const sunInfo = getTimeOfDay(
      event.timestamp,
      event.location.lat,
      event.location.lon
    );

    const contextText = buildContextText(event, null, [], sunInfo.timeOfDay);

    const messageContent: Anthropic.Messages.ContentBlockParam[] = [];

    // Try to include cached frames
    let framesIncluded = 0;
    for (const ts of frameTimestamps) {
      const hash = createHash("md5")
        .update(`${event.videoUrl}-${ts}-1280`)
        .digest("hex");
      const framePath = join(FRAMES_DIR, `${hash}.jpg`);

      if (existsSync(framePath)) {
        const buffer = readFileSync(framePath);
        if (framesIncluded === 0) {
          messageContent.push({
            type: "text",
            text: `Context:\n${contextText}\n\nFrames from the video:`,
          });
        }
        messageContent.push({
          type: "text",
          text: `Frame ${framesIncluded + 1} (at ${ts.toFixed(1)}s):`,
        });
        messageContent.push({
          type: "image",
          source: {
            type: "base64",
            media_type: "image/jpeg",
            data: buffer.toString("base64"),
          },
        });
        framesIncluded++;
      }
    }

    // If no frames found, just provide text context
    if (framesIncluded === 0) {
      messageContent.push({
        type: "text",
        text: `Context:\n${contextText}`,
      });
    }

    // Build multi-turn conversation
    const client = new Anthropic({ apiKey: anthropicKey });

    const systemPrompt = getAnalysisSystemPrompt(event.type) +
      "\n\nYou previously analyzed this video and provided a structured analysis. Now answer the user's follow-up question about this specific event. Be concise and specific.";

    const previousAnalysisText = `Here is my previous analysis of this video:\n\nSummary: ${previousAnalysis.summary}\n\nHazard: ${previousAnalysis.hazard.severity} severity${previousAnalysis.hazard.hazardType ? ` (${previousAnalysis.hazard.hazardType})` : ""}. ${previousAnalysis.hazard.hasNearMiss ? `Near miss: ${previousAnalysis.hazard.nearMissType || "yes"}.` : "No near miss."}\n\nDriving: ${previousAnalysis.driving.assessment}. ${previousAnalysis.driving.speedContext}\n\nRoad: ${previousAnalysis.road.roadType || "unknown type"}. ${previousAnalysis.environment.weather || ""} ${previousAnalysis.environment.lighting || ""}\n\nObjects: ${previousAnalysis.objects.length > 0 ? previousAnalysis.objects.map((o) => `${o.type} (${o.position}, ${o.estimatedDistance})`).join(", ") : "none"}`;

    const response = await client.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1024,
      system: systemPrompt,
      messages: [
        { role: "user", content: messageContent },
        { role: "assistant", content: previousAnalysisText },
        { role: "user", content: question },
      ],
    });

    const textBlock = response.content.find(
      (block): block is Anthropic.TextBlock => block.type === "text"
    );

    if (!textBlock) {
      return NextResponse.json(
        { error: "No response from Claude" },
        { status: 500 }
      );
    }

    return NextResponse.json({ answer: textBlock.text });
  } catch (error) {
    console.error("Chat error:", error);
    const message =
      error instanceof Anthropic.APIError
        ? `Claude API error: ${error.message}`
        : error instanceof Error
          ? error.message
          : "An unexpected error occurred";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

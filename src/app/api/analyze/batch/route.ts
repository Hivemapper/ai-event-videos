import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";

const ANALYSIS_CACHE_DIR = join(tmpdir(), "video-analysis");

interface BatchRequest {
  eventIds: string[];
  anthropicApiKey?: string;
  beemapsApiKey?: string;
  mapboxToken?: string;
}

export async function POST(request: NextRequest) {
  let body: BatchRequest;
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  const { eventIds } = body;
  if (!eventIds || !Array.isArray(eventIds) || eventIds.length === 0) {
    return NextResponse.json(
      { error: "eventIds array is required" },
      { status: 400 }
    );
  }

  const anthropicKey = body.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  if (!anthropicKey) {
    return NextResponse.json({ error: "NO_API_KEY" }, { status: 401 });
  }

  const results: Record<string, unknown> = {};
  const errors: Record<string, string> = {};
  const skipped: string[] = [];

  // Process events sequentially to avoid rate limits
  for (const eventId of eventIds) {
    // Check if already cached
    const cacheKey = createHash("md5").update(eventId).digest("hex");
    const cachePath = join(ANALYSIS_CACHE_DIR, `${cacheKey}.json`);

    if (existsSync(cachePath)) {
      try {
        const cached = JSON.parse(readFileSync(cachePath, "utf-8"));
        results[eventId] = cached.analysis;
        skipped.push(eventId);
        continue;
      } catch {
        // Cache corrupted, re-analyze
      }
    }

    // Call our own analyze endpoint for each event
    try {
      const analyzeUrl = new URL("/api/analyze", request.url);
      const response = await fetch(analyzeUrl.toString(), {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          eventId,
          anthropicApiKey: body.anthropicApiKey,
          beemapsApiKey: body.beemapsApiKey,
          mapboxToken: body.mapboxToken,
        }),
      });

      if (response.ok) {
        const data = await response.json();
        results[eventId] = data.analysis;
      } else {
        const data = await response.json().catch(() => ({ error: "Unknown error" }));
        errors[eventId] = data.error || `HTTP ${response.status}`;
      }
    } catch (err) {
      errors[eventId] = err instanceof Error ? err.message : "Unknown error";
    }
  }

  return NextResponse.json({
    results,
    errors,
    total: eventIds.length,
    analyzed: Object.keys(results).length,
    failed: Object.keys(errors).length,
    skipped: skipped.length,
  });
}

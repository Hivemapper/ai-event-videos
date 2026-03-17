import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import Anthropic from "@anthropic-ai/sdk";
import { ensureDir, cleanupOldFiles } from "@/lib/ffmpeg";
import { visionScanSchema } from "@/lib/schemas";

const CACHE_DIR = join(tmpdir(), "vision-scan");

const MODEL_MAP: Record<string, string> = {
  sonnet: "claude-sonnet-4-5-20250929",
  haiku: "claude-haiku-4-5-20251001",
};

const ROAD_CLASS_LABELS: Record<string, string> = {
  motorway: "Highway",
  motorway_link: "Highway Ramp",
  trunk: "Major Road",
  trunk_link: "Major Road Ramp",
  primary: "Primary Road",
  primary_link: "Primary Road Ramp",
  secondary: "Secondary Road",
  secondary_link: "Secondary Road Ramp",
  tertiary: "Local Road",
  tertiary_link: "Local Road Ramp",
  street: "Residential Street",
  street_limited: "Residential Street (limited)",
  service: "Service Road",
  path: "Path/Trail",
  pedestrian: "Pedestrian Zone",
  track: "Track",
};

const EVALUATE_EVENTS_TOOL: Anthropic.Messages.Tool = {
  name: "evaluate_events",
  description:
    "Evaluate which events are likely to match the user's search query based on road and location context.",
  input_schema: {
    type: "object" as const,
    properties: {
      evaluations: {
        type: "array",
        items: {
          type: "object",
          properties: {
            eventId: { type: "string" },
            match: { type: "boolean" },
            confidence: {
              type: "string",
              enum: ["high", "medium", "low"],
            },
            reason: { type: "string" },
          },
          required: ["eventId", "match", "confidence", "reason"],
        },
        description: "One evaluation per event in the batch.",
      },
    },
    required: ["evaluations"],
  },
};

const SYSTEM_PROMPT = `You are evaluating dashcam event locations against a user's search query. For each event, you are given the road type, event type (the driving behavior that triggered the dashcam), and GPS coordinates.

Based on this context, determine whether each event location is likely to contain what the user is searching for.

Consider:
- Road type: highways/motorways rarely have pedestrians or cyclists; residential streets, local roads, and pedestrian zones are much more likely. Service roads and paths can vary.
- Event type: harsh braking near a residential street could indicate pedestrian interaction; swerving could indicate obstacle avoidance.
- Location patterns: urban coordinates (dense road networks) vs rural (isolated roads).

For each event, provide:
- match: true if the location context suggests a reasonable chance of matching the query
- confidence: "high" if road type strongly suggests a match, "medium" if plausible, "low" if weak signal
- reason: brief 1-sentence explanation referencing the road type and why it matches or doesn't`;

interface EventInput {
  eventId: string;
  lat: number;
  lon: number;
  eventType?: string;
}

interface RoadInfo {
  class: string | null;
  classLabel: string | null;
}

interface ScanMatch {
  eventId: string;
  match: boolean;
  confidence: "high" | "medium" | "low";
  reason: string;
}

async function queryRoadType(
  lon: number,
  lat: number,
  token: string
): Promise<RoadInfo> {
  const url = `https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${lon},${lat}.json?layers=road&radius=10&limit=1&access_token=${token}`;
  try {
    const response = await fetch(url);
    if (!response.ok) return { class: null, classLabel: null };
    const data = await response.json();
    const feature = data.features?.[0];
    if (!feature) return { class: null, classLabel: null };
    const roadClass = feature.properties?.class || null;
    return {
      class: roadClass,
      classLabel: roadClass
        ? ROAD_CLASS_LABELS[roadClass] || roadClass
        : null,
    };
  } catch {
    return { class: null, classLabel: null };
  }
}

export async function POST(request: NextRequest) {
  let body: {
    query: string;
    events: EventInput[];
    model: "sonnet" | "haiku";
    anthropicApiKey?: string;
    mapboxToken?: string;
  };

  try {
    const raw = await request.json();
    const parsed = visionScanSchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    body = parsed.data;
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  const { query, events, model } = body;

  // Check cache
  ensureDir(CACHE_DIR);
  const eventIds = events
    .map((e) => e.eventId)
    .sort()
    .join(",");
  const cacheKey = createHash("md5")
    .update(`${query}-${eventIds}-${model}`)
    .digest("hex");
  const cachePath = join(CACHE_DIR, `${cacheKey}.json`);

  if (existsSync(cachePath)) {
    try {
      const cached = JSON.parse(readFileSync(cachePath, "utf-8"));
      return NextResponse.json(cached);
    } catch {
      // Cache corrupted, continue fresh
    }
  }

  // Resolve API keys
  const anthropicKey = body.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  if (!anthropicKey) {
    return NextResponse.json({ error: "NO_API_KEY" }, { status: 401 });
  }

  const mapboxToken =
    body.mapboxToken || process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

  try {
    // Query road types for all events in parallel
    const roadInfos: Map<string, RoadInfo> = new Map();
    if (mapboxToken) {
      const roadResults = await Promise.all(
        events.map(async (e) => ({
          eventId: e.eventId,
          road: await queryRoadType(e.lon, e.lat, mapboxToken),
        }))
      );
      for (const r of roadResults) {
        roadInfos.set(r.eventId, r.road);
      }
    }

    // Build context strings per event
    const eventContexts = events.map((e) => {
      const road = roadInfos.get(e.eventId);
      const parts = [`Event ${e.eventId}`];
      if (road?.classLabel) {
        parts.push(`Road: ${road.classLabel}`);
      } else {
        parts.push("Road: Unknown");
      }
      if (e.eventType) {
        parts.push(
          `Trigger: ${e.eventType.replace(/_/g, " ").toLowerCase()}`
        );
      }
      parts.push(`Location: (${e.lat.toFixed(5)}, ${e.lon.toFixed(5)})`);
      return parts.join(" | ");
    });

    // Batch 15 events per Claude call (text-only, can batch more)
    const BATCH_SIZE = 15;
    const batches: string[][] = [];
    for (let i = 0; i < eventContexts.length; i += BATCH_SIZE) {
      batches.push(eventContexts.slice(i, i + BATCH_SIZE));
    }

    const client = new Anthropic({ apiKey: anthropicKey });
    const modelId = MODEL_MAP[model];
    const allMatches: ScanMatch[] = [];

    for (const batch of batches) {
      const userMessage = `Search query: "${query}"

Events to evaluate:
${batch.map((ctx, i) => `${i + 1}. ${ctx}`).join("\n")}

Evaluate each event against the search query and call the evaluate_events tool with your results.`;

      const response = await client.messages.create({
        model: modelId,
        max_tokens: 2048,
        system: SYSTEM_PROMPT,
        tools: [EVALUATE_EVENTS_TOOL],
        tool_choice: { type: "tool", name: "evaluate_events" },
        messages: [{ role: "user", content: userMessage }],
      });

      const toolUse = response.content.find(
        (block): block is Anthropic.ToolUseBlock =>
          block.type === "tool_use"
      );

      if (toolUse) {
        const result = toolUse.input as {
          evaluations: ScanMatch[];
        };
        for (const evaluation of result.evaluations) {
          allMatches.push(evaluation);
        }
      }
    }

    const scanResult = {
      matches: allMatches,
      query,
      model: modelId,
      eventsScanned: events.length,
    };

    // Cache result
    try {
      writeFileSync(cachePath, JSON.stringify(scanResult, null, 2));
    } catch {
      // Non-fatal
    }

    cleanupOldFiles(CACHE_DIR, 24 * 60 * 60 * 1000);

    return NextResponse.json(scanResult);
  } catch (error) {
    console.error("Vision scan error:", error);
    const message =
      error instanceof Anthropic.APIError
        ? `Claude API error: ${error.message}`
        : error instanceof Error
          ? error.message
          : "An unexpected error occurred";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

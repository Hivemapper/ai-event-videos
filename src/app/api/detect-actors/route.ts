import { NextRequest, NextResponse } from "next/server";
import { createHash } from "crypto";
import { existsSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { tmpdir } from "os";
import Anthropic from "@anthropic-ai/sdk";
import { projectActorToWorld, CameraIntrinsics } from "@/lib/geo-projection";
import { ActorDetectionResult, DetectedActor, ActorType } from "@/types/actors";
import { extractFrame, ensureDir, cleanupOldFiles } from "@/lib/ffmpeg";
import { detectActorsSchema } from "@/lib/schemas";

const CACHE_DIR = join(tmpdir(), "actor-detection");

const DETECT_ACTORS_TOOL: Anthropic.Messages.Tool = {
  name: "detect_actors",
  description:
    "Report all detected actors (vehicles, pedestrians, cyclists, animals) visible in this Bee camera frame with bounding boxes and distance estimates.",
  input_schema: {
    type: "object" as const,
    properties: {
      actors: {
        type: "array",
        items: {
          type: "object",
          properties: {
            type: {
              type: "string",
              enum: [
                "car", "truck", "suv", "van", "bus", "motorcycle",
                "bicycle", "pedestrian", "animal", "scooter",
                "other_vehicle", "other",
              ],
            },
            label: {
              type: "string",
              description:
                "Short descriptive label, e.g. 'red sedan', 'person crossing', 'white delivery van'",
            },
            confidence: {
              type: "string",
              enum: ["high", "medium", "low"],
            },
            bbox: {
              type: "object",
              properties: {
                x_min: { type: "number", description: "Left edge in pixels" },
                y_min: { type: "number", description: "Top edge in pixels" },
                x_max: { type: "number", description: "Right edge in pixels" },
                y_max: { type: "number", description: "Bottom edge in pixels" },
              },
              required: ["x_min", "y_min", "x_max", "y_max"],
            },
            estimatedDistanceMeters: {
              type: "number",
              description:
                "Estimated distance from camera in meters using visual size heuristics",
            },
            moving: {
              type: ["boolean", "null"],
              description:
                "Whether the actor appears to be moving (null if indeterminate from a single frame)",
            },
            description: {
              type: "string",
              description:
                "Brief description of what the actor is doing, e.g. 'turning left ahead', 'parked on shoulder'",
            },
          },
          required: [
            "type", "label", "confidence", "bbox",
            "estimatedDistanceMeters", "moving", "description",
          ],
        },
      },
      frameWidth: { type: "number" },
      frameHeight: { type: "number" },
    },
    required: ["actors", "frameWidth", "frameHeight"],
  },
};

const SYSTEM_PROMPT = `You are an expert computer vision system analyzing a video frame from a wide-angle (~120° FOV) Bee camera mounted on a vehicle. Your task is to detect every visible actor in the scene.

Instructions:
- Detect ALL visible actors: vehicles (cars, trucks, SUVs, vans, buses, motorcycles), bicycles, scooters, pedestrians, and animals.
- CRITICAL — Vulnerable road users are the highest priority. Scan the entire frame carefully for:
  - Pedestrians: people walking, standing, jogging, or jaywalking — even partially occluded by vehicles or at the edges of the frame.
  - People with strollers, baby carriages, or pushing carts.
  - Wheelchair users or people with mobility aids.
  - Cyclists and e-bike riders, including those in bike lanes or weaving through traffic.
  - Scooter riders (kick scooters and electric scooters).
  - Children, who may be shorter and harder to spot between parked cars.
  - Construction workers or road workers on foot.
  Use type "pedestrian" for all people on foot (including those with strollers, wheelchairs, or working on the road). Use "bicycle" for cyclists and "scooter" for scooter riders.
- Include parked vehicles, partially occluded actors, and actors in the distance.
- Provide bounding boxes in pixel coordinates. The image is 1280 pixels wide.
- IMPORTANT: Bounding boxes must tightly enclose each actor. The horizontal center of the box determines the actor's angular position on the map, so accuracy matters.
- Estimate distances conservatively using these calibrated references:
  - A standard car rear (1.8m wide) at distance d occupies roughly (1.8 * 1280) / (2 * d * tan(60°)) pixels wide.
  - Typical distances on a residential road: car ahead in same lane = 10-30m, car at next intersection = 30-60m, parked cars on the side = 5-15m.
  - A pedestrian (0.5m wide) appearing ~20px wide is roughly 18m away. At ~50px wide, roughly 7m.
  - Distant vehicles near the vanishing point: 80-150m. Do NOT exceed 150m unless clearly on a highway.
  - When in doubt, estimate SHORTER distances. Overestimating distance is the most common error.
- Set "moving" to null since this is a single frame (unless motion blur or position clearly indicates movement).
- Be thorough — it's better to detect more actors than to miss any. After your initial scan, do a second pass specifically looking for small or partially hidden pedestrians and cyclists.`;

interface DetectActorsRequest {
  eventId: string;
  videoUrl: string;
  timestamp: number;
  cameraLat: number;
  cameraLon: number;
  cameraBearing: number;
  fovDegrees: number;
  cameraIntrinsics?: { focal: number; k1: number; k2: number };
  anthropicApiKey?: string;
}

export async function POST(request: NextRequest) {
  let body: DetectActorsRequest;
  try {
    const raw = await request.json();
    const parsed = detectActorsSchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    body = parsed.data as DetectActorsRequest;
  } catch {
    return NextResponse.json(
      { error: "Invalid request body" },
      { status: 400 }
    );
  }

  const {
    eventId, videoUrl, timestamp,
    cameraLat, cameraLon, cameraBearing, fovDegrees,
    cameraIntrinsics,
  } = body;

  // Check cache
  ensureDir(CACHE_DIR);
  const cacheKey = createHash("md5")
    .update(`${eventId}-${timestamp.toFixed(1)}`)
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

  // Resolve API key
  const anthropicKey =
    body.anthropicApiKey || process.env.ANTHROPIC_API_KEY;
  if (!anthropicKey) {
    return NextResponse.json(
      { error: "NO_API_KEY" },
      { status: 401 }
    );
  }

  try {
    // 1. Extract frame
    const frameBuffer = extractFrame(videoUrl, timestamp, 1280);
    if (!frameBuffer) {
      return NextResponse.json(
        { error: "Failed to extract frame from video" },
        { status: 500 }
      );
    }

    // 2. Call Claude Vision
    const client = new Anthropic({ apiKey: anthropicKey });

    const response = await client.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 4096,
      system: SYSTEM_PROMPT,
      tools: [DETECT_ACTORS_TOOL],
      tool_choice: { type: "tool", name: "detect_actors" },
      messages: [
        {
          role: "user",
          content: [
            {
              type: "text",
              text: `Detect all actors in this Bee camera frame. The image is 1280px wide. Report every vehicle, pedestrian, cyclist, scooter rider, and animal you can see, including parked cars and distant actors. Pay special attention to vulnerable road users — pedestrians (including people with strollers, wheelchairs, or on foot near traffic), cyclists, and scooter riders. These are safety-critical and must not be missed.`,
            },
            {
              type: "image",
              source: {
                type: "base64",
                media_type: "image/jpeg",
                data: frameBuffer.toString("base64"),
              },
            },
          ],
        },
      ],
    });

    const toolUse = response.content.find(
      (block): block is Anthropic.ToolUseBlock => block.type === "tool_use"
    );

    if (!toolUse) {
      return NextResponse.json(
        { error: "Claude did not return structured actor detection" },
        { status: 500 }
      );
    }

    const rawResult = toolUse.input as {
      actors: Array<{
        type: ActorType;
        label: string;
        confidence: "high" | "medium" | "low";
        bbox: { x_min: number; y_min: number; x_max: number; y_max: number };
        estimatedDistanceMeters: number;
        moving: boolean | null;
        description: string;
      }>;
      frameWidth: number;
      frameHeight: number;
    };

    // 3. Project each actor to world coordinates
    const imageWidth = rawResult.frameWidth || 1280;
    const intrinsics: CameraIntrinsics | undefined = cameraIntrinsics
      ? { focal: cameraIntrinsics.focal, k1: cameraIntrinsics.k1, k2: cameraIntrinsics.k2 }
      : undefined;

    const actors: DetectedActor[] = rawResult.actors.map((raw) => {
      // Clamp distance to reasonable range (2m–200m)
      const clampedDistance = Math.max(2, Math.min(200, raw.estimatedDistanceMeters));
      const projected = projectActorToWorld(
        raw.bbox,
        clampedDistance,
        cameraLat,
        cameraLon,
        cameraBearing,
        imageWidth,
        fovDegrees,
        intrinsics
      );
      return {
        ...raw,
        estimatedDistanceMeters: clampedDistance,
        worldPosition: { lat: projected.lat, lon: projected.lon },
        bearingFromCamera: projected.bearing,
      };
    });

    const result: ActorDetectionResult = {
      actors,
      timestamp,
      cameraPosition: { lat: cameraLat, lon: cameraLon },
      cameraBearing,
      fovDegrees,
      detectedAt: new Date().toISOString(),
    };

    // 4. Cache result
    try {
      writeFileSync(cachePath, JSON.stringify(result, null, 2));
    } catch {
      // Non-fatal
    }

    // Opportunistic cleanup of old detection caches (24h)
    cleanupOldFiles(CACHE_DIR, 24 * 60 * 60 * 1000);

    return NextResponse.json(result);
  } catch (error) {
    console.error("Actor detection error:", error);
    const message =
      error instanceof Anthropic.APIError
        ? `Claude API error: ${error.message}`
        : error instanceof Error
          ? error.message
          : "An unexpected error occurred";
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

import Anthropic from "@anthropic-ai/sdk";
import { AIEventType, ImuDataPoint } from "@/types/events";

interface SpeedDataPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

export const ANALYZE_VIDEO_TOOL: Anthropic.Tool = {
  name: "analyze_video",
  description:
    "Provide a structured analysis of the video from the vehicle with the Bee camera based on the frames and sensor data provided.",
  input_schema: {
    type: "object" as const,
    properties: {
      summary: {
        type: "string",
        description:
          "2-3 sentence narrative describing what is happening in the video. Always refer to the camera vehicle as 'the vehicle with the Bee camera', never 'dashcam vehicle' or 'ego vehicle'.",
      },
      road: {
        type: "object",
        description: "Road characteristics visible in the frames",
        properties: {
          roadType: {
            type: ["string", "null"],
            description:
              'Descriptive road type, e.g. "residential street", "highway", "rural two-lane", "urban intersection", "divided highway"',
          },
          lanes: {
            type: ["number", "null"],
            description: "Total number of lanes visible (both directions). Count carefully from lane markings.",
          },
          laneWidth: {
            type: ["string", "null"],
            enum: ["narrow", "standard", "wide", null],
          },
          surface: {
            type: ["string", "null"],
            enum: ["paved", "gravel", "dirt", "wet", "snow_covered", null],
          },
          markings: {
            type: ["string", "null"],
            enum: ["clear", "faded", "absent", null],
            description: "Condition of lane markings, crosswalk lines, etc.",
          },
          curvature: {
            type: ["string", "null"],
            enum: ["straight", "gentle_curve", "sharp_curve", null],
          },
          grade: {
            type: ["string", "null"],
            enum: ["flat", "uphill", "downhill", null],
          },
          shoulder: { type: ["boolean", "null"] },
          median: { type: ["boolean", "null"] },
          crosswalk: {
            type: ["boolean", "null"],
            description: "Is a crosswalk visible?",
          },
          intersection: {
            type: ["boolean", "null"],
            description: "Is the vehicle at or approaching an intersection?",
          },
          intersectionType: {
            type: "string",
            enum: ["signalized", "stop_controlled", "yield", "uncontrolled", "roundabout"],
            description: "Type of intersection if one is present",
          },
        },
        required: [
          "roadType",
          "lanes",
          "laneWidth",
          "surface",
          "markings",
          "curvature",
          "grade",
          "shoulder",
          "median",
          "crosswalk",
          "intersection",
        ],
      },
      signage: {
        type: "array",
        description: "All road signs, traffic signals, and regulatory signs visible in the frames. Look carefully for stop signs, speed limit signs, traffic lights, exit signs, highway route markers, warning signs, etc. READ the text on every sign.",
        items: {
          type: "object",
          properties: {
            type: {
              type: "string",
              enum: [
                "speed_limit",
                "stop_sign",
                "yield",
                "traffic_light",
                "warning",
                "regulatory",
                "guide",
                "construction",
                "other",
              ],
              description: 'Use "guide" for exit signs, highway route markers, destination signs, mile markers, and informational signs.',
            },
            state: {
              type: "string",
              enum: ["red", "yellow", "green", "flashing", "off"],
              description: "Current state of traffic lights. Omit for non-light signs.",
            },
            value: {
              type: "string",
              description: 'Transcribe ALL readable text on the sign. Examples: "EXIT 25", "Speed Limit 65", "I-280 South / San Jose", "Woodside Rd / 1 Mile", "No Left Turn", "Merge Right". Include route numbers, exit numbers, destination names, distances — everything you can read.',
            },
            position: {
              type: "string",
              enum: ["ahead", "left", "right", "overhead", "roadside"],
            },
            estimatedDistance: {
              type: "string",
              enum: ["very_close", "close", "medium", "far"],
            },
            description: {
              type: "string",
              description: 'Brief description of the sign type, color, and relevance. Reference video timestamps, e.g. "Green exit sign visible at 5s on right side" — NEVER say "in frame 2".',
            },
          },
          required: ["type", "value", "position", "estimatedDistance", "description"],
        },
      },
      objects: {
        type: "array",
        description: "All vehicles, pedestrians, cyclists, animals, and other notable objects visible in the scene. Be thorough — list every person and vehicle you can see.",
        items: {
          type: "object",
          properties: {
            type: {
              type: "string",
              enum: [
                "vehicle",
                "pedestrian",
                "cyclist",
                "animal",
                "debris",
                "construction",
                "other",
              ],
            },
            subtype: {
              type: "string",
              description:
                'More specific type: for vehicles use "sedan", "SUV", "pickup", "truck", "van", "motorcycle", "bus"; for pedestrians use "adult", "child", "jogger"; for animals use species',
            },
            position: {
              type: "string",
              enum: [
                "ahead",
                "left",
                "right",
                "oncoming",
                "crossing",
                "adjacent",
                "behind",
              ],
            },
            estimatedDistance: {
              type: "string",
              enum: ["very_close", "close", "medium", "far"],
            },
            relevance: {
              type: "string",
              enum: ["high", "medium", "low"],
            },
            moving: {
              type: ["boolean", "null"],
              description: "Is the object moving? null if uncertain.",
            },
            description: {
              type: "string",
              description: 'Brief description of the object, what it\'s doing, and any notable clothing/color for people. Reference video timestamps, e.g. "White sedan passing at 12s" — NEVER say "in frame 3".',
            },
          },
          required: [
            "type",
            "position",
            "estimatedDistance",
            "relevance",
            "moving",
            "description",
          ],
        },
      },
      visibilityIssues: {
        type: "array",
        description: "Any visibility impairments affecting the driver: sun glare, rain on windshield, snow, fog, headlight glare from oncoming vehicles, dirty windshield, etc. Empty array if visibility is clear.",
        items: {
          type: "object",
          properties: {
            type: {
              type: "string",
              enum: [
                "sun_glare",
                "rain",
                "snow",
                "fog",
                "darkness",
                "dirty_windshield",
                "headlight_glare",
                "spray",
                "smoke",
                "other",
              ],
            },
            severity: {
              type: "string",
              enum: ["mild", "moderate", "severe"],
            },
            description: {
              type: "string",
              description: "How this affects the driver's view",
            },
          },
          required: ["type", "severity", "description"],
        },
      },
      environment: {
        type: "object",
        properties: {
          weather: {
            type: ["string", "null"],
            enum: [
              "clear",
              "cloudy",
              "rainy",
              "foggy",
              "snowy",
              "overcast",
              null,
            ],
          },
          lighting: {
            type: ["string", "null"],
            enum: [
              "daylight",
              "dawn",
              "dusk",
              "night_lit",
              "night_dark",
              null,
            ],
          },
          visibility: {
            type: ["string", "null"],
            enum: ["excellent", "good", "moderate", "poor", null],
          },
          setting: {
            type: ["string", "null"],
            enum: [
              "urban",
              "suburban",
              "rural",
              "highway",
              "industrial",
              "parking",
              null,
            ],
          },
          glare: { type: ["boolean", "null"] },
        },
        required: ["weather", "lighting", "visibility", "setting", "glare"],
      },
      hazard: {
        type: "object",
        properties: {
          hasNearMiss: { type: "boolean" },
          nearMissType: { type: "string" },
          severity: {
            type: "string",
            enum: ["none", "low", "moderate", "high", "critical"],
          },
          hazardType: { type: "string" },
          contributingFactors: {
            type: "array",
            items: { type: "string" },
          },
        },
        required: ["hasNearMiss", "severity", "contributingFactors"],
      },
      driving: {
        type: "object",
        properties: {
          assessment: {
            type: "string",
            enum: ["normal", "cautious", "aggressive", "erratic", "emergency"],
          },
          speedContext: {
            type: "string",
            description:
              "Context about speed relative to conditions and limits",
          },
          brakingContext: { type: "string" },
          steeringContext: { type: "string" },
        },
        required: ["assessment", "speedContext"],
      },
      confidence: {
        type: "string",
        enum: ["high", "medium", "low"],
        description: "Overall confidence in the analysis",
      },
      frameNotes: {
        type: "array",
        items: { type: "string" },
        description: "One observation per analyzed frame, in chronological order",
      },
    },
    required: [
      "summary",
      "road",
      "signage",
      "objects",
      "visibilityIssues",
      "environment",
      "hazard",
      "driving",
      "confidence",
      "frameNotes",
    ],
  },
};

const EVENT_TYPE_DESCRIPTIONS: Record<AIEventType, string> = {
  HARSH_BRAKING: "harsh/emergency braking",
  AGGRESSIVE_ACCELERATION: "aggressive acceleration",
  SWERVING: "swerving or sudden lane change",
  HIGH_SPEED: "high speed driving",
  HIGH_G_FORCE: "high lateral or longitudinal g-force",
  STOP_SIGN_VIOLATION: "potential stop sign violation",
  TRAFFIC_LIGHT_VIOLATION: "potential traffic light violation",
  TAILGATING: "following too closely / tailgating",
  MANUAL_REQUEST: "manually triggered recording",
  UNKNOWN: "unclassified event",
};

export function getAnalysisSystemPrompt(eventType: AIEventType): string {
  const eventDesc = EVENT_TYPE_DESCRIPTIONS[eventType] || "driving event";

  return `You are an expert video analyst specializing in driving safety and road scene understanding. You are analyzing frames from a vehicle with a Bee camera that was flagged as: ${eventDesc}.

## Priority Fields — Be Thorough

1. **Road Type & Lanes**: Identify the road type precisely (e.g. "4-lane divided highway", "2-lane residential", "signalized intersection"). Count lanes carefully from markings — include turn lanes. Note if it's an intersection and what type.

2. **Signage — CRITICAL**: Carefully scan EVERY frame for ALL signs and signals, including small or distant ones. **Read and transcribe the full text on every sign you find**:
   - **Stop signs**: Red octagonal signs, even partially visible or at edges of frame. Note if the vehicle appears to be stopping for one.
   - **Traffic lights**: Note the current color (red/yellow/green). Check for lights mounted overhead, on poles, and on wire spans.
   - **Speed limit signs**: Read the posted number (e.g. "Speed Limit 35").
   - **Exit/guide signs**: Green highway signs — read the exit number, route number, destination name, and distance (e.g. "EXIT 25", "I-280 South / San Jose", "Woodside Rd 1 Mile"). These are important for understanding location.
   - **Route markers**: Interstate shields, US route markers, state route markers — read the number.
   - **Warning/regulatory signs**: Yield, no turn, one way, school zone, construction, merge, lane ends, curve ahead, etc.
   - Signs may appear small due to wide-angle lens — look carefully at both sides of the road, overhead gantries, and median areas.

3. **Objects & People**: List EVERY vehicle, pedestrian, cyclist, and animal visible. For vehicles describe type (sedan, SUV, truck, pickup, van, motorcycle, bus). For pedestrians note clothing, approximate age (adult/child), and what they're doing (standing, walking, crossing). Include parked vehicles if they're notable.

4. **Visibility Issues**: Identify anything impairing the driver's view — sun glare, rain/water on windshield, snow, fog, darkness, headlight glare from oncoming traffic, dirty windshield, spray from other vehicles. If visibility is clear, return an empty array.

5. **Road Surface & Conditions**: Note if the road is wet, snow-covered, icy, or has debris. Note crosswalks, whether at an intersection, and road markings condition.

## Connecting Events to Context

- **If the vehicle is braking or stopped**: Look for what caused it — a stop sign, traffic light, pedestrian, vehicle ahead, or obstacle. The signage section should explain what the driver is responding to.
- **If at an intersection**: Identify ALL traffic control devices (signs, signals, markings). Note whether the vehicle appears to be complying with them.
- **Speed changes**: When the sensor data shows the vehicle decelerating to a stop, examine the frames around that moment for stop signs, traffic signals, or crosswalks that explain the stop.

## General Instructions

- Analyze all provided frames together to understand the temporal progression of the scene.
- Cross-reference what you see in the frames with the provided sensor data (speed, IMU, road type, map features).
- Be specific and precise in your observations. Use null for any field you cannot determine with reasonable confidence.
- Bee camera lenses have wide-angle distortion, especially at edges. Signs near edges of frame will appear smaller and distorted — still identify them.
- Frame notes should describe what is specifically notable in each frame, one sentence each.
- The summary should read as a coherent narrative of what happened, including WHY the vehicle braked or took action.
- **IMPORTANT — Use video timestamps, not frame numbers**: Each frame is labeled with its video timestamp (e.g. "Frame 3 (at 8.5s)"). In ALL description fields for objects, signage, and visibility issues, reference the **video second** not the frame number. Write "visible at 8s" or "appears between 3-9s", NEVER "in frame 3" or "in frames 1-2".
- **IMPORTANT — Vehicle naming**: Always refer to the camera vehicle as "the vehicle with the Bee camera". Never use "dashcam vehicle", "ego vehicle", or "our vehicle".`;
}

export function selectFrameTimestamps(
  duration: number,
  speedArray?: SpeedDataPoint[],
  imuData?: ImuDataPoint[]
): number[] {
  if (duration < 2) return [duration / 2];

  const minGap = 0.8;
  const margin = 0.5;
  const clamp = (t: number) =>
    Math.max(margin, Math.min(duration - margin, t));

  // Scale frame count with duration: 4 for <10s, 6 for 10-20s, 8 for 20s+
  const targetCount = duration < 10 ? 4 : duration < 20 ? 6 : 8;

  // Find key moments from speed data
  let peakDeltaTime = duration * 0.5;
  let stopTime: number | null = null;    // moment vehicle reaches ~0 speed
  let preBrakeTime: number | null = null; // just before deceleration begins

  if (speedArray && speedArray.length > 2) {
    let maxDelta = 0;
    let brakeStartIdx: number | null = null;

    for (let i = 1; i < speedArray.length; i++) {
      const delta = Math.abs(
        speedArray[i].AVG_SPEED_MS - speedArray[i - 1].AVG_SPEED_MS
      );
      if (delta > maxDelta) {
        maxDelta = delta;
        peakDeltaTime = (i / (speedArray.length - 1)) * duration;
      }

      // Detect start of deceleration (speed dropping)
      if (
        brakeStartIdx === null &&
        speedArray[i].AVG_SPEED_MS < speedArray[i - 1].AVG_SPEED_MS - 0.5
      ) {
        brakeStartIdx = i - 1;
      }

      // Detect vehicle reaching near-stop (< 1 m/s ≈ 2.2 mph)
      if (
        stopTime === null &&
        speedArray[i].AVG_SPEED_MS < 1.0 &&
        i > 0 &&
        speedArray[i - 1].AVG_SPEED_MS >= 1.0
      ) {
        stopTime = (i / (speedArray.length - 1)) * duration;
      }
    }

    if (brakeStartIdx !== null) {
      // Frame just before braking — vehicle approaching, signs should be visible
      preBrakeTime = (brakeStartIdx / (speedArray.length - 1)) * duration;
    }
  } else if (imuData && imuData.length > 2) {
    let maxMag = 0;
    const startTs = imuData[0].timestamp;
    const endTs = imuData[imuData.length - 1].timestamp;
    const span = endTs - startTs;

    for (const point of imuData) {
      const accel = point.accelerometer;
      if (!accel) continue;
      const mag = Math.sqrt(accel.x ** 2 + accel.y ** 2 + accel.z ** 2);
      if (mag > maxMag) {
        maxMag = mag;
        peakDeltaTime = span > 0 ? ((point.timestamp - startTs) / span) * duration : duration * 0.5;
      }
    }
  }

  // Build candidate list
  const candidates: number[] = [];

  // Always include: early, peak event, late
  candidates.push(clamp(duration * 0.1));      // early approach
  if (preBrakeTime !== null) {
    candidates.push(clamp(preBrakeTime));        // just before braking (signs visible ahead)
  }
  candidates.push(clamp(peakDeltaTime));         // peak deceleration/event
  if (stopTime !== null) {
    candidates.push(clamp(stopTime));             // at the stop (sign should be close)
  }
  candidates.push(clamp(duration * 0.5));        // midpoint
  candidates.push(clamp(duration * 0.85));       // late / aftermath

  // Fill remaining slots with evenly spaced frames
  if (candidates.length < targetCount) {
    const step = duration / (targetCount + 1);
    for (let i = 1; i <= targetCount; i++) {
      candidates.push(clamp(step * i));
    }
  }

  // Sort and deduplicate (ensure min gap), take up to targetCount
  candidates.sort((a, b) => a - b);
  const result = [candidates[0]];
  for (let i = 1; i < candidates.length; i++) {
    if (candidates[i] - result[result.length - 1] >= minGap) {
      result.push(candidates[i]);
    }
    if (result.length >= targetCount) break;
  }

  return result;
}

export function buildContextText(
  event: {
    type: AIEventType;
    timestamp: string;
    location: { lat: number; lon: number };
    metadata?: Record<string, unknown>;
  },
  roadType?: string | null,
  mapFeatures?: Array<{ class: string; distance: number; speedLimit?: number; unit?: string }>,
  timeOfDay?: string
): string {
  const lines: string[] = [];

  lines.push(`Event type: ${EVENT_TYPE_DESCRIPTIONS[event.type] || event.type}`);
  lines.push(`Timestamp: ${event.timestamp}`);
  lines.push(
    `Location: ${event.location.lat.toFixed(6)}, ${event.location.lon.toFixed(6)}`
  );

  if (timeOfDay) lines.push(`Time of day: ${timeOfDay}`);
  if (roadType) lines.push(`Road classification: ${roadType}`);

  // Speed data summary
  const speedArray = event.metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;
  if (speedArray && speedArray.length > 0) {
    const speeds = speedArray.map((s) => s.AVG_SPEED_MS);
    const maxMs = Math.max(...speeds);
    const minMs = Math.min(...speeds);
    const avgMs = speeds.reduce((a, b) => a + b, 0) / speeds.length;
    lines.push(
      `Speed: min ${(minMs * 2.237).toFixed(1)} mph, max ${(maxMs * 2.237).toFixed(1)} mph, avg ${(avgMs * 2.237).toFixed(1)} mph`
    );
  }

  const acceleration = event.metadata?.ACCELERATION_MS2 as number | undefined;
  if (acceleration !== undefined) {
    lines.push(`Peak acceleration: ${acceleration.toFixed(2)} m/s²`);
  }

  // Map features
  if (mapFeatures && mapFeatures.length > 0) {
    const featureDescs = mapFeatures
      .slice(0, 5)
      .map((f) => {
        let desc = `${f.class} at ${f.distance}m`;
        if (f.speedLimit) desc += ` (limit: ${f.speedLimit} ${f.unit || "mph"})`;
        return desc;
      });
    lines.push(`Nearby map features: ${featureDescs.join(", ")}`);
  }

  return lines.join("\n");
}

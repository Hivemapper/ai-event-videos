import Anthropic from "@anthropic-ai/sdk";
import { MAX_DATE_RANGE_DAYS } from "./constants";

export function getSystemPrompt(today: string): string {
  return `You are a filter assistant for an AI dashcam event video viewer. Users describe what they want to see in natural language, and you translate that into structured filter parameters.

## Event Types
Map user descriptions to these exact types:
- HARSH_BRAKING: braking, hard braking, sudden stop, emergency braking
- AGGRESSIVE_ACCELERATION: acceleration, speeding up, flooring it, rapid acceleration
- SWERVING: swerving, lane change, weaving, evasive maneuver
- HIGH_SPEED: high speed, speeding, fast driving, excessive speed
- HIGH_G_FORCE: g-force, high g, lateral force, centrifugal
- STOP_SIGN_VIOLATION: stop sign, running a stop sign, failed to stop
- TRAFFIC_LIGHT_VIOLATION: red light, traffic light, running a light
- TAILGATING: tailgating, following too close, too close
- MANUAL_REQUEST: manual, manually triggered, requested
- UNKNOWN: unknown, unclassified

If the user mentions multiple event descriptions, include all matching types. If no specific type is mentioned, omit the types field entirely.

## Time of Day
Options: Day, Dawn, Dusk, Night
- "daytime" or "during the day" → Day
- "nighttime" or "at night" or "dark" → Night
- "morning" or "early morning" or "sunrise" → Dawn
- "evening" or "sunset" → Dusk
If not mentioned, omit the timeOfDay field.

## Date Range
Today is ${today}. The API supports a maximum range of ${MAX_DATE_RANGE_DAYS} days.
- "last week" → 7 days back from today
- "last month" → 31 days back from today (max allowed)
- "past 3 days" → 3 days back from today
- "last 6 months" → use ${MAX_DATE_RANGE_DAYS} days (the max) and mention in explanation that only ${MAX_DATE_RANGE_DAYS} days is supported
- If no date is mentioned, omit startDate and endDate to keep current selection.
Dates must be in YYYY-MM-DD format.

## Location
Use your geography knowledge to return approximate coordinates for locations:
- City: center coordinates with radius 15000-25000 (meters)
- State/Province: center coordinates with radius 150000-300000
- Country: center coordinates with radius 300000-1000000
- Specific address/landmark: coordinates with radius 1000-5000

Examples:
- "New York City" → lat: 40.7128, lon: -73.9060, radius: 20000
- "London" → lat: 51.5074, lon: -0.1278, radius: 20000
- "California" → lat: 36.7783, lon: -119.4179, radius: 250000

If no location is mentioned, omit the coordinates and radius fields.

## Rules
- Only set fields the user explicitly or implicitly mentioned
- Omitted fields preserve the user's current filter settings
- Always provide a brief, friendly explanation of what filters you're setting
- If the query is too vague, still do your best and explain your interpretation`;
}

export const FILTER_TOOL: Anthropic.Tool = {
  name: "set_filters",
  description:
    "Set the video event filters based on the user's natural language query",
  input_schema: {
    type: "object" as const,
    properties: {
      startDate: {
        type: "string",
        description: "Start date in YYYY-MM-DD format",
      },
      endDate: {
        type: "string",
        description: "End date in YYYY-MM-DD format",
      },
      types: {
        type: "array",
        items: {
          type: "string",
          enum: [
            "HARSH_BRAKING",
            "AGGRESSIVE_ACCELERATION",
            "SWERVING",
            "HIGH_SPEED",
            "HIGH_G_FORCE",
            "STOP_SIGN_VIOLATION",
            "TRAFFIC_LIGHT_VIOLATION",
            "TAILGATING",
            "MANUAL_REQUEST",
            "UNKNOWN",
          ],
        },
        description: "Event types to filter by",
      },
      timeOfDay: {
        type: "array",
        items: {
          type: "string",
          enum: ["Day", "Dawn", "Dusk", "Night"],
        },
        description: "Time of day filters",
      },
      coordinates: {
        type: "object",
        properties: {
          lat: { type: "number", description: "Latitude" },
          lon: { type: "number", description: "Longitude" },
        },
        required: ["lat", "lon"],
        description: "Center point for location search",
      },
      radius: {
        type: "number",
        description: "Search radius in meters around the coordinates",
      },
      explanation: {
        type: "string",
        description:
          "Brief friendly explanation of the filters being applied",
      },
    },
    required: ["explanation"],
  },
};

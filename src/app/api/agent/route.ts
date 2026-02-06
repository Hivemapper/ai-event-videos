import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { getSystemPrompt, FILTER_TOOL } from "@/lib/agent-skills";
import { AgentFilterResponse, AgentApiResult } from "@/types/agent";
import { AIEventsRequest } from "@/types/events";
import { API_BASE_URL } from "@/lib/constants";

function createCirclePolygon(
  lat: number,
  lon: number,
  radiusMeters: number,
  numPoints = 32
): [number, number][] {
  const coords: [number, number][] = [];
  const earthRadius = 6371000;
  for (let i = 0; i <= numPoints; i++) {
    const angle = (i / numPoints) * 2 * Math.PI;
    const dLat = (radiusMeters / earthRadius) * Math.cos(angle);
    const dLon =
      (radiusMeters / (earthRadius * Math.cos((lat * Math.PI) / 180))) *
      Math.sin(angle);
    coords.push([
      lon + (dLon * 180) / Math.PI,
      lat + (dLat * 180) / Math.PI,
    ]);
  }
  return coords;
}

function getDefaultDates() {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 7);
  return {
    startDate: start.toISOString().split("T")[0],
    endDate: end.toISOString().split("T")[0],
  };
}

export async function POST(
  request: NextRequest
): Promise<NextResponse<AgentApiResult>> {
  let query: string;
  let clientApiKey: string | undefined;
  let beemapsApiKey: string | undefined;
  try {
    const body = await request.json();
    query = body.query;
    clientApiKey = body.apiKey;
    beemapsApiKey = body.beemapsApiKey;
  } catch {
    return NextResponse.json(
      { success: false, error: "Invalid request body" },
      { status: 400 }
    );
  }

  if (!query || typeof query !== "string" || query.trim().length === 0) {
    return NextResponse.json(
      { success: false, error: "Query is required" },
      { status: 400 }
    );
  }

  const apiKey = clientApiKey || process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return NextResponse.json(
      { success: false, error: "NO_API_KEY" },
      { status: 401 }
    );
  }

  const client = new Anthropic({ apiKey });
  const today = new Date().toISOString().split("T")[0];

  try {
    const response = await client.messages.create({
      model: "claude-sonnet-4-5-20250929",
      max_tokens: 1024,
      system: getSystemPrompt(today),
      tools: [FILTER_TOOL],
      tool_choice: { type: "tool", name: "set_filters" },
      messages: [{ role: "user", content: query.trim() }],
    });

    const toolUse = response.content.find(
      (block): block is Anthropic.ToolUseBlock => block.type === "tool_use"
    );

    if (!toolUse) {
      return NextResponse.json(
        {
          success: false,
          error: "Failed to interpret your query. Please try rephrasing.",
        },
        { status: 500 }
      );
    }

    const filters = toolUse.input as AgentFilterResponse;

    // Fetch matching events if we have a Beemaps API key
    const beeKey = beemapsApiKey || process.env.BEEMAPS_API_KEY;
    if (beeKey) {
      try {
        const defaults = getDefaultDates();
        const eventsRequest: AIEventsRequest = {
          startDate: filters.startDate || defaults.startDate,
          endDate: filters.endDate || defaults.endDate,
          types: filters.types,
          limit: 20,
        };

        if (filters.coordinates && filters.radius) {
          eventsRequest.polygon = createCirclePolygon(
            filters.coordinates.lat,
            filters.coordinates.lon,
            filters.radius
          );
        }

        const authHeader = beeKey.startsWith("Basic ") ? beeKey : `Basic ${beeKey}`;
        const eventsResponse = await fetch(`${API_BASE_URL}/search`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: authHeader,
          },
          body: JSON.stringify(eventsRequest),
        });

        if (eventsResponse.ok) {
          const data = await eventsResponse.json();
          return NextResponse.json({
            success: true,
            filters,
            events: data.events || [],
            totalCount: data.pagination?.total || 0,
          });
        }
      } catch {
        // If event fetching fails, still return filters without events
      }
    }

    return NextResponse.json({ success: true, filters, events: [], totalCount: 0 });
  } catch (error) {
    const message =
      error instanceof Anthropic.APIError
        ? `Claude API error: ${error.message}`
        : "An unexpected error occurred";
    return NextResponse.json(
      { success: false, error: message },
      { status: 500 }
    );
  }
}

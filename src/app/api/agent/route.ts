import { NextRequest, NextResponse } from "next/server";
import Anthropic from "@anthropic-ai/sdk";
import { getSystemPrompt, FILTER_TOOL } from "@/lib/agent-skills";
import { AgentFilterResponse, AgentApiResult } from "@/types/agent";
import { AIEventsRequest } from "@/types/events";
import { API_BASE_URL } from "@/lib/constants";
import { createCirclePolygon } from "@/lib/geo-utils";
import { agentQuerySchema } from "@/lib/schemas";

function getDefaultDates() {
  const end = new Date();
  const start = new Date();
  start.setDate(start.getDate() - 31);
  return {
    startDate: start.toISOString(),
    endDate: end.toISOString(),
  };
}

// Convert YYYY-MM-DD to full ISO datetime
function toISO(date: string, end = false): string {
  if (date.includes("T")) return date;
  return end ? `${date}T23:59:59.999Z` : `${date}T00:00:00.000Z`;
}

export async function POST(
  request: NextRequest
): Promise<NextResponse<AgentApiResult>> {
  let query: string;
  let clientApiKey: string | undefined;
  let beemapsApiKey: string | undefined;
  try {
    const raw = await request.json();
    const parsed = agentQuerySchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { success: false, error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    query = parsed.data.query;
    clientApiKey = parsed.data.apiKey;
    beemapsApiKey = parsed.data.beemapsApiKey;
  } catch {
    return NextResponse.json(
      { success: false, error: "Invalid request body" },
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
          startDate: filters.startDate ? toISO(filters.startDate) : defaults.startDate,
          endDate: filters.endDate ? toISO(filters.endDate, true) : defaults.endDate,
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
        } else {
          const errText = await eventsResponse.text();
          console.error("Agent Bee Maps API error:", eventsResponse.status, errText);
        }
      } catch (fetchError) {
        console.error("Agent event fetch error:", fetchError);
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

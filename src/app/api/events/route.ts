import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";
import { eventsSearchSchema } from "@/lib/schemas";

export async function POST(request: NextRequest) {
  try {
    const apiKey = request.headers.get("Authorization");

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    const raw = await request.json();
    const parsed = eventsSearchSchema.safeParse(raw);
    if (!parsed.success) {
      return NextResponse.json(
        { error: parsed.error.issues.map((i) => i.message).join(", ") },
        { status: 400 }
      );
    }
    const body = parsed.data;
    console.log("Request body:", JSON.stringify(body));

    // Use Basic auth for base64-encoded API keys
    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    const response = await fetchWithRetry(`${API_BASE_URL}/search`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text();
      let errorMessage = `API error: ${response.status}`;

      try {
        const errorJson = JSON.parse(errorText);
        // Handle various error response formats
        if (typeof errorJson === 'string') {
          errorMessage = errorJson;
        } else if (Array.isArray(errorJson)) {
          // Handle array of errors (e.g., validation errors)
          errorMessage = errorJson.map((e: unknown) => {
            if (typeof e === 'string') return e;
            const err = e as Record<string, unknown>;
            return err.message || JSON.stringify(e);
          }).join(', ');
        } else if (errorJson.message) {
          errorMessage = typeof errorJson.message === 'string'
            ? errorJson.message
            : JSON.stringify(errorJson.message);
        } else if (errorJson.error) {
          errorMessage = typeof errorJson.error === 'string'
            ? errorJson.error
            : JSON.stringify(errorJson.error);
        } else if (errorJson.errors && Array.isArray(errorJson.errors)) {
          errorMessage = errorJson.errors.map((e: unknown) =>
            typeof e === 'string' ? e : (e as Record<string, unknown>).message || JSON.stringify(e)
          ).join(', ');
        } else {
          errorMessage = JSON.stringify(errorJson);
        }
      } catch {
        if (errorText) {
          errorMessage = errorText;
        }
      }

      console.error("Beemaps API error:", response.status, errorMessage);

      return NextResponse.json(
        { error: errorMessage },
        { status: response.status }
      );
    }

    const data = await response.json();
    return NextResponse.json(data);
  } catch (error) {
    console.error("API proxy error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

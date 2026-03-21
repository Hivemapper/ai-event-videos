import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

const DAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];

export async function GET(request: NextRequest) {
  try {
    const apiKey =
      request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;
    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }
    const authHeader = apiKey.startsWith("Basic ")
      ? apiKey
      : `Basic ${apiKey}`;

    const now = new Date();
    const days: { date: string; day: string; startDate: string; endDate: string }[] = [];

    for (let i = 6; i >= 0; i--) {
      const d = new Date(now);
      d.setDate(d.getDate() - i);
      const yyyy = d.getFullYear();
      const mm = String(d.getMonth() + 1).padStart(2, "0");
      const dd = String(d.getDate()).padStart(2, "0");
      const dateStr = `${yyyy}-${mm}-${dd}`;
      days.push({
        date: dateStr,
        day: DAY_NAMES[d.getDay()],
        startDate: `${dateStr}T00:00:00.000Z`,
        endDate: `${dateStr}T23:59:59.999Z`,
      });
    }

    const counts = await Promise.all(
      days.map(async (day) => {
        const res = await fetchWithRetry(`${API_BASE_URL}/search`, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Authorization: authHeader,
          },
          body: JSON.stringify({
            startDate: day.startDate,
            endDate: day.endDate,
            limit: 1,
            offset: 0,
          }),
        });
        if (!res.ok) return 0;
        const data = await res.json();
        return data.pagination?.total ?? 0;
      })
    );

    const result = days.map((day, i) => ({
      date: day.date,
      day: day.day,
      total: counts[i],
    }));

    return NextResponse.json(result);
  } catch (error) {
    console.error("Daily metrics API error:", error);
    return NextResponse.json(
      {
        error:
          error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

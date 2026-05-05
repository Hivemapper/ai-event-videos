import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { getPipelineCounts } from "@/lib/pipeline-dashboard";
import { isPipelineRunning } from "@/lib/pipeline-manager";

export const runtime = "nodejs";

const FRESH_MS = 5_000;
const MAX_STALE_MS = 30_000;

let cachedCounts: {
  counts: Awaited<ReturnType<typeof getPipelineCounts>>;
  updatedAt: number;
} | null = null;
let refreshPromise: Promise<void> | null = null;

async function refreshCounts() {
  const db = await getDb();
  const counts = await getPipelineCounts(db);
  cachedCounts = { counts, updatedAt: Date.now() };
}

function kickRefresh() {
  if (!refreshPromise) {
    refreshPromise = refreshCounts().finally(() => {
      refreshPromise = null;
    });
  }
}

export async function GET() {
  const now = Date.now();
  const age = cachedCounts ? now - cachedCounts.updatedAt : Infinity;

  if (!cachedCounts || age > MAX_STALE_MS) {
    await refreshCounts();
  } else if (age > FRESH_MS) {
    kickRefresh();
  }

  return NextResponse.json({
    counts: cachedCounts!.counts,
    pipelineRunning: isPipelineRunning(),
  });
}

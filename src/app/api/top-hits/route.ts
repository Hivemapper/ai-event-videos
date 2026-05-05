import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  ensureTopHitsReady,
  invalidateTopHitsCache,
  loadCachedTopHitsResponse,
} from "@/lib/top-hits-store";

export async function GET() {
  try {
    const db = await getDb();
    return NextResponse.json(await loadCachedTopHitsResponse(db));
  } catch (error) {
    console.error("Top Hits GET error:", error);
    return NextResponse.json(
      { error: "Failed to load Top Hits", ids: [] },
      { status: 500 }
    );
  }
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const id = typeof body?.id === "string" ? body.id.trim() : "";
    if (!id) {
      return NextResponse.json(
        { error: "Missing `id` in request body" },
        { status: 400 }
      );
    }

    const db = await getDb();
    await ensureTopHitsReady(db);

    // Remove + reinsert bumps the event to the top (new row_id).
    // This also turns a re-add into a no-op surface change (idempotent UX).
    await db.run("DELETE FROM top_hits WHERE event_id = ?", [id]);
    await db.run("INSERT INTO top_hits (event_id) VALUES (?)", [id]);
    invalidateTopHitsCache();

    return NextResponse.json(await loadCachedTopHitsResponse(db, { force: true }));
  } catch (error) {
    console.error("Top Hits POST error:", error);
    return NextResponse.json(
      { error: "Failed to add Top Hit" },
      { status: 500 }
    );
  }
}

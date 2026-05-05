import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import {
  ensureTopHitsReady,
  invalidateTopHitsCache,
  loadCachedTopHitsResponse,
} from "@/lib/top-hits-store";

export async function DELETE(
  _request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const db = await getDb();
    await ensureTopHitsReady(db);
    await db.run("DELETE FROM top_hits WHERE event_id = ?", [id]);
    invalidateTopHitsCache();

    return NextResponse.json(await loadCachedTopHitsResponse(db, { force: true }));
  } catch (error) {
    console.error("Top Hits DELETE error:", error);
    return NextResponse.json(
      { error: "Failed to remove Top Hit" },
      { status: 500 }
    );
  }
}

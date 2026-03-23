import { NextRequest, NextResponse } from "next/server";
import { listCompletedDetectionRuns } from "@/lib/pipeline-store";

export async function GET(request: NextRequest) {
  const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
  const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);

  const { runs, total } = await listCompletedDetectionRuns(limit, offset);
  return NextResponse.json({ runs, total });
}

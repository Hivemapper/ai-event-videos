import { NextResponse } from "next/server";
import { getPipelineRun } from "@/lib/pipeline-store";

export const runtime = "nodejs";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  const run = getPipelineRun(id);

  if (!run) {
    return NextResponse.json({ error: "Run not found" }, { status: 404 });
  }

  return NextResponse.json({ run });
}

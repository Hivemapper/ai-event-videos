import { NextResponse } from "next/server";
import {
  getVideoDetectionSegments,
  getVideoPipelineState,
} from "@/lib/pipeline-store";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const [state, segments] = await Promise.all([
    getVideoPipelineState(videoId),
    getVideoDetectionSegments(videoId),
  ]);
  return NextResponse.json({ state, segments });
}

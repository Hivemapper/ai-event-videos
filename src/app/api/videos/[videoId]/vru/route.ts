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
  return NextResponse.json({
    state: getVideoPipelineState(videoId),
    segments: getVideoDetectionSegments(videoId),
  });
}

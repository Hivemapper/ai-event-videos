import { NextResponse } from "next/server";
import {
  getFrameDetections,
  getFrameDetectionModels,
  getFrameDetectionTimestamps,
} from "@/lib/pipeline-store";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const { searchParams } = new URL(request.url);
  const frameMsParam = searchParams.get("frameMs");
  if (frameMsParam !== null && Number.isNaN(parseInt(frameMsParam, 10))) {
    return NextResponse.json(
      { error: "frameMs must be an integer" },
      { status: 400 }
    );
  }
  const frameMs =
    frameMsParam !== null ? parseInt(frameMsParam, 10) : undefined;
  const modelName = searchParams.get("model") ?? undefined;
  const runId = searchParams.get("runId") ?? undefined;

  const [detections, timestamps, models] = await Promise.all([
    getFrameDetections(videoId, frameMs, modelName, runId),
    getFrameDetectionTimestamps(videoId, modelName, runId),
    getFrameDetectionModels(videoId),
  ]);

  return NextResponse.json({ detections, timestamps, models });
}

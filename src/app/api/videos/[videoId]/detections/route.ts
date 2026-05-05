import { NextResponse } from "next/server";
import {
  loadLocalEditedMetadata,
  localEditedMetadataToDetectionsResponse,
} from "@/lib/local-edited-events";
import { isVruDetectionLabel } from "@/lib/vru-labels";
import {
  getFrameDetections,
  getFrameDetectionModels,
  getDetectionSegmentsByRunId,
  getSceneAttributesByRunId,
  getTimelineByRunId,
  deleteVideoDetectionSegment,
} from "@/lib/pipeline-store";

export async function GET(
  request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const localMetadata = await loadLocalEditedMetadata(videoId);
  if (localMetadata) {
    return NextResponse.json(localEditedMetadataToDetectionsResponse(videoId, localMetadata));
  }

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

  const [detections, models, segments, sceneAttributes, timeline] = await Promise.all([
    getFrameDetections(videoId, frameMs, modelName, runId),
    getFrameDetectionModels(videoId),
    runId ? getDetectionSegmentsByRunId(videoId, runId) : Promise.resolve([]),
    runId ? getSceneAttributesByRunId(videoId, runId) : Promise.resolve({}),
    runId ? getTimelineByRunId(videoId, runId) : Promise.resolve(null),
  ]);

  // Keep non-VRU detections internal. Vehicles/signs can remain in the DB for
  // near-accident scoring, but the event UI receives only VRU detections.
  const vruDetections = detections.filter((detection) => isVruDetectionLabel(detection.label));
  const vruSegments = segments.filter((segment) => isVruDetectionLabel(segment.label));
  const vruTimestamps = Array.from(new Set(vruDetections.map((detection) => detection.frameMs))).sort(
    (a, b) => a - b
  );

  return NextResponse.json({
    detections: vruDetections,
    timestamps: vruTimestamps,
    models,
    segments: vruSegments,
    sceneAttributes,
    timeline,
  });
}

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const { searchParams } = new URL(request.url);
  const label = searchParams.get("label");
  const startMs = searchParams.get("startMs");
  const endMs = searchParams.get("endMs");
  if (!label || startMs === null || endMs === null) {
    return NextResponse.json({ error: "label, startMs, and endMs are required" }, { status: 400 });
  }
  const runId = searchParams.get("runId") ?? undefined;
  const deleted = await deleteVideoDetectionSegment(videoId, label, parseInt(startMs, 10), parseInt(endMs, 10), runId);
  return NextResponse.json({ deleted });
}

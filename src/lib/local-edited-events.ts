import { readFile } from "fs/promises";
import path from "path";
import { isVruDetectionLabel } from "@/lib/vru-labels";

const LOCAL_EDITED_EVENT_ID_RE = /^[A-Za-z0-9]+-[A-Za-z0-9_-]+$/;
const LOCAL_MODEL_NAME = "local-edited-metadata";

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonRecord)
    : null;
}

function asNumber(value: unknown): number | null {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function localEditedMetadataPath(id: string): string | null {
  if (!LOCAL_EDITED_EVENT_ID_RE.test(id)) return null;
  return path.join(process.cwd(), "data", "metadata", `${id}.json`);
}

export async function readLocalEditedMetadataFile(id: string): Promise<Buffer | null> {
  const filePath = localEditedMetadataPath(id);
  if (!filePath) return null;

  try {
    return await readFile(filePath);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

export async function loadLocalEditedMetadata(id: string): Promise<JsonRecord | null> {
  const file = await readLocalEditedMetadataFile(id);
  if (!file) return null;
  return JSON.parse(file.toString("utf8")) as JsonRecord;
}

function compactSegments(
  metadata: JsonRecord
): Array<{ label: string; startMs: number; endMs: number; maxConfidence: number; supportLevel: string }> {
  const segments = Array.isArray(metadata.detectionSegments) ? metadata.detectionSegments : [];
  return segments.flatMap((segment) => {
    const record = asRecord(segment);
    if (!record) return [];

    const label = asString(record.label);
    const startMs = asNumber(record.startMs);
    const endMs = asNumber(record.endMs);
    const maxConfidence = asNumber(record.maxConfidence);
    if (!label || startMs === null || endMs === null || maxConfidence === null) return [];
    if (!isVruDetectionLabel(label)) return [];

    return [{ label, startMs, endMs, maxConfidence, supportLevel: asString(record.supportLevel) ?? "supported" }];
  });
}

export function localEditedMetadataToEvent(id: string, metadata: JsonRecord): JsonRecord {
  const event = asRecord(metadata.event) ?? {};
  const location = asRecord(event.location) ?? {};
  const lat = asNumber(location.lat);
  const lon = asNumber(location.lon);
  const roadClass = asString(event.roadClass);
  const city = asString(event.city);
  const country = asString(event.country);
  const summary = asString(metadata.summary);

  const segmentsByLabel = new Map<string, Array<{ startMs: number; endMs: number; maxConfidence: number }>>();
  for (const segment of compactSegments(metadata)) {
    const existing = segmentsByLabel.get(segment.label) ?? [];
    existing.push({
      startMs: segment.startMs,
      endMs: segment.endMs,
      maxConfidence: segment.maxConfidence,
    });
    segmentsByLabel.set(segment.label, existing);
  }

  return {
    id: asString(metadata.id) ?? id,
    type: asString(event.type) ?? "UNKNOWN",
    timestamp: asString(event.timestamp) ?? "",
    location: {
      lat: lat ?? 0,
      lon: lon ?? 0,
    },
    metadata: asRecord(event.metadata) ?? {},
    videoUrl: asString(event.videoUrl) ?? "",
    gnssData: Array.isArray(metadata.gnssData) ? metadata.gnssData : [],
    imuData: Array.isArray(metadata.imuData) ? metadata.imuData : [],
    enrichment: {
      nearMiss: null,
      vruDetections: Array.from(segmentsByLabel, ([label, segments]) => ({ label, segments })),
      weather: null,
      road: {
        type: roadClass,
        label: roadClass,
        name: null,
        speedLimit: null,
      },
      summary,
      timeOfDay: asString(event.timeOfDay),
      location: city || country ? { city, country } : null,
      timeline: null,
    },
  };
}

export function localEditedMetadataToDetectionsResponse(videoId: string, metadata: JsonRecord): JsonRecord {
  const rawDetections = Array.isArray(metadata.frameDetections) ? metadata.frameDetections : [];
  const detections = rawDetections.flatMap((detection, index) => {
    const record = asRecord(detection);
    const bbox = asRecord(record?.bbox);
    if (!record || !bbox) return [];

    const frameMs = asNumber(record.frameMs);
    const label = asString(record.label);
    const confidence = asNumber(record.confidence);
    const xMin = asNumber(bbox.xMin);
    const yMin = asNumber(bbox.yMin);
    const xMax = asNumber(bbox.xMax);
    const yMax = asNumber(bbox.yMax);
    const frameWidth = asNumber(record.frameWidth);
    const frameHeight = asNumber(record.frameHeight);

    if (
      frameMs === null ||
      !label ||
      !isVruDetectionLabel(label) ||
      confidence === null ||
      xMin === null ||
      yMin === null ||
      xMax === null ||
      yMax === null ||
      frameWidth === null ||
      frameHeight === null
    ) {
      return [];
    }

    return [{
      id: index + 1,
      videoId,
      frameMs,
      label,
      confidence,
      xMin,
      yMin,
      xMax,
      yMax,
      frameWidth,
      frameHeight,
      pipelineVersion: "local-edited",
      modelName: LOCAL_MODEL_NAME,
      runId: null,
    }];
  });

  const segments = compactSegments(metadata).map((segment, index) => ({
    id: index + 1,
    videoId,
    label: segment.label,
    startMs: segment.startMs,
    endMs: segment.endMs,
    maxConfidence: segment.maxConfidence,
    supportLevel: segment.supportLevel,
    pipelineVersion: "local-edited",
    source: LOCAL_MODEL_NAME,
  }));

  return {
    detections,
    timestamps: Array.from(new Set(detections.map((detection) => detection.frameMs))).sort((a, b) => a - b),
    models: detections.length > 0 ? [LOCAL_MODEL_NAME] : [],
    segments,
    sceneAttributes: {},
    timeline: null,
  };
}

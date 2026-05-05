import { execFile } from "child_process";
import { createHash } from "crypto";
import { access, mkdir, readFile, writeFile } from "fs/promises";
import path from "path";
import { promisify } from "util";
import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";
import { isVruDetectionLabel } from "@/lib/vru-labels";

export const runtime = "nodejs";

const execFileAsync = promisify(execFile);
const SAFE_ID_RE = /^[A-Za-z0-9_-]+$/;
const SAFE_SUFFIX_RE = /^[A-Za-z0-9_-]+$/;
const SAFE_S3_SEGMENT_RE = /^[A-Za-z0-9_-]+$/;
const MIN_CLIP_SECONDS = 0.25;

type JsonRecord = Record<string, unknown>;

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonRecord)
    : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

function asNumber(value: unknown): number | null {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function normalizeAuthHeader(value: string | null | undefined): string | undefined {
  if (!value) return undefined;
  return value.startsWith("Basic ") ? value : `Basic ${value}`;
}

function normalizeProductionPrefix(value: string | undefined): string[] {
  if (!value) return [];
  return value
    .split("/")
    .map((segment) => segment.trim())
    .filter(Boolean)
    .map((segment) => {
      if (!SAFE_S3_SEGMENT_RE.test(segment)) {
        throw new Error(
          `PRODUCTION_S3_PREFIX segment must contain only letters, numbers, hyphens, and underscores: ${segment}`
        );
      }
      return segment;
    });
}

function formatSecondsForFfmpeg(seconds: number): string {
  return seconds.toFixed(3).replace(/\.?0+$/, "");
}

function formatMsLabel(milliseconds: number): string {
  if (milliseconds % 1000 === 0) return `${milliseconds / 1000}Sec`;
  return `${Math.round(milliseconds)}ms`;
}

function defaultEditedId({
  originalEventId,
  totalRemovedLeadingMs,
  totalRemovedTrailingMs,
  clipDurationMs,
}: {
  originalEventId: string;
  totalRemovedLeadingMs: number;
  totalRemovedTrailingMs: number;
  clipDurationMs: number;
}): string {
  if (totalRemovedTrailingMs === 0) {
    return `${originalEventId}-${formatMsLabel(totalRemovedLeadingMs)}`;
  }

  const originalClipEndMs = totalRemovedLeadingMs + clipDurationMs;
  return `${originalEventId}-${formatMsLabel(totalRemovedLeadingMs)}to${formatMsLabel(originalClipEndMs)}`;
}

async function fileExists(filePath: string): Promise<boolean> {
  try {
    await access(filePath);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

async function loadMetadata(id: string, apiKey: string | undefined): Promise<JsonRecord> {
  const cwd = process.cwd();
  const metadataPath = path.join(cwd, "data", "metadata", `${id}.json`);
  if (await fileExists(metadataPath)) {
    return JSON.parse(await readFile(metadataPath, "utf8")) as JsonRecord;
  }

  const outputPath = path.join(
    cwd,
    "data",
    "metadata",
    ...normalizeProductionPrefix(process.env.PRODUCTION_S3_PREFIX),
    `${id}.json`
  );
  const scriptPath = path.join(cwd, "scripts", "export-metadata.py");

  await execFileAsync(
    process.env.PYTHON ?? "python3",
    [scriptPath, "--event-id", id, "--production", "--overwrite"],
    {
      cwd,
      timeout: 180_000,
      maxBuffer: 1024 * 1024 * 4,
      env: {
        ...process.env,
        ...(apiKey ? { BEEMAPS_API_KEY: apiKey } : {}),
      },
    }
  );

  return JSON.parse(await readFile(outputPath, "utf8")) as JsonRecord;
}

async function loadFreshEventVideoUrl(id: string, apiKey: string | undefined): Promise<string | null> {
  if (!apiKey) return null;
  if (id.includes("-")) return null;

  const response = await fetchWithRetry(`${API_BASE_URL}/${id}`, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: apiKey,
    },
  });
  if (!response.ok) return null;

  const event = await response.json();
  return asString(event.videoUrl);
}

function localPublicVideoPath(videoUrl: string): string | null {
  let url: URL;
  try {
    url = new URL(videoUrl);
  } catch {
    return null;
  }

  if (!["localhost", "127.0.0.1", "::1"].includes(url.hostname)) return null;
  if (!url.pathname.startsWith("/videos/")) return null;

  const cwd = process.cwd();
  const publicDir = path.join(cwd, "public");
  const relativePath = decodeURIComponent(url.pathname).replace(/^\/+/, "");
  const resolved = path.normalize(path.join(publicDir, relativePath));
  const videosDir = path.join(publicDir, "videos");
  if (!resolved.startsWith(videosDir)) return null;
  return resolved;
}

async function fetchVideoBytes(videoUrl: string): Promise<Buffer> {
  const retryDelays = [0, 2000, 5000];
  let lastStatus = 0;

  for (const delay of retryDelays) {
    if (delay > 0) {
      await new Promise((resolve) => setTimeout(resolve, delay));
    }

    const response = await fetch(videoUrl);
    lastStatus = response.status;
    if (response.status === 403) continue;

    if (!response.ok) {
      throw new Error(`Failed to download source video: HTTP ${response.status}`);
    }

    const contentType = response.headers.get("content-type") ?? "";
    if (contentType.includes("text/html")) {
      throw new Error("Source video returned an HTML challenge page");
    }

    return Buffer.from(await response.arrayBuffer());
  }

  throw new Error(`Failed to download source video after retries: HTTP ${lastStatus}`);
}

async function resolveVideoInput(videoUrl: string): Promise<string> {
  const localPath = localPublicVideoPath(videoUrl);
  if (localPath) return localPath;

  const cwd = process.cwd();
  const cacheDir = path.join(cwd, "data", "pipeline-video-cache");
  await mkdir(cacheDir, { recursive: true });

  let extension = ".mp4";
  try {
    const pathname = new URL(videoUrl).pathname;
    const ext = path.extname(pathname);
    if (ext && ext.length <= 8) extension = ext;
  } catch {
    // Keep the default extension.
  }

  const cacheKey = createHash("sha256").update(videoUrl).digest("hex").slice(0, 32);
  const cachePath = path.join(cacheDir, `${cacheKey}${extension}`);
  if (await fileExists(cachePath)) return cachePath;

  await writeFile(cachePath, await fetchVideoBytes(videoUrl));
  return cachePath;
}

async function probeDurationSeconds(inputPath: string): Promise<number> {
  const { stdout } = await execFileAsync(
    "ffprobe",
    ["-v", "error", "-show_entries", "format=duration", "-of", "default=nw=1:nk=1", inputPath],
    { timeout: 30_000 }
  );
  const duration = Number(stdout.trim());
  if (!Number.isFinite(duration) || duration <= 0) {
    throw new Error("Unable to determine source video duration");
  }
  return duration;
}

function firstTelemetryTimestamp(metadata: JsonRecord): number | null {
  const values = [
    ...(Array.isArray(metadata.gnssData) ? metadata.gnssData : []),
    ...(Array.isArray(metadata.imuData) ? metadata.imuData : []),
  ]
    .map((point) => asNumber(asRecord(point)?.timestamp))
    .filter((timestamp): timestamp is number => timestamp !== null);

  return values.length > 0 ? Math.min(...values) : null;
}

function clipTimedPoints(points: unknown, startTimestampMs: number, endTimestampMs: number): unknown[] {
  if (!Array.isArray(points)) return [];
  return points.filter((point) => {
    const timestamp = asNumber(asRecord(point)?.timestamp);
    return timestamp !== null && timestamp >= startTimestampMs && timestamp <= endTimestampMs;
  });
}

function clipFrameDetections(detections: unknown, clipStartMs: number, clipEndMs: number): unknown[] {
  if (!Array.isArray(detections)) return [];
  return detections
    .filter((detection) => {
      const record = asRecord(detection);
      const frameMs = asNumber(record?.frameMs);
      return (
        frameMs !== null &&
        frameMs >= clipStartMs &&
        frameMs <= clipEndMs &&
        isVruDetectionLabel(record?.label)
      );
    })
    .map((detection) => {
      const record = asRecord(detection);
      return {
        ...record,
        frameMs: Math.round((asNumber(record?.frameMs) ?? 0) - clipStartMs),
      };
    });
}

function clipSegments(segments: unknown, clipStartMs: number, clipEndMs: number): unknown[] {
  if (!Array.isArray(segments)) return [];
  const clipDurationMs = clipEndMs - clipStartMs;

  return segments.flatMap((segment) => {
    const record = asRecord(segment);
    if (!record) return [];
    if (!isVruDetectionLabel(record.label)) return [];

    const startMs = asNumber(record.startMs);
    const endMs = asNumber(record.endMs);
    if (startMs === null || endMs === null) return [];
    if (endMs < clipStartMs || startMs > clipEndMs) return [];

    const shiftedStartMs = Math.max(0, Math.round(startMs - clipStartMs));
    const shiftedEndMs = Math.min(clipDurationMs, Math.max(0, Math.round(endMs - clipStartMs)));
    if (shiftedEndMs < shiftedStartMs) return [];

    return [{
      ...record,
      startMs: shiftedStartMs,
      endMs: shiftedEndMs,
    }];
  });
}

function clipMapFeatureArray(
  value: unknown,
  clipStartTimestampMs: number,
  clipEndTimestampMs: number,
  clipStartSeconds: number,
  clipEndSeconds: number
): unknown[] | null {
  if (!Array.isArray(value)) return null;

  return value.flatMap((item) => {
    const record = asRecord(item);
    if (!record) return [];

    const nearestGnssTimestamp = asNumber(record.nearestGnssTimestamp);
    const offsetSeconds = asNumber(record.offsetSeconds);
    const keepByTimestamp =
      nearestGnssTimestamp !== null &&
      nearestGnssTimestamp >= clipStartTimestampMs &&
      nearestGnssTimestamp <= clipEndTimestampMs;
    const keepByOffset =
      nearestGnssTimestamp === null &&
      offsetSeconds !== null &&
      offsetSeconds >= clipStartSeconds &&
      offsetSeconds <= clipEndSeconds;

    if (!keepByTimestamp && !keepByOffset) return [];

    return [{
      ...record,
      ...(offsetSeconds !== null ? { offsetSeconds: Math.max(0, offsetSeconds - clipStartSeconds) } : {}),
    }];
  });
}

function clipMapFeatures(
  metadata: JsonRecord,
  clipStartTimestampMs: number,
  clipEndTimestampMs: number,
  clipStartSeconds: number,
  clipEndSeconds: number
) {
  const mapFeatures = asRecord(metadata.mapFeatures);
  if (!mapFeatures) return;

  const nextMapFeatures: JsonRecord = { ...mapFeatures };
  const features = clipMapFeatureArray(
    mapFeatures.features,
    clipStartTimestampMs,
    clipEndTimestampMs,
    clipStartSeconds,
    clipEndSeconds
  );
  const speedLimits = clipMapFeatureArray(
    mapFeatures.speedLimits,
    clipStartTimestampMs,
    clipEndTimestampMs,
    clipStartSeconds,
    clipEndSeconds
  );

  if (features) nextMapFeatures.features = features;
  if (speedLimits) nextMapFeatures.speedLimits = speedLimits;

  const hasFeatures = Array.isArray(nextMapFeatures.features) && nextMapFeatures.features.length > 0;
  const hasSpeedLimits = Array.isArray(nextMapFeatures.speedLimits) && nextMapFeatures.speedLimits.length > 0;
  if (!hasFeatures && !hasSpeedLimits) {
    delete metadata.mapFeatures;
    return;
  }

  metadata.mapFeatures = nextMapFeatures;
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;
  if (!SAFE_ID_RE.test(id)) {
    return NextResponse.json({ error: "Invalid event id" }, { status: 400 });
  }

  try {
    const body = (await request.json()) as JsonRecord;
    const startSeconds = asNumber(body.startSeconds) ?? 0;
    const requestedEndSeconds = asNumber(body.endSeconds);
    const suffix = asString(body.suffix);

    if (startSeconds < 0) {
      return NextResponse.json({ error: "Clip start must be 0 or greater" }, { status: 400 });
    }
    if (suffix && !SAFE_SUFFIX_RE.test(suffix)) {
      return NextResponse.json({ error: "Clip suffix can only contain letters, numbers, hyphens, and underscores" }, { status: 400 });
    }

    const apiKey =
      normalizeAuthHeader(request.headers.get("Authorization")) ??
      normalizeAuthHeader(process.env.BEEMAPS_API_KEY);
    const metadata = await loadMetadata(id, apiKey);
    const event = asRecord(metadata.event);
    const metadataVideoUrl = asString(event?.videoUrl);
    if (!event || !metadataVideoUrl) {
      return NextResponse.json({ error: "Source metadata does not include an event videoUrl" }, { status: 400 });
    }
    const sourceGnssSampleCount = Array.isArray(metadata.gnssData) ? metadata.gnssData.length : 0;
    const sourceImuSampleCount = Array.isArray(metadata.imuData) ? metadata.imuData.length : 0;
    const freshVideoUrl = await loadFreshEventVideoUrl(id, apiKey).catch(() => null);
    const sourceVideoUrl = freshVideoUrl ?? metadataVideoUrl;

    const sourceVideoPath = await resolveVideoInput(sourceVideoUrl);
    const sourceDurationSeconds = await probeDurationSeconds(sourceVideoPath);
    const endSeconds =
      requestedEndSeconds === null || requestedEndSeconds > sourceDurationSeconds
        ? sourceDurationSeconds
        : requestedEndSeconds;

    if (endSeconds - startSeconds < MIN_CLIP_SECONDS) {
      return NextResponse.json(
        { error: `Clip must be at least ${MIN_CLIP_SECONDS} seconds long` },
        { status: 400 }
      );
    }
    if (startSeconds >= sourceDurationSeconds) {
      return NextResponse.json({ error: "Clip start is beyond the source video duration" }, { status: 400 });
    }

    const clipStartMs = Math.round(startSeconds * 1000);
    const clipEndMs = Math.round(endSeconds * 1000);
    const clipDurationMs = clipEndMs - clipStartMs;
    const previousEdit = asRecord(metadata.videoEdit) ?? {};
    const originalEventId = asString(previousEdit.originalEventId) ?? id;
    const previousRemovedLeadingMs = asNumber(previousEdit.removedLeadingMs) ?? 0;
    const previousRemovedTrailingMs = asNumber(previousEdit.removedTrailingMs) ?? 0;
    const additionalRemovedTrailingMs = Math.max(0, Math.round((sourceDurationSeconds - endSeconds) * 1000));
    const totalRemovedLeadingMs = previousRemovedLeadingMs + clipStartMs;
    const totalRemovedTrailingMs = previousRemovedTrailingMs + additionalRemovedTrailingMs;
    const editedId = suffix
      ? `${originalEventId}-${suffix}`
      : defaultEditedId({
        originalEventId,
        totalRemovedLeadingMs,
        totalRemovedTrailingMs,
        clipDurationMs,
      });

    if (!SAFE_ID_RE.test(editedId)) {
      return NextResponse.json({ error: "Generated event id is invalid" }, { status: 500 });
    }

    const cwd = process.cwd();
    const packageDir = path.join(cwd, "data", "edited-events", originalEventId);
    const metadataDir = path.join(cwd, "data", "metadata");
    const publicVideosDir = path.join(cwd, "public", "videos");
    await Promise.all([
      mkdir(packageDir, { recursive: true }),
      mkdir(metadataDir, { recursive: true }),
      mkdir(publicVideosDir, { recursive: true }),
    ]);

    const editedVideoPath = path.join(packageDir, `${editedId}.mp4`);
    const publicVideoPath = path.join(publicVideosDir, `${editedId}.mp4`);
    const publicVideoUrl = `${request.nextUrl.origin}/videos/${editedId}.mp4`;

    await execFileAsync(
      "ffmpeg",
      [
        "-y",
        "-ss",
        formatSecondsForFfmpeg(startSeconds),
        "-i",
        sourceVideoPath,
        "-t",
        formatSecondsForFfmpeg(endSeconds - startSeconds),
        "-map",
        "0:v:0",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        editedVideoPath,
      ],
      { cwd, timeout: 180_000, maxBuffer: 1024 * 1024 * 8 }
    );

    await writeFile(publicVideoPath, await readFile(editedVideoPath));

    const sourceTelemetryStartMs =
      asNumber(previousEdit.telemetryCutoffTimestampMs) ?? firstTelemetryTimestamp(metadata);
    if (sourceTelemetryStartMs !== null) {
      const clipStartTimestampMs = sourceTelemetryStartMs + clipStartMs;
      const clipEndTimestampMs = sourceTelemetryStartMs + clipEndMs;
      metadata.gnssData = clipTimedPoints(metadata.gnssData, clipStartTimestampMs, clipEndTimestampMs);
      metadata.imuData = clipTimedPoints(metadata.imuData, clipStartTimestampMs, clipEndTimestampMs);
      clipMapFeatures(metadata, clipStartTimestampMs, clipEndTimestampMs, startSeconds, endSeconds);

      const firstGnss = asRecord(Array.isArray(metadata.gnssData) ? metadata.gnssData[0] : null);
      const lat = asNumber(firstGnss?.lat);
      const lon = asNumber(firstGnss?.lon);
      if (lat !== null && lon !== null) {
        event.location = { ...(asRecord(event.location) ?? {}), lat, lon };
      }
    }

    metadata.frameDetections = clipFrameDetections(metadata.frameDetections, clipStartMs, clipEndMs);
    metadata.detectionSegments = clipSegments(metadata.detectionSegments, clipStartMs, clipEndMs);
    metadata.vruLabelsDetected = Array.from(new Set(
      (Array.isArray(metadata.detectionSegments) ? metadata.detectionSegments : [])
        .map((segment) => asString(asRecord(segment)?.label))
        .filter((label): label is string => Boolean(label) && isVruDetectionLabel(label))
    )).sort();
    metadata.id = editedId;
    event.id = editedId;
    event.videoUrl = publicVideoUrl;
    metadata.event = event;
    metadata.exportedAt = new Date().toISOString();
    metadata.videoEdit = {
      ...previousEdit,
      operation: "clip",
      originalEventId,
      sourceEventId: id,
      editedEventId: editedId,
      sourceVideoUrl,
      sourceVideoPath,
      sourceDurationSeconds,
      clipStartMs,
      clipStartSeconds: startSeconds,
      clipEndMs,
      clipEndSeconds: endSeconds,
      clipDurationMs,
      clipDurationSeconds: clipDurationMs / 1000,
      removedLeadingMs: totalRemovedLeadingMs,
      removedLeadingSeconds: totalRemovedLeadingMs / 1000,
      additionalRemovedLeadingMs: clipStartMs,
      additionalRemovedLeadingSeconds: clipStartMs / 1000,
      removedTrailingMs: totalRemovedTrailingMs,
      removedTrailingSeconds: totalRemovedTrailingMs / 1000,
      additionalRemovedTrailingMs,
      additionalRemovedTrailingSeconds: additionalRemovedTrailingMs / 1000,
      originalVideoUrl: asString(previousEdit.originalVideoUrl) ?? sourceVideoUrl,
      previousVideoUrl: sourceVideoUrl,
      trimmedVideoUrl: publicVideoUrl,
      trimmedVideoPath: editedVideoPath,
      publicVideoPath,
      telemetryCutoffTimestampMs:
        sourceTelemetryStartMs !== null ? sourceTelemetryStartMs + clipStartMs : null,
      telemetryEndTimestampMs:
        sourceTelemetryStartMs !== null ? sourceTelemetryStartMs + clipEndMs : null,
      telemetryTimestampsRemainAbsolute: true,
      relativeFrameTimesShiftedMs: -totalRemovedLeadingMs,
      additionalRelativeFrameShiftMs: -clipStartMs,
      originalGnssSampleCount: asNumber(previousEdit.originalGnssSampleCount) ?? sourceGnssSampleCount,
      trimmedGnssSampleCount: Array.isArray(metadata.gnssData) ? metadata.gnssData.length : 0,
      originalImuSampleCount: asNumber(previousEdit.originalImuSampleCount) ?? sourceImuSampleCount,
      trimmedImuSampleCount: Array.isArray(metadata.imuData) ? metadata.imuData.length : 0,
    };

    const metadataText = `${JSON.stringify(metadata, null, 2)}\n`;
    const metadataPath = path.join(metadataDir, `${editedId}.json`);
    const packageMetadataPath = path.join(packageDir, `${editedId}_metadata.json`);
    await Promise.all([
      writeFile(metadataPath, metadataText),
      writeFile(packageMetadataPath, metadataText),
    ]);

    return NextResponse.json({
      id: editedId,
      eventUrl: `/event/${editedId}`,
      videoUrl: publicVideoUrl,
      metadataPath,
      packageMetadataPath,
      videoPath: editedVideoPath,
      publicVideoPath,
      stats: {
        sourceDurationSeconds,
        clipDurationSeconds: clipDurationMs / 1000,
        gnssSamples: Array.isArray(metadata.gnssData) ? metadata.gnssData.length : 0,
        imuSamples: Array.isArray(metadata.imuData) ? metadata.imuData.length : 0,
        frameDetections: Array.isArray(metadata.frameDetections) ? metadata.frameDetections.length : 0,
        detectionSegments: Array.isArray(metadata.detectionSegments) ? metadata.detectionSegments.length : 0,
      },
    }, {
      headers: { "Cache-Control": "no-store" },
    });
  } catch (error) {
    console.error("Clip event error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Failed to clip event" },
      { status: 500 }
    );
  }
}

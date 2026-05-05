import { spawn } from "child_process";
import path from "path";
import { NextRequest, NextResponse } from "next/server";
import { getDb, type DbClient } from "@/lib/db";

const MIN_FIRMWARE_VERSION = [7, 4, 3] as const;
const LATE_FRAME_CLUSTER_WINDOW_SECONDS = 2;
const OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW = 4;
const LATE_FRAME_CLUSTER_RULE = "late_frame_cluster_gte_5_in_2s";

interface ProbeResult {
  video_id: string | null;
  source: string | null;
  firmware_version: string | null;
  bucket: "perfect" | "ok" | "filter_out";
  frame_count: number;
  duration_s: number;
  effective_fps: number;
  gap_pct: number;
  single_gaps: number;
  double_gaps: number;
  triple_plus_gaps: number;
  max_delta_ms: number;
  late_frames: number;
  max_late_frames_per_2s: number;
  late_frame_clusters: number;
  non_monotonic_deltas: number;
  failed_rules: string[];
  probe_status: "ok" | "failed";
  probe_error: string | null;
  deltas_ms: number[];
}

function parseFirmwareVersion(value: unknown): [number, number, number] | null {
  if (typeof value !== "string") return null;
  const match = value.trim().match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) return null;
  return [Number(match[1]), Number(match[2]), Number(match[3])];
}

function isFirmwareEligible(value: unknown): boolean {
  const version = parseFirmwareVersion(value);
  if (!version) return false;
  for (let i = 0; i < MIN_FIRMWARE_VERSION.length; i++) {
    if (version[i] > MIN_FIRMWARE_VERSION[i]) return true;
    if (version[i] < MIN_FIRMWARE_VERSION[i]) return false;
  }
  return true;
}

async function ensureFrameTimingTable(db: DbClient): Promise<void> {
  await db.exec(`
    CREATE TABLE IF NOT EXISTS video_frame_timing_qc (
      video_id TEXT PRIMARY KEY,
      firmware_version TEXT,
      bucket TEXT NOT NULL,
      frame_count INTEGER NOT NULL,
      duration_s REAL NOT NULL,
      effective_fps REAL NOT NULL,
      gap_pct REAL NOT NULL,
      single_gaps INTEGER NOT NULL,
      double_gaps INTEGER NOT NULL,
      triple_plus_gaps INTEGER NOT NULL DEFAULT 0,
      max_delta_ms REAL NOT NULL,
      late_frames INTEGER NOT NULL DEFAULT 0,
      max_late_frames_per_2s INTEGER NOT NULL DEFAULT 0,
      late_frame_clusters INTEGER NOT NULL DEFAULT 0,
      non_monotonic_deltas INTEGER NOT NULL DEFAULT 0,
      failed_rules TEXT NOT NULL DEFAULT '[]',
      probe_status TEXT NOT NULL DEFAULT 'ok',
      probe_error TEXT,
      deltas_json TEXT NOT NULL DEFAULT '[]',
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
  `);
  await db.exec("ALTER TABLE video_frame_timing_qc ADD COLUMN triple_plus_gaps INTEGER NOT NULL DEFAULT 0").catch(() => {});
  await db.exec("ALTER TABLE video_frame_timing_qc ADD COLUMN max_late_frames_per_2s INTEGER NOT NULL DEFAULT 0").catch(() => {});
  await db.exec("ALTER TABLE video_frame_timing_qc ADD COLUMN late_frame_clusters INTEGER NOT NULL DEFAULT 0").catch(() => {});
}

function parseJsonArray(value: unknown): unknown[] {
  if (typeof value !== "string") return [];
  try {
    const parsed = JSON.parse(value);
    return Array.isArray(parsed) ? parsed : [];
  } catch {
    return [];
  }
}

function lateFrameClusterMetrics(deltas: number[]): { maxLateFramesPer2s: number; clusters: number } {
  let elapsedSeconds = 0;
  const lateTimes: number[] = [];
  for (const delta of deltas) {
    elapsedSeconds += Math.max(delta, 0) / 1000;
    if (delta > 50) lateTimes.push(elapsedSeconds);
  }

  let start = 0;
  let maxLateFramesPer2s = 0;
  for (let end = 0; end < lateTimes.length; end++) {
    while (lateTimes[end] - lateTimes[start] > LATE_FRAME_CLUSTER_WINDOW_SECONDS) {
      start += 1;
    }
    maxLateFramesPer2s = Math.max(maxLateFramesPer2s, end - start + 1);
  }

  let clusters = 0;
  let index = 0;
  while (index < lateTimes.length) {
    const windowEnd = lateTimes[index] + LATE_FRAME_CLUSTER_WINDOW_SECONDS;
    let nextIndex = index;
    while (nextIndex < lateTimes.length && lateTimes[nextIndex] <= windowEnd) {
      nextIndex += 1;
    }
    if (nextIndex - index > OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW) {
      clusters += 1;
      index = nextIndex;
    } else {
      index += 1;
    }
  }

  return { maxLateFramesPer2s, clusters };
}

function rowToQc(row: Record<string, unknown>): ProbeResult & { updated_at?: string } {
  const deltas = parseJsonArray(row.deltas_json)
    .map((v) => Number(v))
    .filter((v) => Number.isFinite(v));
  const computedClusterMetrics = lateFrameClusterMetrics(deltas);
  const maxLateFramesPer2s = Math.max(
    Number(row.max_late_frames_per_2s ?? 0),
    computedClusterMetrics.maxLateFramesPer2s
  );
  const lateFrameClusters = Math.max(
    Number(row.late_frame_clusters ?? 0),
    computedClusterMetrics.clusters
  );
  const failedRules = parseJsonArray(row.failed_rules).filter((v): v is string => typeof v === "string");
  let bucket: ProbeResult["bucket"] = row.bucket === "perfect" || row.bucket === "ok" ? row.bucket : "filter_out";
  if (maxLateFramesPer2s > OK_MAX_LATE_FRAMES_PER_CLUSTER_WINDOW) {
    bucket = "filter_out";
    if (!failedRules.includes(LATE_FRAME_CLUSTER_RULE)) {
      failedRules.push(LATE_FRAME_CLUSTER_RULE);
    }
  }

  return {
    video_id: String(row.video_id ?? ""),
    source: null,
    firmware_version: typeof row.firmware_version === "string" ? row.firmware_version : null,
    bucket,
    frame_count: Number(row.frame_count ?? 0),
    duration_s: Number(row.duration_s ?? 0),
    effective_fps: Number(row.effective_fps ?? 0),
    gap_pct: Number(row.gap_pct ?? 0),
    single_gaps: Number(row.single_gaps ?? 0),
    double_gaps: Number(row.double_gaps ?? 0),
    triple_plus_gaps: Number(row.triple_plus_gaps ?? 0),
    max_delta_ms: Number(row.max_delta_ms ?? 0),
    late_frames: Number(row.late_frames ?? 0),
    max_late_frames_per_2s: maxLateFramesPer2s,
    late_frame_clusters: lateFrameClusters,
    non_monotonic_deltas: Number(row.non_monotonic_deltas ?? 0),
    failed_rules: failedRules,
    probe_status: row.probe_status === "failed" ? "failed" : "ok",
    probe_error: typeof row.probe_error === "string" ? row.probe_error : null,
    deltas_ms: deltas,
    updated_at: typeof row.updated_at === "string" ? row.updated_at : undefined,
  };
}

function runProbe(args: string[]): Promise<{ code: number | null; stdout: string; stderr: string }> {
  return new Promise((resolve) => {
    const proc = spawn(process.env.PYTHON ?? "python3", args, {
      cwd: process.cwd(),
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    proc.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    proc.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    proc.on("error", (error) => {
      resolve({ code: 1, stdout, stderr: error.message });
    });
    proc.on("close", (code) => {
      resolve({ code, stdout, stderr });
    });
  });
}

async function saveQc(db: DbClient, videoId: string, qc: ProbeResult): Promise<void> {
  await db.run(
    `INSERT INTO video_frame_timing_qc
     (video_id, firmware_version, bucket, frame_count, duration_s,
        effective_fps, gap_pct, single_gaps, double_gaps, triple_plus_gaps, max_delta_ms,
        late_frames, max_late_frames_per_2s, late_frame_clusters,
        non_monotonic_deltas, failed_rules, probe_status,
        probe_error, deltas_json, created_at, updated_at)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'), datetime('now'))
     ON CONFLICT(video_id) DO UPDATE SET
       firmware_version = excluded.firmware_version,
       bucket = excluded.bucket,
       frame_count = excluded.frame_count,
       duration_s = excluded.duration_s,
       effective_fps = excluded.effective_fps,
       gap_pct = excluded.gap_pct,
       single_gaps = excluded.single_gaps,
       double_gaps = excluded.double_gaps,
       triple_plus_gaps = excluded.triple_plus_gaps,
       max_delta_ms = excluded.max_delta_ms,
       late_frames = excluded.late_frames,
       max_late_frames_per_2s = excluded.max_late_frames_per_2s,
       late_frame_clusters = excluded.late_frame_clusters,
       non_monotonic_deltas = excluded.non_monotonic_deltas,
       failed_rules = excluded.failed_rules,
       probe_status = excluded.probe_status,
       probe_error = excluded.probe_error,
       deltas_json = excluded.deltas_json,
       updated_at = datetime('now')`,
    [
      videoId,
      qc.firmware_version,
      qc.bucket,
      qc.frame_count,
      qc.duration_s,
      qc.effective_fps,
      qc.gap_pct,
      qc.single_gaps,
      qc.double_gaps,
      qc.triple_plus_gaps ?? 0,
      qc.max_delta_ms,
      qc.late_frames,
      qc.max_late_frames_per_2s ?? 0,
      qc.late_frame_clusters ?? 0,
      qc.non_monotonic_deltas,
      JSON.stringify(qc.failed_rules ?? []),
      qc.probe_status,
      qc.probe_error,
      JSON.stringify(qc.deltas_ms ?? []),
    ]
  );
}

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const db = await getDb();
  await ensureFrameTimingTable(db);
  const result = await db.query(
    "SELECT * FROM video_frame_timing_qc WHERE video_id = ?",
    [videoId]
  );
  return NextResponse.json({
    qc: result.rows[0] ? rowToQc(result.rows[0]) : null,
  });
}

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  const body = await request.json().catch(() => ({}));
  const videoUrl = typeof body.videoUrl === "string" ? body.videoUrl : null;
  const firmwareVersion = typeof body.firmwareVersion === "string" ? body.firmwareVersion : null;
  const force = body.force === true;

  if (!videoUrl) {
    return NextResponse.json({ error: "videoUrl is required" }, { status: 400 });
  }

  if (!force && !isFirmwareEligible(firmwareVersion)) {
    return NextResponse.json({
      qc: null,
      eligible: false,
      skippedReason: "firmware_below_7_4_3_or_missing",
    });
  }

  const scriptPath = path.resolve(process.cwd(), "scripts/frame_timing_qc.py");
  const probe = await runProbe([
    scriptPath,
    "probe",
    videoUrl,
    "--json",
    "--video-id",
    videoId,
    ...(firmwareVersion ? ["--firmware-version", firmwareVersion] : []),
  ]);

  let qc: ProbeResult;
  try {
    qc = JSON.parse(probe.stdout.trim()) as ProbeResult;
  } catch {
    return NextResponse.json(
      { error: probe.stderr || "Failed to parse frame timing QC output" },
      { status: 500 }
    );
  }

  const db = await getDb();
  await ensureFrameTimingTable(db);
  await saveQc(db, videoId, qc);

  return NextResponse.json({
    qc,
    eligible: isFirmwareEligible(firmwareVersion),
    forced: force,
    probeExitCode: probe.code,
  });
}

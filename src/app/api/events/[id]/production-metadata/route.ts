import { execFile } from "child_process";
import { readFile } from "fs/promises";
import path from "path";
import { promisify } from "util";
import { NextRequest, NextResponse } from "next/server";

export const runtime = "nodejs";

const execFileAsync = promisify(execFile);
const SAFE_S3_SEGMENT_RE = /^[A-Za-z0-9_-]+$/;

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

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  const { id } = await params;

  if (!/^[a-zA-Z0-9_-]+$/.test(id)) {
    return NextResponse.json({ error: "Invalid event id" }, { status: 400 });
  }

  const cwd = process.cwd();
  const scriptPath = path.join(cwd, "scripts", "export-metadata.py");
  const apiKey =
    normalizeAuthHeader(request.headers.get("Authorization")) ??
    normalizeAuthHeader(process.env.BEEMAPS_API_KEY);

  try {
    const outputPath = path.join(
      cwd,
      "data",
      "metadata",
      ...normalizeProductionPrefix(process.env.PRODUCTION_S3_PREFIX),
      `${id}.json`
    );

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

    const file = await readFile(outputPath);

    return new NextResponse(file, {
      headers: {
        "Cache-Control": "no-store",
        "Content-Disposition": `attachment; filename="event_${id}_metadata.json"`,
        "Content-Type": "application/json; charset=utf-8",
      },
    });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json(
      { error: `Failed to generate production metadata: ${message}` },
      { status: 500 }
    );
  }
}

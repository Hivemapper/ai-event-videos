import { spawn } from "child_process";
import fs from "fs";
import path from "path";
import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { loadCachedTopHitsResponse } from "@/lib/top-hits-store";

export const runtime = "nodejs";

function resolvePythonExecutable(cwd: string) {
  const localVenvPython = path.join(cwd, ".venv", "bin", "python3");
  if (fs.existsSync(localVenvPython)) return localVenvPython;
  return process.env.PYTHON ?? "python3";
}

function logTimestamp() {
  return new Date()
    .toISOString()
    .replace(/[-:]/g, "")
    .replace(/\.\d{3}Z$/, "Z");
}

export async function POST(request: NextRequest) {
  try {
    const cwd = process.cwd();
    const db = await getDb();
    const topHits = await loadCachedTopHitsResponse(db);

    const logDir = path.join(cwd, "data", "pipeline-logs");
    fs.mkdirSync(logDir, { recursive: true });
    const logPath = path.join(logDir, `top-hits-vru-${logTimestamp()}.log`);
    const scriptPath = path.join(cwd, "scripts", "run-top-hits-vru.py");
    const pythonExecutable = resolvePythonExecutable(cwd);
    const logFd = fs.openSync(logPath, "a");

    const child = spawn(
      pythonExecutable,
      [
        scriptPath,
        "--api-url",
        `${request.nextUrl.origin}/api/top-hits`,
      ],
      {
        cwd,
        detached: true,
        stdio: ["ignore", logFd, logFd],
        env: {
          ...process.env,
          PYTHONUNBUFFERED: "1",
          AI_EVENT_VIDEOS_TURSO_HTTP_ONLY: "1",
        },
      }
    );

    child.unref();
    fs.closeSync(logFd);

    return NextResponse.json(
      {
        started: true,
        pid: child.pid ?? null,
        targetCount: topHits.ids.length,
        logPath,
      },
      { status: 201 }
    );
  } catch (error) {
    console.error("Top Hits VRU launch error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : String(error) },
      { status: 500 }
    );
  }
}

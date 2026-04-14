import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const numEvents = body.numEvents ?? 500;
    const period = body.period ?? null;

    const scriptPath = path.resolve(process.cwd(), "scripts/run-triage.py");
    const args = [scriptPath, String(numEvents)];
    if (period) {
      args.push("--period", String(period));
    }

    // Run triage script in background
    const proc = spawn("python3", args, {
      cwd: process.cwd(),
      detached: true,
      stdio: "ignore",
    });
    proc.unref();

    return NextResponse.json({
      ok: true,
      message: `Triage started: ${numEvents} events${period ? ` (period ${period})` : ""}`,
      pid: proc.pid,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: String(error) },
      { status: 500 }
    );
  }
}

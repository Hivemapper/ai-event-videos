import { NextRequest, NextResponse } from "next/server";
import { spawn } from "child_process";
import path from "path";

const SUPPORTED_TRIAGE_PERIODS = new Set([4, 5, 6, 7]);

function normalizeTriagePeriod(value: unknown): number | null {
  if (typeof value !== "number" && typeof value !== "string") return null;
  const period = typeof value === "number" ? value : Number(value);
  return Number.isInteger(period) && SUPPORTED_TRIAGE_PERIODS.has(period) ? period : null;
}

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const numEvents = body.numEvents ?? 500;
    const period = normalizeTriagePeriod(body.period);

    if (!period) {
      return NextResponse.json(
        { ok: false, error: "Choose Period 4, 5, 6, or 7 before running triage." },
        { status: 400 }
      );
    }

    const scriptPath = path.resolve(process.cwd(), "scripts/run-triage.py");
    const args = [scriptPath, String(numEvents), "--period", String(period)];

    // Run triage script in background
    const proc = spawn("python3", args, {
      cwd: process.cwd(),
      detached: true,
      stdio: "ignore",
    });
    proc.unref();

    return NextResponse.json({
      ok: true,
      message: `Triage started: ${numEvents} events (period ${period})`,
      pid: proc.pid,
    });
  } catch (error) {
    return NextResponse.json(
      { ok: false, error: String(error) },
      { status: 500 }
    );
  }
}

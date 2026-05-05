import { execFile, spawn } from "child_process";
import { promises as fs } from "fs";
import path from "path";
import { promisify } from "util";
import {
  normalizeTriagePeriod,
  type TriagePeriod,
} from "@/lib/triage-source-total";

const execFileAsync = promisify(execFile);

export const AUTO_TRIAGE_THRESHOLD = 200;
const AUTO_TRIAGE_RETRY_GRACE_MS = 2 * 60 * 1000;
const AUTO_TRIAGE_MAX_BATCH_SIZE = 5000;

const lastAutoTriageAttemptByPeriod = new Map<TriagePeriod, number>();

export interface TriageProcessInfo {
  pid: number;
  command: string;
}

export interface AutoTriageResult {
  started: boolean;
  threshold: number;
  awaitingCount: number;
  reason?: string;
  period?: TriagePeriod;
  numEvents?: number;
  pid?: number;
  logPath?: string;
  activeProcess?: TriageProcessInfo;
}

function parsePsLine(line: string): TriageProcessInfo | null {
  const match = line.match(/^\s*(\d+)\s+(.+)$/);
  if (!match) return null;
  const pid = Number(match[1]);
  const command = match[2];
  if (!Number.isInteger(pid) || !command.includes("scripts/run-triage.py")) {
    return null;
  }
  return { pid, command };
}

export async function findActiveTriageProcess(): Promise<TriageProcessInfo | null> {
  try {
    const { stdout } = await execFileAsync("ps", ["-ax", "-o", "pid=", "-o", "command="], {
      maxBuffer: 1024 * 1024,
    });
    for (const line of stdout.split("\n")) {
      const processInfo = parsePsLine(line);
      if (processInfo) return processInfo;
    }
  } catch (error) {
    console.warn("Failed to inspect active triage processes:", error);
  }
  return null;
}

function formatLogTimestamp(date: Date): string {
  return date.toISOString().replace(/[-:]/g, "").replace(/\.\d{3}Z$/, "Z");
}

export async function startTriageRun(params: {
  period: TriagePeriod;
  numEvents: number;
  reason: string;
}): Promise<{ pid: number | undefined; logPath: string; numEvents: number }> {
  const projectRoot = process.cwd();
  const logsDir = path.join(projectRoot, "logs");
  await fs.mkdir(logsDir, { recursive: true });

  const numEvents = Math.max(1, Math.min(Math.ceil(params.numEvents), AUTO_TRIAGE_MAX_BATCH_SIZE));
  const logPath = path.join(
    logsDir,
    `auto-triage-period${params.period}-${formatLogTimestamp(new Date())}.log`
  );
  const logFile = await fs.open(logPath, "a");

  try {
    const scriptPath = path.join(projectRoot, "scripts", "run-triage.py");
    const args = ["-u", scriptPath, String(numEvents), "--period", params.period];
    const child = spawn(process.env.PYTHON ?? "python3", args, {
      cwd: projectRoot,
      detached: true,
      env: {
        ...process.env,
        TRIAGE_FETCH_CONCURRENCY: process.env.TRIAGE_FETCH_CONCURRENCY ?? "4",
        TRIAGE_PROCESS_WORKERS: process.env.TRIAGE_PROCESS_WORKERS ?? "2",
        AUTO_TRIAGE_REASON: params.reason,
      },
      stdio: ["ignore", logFile.fd, logFile.fd],
    });
    child.unref();
    return { pid: child.pid, logPath, numEvents };
  } finally {
    await logFile.close().catch(() => undefined);
  }
}

export async function maybeStartAutoTriage(params: {
  period: string | null;
  awaitingCount: number | null | undefined;
  threshold?: number;
}): Promise<AutoTriageResult | null> {
  if (typeof params.awaitingCount !== "number") return null;

  const threshold = params.threshold ?? AUTO_TRIAGE_THRESHOLD;
  const baseResult = {
    started: false,
    threshold,
    awaitingCount: params.awaitingCount,
  };

  if (params.awaitingCount <= threshold) {
    return { ...baseResult, reason: "below_threshold" };
  }

  const period = normalizeTriagePeriod(params.period);
  if (!period) {
    return { ...baseResult, reason: "supported_period_required" };
  }

  const activeProcess = await findActiveTriageProcess();
  if (activeProcess) {
    return { ...baseResult, period, reason: "triage_already_running", activeProcess };
  }

  const now = Date.now();
  const lastAttemptAt = lastAutoTriageAttemptByPeriod.get(period);
  if (lastAttemptAt && now - lastAttemptAt < AUTO_TRIAGE_RETRY_GRACE_MS) {
    return { ...baseResult, period, reason: "recently_attempted" };
  }
  lastAutoTriageAttemptByPeriod.set(period, now);

  const run = await startTriageRun({
    period,
    numEvents: params.awaitingCount,
    reason: `auto-awaiting-over-${threshold}`,
  });

  return {
    ...baseResult,
    started: true,
    period,
    numEvents: run.numEvents,
    pid: run.pid,
    logPath: run.logPath,
  };
}

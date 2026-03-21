import { spawn } from "child_process";
import path from "path";
import fs from "fs";

function resolvePythonExecutable(cwd: string) {
  const localVenvPython = path.join(cwd, ".venv", "bin", "python3");
  if (fs.existsSync(localVenvPython)) {
    return localVenvPython;
  }

  const activeVenv = process.env.VIRTUAL_ENV;
  if (activeVenv) {
    const activeVenvPython = path.join(activeVenv, "bin", "python3");
    if (fs.existsSync(activeVenvPython)) {
      return activeVenvPython;
    }
  }

  return "python3";
}

function getWorkerPaths(runId: string) {
  const cwd = process.cwd();
  const scriptPath = path.join(cwd, "scripts", "run_detection.py");
  const logDir = path.join(cwd, "data", "pipeline-logs");
  fs.mkdirSync(logDir, { recursive: true });
  const logPath = path.join(logDir, `detection-${runId}.log`);
  return { cwd, scriptPath, logDir, logPath };
}

export function spawnDetectionWorker(params: { runId: string }) {
  const { cwd, scriptPath, logPath } = getWorkerPaths(params.runId);
  const pythonExecutable = resolvePythonExecutable(cwd);
  const logFd = fs.openSync(logPath, "a");

  const child = spawn(
    pythonExecutable,
    [scriptPath, "--run-id", params.runId],
    {
      cwd,
      detached: true,
      stdio: ["ignore", logFd, logFd],
      env: {
        ...process.env,
        PYTHONUNBUFFERED: "1",
      },
    }
  );

  child.unref();
  fs.closeSync(logFd);
  return { pid: child.pid ?? null, logPath };
}

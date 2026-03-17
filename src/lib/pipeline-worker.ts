import { spawn } from "child_process";
import path from "path";
import fs from "fs";
import {
  CURRENT_PIPELINE_VERSION,
  DEFAULT_PIPELINE_MODEL_NAME,
} from "@/lib/pipeline-config";

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

function getWorkerPaths() {
  const cwd = process.cwd();
  const scriptPath = path.join(cwd, "scripts", "vru_pipeline_worker.py");
  const logDir = path.join(cwd, "data", "pipeline-logs");
  fs.mkdirSync(logDir, { recursive: true });
  return { cwd, scriptPath, logDir };
}

export function spawnPipelineWorker(params: {
  runId: string;
  beeMapsKey: string;
  day: string;
  batchSize: number;
  modelName?: string | null;
}) {
  const { cwd, scriptPath, logDir } = getWorkerPaths();
  const pythonExecutable = resolvePythonExecutable(cwd);
  const logPath = path.join(logDir, `${params.runId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const child = spawn(
    pythonExecutable,
    [
      scriptPath,
      "--run-id",
      params.runId,
      "--day",
      params.day,
      "--batch-size",
      String(params.batchSize),
      "--db-path",
      path.join(cwd, "data", "labels.db"),
      "--pipeline-version",
      CURRENT_PIPELINE_VERSION,
      "--model-name",
      params.modelName ?? DEFAULT_PIPELINE_MODEL_NAME,
    ],
    {
      cwd,
      detached: true,
      stdio: ["ignore", logFd, logFd],
      env: {
        ...process.env,
        BEEMAPS_API_KEY: params.beeMapsKey,
        PYTHONUNBUFFERED: "1",
      },
    }
  );

  child.unref();
  fs.closeSync(logFd);
  return { pid: child.pid ?? null, logPath };
}

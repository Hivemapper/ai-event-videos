# Detection Runs Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add per-video, per-model detection runs with status tracking, history, and UI-triggered execution.

**Architecture:** New `detection_runs` table tracks each model execution on a video (queued→running→completed/failed). A unified Python runner script accepts `--run-id` and reads config from the DB. The API spawns the Python process; the UI shows a "Run" button with model selector and live status polling.

**Tech Stack:** @libsql/client (Turso), Next.js API routes, Python (GDINO/YOLO), SWR polling, shadcn/ui components.

---

## Data Model

### New table: `detection_runs`

```sql
CREATE TABLE IF NOT EXISTS detection_runs (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  config_json TEXT NOT NULL DEFAULT '{}',
  detection_count INTEGER,
  worker_pid INTEGER,
  started_at TEXT,
  completed_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_detection_runs_video
  ON detection_runs (video_id, created_at DESC);
```

**Status values:** `queued` | `running` | `completed` | `failed`

**config_json example:**
```json
{
  "boxThreshold": 0.30,
  "textThreshold": 0.25,
  "framesPerVideo": 75,
  "clipVerification": true
}
```

### Modify `frame_detections`: add `run_id` column

```sql
ALTER TABLE frame_detections ADD COLUMN run_id TEXT;
```

`run_id` is nullable for backwards compatibility with existing detections (which have no run). New detections created by detection runs will always have a `run_id`.

---

## Supported Models (hardcoded list)

```typescript
export const AVAILABLE_DETECTION_MODELS = [
  { id: "gdino-base-clip", name: "GDINO Base + CLIP", script: "run_detection.py" },
  { id: "yolo-world", name: "YOLO-World v2", script: "run_detection.py" },
  { id: "yolo11x", name: "YOLO11x (COCO-80)", script: "run_detection.py" },
] as const;
```

---

## Tasks

### Task 1: Database schema — add `detection_runs` table and `run_id` column

**Files:**
- Modify: `src/lib/db.ts` — add CREATE TABLE for `detection_runs`, add `run_id` column migration to `frame_detections`

**Step 1: Add table and migration to `initSchema()` in db.ts**

In the `executeMultiple` block (after the `frame_detections` table), add:

```sql
CREATE TABLE IF NOT EXISTS detection_runs (
  id TEXT PRIMARY KEY,
  video_id TEXT NOT NULL,
  model_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'queued',
  config_json TEXT NOT NULL DEFAULT '{}',
  detection_count INTEGER,
  worker_pid INTEGER,
  started_at TEXT,
  completed_at TEXT,
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_detection_runs_video
  ON detection_runs (video_id, created_at DESC);
```

After the existing `ensureColumn` calls, add:

```typescript
await ensureColumn("frame_detections", "run_id", "TEXT");
```

**Step 2: Run `npx tsc --noEmit` to verify**

**Step 3: Commit**

```
feat: add detection_runs table and run_id column
```

---

### Task 2: Types — add DetectionRun type and model constants

**Files:**
- Modify: `src/types/pipeline.ts` — add `DetectionRun` interface and `DetectionRunStatus` type
- Modify: `src/lib/pipeline-config.ts` — add `AVAILABLE_DETECTION_MODELS` constant

**Step 1: Add types to `src/types/pipeline.ts`**

```typescript
export type DetectionRunStatus = "queued" | "running" | "completed" | "failed";

export interface DetectionRun {
  id: string;
  videoId: string;
  modelName: string;
  status: DetectionRunStatus;
  config: Record<string, unknown>;
  detectionCount: number | null;
  workerPid: number | null;
  startedAt: string | null;
  completedAt: string | null;
  lastError: string | null;
  createdAt: string;
}
```

**Step 2: Add model config to `src/lib/pipeline-config.ts`**

```typescript
export interface DetectionModelConfig {
  id: string;
  name: string;
}

export const AVAILABLE_DETECTION_MODELS: DetectionModelConfig[] = [
  { id: "gdino-base-clip", name: "GDINO Base + CLIP" },
  { id: "yolo-world", name: "YOLO-World v2" },
  { id: "yolo11x", name: "YOLO11x (COCO-80)" },
];
```

**Step 3: Run `npx tsc --noEmit`**

**Step 4: Commit**

```
feat: add DetectionRun types and available model config
```

---

### Task 3: Store functions — CRUD for detection runs

**Files:**
- Modify: `src/lib/pipeline-store.ts` — add detection run functions

**Step 1: Add these async functions to pipeline-store.ts**

```typescript
// --- Detection Runs ---

export async function createDetectionRun(params: {
  videoId: string;
  modelName: string;
  config?: Record<string, unknown>;
}): Promise<DetectionRun> {
  const db = await getDb();
  const id = randomUUID();
  await db.execute({
    sql: `INSERT INTO detection_runs (id, video_id, model_name, status, config_json)
          VALUES (?, ?, ?, 'queued', ?)`,
    args: [id, params.videoId, params.modelName, JSON.stringify(params.config ?? {})],
  });
  return (await getDetectionRun(id))!;
}

export async function getDetectionRun(id: string): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.execute({
    sql: "SELECT * FROM detection_runs WHERE id = ?",
    args: [id],
  });
  return result.rows.length > 0 ? mapDetectionRun(result.rows[0]) : null;
}

export async function listDetectionRuns(videoId: string): Promise<DetectionRun[]> {
  const db = await getDb();
  const result = await db.execute({
    sql: `SELECT * FROM detection_runs WHERE video_id = ? ORDER BY created_at DESC`,
    args: [videoId],
  });
  return result.rows.map(mapDetectionRun);
}

export async function getActiveDetectionRun(): Promise<DetectionRun | null> {
  const db = await getDb();
  const result = await db.execute(
    `SELECT * FROM detection_runs WHERE status IN ('queued', 'running') ORDER BY created_at DESC LIMIT 1`
  );
  return result.rows.length > 0 ? mapDetectionRun(result.rows[0]) : null;
}

export async function updateDetectionRunStatus(
  id: string,
  status: DetectionRunStatus,
  extra?: { detectionCount?: number; lastError?: string }
) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE detection_runs
          SET status = ?,
              detection_count = COALESCE(?, detection_count),
              last_error = COALESCE(?, last_error),
              started_at = CASE WHEN ? = 'running' AND started_at IS NULL THEN datetime('now') ELSE started_at END,
              completed_at = CASE WHEN ? IN ('completed', 'failed') THEN datetime('now') ELSE completed_at END
          WHERE id = ?`,
    args: [status, extra?.detectionCount ?? null, extra?.lastError ?? null, status, status, id],
  });
}

export async function setDetectionRunWorkerPid(id: string, pid: number | null) {
  const db = await getDb();
  await db.execute({
    sql: `UPDATE detection_runs SET worker_pid = ? WHERE id = ?`,
    args: [pid, id],
  });
}
```

**Step 2: Add the mapper function (near the other mappers)**

```typescript
function mapDetectionRun(row: Row): DetectionRun {
  return {
    id: str(row.id),
    videoId: str(row.video_id),
    modelName: str(row.model_name),
    status: str(row.status) as DetectionRunStatus,
    config: parseJson<Record<string, unknown>>(strOrNull(row.config_json), {}),
    detectionCount: numOrNull(row.detection_count),
    workerPid: numOrNull(row.worker_pid),
    startedAt: strOrNull(row.started_at),
    completedAt: strOrNull(row.completed_at),
    lastError: strOrNull(row.last_error),
    createdAt: str(row.created_at),
  };
}
```

**Step 3: Add imports at top of file**

Add `DetectionRun`, `DetectionRunStatus` to the import from `@/types/pipeline`.

**Step 4: Run `npx tsc --noEmit`**

**Step 5: Commit**

```
feat: add detection run CRUD functions to pipeline-store
```

---

### Task 4: Unified Python detection runner script

**Files:**
- Create: `scripts/run_detection.py`

This script replaces the ad-hoc `run_gdino_clip_pipeline.py` and `run_yolo_world_single.py` for UI-triggered runs. It:

1. Accepts `--run-id` as the only required arg
2. Reads run config (video_id, model_name, config_json) from the DB
3. Fetches the event from Bee Maps API to get the video URL
4. Downloads video, extracts frames
5. Runs the selected model (gdino-base-clip, yolo-world, or yolo11x)
6. Saves detections to `frame_detections` with `run_id`
7. Updates run status throughout (queued→running→completed/failed)

**Step 1: Create `scripts/run_detection.py`**

The script should:

- Parse `--run-id` arg
- Connect to DB (Turso or local, same pattern as existing scripts using `_load_env_var`)
- Read the detection_runs row to get video_id, model_name, config_json
- Update status to 'running'
- Fetch event from Bee Maps API (`GET /aievents/{video_id}`)
- Download video (reuse `download_video()` pattern)
- Extract frames (reuse `extract_frames()` pattern, default 75 frames)
- Branch on model_name:
  - `gdino-base-clip`: load GDINO-base, run inference, NMS, then CLIP verification (reuse from run_gdino_clip_pipeline.py)
  - `yolo-world`: load YOLO-World, run inference (reuse from run_yolo_world_single.py)
  - `yolo11x`: load YOLO11x, run inference (standard ultralytics YOLO)
- Delete old detections for this run_id (if re-running)
- Save detections to `frame_detections` with run_id
- Update run status to 'completed' with detection_count
- On error: update run status to 'failed' with last_error

**Step 2: Test manually**

```bash
# Create a run manually in DB, then:
source .venv/bin/activate
python scripts/run_detection.py --run-id <test-run-id>
```

**Step 3: Commit**

```
feat: add unified detection runner script
```

---

### Task 5: Detection worker spawner

**Files:**
- Create: `src/lib/detection-worker.ts`

This mirrors `pipeline-worker.ts` but spawns `run_detection.py` with `--run-id`.

```typescript
import { spawn } from "child_process";
import path from "path";
import fs from "fs";

function resolvePythonExecutable(cwd: string) {
  const localVenvPython = path.join(cwd, ".venv", "bin", "python3");
  if (fs.existsSync(localVenvPython)) return localVenvPython;
  const activeVenv = process.env.VIRTUAL_ENV;
  if (activeVenv) {
    const p = path.join(activeVenv, "bin", "python3");
    if (fs.existsSync(p)) return p;
  }
  return "python3";
}

export function spawnDetectionWorker(params: { runId: string }) {
  const cwd = process.cwd();
  const scriptPath = path.join(cwd, "scripts", "run_detection.py");
  const logDir = path.join(cwd, "data", "pipeline-logs");
  fs.mkdirSync(logDir, { recursive: true });
  const logPath = path.join(logDir, `detection-${params.runId}.log`);
  const logFd = fs.openSync(logPath, "a");

  const child = spawn(pythonExecutable, [scriptPath, "--run-id", params.runId], {
    cwd,
    detached: true,
    stdio: ["ignore", logFd, logFd],
    env: { ...process.env, PYTHONUNBUFFERED: "1" },
  });

  child.unref();
  fs.closeSync(logFd);
  return { pid: child.pid ?? null, logPath };
}
```

Note: use `const pythonExecutable = resolvePythonExecutable(cwd);` before the spawn call.

**Step 1: Create the file**

**Step 2: Run `npx tsc --noEmit`**

**Step 3: Commit**

```
feat: add detection worker spawner
```

---

### Task 6: API route — create and list detection runs

**Files:**
- Create: `src/app/api/videos/[videoId]/runs/route.ts`

**Step 1: Create the route**

```typescript
// GET — list runs for this video
// POST — create a new run (triggers worker)
```

**GET handler:**
- Call `listDetectionRuns(videoId)`
- Return `{ runs: DetectionRun[] }`

**POST handler:**
- Parse body: `{ modelName: string }`
- Validate modelName is in `AVAILABLE_DETECTION_MODELS`
- Check `getActiveDetectionRun()` — reject if one exists (409)
- Call `createDetectionRun({ videoId, modelName })`
- Call `spawnDetectionWorker({ runId: run.id })`
- Call `setDetectionRunWorkerPid(run.id, pid)`
- Return `{ run }` with status 201

**Step 2: Run `npx tsc --noEmit`**

**Step 3: Commit**

```
feat: add detection runs API route (create + list)
```

---

### Task 7: API route — get single detection run status

**Files:**
- Create: `src/app/api/videos/[videoId]/runs/[runId]/route.ts`

**Step 1: Create the route**

**GET handler:**
- Call `getDetectionRun(runId)`
- Return 404 if not found
- Return `{ run: DetectionRun }`

**Step 2: Run `npx tsc --noEmit`**

**Step 3: Commit**

```
feat: add single detection run status API route
```

---

### Task 8: Frontend hook — useDetectionRuns

**Files:**
- Create: `src/hooks/use-detection-runs.ts`

**Step 1: Create the hook**

```typescript
import useSWR from "swr";
import type { DetectionRun } from "@/types/pipeline";

const fetcher = (url: string) => fetch(url).then((r) => r.json());

export function useDetectionRuns(videoId: string | null) {
  const { data, error, isLoading, mutate } = useSWR<{ runs: DetectionRun[] }>(
    videoId ? `/api/videos/${videoId}/runs` : null,
    fetcher
  );

  // Find active run (queued or running)
  const activeRun = data?.runs?.find((r) => r.status === "queued" || r.status === "running") ?? null;

  return {
    runs: data?.runs ?? [],
    activeRun,
    isLoading,
    error,
    mutate,
  };
}
```

**Step 2: Run `npx tsc --noEmit`**

**Step 3: Commit**

```
feat: add useDetectionRuns hook
```

---

### Task 9: UI — Run Detection button and status in VRU panel

**Files:**
- Modify: `src/components/events/video-vru-panel.tsx`
- Modify: `src/app/event/[id]/page.tsx`

**Step 1: Update VideoVruPanel props**

Add to `VideoVruPanelProps`:

```typescript
activeDetectionRun?: DetectionRun | null;
detectionRuns?: DetectionRun[];
onRunDetection?: (modelName: string) => void;
```

**Step 2: Add "Run Detection" UI section**

Below the model selector and confidence slider, add:

- If `activeDetectionRun` exists and is `queued` or `running`:
  - Show a status indicator: "Running {modelName}..." with a spinner
  - Show elapsed time since `startedAt`
- Else:
  - Show a model selector dropdown (from `AVAILABLE_DETECTION_MODELS`)
  - Show a "Run Detection" button
  - When clicked, call `onRunDetection(selectedModelForRun)`

Also show a small "Run History" section below showing recent completed/failed runs with model name, detection count, and timestamp.

**Step 3: Wire up in page.tsx**

In the event detail page:

```typescript
const { runs: detectionRuns, activeRun: activeDetectionRun, mutate: mutateRuns } = useDetectionRuns(id);

// Poll for active run updates
useSWR(
  activeDetectionRun ? `/api/videos/${id}/runs/${activeDetectionRun.id}` : null,
  fetcher,
  { refreshInterval: 2000, onSuccess: () => mutateRuns() }
);

const handleRunDetection = async (modelName: string) => {
  await fetch(`/api/videos/${id}/runs`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ modelName }),
  });
  mutateRuns();
};
```

Pass `activeDetectionRun`, `detectionRuns`, and `onRunDetection={handleRunDetection}` to `VideoVruPanel`.

When a run completes (status changes from running→completed), also mutate the detections SWR to refresh bboxes.

**Step 4: Run `npx tsc --noEmit` and `npx eslint` on changed files**

**Step 5: Commit**

```
feat: add Run Detection button and status UI
```

---

### Task 10: Auto-refresh detections when run completes

**Files:**
- Modify: `src/app/event/[id]/page.tsx`

**Step 1: Add effect to refresh detections on run completion**

When polling detects that `activeDetectionRun` transitions to `completed`:

```typescript
const prevActiveRunRef = useRef<string | null>(null);

useEffect(() => {
  const prevId = prevActiveRunRef.current;
  const currentId = activeDetectionRun?.id ?? null;

  // If we had an active run and now we don't, a run just completed
  if (prevId && !currentId) {
    // Refresh detections data
    mutateDetections();
  }

  prevActiveRunRef.current = currentId;
}, [activeDetectionRun?.id]);
```

Where `mutateDetections` is the `mutate` from the `useDetectionTimestamps` hook (needs to be exposed/returned).

**Step 2: Run `npx tsc --noEmit`**

**Step 3: Commit**

```
feat: auto-refresh detections when detection run completes
```

---

### Task 11: Final verification and cleanup

**Step 1: Run full type check**

```bash
npx tsc --noEmit
```

**Step 2: Run lint on all changed files**

```bash
npx eslint src/lib/db.ts src/lib/pipeline-store.ts src/lib/pipeline-config.ts \
  src/lib/detection-worker.ts src/types/pipeline.ts \
  src/app/api/videos/\[videoId\]/runs/route.ts \
  src/app/api/videos/\[videoId\]/runs/\[runId\]/route.ts \
  src/hooks/use-detection-runs.ts \
  src/components/events/video-vru-panel.tsx \
  src/app/event/\[id\]/page.tsx
```

**Step 3: Manual test**

1. Open an event page in browser
2. Select "GDINO Base + CLIP" from the run model dropdown
3. Click "Run Detection"
4. Verify status shows "Running..."
5. Wait for completion (~1-2 min)
6. Verify detections appear on the video overlay
7. Verify run appears in history

**Step 4: Commit**

```
feat: detection runs — final cleanup
```

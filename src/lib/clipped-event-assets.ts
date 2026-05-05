import { execFile } from "child_process";
import { readFile, stat } from "fs/promises";
import os from "os";
import path from "path";
import { promisify } from "util";

const execFileAsync = promisify(execFile);

const LOCAL_EDITED_EVENT_ID_RE = /^[A-Za-z0-9]+-[A-Za-z0-9_-]+$/;
const SAFE_HOST_RE = /^[A-Za-z0-9._-]+$/;
const SAFE_REMOTE_PATH_RE = /^[A-Za-z0-9._~/$-]+$/;

type JsonRecord = Record<string, unknown>;

type AssetSource = {
  localPath: string;
  remoteDir: string;
};

export type ClippedEventAssetSyncResult = {
  attempted: boolean;
  skippedReason?: string;
  target?: "detection" | "production";
  videoId: string;
  originalEventId?: string;
  hosts: string[];
  files: string[];
};

function asRecord(value: unknown): JsonRecord | null {
  return value && typeof value === "object" && !Array.isArray(value)
    ? (value as JsonRecord)
    : null;
}

function asString(value: unknown): string | null {
  return typeof value === "string" ? value : null;
}

async function pathExists(filePath: string): Promise<boolean> {
  try {
    await stat(filePath);
    return true;
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return false;
    throw error;
  }
}

function expandHome(filePath: string): string {
  if (filePath === "~") return os.homedir();
  if (filePath.startsWith("~/")) return path.join(os.homedir(), filePath.slice(2));
  return filePath;
}

function shellQuote(value: string): string {
  return `'${value.replace(/'/g, "'\\''")}'`;
}

function splitHosts(rawHosts: string | undefined): string[] {
  if (!rawHosts) return [];
  return Array.from(
    new Set(
      rawHosts
        .split(/[,\s]+/)
        .map((host) => host.trim())
        .filter(Boolean)
        .filter((host) => SAFE_HOST_RE.test(host))
    )
  );
}

function hostsFromEnv(names: string[]): string[] {
  return splitHosts(
    names
      .map((name) => process.env[name])
      .filter((value): value is string => Boolean(value))
      .join(",")
  );
}

function detectorHostsFromEnv(): string[] {
  return hostsFromEnv([
    "DETECTION_AWS_ASSET_HOSTS",
    "DETECTION_AWS_HOSTS",
    "DETECTION_AWS_HOST",
  ]);
}

function productionHostsFromEnv(): string[] {
  return hostsFromEnv([
    "PRODUCTION_AWS_ASSET_HOSTS",
    "PRODUCTION_AWS_HOSTS",
    "PRODUCTION_AWS_HOST",
  ]);
}

async function discoverAwsDetectorHosts(): Promise<string[]> {
  if (process.env.DETECTION_AWS_DISCOVER_HOSTS === "0") return [];

  try {
    const { stdout } = await execFileAsync(
      "aws",
      [
        "ec2",
        "describe-instances",
        "--filters",
        "Name=instance-state-name,Values=running",
        "Name=tag:Name,Values=*vru*,*detect*",
        "--query",
        "Reservations[].Instances[].PublicIpAddress",
        "--output",
        "json",
      ],
      { timeout: 15_000, maxBuffer: 1024 * 1024 }
    );
    const parsed = JSON.parse(stdout) as unknown;
    if (!Array.isArray(parsed)) return [];
    return splitHosts(parsed.filter((value): value is string => typeof value === "string").join(","));
  } catch {
    return [];
  }
}

async function discoverAwsProductionHosts(): Promise<string[]> {
  if (process.env.PRODUCTION_AWS_DISCOVER_HOSTS === "0") return [];

  try {
    const { stdout } = await execFileAsync(
      "aws",
      [
        "ec2",
        "describe-instances",
        "--filters",
        "Name=instance-state-name,Values=running",
        "Name=tag:fleet,Values=prod-pipeline-fleet",
        "--query",
        "Reservations[].Instances[].PublicIpAddress",
        "--output",
        "json",
      ],
      { timeout: 15_000, maxBuffer: 1024 * 1024 }
    );
    const parsed = JSON.parse(stdout) as unknown;
    if (!Array.isArray(parsed)) return [];
    return splitHosts(parsed.filter((value): value is string => typeof value === "string").join(","));
  } catch {
    return [];
  }
}

async function resolveAssets(videoId: string): Promise<{
  originalEventId: string;
  sources: AssetSource[];
} | null> {
  if (!LOCAL_EDITED_EVENT_ID_RE.test(videoId)) return null;

  const cwd = process.cwd();
  const metadataPath = path.join(cwd, "data", "metadata", `${videoId}.json`);
  if (!(await pathExists(metadataPath))) return null;

  const metadata = JSON.parse(await readFile(metadataPath, "utf8")) as JsonRecord;
  const videoEdit = asRecord(metadata.videoEdit);
  const originalEventId =
    asString(videoEdit?.originalEventId) ?? videoId.slice(0, videoId.indexOf("-"));
  if (!originalEventId) return null;

  const candidates: AssetSource[] = [
    { localPath: metadataPath, remoteDir: "data/metadata" },
    {
      localPath: path.join(cwd, "data", "metadata", `${originalEventId}.json`),
      remoteDir: "data/metadata",
    },
    {
      localPath: path.join(cwd, "public", "videos", `${videoId}.mp4`),
      remoteDir: "public/videos",
    },
    {
      localPath: path.join(cwd, "data", "edited-events", originalEventId),
      remoteDir: "data/edited-events",
    },
  ];

  const sources: AssetSource[] = [];
  for (const candidate of candidates) {
    if (await pathExists(candidate.localPath)) sources.push(candidate);
  }

  return { originalEventId, sources };
}

async function syncToHost(
  host: string,
  sources: AssetSource[],
  target: "detection" | "production"
): Promise<void> {
  const envPrefix = target === "production" ? "PRODUCTION" : "DETECTION";
  const user =
    process.env[`${envPrefix}_AWS_SSH_USER`]?.trim() ||
    process.env.DETECTION_AWS_SSH_USER?.trim() ||
    "ec2-user";
  const keyPath = expandHome(
    process.env[`${envPrefix}_AWS_SSH_KEY`]?.trim() ||
      process.env.DETECTION_AWS_SSH_KEY?.trim() ||
      "~/Downloads/vru.pem"
  );
  const remoteProjectDir =
    process.env[`${envPrefix}_AWS_PROJECT_DIR`]?.trim() ||
    process.env.DETECTION_AWS_PROJECT_DIR?.trim() ||
    `/home/${user}/ai-event-videos`;

  if (!SAFE_REMOTE_PATH_RE.test(remoteProjectDir)) {
    throw new Error(`Unsafe ${envPrefix}_AWS_PROJECT_DIR: ${remoteProjectDir}`);
  }

  const remoteDirs = Array.from(
    new Set(sources.map((source) => source.remoteDir))
  );
  await execFileAsync(
    "ssh",
    [
      "-i",
      keyPath,
      "-o",
      "StrictHostKeyChecking=no",
      `${user}@${host}`,
      `mkdir -p ${remoteDirs
        .map((remoteDir) => shellQuote(`${remoteProjectDir}/${remoteDir}`))
        .join(" ")}`,
    ],
    { timeout: 15_000, maxBuffer: 1024 * 1024 }
  );

  const sshCommand = `ssh -i ${shellQuote(keyPath)} -o StrictHostKeyChecking=no`;
  for (const source of sources) {
    await execFileAsync(
      "rsync",
      [
        "-az",
        "-e",
        sshCommand,
        source.localPath,
        `${user}@${host}:${remoteProjectDir}/${source.remoteDir}/`,
      ],
      { timeout: 120_000, maxBuffer: 1024 * 1024 * 4 }
    );
  }
}

export async function syncClippedEventAssetsForAws(
  videoId: string
): Promise<ClippedEventAssetSyncResult> {
  if (process.env.DETECTION_AWS_SYNC_CLIPPED_ASSETS === "0") {
    return {
      attempted: false,
      skippedReason: "disabled",
      target: "detection",
      videoId,
      hosts: [],
      files: [],
    };
  }

  const assets = await resolveAssets(videoId);
  if (!assets) {
    return {
      attempted: false,
      skippedReason: "not-local-edited-clip",
      target: "detection",
      videoId,
      hosts: [],
      files: [],
    };
  }

  const hosts = detectorHostsFromEnv();
  const discoveredHosts =
    hosts.length > 0 ? [] : await discoverAwsDetectorHosts();
  const targetHosts = hosts.length > 0 ? hosts : discoveredHosts;
  if (targetHosts.length === 0) {
    return {
      attempted: false,
      skippedReason: "no-detector-hosts",
      target: "detection",
      videoId,
      originalEventId: assets.originalEventId,
      hosts: [],
      files: assets.sources.map((source) =>
        path.relative(process.cwd(), source.localPath)
      ),
    };
  }

  await Promise.all(
    targetHosts.map((host) => syncToHost(host, assets.sources, "detection"))
  );

  return {
    attempted: true,
    target: "detection",
    videoId,
    originalEventId: assets.originalEventId,
    hosts: targetHosts,
    files: assets.sources.map((source) =>
      path.relative(process.cwd(), source.localPath)
    ),
  };
}

export async function syncClippedEventAssetsForProductionAws(
  videoId: string
): Promise<ClippedEventAssetSyncResult> {
  if (process.env.PRODUCTION_AWS_SYNC_CLIPPED_ASSETS === "0") {
    return {
      attempted: false,
      skippedReason: "disabled",
      target: "production",
      videoId,
      hosts: [],
      files: [],
    };
  }

  const assets = await resolveAssets(videoId);
  if (!assets) {
    return {
      attempted: false,
      skippedReason: "not-local-edited-clip",
      target: "production",
      videoId,
      hosts: [],
      files: [],
    };
  }

  const hosts = productionHostsFromEnv();
  const discoveredHosts =
    hosts.length > 0 ? [] : await discoverAwsProductionHosts();
  const targetHosts = hosts.length > 0 ? hosts : discoveredHosts;
  if (targetHosts.length === 0) {
    return {
      attempted: false,
      skippedReason: "no-production-hosts",
      target: "production",
      videoId,
      originalEventId: assets.originalEventId,
      hosts: [],
      files: assets.sources.map((source) =>
        path.relative(process.cwd(), source.localPath)
      ),
    };
  }

  await Promise.all(
    targetHosts.map((host) => syncToHost(host, assets.sources, "production"))
  );

  return {
    attempted: true,
    target: "production",
    videoId,
    originalEventId: assets.originalEventId,
    hosts: targetHosts,
    files: assets.sources.map((source) =>
      path.relative(process.cwd(), source.localPath)
    ),
  };
}

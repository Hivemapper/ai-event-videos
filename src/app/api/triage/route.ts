import { promises as fs } from "fs";
import path from "path";
import { gunzipSync } from "zlib";
import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

const MIN_STANDARD_VIDEO_DURATION_SEC = 28;
const MIN_NON_LINEAR_BITRATE_BPS = 3_300_000;
const MIN_PERIOD_7_FIRMWARE_NUM = 7_004_003;
const NON_LINEAR_SHORT_RULE = "short_video_lt_28s";
const NON_LINEAR_BITRATE_RULE = "bitrate_lt_3_3mbps";
const EVENT_TOTAL_CACHE_TTL_MS = 5 * 60 * 1000;
const MAX_EVENT_TOTAL_RANGE_MS = 31 * 24 * 60 * 60 * 1000;

const PERIODS: Record<string, [string, string]> = {
  "1": ["2025-01-01T00:00:00.000Z", "2025-09-15T00:00:00.000Z"],
  "2": ["2025-09-15T00:00:00.000Z", "2026-01-20T00:00:00.000Z"],
  "3": ["2026-01-20T00:00:00.000Z", "2026-02-25T00:00:00.000Z"],
  "4": ["2026-02-25T00:00:00.000Z", "2026-03-15T00:00:00.000Z"],
  "5": ["2026-03-15T00:00:00.000Z", "2026-04-17T00:00:00.000Z"],
  "6": ["2026-04-17T00:00:00.000Z", "2026-04-22T20:45:00.500Z"],
  "7": ["2026-04-22T20:45:00.500Z", "2099-01-01T00:00:00.000Z"],
};

const PERIOD_6_START = PERIODS["6"][0];
const TRIAGE_MIN_PERIOD = "4";
const TRIAGE_MIN_START = PERIODS[TRIAGE_MIN_PERIOD][0];
const TRIAGE_END = PERIODS["7"][1];
const SUPPORTED_TRIAGE_PERIODS = new Set(["4", "5", "6", "7"]);

const EFFECTIVE_TRIAGE_EXPR = `
  CASE
    WHEN rules_triggered LIKE '%manual%' THEN triage_result
    WHEN triage_result IN ('duplicate', 'privacy', 'non_linear') THEN triage_result
    WHEN julianday(event_timestamp) >= julianday('${PERIOD_6_START}') AND (
      (video_length_sec IS NOT NULL AND video_length_sec < ${MIN_STANDARD_VIDEO_DURATION_SEC}) OR
      (bitrate_bps IS NOT NULL AND bitrate_bps < ${MIN_NON_LINEAR_BITRATE_BPS})
    ) THEN 'non_linear'
    ELSE triage_result
  END
`;

const NON_STANDARD_CONDITION = `
  julianday(event_timestamp) >= julianday('${TRIAGE_MIN_START}') AND
  julianday(event_timestamp) < julianday('${PERIOD_6_START}') AND
  video_length_sec IS NOT NULL AND
  video_length_sec < ${MIN_STANDARD_VIDEO_DURATION_SEC}
`;

const TRIAGE_WITH_FRAME_QC_SELECT = `
  SELECT
    triage_rows.*,
    q.bucket AS fps_qc,
    q.gap_pct AS late_frame_pct
  FROM triage_rows
  LEFT JOIN video_frame_timing_qc q ON q.video_id = triage_rows.id
`;

type TriageSort = "fps_qc" | "event_type" | null;
type SortDir = "asc" | "desc";

interface CachedEventData {
  metadata?: {
    FIRMWARE_VERSION?: unknown;
  };
  gnssData?: Array<{ timestamp?: number }>;
}

interface RawTriageRow extends Record<string, unknown> {
  id: string;
  event_type: string;
  triage_result: string;
  rules_triggered: string;
  video_size: number | null;
  video_length_sec: number | null;
  bitrate_bps: number | null;
  firmware_version: string | null;
  firmware_version_num: number | null;
  event_timestamp: string | null;
  created_at: string;
  fps_qc?: string | null;
  late_frame_pct?: number | null;
  effective_triage_result?: string;
}

function getFpsQcRank(value: unknown): number {
  if (value === "perfect") return 0;
  if (value === "ok") return 1;
  if (value === "filter_out") return 2;
  return 3;
}

function getOrderBy(sort: TriageSort, dir: SortDir): string {
  const dateDesc = `
        CASE WHEN triage_rows.event_timestamp IS NULL THEN 1 ELSE 0 END ASC,
        julianday(triage_rows.event_timestamp) DESC,
        triage_rows.created_at DESC
  `;

  if (sort === "event_type") {
    return `
      ORDER BY
        triage_rows.event_type COLLATE NOCASE ${dir.toUpperCase()},
${dateDesc}
    `;
  }

  if (sort === "fps_qc") {
    return `
      ORDER BY
        CASE WHEN q.bucket IS NULL THEN 1 ELSE 0 END ASC,
        CASE q.bucket
          WHEN 'perfect' THEN 0
          WHEN 'ok' THEN 1
          WHEN 'filter_out' THEN 2
          ELSE 3
        END ${dir.toUpperCase()},
${dateDesc}
    `;
  }

  return `ORDER BY ${dateDesc}`;
}

function compareRowsByEventDateDesc(a: RawTriageRow, b: RawTriageRow): number {
  const aTimestamp = getTimestampMs(a.event_timestamp);
  const bTimestamp = getTimestampMs(b.event_timestamp);

  if (aTimestamp !== null && bTimestamp !== null && aTimestamp !== bTimestamp) {
    return bTimestamp - aTimestamp;
  }
  if (aTimestamp !== null && bTimestamp === null) return -1;
  if (aTimestamp === null && bTimestamp !== null) return 1;

  const aCreatedAt = getTimestampMs(a.created_at);
  const bCreatedAt = getTimestampMs(b.created_at);
  if (aCreatedAt !== null && bCreatedAt !== null && aCreatedAt !== bCreatedAt) {
    return bCreatedAt - aCreatedAt;
  }
  if (aCreatedAt !== null && bCreatedAt === null) return -1;
  if (aCreatedAt === null && bCreatedAt !== null) return 1;

  return a.id.localeCompare(b.id);
}

function sortRows(rows: RawTriageRow[], sort: TriageSort, dir: SortDir): RawTriageRow[] {
  if (!sort) return [...rows].sort(compareRowsByEventDateDesc);
  const direction = dir === "desc" ? -1 : 1;

  return [...rows].sort((a, b) => {
    if (sort === "event_type") {
      const typeCmp = a.event_type.localeCompare(b.event_type, undefined, { sensitivity: "base" });
      if (typeCmp !== 0) return typeCmp * direction;
      return compareRowsByEventDateDesc(a, b);
    }

    const aRank = getFpsQcRank(a.fps_qc);
    const bRank = getFpsQcRank(b.fps_qc);
    if (aRank === 3 && bRank !== 3) return 1;
    if (bRank === 3 && aRank !== 3) return -1;
    if (aRank !== bRank) return (aRank - bRank) * direction;
    return compareRowsByEventDateDesc(a, b);
  });
}

function parseRules(rulesJson: string): string[] {
  try {
    const parsed = JSON.parse(rulesJson);
    return Array.isArray(parsed) ? parsed.filter((rule): rule is string => typeof rule === "string") : [];
  } catch {
    return [];
  }
}

function stringifyRules(rules: string[]): string {
  return JSON.stringify(Array.from(new Set(rules)));
}

function getFirmwareVersion(event: CachedEventData | null): string | null {
  const value = event?.metadata?.FIRMWARE_VERSION;
  return typeof value === "string" && value.trim() ? value : null;
}

function firmwareVersionToNum(value: string | null): number | null {
  if (!value) return null;
  const match = value.match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) return null;
  const [, major, minor, patch] = match;
  return Number(major) * 1_000_000 + Number(minor) * 1_000 + Number(patch);
}

function normalizeTriagePeriod(period: string | null): string | null {
  return period && SUPPORTED_TRIAGE_PERIODS.has(period) ? period : null;
}

function isUnsupportedTriagePeriod(period: string | null): boolean {
  return period !== null && !SUPPORTED_TRIAGE_PERIODS.has(period);
}

function getPeriodCondition(
  period: string | null,
  options: { includeFirmwareGate?: boolean } = {}
): { condition: string; params: unknown[] } | null {
  const normalizedPeriod = normalizeTriagePeriod(period);

  if (!normalizedPeriod) {
    return {
      condition: "julianday(event_timestamp) >= julianday(?) AND julianday(event_timestamp) < julianday(?)",
      params: [TRIAGE_MIN_START, TRIAGE_END],
    };
  }

  const [pStart, pEnd] = PERIODS[normalizedPeriod];
  if (normalizedPeriod === "7" && options.includeFirmwareGate) {
    return {
      condition:
        "julianday(event_timestamp) >= julianday(?) AND julianday(event_timestamp) < julianday(?) AND firmware_version_num >= ?",
      params: [pStart, pEnd, MIN_PERIOD_7_FIRMWARE_NUM],
    };
  }

  return {
    condition: "julianday(event_timestamp) >= julianday(?) AND julianday(event_timestamp) < julianday(?)",
    params: [pStart, pEnd],
  };
}

const eventTotalCache = new Map<string, { total: number; expiresAt: number }>();

function normalizeAuthHeader(value: string): string {
  return value.startsWith("Basic ") ? value : `Basic ${value}`;
}

function getSourceDateRange(period: string | null): { startDate: string; endDate: string } {
  const normalizedPeriod = normalizeTriagePeriod(period);
  const [periodStart, periodEnd] = normalizedPeriod ? PERIODS[normalizedPeriod] : [TRIAGE_MIN_START, TRIAGE_END];
  const now = Date.now();
  const endMs = Math.min(new Date(periodEnd).getTime(), now);

  return {
    startDate: periodStart,
    endDate: new Date(endMs).toISOString(),
  };
}

function splitDateRange(startDate: string, endDate: string): Array<{ startDate: string; endDate: string }> {
  const startMs = new Date(startDate).getTime();
  const endMs = new Date(endDate).getTime();
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs) || endMs <= startMs) {
    return [];
  }

  const chunks: Array<{ startDate: string; endDate: string }> = [];
  for (let chunkStart = startMs; chunkStart < endMs; chunkStart += MAX_EVENT_TOTAL_RANGE_MS) {
    const chunkEnd = Math.min(chunkStart + MAX_EVENT_TOTAL_RANGE_MS, endMs);
    chunks.push({
      startDate: new Date(chunkStart).toISOString(),
      endDate: new Date(chunkEnd).toISOString(),
    });
  }
  return chunks;
}

async function fetchSourceEventTotal(period: string | null, apiKey: string | null): Promise<number | null> {
  if (!apiKey) return null;

  const normalizedPeriod = normalizeTriagePeriod(period);
  const cacheKey = normalizedPeriod ? `period-${normalizedPeriod}` : "period-4-plus";
  const cached = eventTotalCache.get(cacheKey);
  if (cached && cached.expiresAt > Date.now()) {
    return cached.total;
  }

  const authHeader = normalizeAuthHeader(apiKey);
  const { startDate, endDate } = getSourceDateRange(period);
  const chunks = splitDateRange(startDate, endDate);
  if (chunks.length === 0) return 0;

  try {
    const totals = await Promise.all(
      chunks.map(async (chunk) => {
        const response = await fetchWithRetry(
          `${API_BASE_URL}/search`,
          {
            method: "POST",
            headers: {
              "Content-Type": "application/json",
              Authorization: authHeader,
            },
            body: JSON.stringify({ ...chunk, limit: 1, offset: 0 }),
          },
          { retries: 1, backoffMs: 1000 }
        );

        if (!response.ok) {
          throw new Error(`Bee Maps event count failed with HTTP ${response.status}`);
        }

        const data = await response.json();
        return Number(data.pagination?.total ?? data.events?.length ?? 0);
      })
    );

    const total = totals.reduce((sum, value) => sum + value, 0);
    eventTotalCache.set(cacheKey, {
      total,
      expiresAt: Date.now() + EVENT_TOTAL_CACHE_TTL_MS,
    });
    return total;
  } catch (error) {
    console.warn("Failed to load Bee Maps source total for triage:", error);
    return null;
  }
}

function getAwaitingTriageTotal(sourceTotal: number | null, triagedTotal: number): number | null {
  if (sourceTotal === null) return null;
  return Math.max(0, sourceTotal - triagedTotal);
}

function isPeriod7FirmwareEligible(row: RawTriageRow): boolean {
  const versionNum =
    typeof row.firmware_version_num === "number" && Number.isFinite(row.firmware_version_num)
      ? row.firmware_version_num
      : firmwareVersionToNum(row.firmware_version);

  return versionNum !== null && versionNum >= MIN_PERIOD_7_FIRMWARE_NUM;
}

function getTimestampMs(value: string | null): number | null {
  if (!value) return null;
  const parsed = new Date(value.endsWith("Z") ? value : `${value}Z`).getTime();
  return Number.isFinite(parsed) ? parsed : null;
}

function isNonStandardRow(row: RawTriageRow): boolean {
  const timestampMs = getTimestampMs(row.event_timestamp);
  return (
    timestampMs !== null &&
    timestampMs >= new Date(TRIAGE_MIN_START).getTime() &&
    timestampMs < new Date(PERIOD_6_START).getTime() &&
    row.video_length_sec !== null &&
    row.video_length_sec < MIN_STANDARD_VIDEO_DURATION_SEC
  );
}

function rowMatchesFilter(row: RawTriageRow, filter: string | null): boolean {
  if (filter === "filtered") {
    return row.triage_result !== "signal";
  }
  if (filter === "non_standard") {
    return isNonStandardRow(row);
  }
  if (filter) {
    return row.triage_result === filter;
  }
  return true;
}

function parseExcludedEventTypes(value: string | null): string[] {
  if (!value) return [];
  return Array.from(
    new Set(
      value
        .split(",")
        .map((type) => type.trim())
        .filter((type) => /^[A-Z0-9_]+$/.test(type))
    )
  );
}

function summarizeRows(rows: RawTriageRow[]): Record<string, number> {
  return rows.reduce<Record<string, number>>(
    (summary, row) => {
      summary[row.triage_result] = (summary[row.triage_result] ?? 0) + 1;
      if (isNonStandardRow(row)) {
        summary.non_standard += 1;
      }
      return summary;
    },
    {
      missing_video: 0,
      missing_metadata: 0,
      ghost: 0,
      open_road: 0,
      duplicate: 0,
      signal: 0,
      non_linear: 0,
      privacy: 0,
      skipped_firmware: 0,
      non_standard: 0,
    }
  );
}

async function loadCachedEvent(eventId: string): Promise<CachedEventData | null> {
  const cacheDir = path.join(process.cwd(), "data", "event-cache");

  for (const ext of [".json.gz", ".json"]) {
    const filePath = path.join(cacheDir, `${eventId}${ext}`);
    try {
      const raw = await fs.readFile(filePath);
      const text = ext === ".json.gz" ? gunzipSync(raw).toString("utf8") : raw.toString("utf8");
      return JSON.parse(text) as CachedEventData;
    } catch (error) {
      const code = (error as NodeJS.ErrnoException).code;
      if (code === "ENOENT") continue;
      return null;
    }
  }

  return null;
}

function getVideoLengthSec(event: CachedEventData | null): number | null {
  const gnssData = event?.gnssData;
  if (!Array.isArray(gnssData) || gnssData.length < 2) return null;

  const first = gnssData.find((point) => typeof point?.timestamp === "number")?.timestamp;
  const last = [...gnssData].reverse().find((point) => typeof point?.timestamp === "number")?.timestamp;

  if (typeof first !== "number" || typeof last !== "number" || last <= first) {
    return null;
  }

  return (last - first) / 1000;
}

async function enrichTriageRow<T extends RawTriageRow>(row: T): Promise<T> {
  let videoLengthSec =
    typeof row.video_length_sec === "number" && Number.isFinite(row.video_length_sec)
      ? row.video_length_sec
      : null;
  let bitrateBps =
    typeof row.bitrate_bps === "number" && Number.isFinite(row.bitrate_bps)
      ? row.bitrate_bps
      : null;
  let firmwareVersion =
    typeof row.firmware_version === "string" && row.firmware_version.trim()
      ? row.firmware_version
      : null;
  let firmwareVersionNum =
    typeof row.firmware_version_num === "number" && Number.isFinite(row.firmware_version_num)
      ? row.firmware_version_num
      : firmwareVersionToNum(firmwareVersion);

  if (
    videoLengthSec === null ||
    (bitrateBps === null && row.video_size !== null) ||
    firmwareVersion === null ||
    firmwareVersionNum === null
  ) {
    const cachedEvent = await loadCachedEvent(row.id);
    if (videoLengthSec === null) {
      videoLengthSec = getVideoLengthSec(cachedEvent);
    }
    if (bitrateBps === null && row.video_size !== null && videoLengthSec && videoLengthSec > 0) {
      bitrateBps = (row.video_size * 8) / videoLengthSec;
    }
    if (firmwareVersion === null) {
      firmwareVersion = getFirmwareVersion(cachedEvent);
    }
    if (firmwareVersionNum === null) {
      firmwareVersionNum = firmwareVersionToNum(firmwareVersion);
    }
  }

  const effectiveTriageResult =
    typeof row.effective_triage_result === "string"
      ? row.effective_triage_result
      : row.triage_result;

  const rules = parseRules(row.rules_triggered);
  if (effectiveTriageResult === "non_linear") {
    if (
      videoLengthSec !== null &&
      videoLengthSec < MIN_STANDARD_VIDEO_DURATION_SEC &&
      !rules.includes(NON_LINEAR_SHORT_RULE)
    ) {
      rules.push(NON_LINEAR_SHORT_RULE);
    }
    if (
      bitrateBps !== null &&
      bitrateBps < MIN_NON_LINEAR_BITRATE_BPS &&
      !rules.includes(NON_LINEAR_BITRATE_RULE)
    ) {
      rules.push(NON_LINEAR_BITRATE_RULE);
    }
  }

  return {
    ...row,
    triage_result: effectiveTriageResult,
    rules_triggered: stringifyRules(rules),
    video_length_sec: videoLengthSec,
    bitrate_bps: bitrateBps,
    firmware_version: firmwareVersion,
    firmware_version_num: firmwareVersionNum,
  };
}

export async function GET(request: NextRequest) {
  try {
    const limit = parseInt(request.nextUrl.searchParams.get("limit") ?? "50", 10);
    const offset = parseInt(request.nextUrl.searchParams.get("offset") ?? "0", 10);
    const filter = request.nextUrl.searchParams.get("filter"); // filtered, triage result, or "non_standard"
    const requestedPeriod = request.nextUrl.searchParams.get("period");
    if (isUnsupportedTriagePeriod(requestedPeriod)) {
      return NextResponse.json(
        {
          error: "Triage only supports Period 4 or newer.",
          results: [],
          total: 0,
          summary: {},
          summaryTotal: 0,
          sourceTotal: null,
          awaitingTriageTotal: null,
        },
        { status: 400 }
      );
    }
    const period = normalizeTriagePeriod(requestedPeriod); // 4-7
    const requestedSort = request.nextUrl.searchParams.get("sort");
    const sort: TriageSort =
      requestedSort === "fps_qc" || requestedSort === "event_type" ? requestedSort : null;
    const sortDir: SortDir = request.nextUrl.searchParams.get("dir") === "desc" ? "desc" : "asc";
    const excludedEventTypes = parseExcludedEventTypes(request.nextUrl.searchParams.get("excludeTypes"));
    const apiKey = request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY || null;
    const sourceTotalPromise = fetchSourceEventTotal(period, apiKey);

    const db = await getDb();

    await db.exec(`CREATE TABLE IF NOT EXISTS triage_results (
      id TEXT PRIMARY KEY,
      event_type TEXT NOT NULL,
      triage_result TEXT NOT NULL,
      rules_triggered TEXT NOT NULL DEFAULT '[]',
      speed_min REAL,
      speed_max REAL,
      speed_mean REAL,
      speed_stddev REAL,
      gnss_displacement_m REAL,
      video_size INTEGER,
      video_length_sec REAL,
      bitrate_bps REAL,
      firmware_version TEXT,
      firmware_version_num INTEGER,
      event_timestamp TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`);

    const cte = `WITH triage_rows AS (SELECT *, ${EFFECTIVE_TRIAGE_EXPR} AS effective_triage_result FROM triage_results)`;

    const conditions: string[] = [];
    const params: unknown[] = [];

    const periodCondition = getPeriodCondition(period, {
      includeFirmwareGate: period !== "7",
    });
    if (periodCondition) {
      conditions.push(periodCondition.condition);
      params.push(...periodCondition.params);
    }

    if (period === "7" && periodCondition) {
      const allPeriodRows = await db.query(
        `${cte}
         ${TRIAGE_WITH_FRAME_QC_SELECT}
         WHERE ${periodCondition.condition}
         ${getOrderBy(null, sortDir)}`,
        periodCondition.params
      );
      const enrichedPeriodRows = await Promise.all(
        (allPeriodRows.rows as Array<RawTriageRow>).map((row) => enrichTriageRow(row))
      );
      const eligiblePeriodRows = enrichedPeriodRows.filter(isPeriod7FirmwareEligible);
      const filteredPeriodRows = eligiblePeriodRows.filter(
        (row) => rowMatchesFilter(row, filter) && !excludedEventTypes.includes(row.event_type)
      );
      const sortedPeriodRows = sortRows(filteredPeriodRows, sort, sortDir);
      const summary = summarizeRows(eligiblePeriodRows);
      const sourceTotal = await sourceTotalPromise;

      return NextResponse.json({
        results: sortedPeriodRows.slice(offset, offset + limit),
        total: filteredPeriodRows.length,
        summary,
        summaryTotal: eligiblePeriodRows.length,
        sourceTotal,
        awaitingTriageTotal: getAwaitingTriageTotal(sourceTotal, allPeriodRows.rows.length),
      });
    }

    if (filter === "filtered") {
      conditions.push("effective_triage_result != ?");
      params.push("signal");
    } else if (filter === "non_standard") {
      conditions.push(NON_STANDARD_CONDITION);
    } else if (filter) {
      conditions.push("effective_triage_result = ?");
      params.push(filter);
    }
    if (excludedEventTypes.length > 0) {
      conditions.push(`event_type NOT IN (${excludedEventTypes.map(() => "?").join(", ")})`);
      params.push(...excludedEventTypes);
    }

    const whereClause = conditions.length > 0 ? `WHERE ${conditions.join(" AND ")}` : "";

    const countResult = await db.query(
      `${cte} SELECT COUNT(*) as count FROM triage_rows ${whereClause}`,
      params
    );
    const total = Number((countResult.rows[0] as { count: number } | undefined)?.count ?? 0);

    const result = await db.query(
      `${cte}
       ${TRIAGE_WITH_FRAME_QC_SELECT} ${whereClause}
       ${getOrderBy(sort, sortDir)} LIMIT ? OFFSET ?`,
      [...params, limit, offset]
    );

    const summaryConditions: string[] = [];
    const summaryParams: unknown[] = [];
    if (periodCondition) {
      summaryConditions.push(periodCondition.condition);
      summaryParams.push(...periodCondition.params);
    }
    const summaryWhere = summaryConditions.length > 0 ? `WHERE ${summaryConditions.join(" AND ")}` : "";

    const summaryResult = await db.query(
      `${cte}
       SELECT
         COUNT(*) as summary_total,
         SUM(CASE WHEN effective_triage_result = 'missing_video' THEN 1 ELSE 0 END) as missing_video,
         SUM(CASE WHEN effective_triage_result = 'missing_metadata' THEN 1 ELSE 0 END) as missing_metadata,
         SUM(CASE WHEN effective_triage_result = 'ghost' THEN 1 ELSE 0 END) as ghost,
         SUM(CASE WHEN effective_triage_result = 'open_road' THEN 1 ELSE 0 END) as open_road,
         SUM(CASE WHEN effective_triage_result = 'duplicate' THEN 1 ELSE 0 END) as duplicate,
         SUM(CASE WHEN effective_triage_result = 'signal' THEN 1 ELSE 0 END) as signal,
         SUM(CASE WHEN effective_triage_result = 'non_linear' THEN 1 ELSE 0 END) as non_linear,
         SUM(CASE WHEN effective_triage_result = 'privacy' THEN 1 ELSE 0 END) as privacy,
         SUM(CASE WHEN effective_triage_result = 'skipped_firmware' THEN 1 ELSE 0 END) as skipped_firmware,
         SUM(CASE WHEN ${NON_STANDARD_CONDITION} THEN 1 ELSE 0 END) as non_standard
       FROM triage_rows
       ${summaryWhere}`,
      summaryParams
    );

    const summaryRow = (summaryResult.rows[0] as Record<string, number | null> | undefined) ?? {};
    const summary = {
      missing_video: Number(summaryRow.missing_video ?? 0),
      missing_metadata: Number(summaryRow.missing_metadata ?? 0),
      ghost: Number(summaryRow.ghost ?? 0),
      open_road: Number(summaryRow.open_road ?? 0),
      duplicate: Number(summaryRow.duplicate ?? 0),
      signal: Number(summaryRow.signal ?? 0),
      non_linear: Number(summaryRow.non_linear ?? 0),
      privacy: Number(summaryRow.privacy ?? 0),
      skipped_firmware: Number(summaryRow.skipped_firmware ?? 0),
      non_standard: Number(summaryRow.non_standard ?? 0),
    };
    const summaryTotal = Number(summaryRow.summary_total ?? 0);

    const enrichedResults = await Promise.all(
      (result.rows as Array<RawTriageRow>).map((row) => enrichTriageRow(row))
    );
    const sourceTotal = await sourceTotalPromise;

    return NextResponse.json({
      results: enrichedResults,
      total,
      summary,
      summaryTotal,
      sourceTotal,
      awaitingTriageTotal: getAwaitingTriageTotal(sourceTotal, summaryTotal),
    });
  } catch (error) {
    console.error("Triage API error:", error);
    return NextResponse.json(
      { error: String(error), results: [], total: 0, summary: {}, summaryTotal: 0, sourceTotal: null, awaitingTriageTotal: null },
      { status: 500 }
    );
  }
}

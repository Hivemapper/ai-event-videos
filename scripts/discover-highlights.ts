#!/usr/bin/env npx tsx
/**
 * Discover new highlight-worthy events from Bee Maps and append them to highlights.ts.
 *
 * Usage:
 *   npm run discover                  # uses .env.local keys
 *   npm run discover -- --dry-run     # preview without writing
 *   npm run discover -- --days 14     # search last 14 days (default: 31)
 *   npm run discover -- --limit 3     # max new events per section (default: 5)
 */

import * as fs from "fs";
import * as path from "path";
import { createClient, type Client as LibsqlClient } from "@libsql/client";
import { isVruDetectionLabel } from "../src/lib/vru-labels";

const API_BASE_URL = "https://beemaps.com/api/developer/aievents";

// ---------------------------------------------------------------------------
// VRU config (mirrors src/lib/detection-summary.ts + enrichment route)
// ---------------------------------------------------------------------------

/** Type weights per VRU label. Higher = more "interesting" / higher risk. */
const VRU_TYPE_WEIGHTS: Record<string, number> = {
  person: 1.0,
  pedestrian: 1.0,
  child: 1.3,
  stroller: 1.3,
  wheelchair: 1.2,
  bicycle: 1.0,
  scooter: 0.9,
  skateboard: 0.9,
  motorcycle: 0.7,
  dog: 0.5,
  cat: 0.4,
  bird: 0.2,
  horse: 0.6,
  sheep: 0.4,
  cow: 0.5,
  bear: 0.8,
};

/** Match detection-summary.ts VRU_MIN_CONFIDENCE */
const VRU_MIN_CONFIDENCE = 0.46;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const TYPES = [
  "HARSH_BRAKING",
  "HIGH_SPEED",
  "HIGH_G_FORCE",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
] as const;

type EventType = (typeof TYPES)[number];

interface RawEvent {
  id: string;
  type: EventType;
  timestamp: string;
  location: { lat: number; lon: number };
  metadata?: Record<string, unknown>;
}

interface ScoredEvent {
  event: RawEvent;
  maxSpeed: number;
  minSpeed: number;
  acceleration: number;
  vruScore: number;
  vruLabels: string[]; // distinct VRU labels seen in this event, for logging
}

interface HighlightEntry {
  id: string;
  type: string;
  location: string;
  coords: { lat: number; lon: number };
  date: string;
  maxSpeed: number;
  minSpeed: number;
  acceleration: number;
}

// ---------------------------------------------------------------------------
// Args
// ---------------------------------------------------------------------------

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const daysIdx = args.indexOf("--days");
const days = daysIdx >= 0 ? parseInt(args[daysIdx + 1], 10) || 31 : 31;
const limitIdx = args.indexOf("--limit");
const perSectionLimit = limitIdx >= 0 ? parseInt(args[limitIdx + 1], 10) || 5 : 5;

// ---------------------------------------------------------------------------
// Load env from .env.local
// ---------------------------------------------------------------------------

function loadEnv() {
  const envPath = path.resolve(__dirname, "../.env.local");
  if (!fs.existsSync(envPath)) return;
  const lines = fs.readFileSync(envPath, "utf-8").split("\n");
  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) continue;
    const eqIdx = trimmed.indexOf("=");
    if (eqIdx < 0) continue;
    const key = trimmed.slice(0, eqIdx).trim();
    const val = trimmed.slice(eqIdx + 1).trim();
    if (!process.env[key]) process.env[key] = val;
  }
}

loadEnv();

const BEEMAPS_KEY = process.env.BEEMAPS_API_KEY || process.env.NEXT_PUBLIC_BEEMAPS_API_KEY;
if (!BEEMAPS_KEY) {
  console.error("Error: No BEEMAPS_API_KEY found in .env.local");
  process.exit(1);
}
const AUTH = BEEMAPS_KEY.startsWith("Basic ") ? BEEMAPS_KEY : `Basic ${BEEMAPS_KEY}`;

// ---------------------------------------------------------------------------
// Speed / acceleration parsing (mirrors highlights-utils.ts)
// ---------------------------------------------------------------------------

function parseSpeedArray(metadata: Record<string, unknown> | undefined) {
  if (!metadata?.SPEED_ARRAY) return { maxSpeed: 0, minSpeed: 0 };
  const arr = metadata.SPEED_ARRAY as Array<number | { AVG_SPEED_MS: number }>;
  if (!Array.isArray(arr) || arr.length === 0) return { maxSpeed: 0, minSpeed: 0 };
  // Handle both formats: plain m/s numbers or { AVG_SPEED_MS, TIMESTAMP } objects
  const ms = arr.map((s) => (typeof s === "number" ? s : s.AVG_SPEED_MS));
  const kmh = ms.map((s) => s * 3.6);
  return { maxSpeed: Math.max(...kmh), minSpeed: Math.min(...kmh) };
}

function getAcceleration(metadata: Record<string, unknown> | undefined): number {
  if (!metadata?.ACCELERATION_MS2) return 0;
  const val = Number(metadata.ACCELERATION_MS2);
  return isNaN(val) ? 0 : val;
}

// ---------------------------------------------------------------------------
// VRU metadata (queried from Turso detection pipeline DB)
// ---------------------------------------------------------------------------

interface VruInfo {
  score: number;
  labels: string[];
}

/** Fetch VRU detection data for a batch of event/video IDs from Turso.
 *  Returns a map from video_id → VruInfo. IDs with no detection run or no
 *  qualifying segments map to { score: 0, labels: [] }. */
async function fetchVruScores(ids: string[]): Promise<Map<string, VruInfo>> {
  const out = new Map<string, VruInfo>();
  if (ids.length === 0) return out;

  const url = process.env.TURSO_DATABASE_URL;
  const authToken = process.env.TURSO_AUTH_TOKEN;
  if (!url) {
    console.warn("  (skipping VRU scoring — TURSO_DATABASE_URL not set)");
    return out;
  }

  let db: LibsqlClient;
  try {
    db = createClient({ url, authToken });
  } catch (e) {
    console.warn(`  (VRU DB connect failed: ${e instanceof Error ? e.message : e})`);
    return out;
  }

  // Turso placeholder limit is generous but chunk to be safe.
  const CHUNK = 400;
  const latestRunByVideo = new Map<string, string>();

  for (let i = 0; i < ids.length; i += CHUNK) {
    const chunk = ids.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => "?").join(",");
    // Latest completed run per video_id.
    const runsRes = await db.execute({
      sql: `SELECT video_id, id, created_at FROM detection_runs
            WHERE status = 'completed' AND video_id IN (${placeholders})
            ORDER BY video_id ASC, created_at DESC`,
      args: chunk,
    });
    for (const row of runsRes.rows as unknown as Array<{
      video_id: string;
      id: string;
    }>) {
      if (!latestRunByVideo.has(row.video_id)) {
        latestRunByVideo.set(row.video_id, row.id);
      }
    }
  }

  if (latestRunByVideo.size === 0) return out;

  const runIds = Array.from(new Set(latestRunByVideo.values()));
  const runToVideo = new Map<string, string>();
  for (const [videoId, runId] of latestRunByVideo) {
    runToVideo.set(runId, videoId);
  }

  // Aggregate score: Σ(typeWeight × max_confidence) across qualifying segments.
  // Distinct labels stored for logging / "interesting" callouts.
  const scores = new Map<string, { score: number; labels: Set<string> }>();

  for (let i = 0; i < runIds.length; i += CHUNK) {
    const chunk = runIds.slice(i, i + CHUNK);
    const placeholders = chunk.map(() => "?").join(",");
    const segRes = await db.execute({
      sql: `SELECT run_id, label, max_confidence
            FROM video_detection_segments
            WHERE run_id IN (${placeholders}) AND max_confidence >= ?`,
      args: [...chunk, VRU_MIN_CONFIDENCE],
    });
    for (const row of segRes.rows as unknown as Array<{
      run_id: string;
      label: string;
      max_confidence: number;
    }>) {
      if (!isVruDetectionLabel(row.label)) continue;
      const weight = VRU_TYPE_WEIGHTS[row.label] ?? 0.3; // unknown VRU-ish → small weight
      const videoId = runToVideo.get(row.run_id);
      if (!videoId) continue;
      const cur = scores.get(videoId) ?? { score: 0, labels: new Set<string>() };
      cur.score += weight * row.max_confidence;
      cur.labels.add(row.label);
      scores.set(videoId, cur);
    }
  }

  for (const [videoId, v] of scores) {
    out.set(videoId, { score: v.score, labels: Array.from(v.labels).sort() });
  }
  return out;
}

// ---------------------------------------------------------------------------
// Bee Maps API
// ---------------------------------------------------------------------------

async function searchEvents(type: EventType, startDate: string, endDate: string): Promise<RawEvent[]> {
  const body = { startDate, endDate, types: [type], limit: 200, offset: 0 };
  const res = await fetch(`${API_BASE_URL}/search`, {
    method: "POST",
    headers: { "Content-Type": "application/json", Authorization: AUTH },
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    console.warn(`  Warning: ${type} query returned ${res.status}`);
    return [];
  }
  const data = await res.json();
  return (data.events || []) as RawEvent[];
}

// ---------------------------------------------------------------------------
// Reverse geocoding (Nominatim, 1 req/sec)
// ---------------------------------------------------------------------------

async function reverseGeocode(lat: number, lon: number): Promise<string> {
  try {
    const res = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      { headers: { "User-Agent": "AI-Event-Videos-Discover-Script" } }
    );
    if (!res.ok) return `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    const data = await res.json();
    const addr = data.address;
    const name = addr?.city || addr?.town || addr?.village || addr?.county || addr?.state || `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    const country = addr?.country;
    return country ? `${name}, ${country}` : name;
  } catch {
    return `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
  }
}

function delay(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

// ---------------------------------------------------------------------------
// Ranking
// ---------------------------------------------------------------------------

function isUSCoords(lat: number, lon: number): boolean {
  if (lat >= 24.5 && lat <= 49.5 && lon >= -125 && lon <= -66.5) return true;
  if (lat >= 51 && lat <= 72 && lon >= -180 && lon <= -129) return true;
  if (lat >= 18.5 && lat <= 22.5 && lon >= -161 && lon <= -154) return true;
  return false;
}

interface Section {
  index: number;
  name: string;
  types: EventType[];
  rank: (a: ScoredEvent, b: ScoredEvent) => number;
  filter?: (s: ScoredEvent) => boolean;
  limit?: number; // per-section override (defaults to perSectionLimit)
}

/** Baseline severity signal for an event: combines speed delta, peak speed,
 *  and acceleration. Used as the multiplier for VRU scoring and as a
 *  tie-breaker across existing sections. */
function severity(s: ScoredEvent): number {
  return (s.maxSpeed - s.minSpeed) + s.acceleration * 30 + s.maxSpeed * 0.3;
}

/** Wrap a primary ranker so VRU score acts as a secondary tie-breaker boost.
 *  The boost is bounded so it cannot override a clear winner on the primary. */
function withVruBoost(
  primary: (a: ScoredEvent, b: ScoredEvent) => number,
  boost = 0.15
): (a: ScoredEvent, b: ScoredEvent) => number {
  return (a, b) => {
    const p = primary(a, b);
    if (Math.abs(p) > 1) return p; // clear winner on primary signal
    return p + (b.vruScore - a.vruScore) * boost;
  };
}

const SECTIONS: Section[] = [
  {
    index: 0,
    name: "Extreme Braking",
    types: ["HARSH_BRAKING"],
    rank: withVruBoost((a, b) => (b.maxSpeed - b.minSpeed) - (a.maxSpeed - a.minSpeed)),
    limit: 10,
  },
  {
    index: 1,
    name: "High Speed",
    types: ["HIGH_SPEED"],
    rank: withVruBoost((a, b) => b.maxSpeed - a.maxSpeed),
  },
  {
    index: 2,
    name: "Highest G-Force",
    types: ["HIGH_G_FORCE"],
    rank: withVruBoost((a, b) => b.acceleration - a.acceleration),
  },
  {
    index: 3,
    name: "Aggressive Acceleration",
    types: ["AGGRESSIVE_ACCELERATION"],
    rank: withVruBoost((a, b) => b.acceleration - a.acceleration),
  },
  {
    index: 4,
    name: "Swerving",
    types: ["SWERVING"],
    rank: withVruBoost((a, b) => b.acceleration - a.acceleration),
  },
  {
    index: 5,
    name: "International Highlights",
    types: [...TYPES],
    rank: withVruBoost((a, b) => severity(b) - severity(a)),
    filter: (s) => !isUSCoords(s.event.location.lat, s.event.location.lon),
  },
  {
    index: 6,
    name: "VRU Close Calls",
    types: [...TYPES],
    // Rank by combined VRU-presence * event-severity. Events without any
    // detected VRU get filtered out below.
    rank: (a, b) => b.vruScore * severity(b) - a.vruScore * severity(a),
    filter: (s) => s.vruScore > 0.5, // requires at least one confident VRU
    limit: 8,
  },
];

// ---------------------------------------------------------------------------
// Read existing IDs from highlights.ts
// ---------------------------------------------------------------------------

function getExistingIds(): Set<string> {
  const hlPath = path.resolve(__dirname, "../src/lib/highlights.ts");
  const content = fs.readFileSync(hlPath, "utf-8");
  const ids = new Set<string>();
  const re = /id:\s*"([a-f0-9]+)"/g;
  let m;
  while ((m = re.exec(content)) !== null) {
    ids.add(m[1]);
  }
  return ids;
}

// ---------------------------------------------------------------------------
// Write new events into highlights.ts
// ---------------------------------------------------------------------------

/** Short human description used only when creating a brand-new section. */
const SECTION_DESCRIPTIONS: Record<string, string> = {
  "VRU Close Calls":
    "Events featuring vulnerable road users — pedestrians, cyclists, and motorcyclists detected near the vehicle during high-severity maneuvers.",
};

function buildEntryCode(entries: HighlightEntry[], now: number): string {
  return entries
    .map(
      (e) =>
        `      {\n` +
        `        id: "${e.id}",\n` +
        `        type: "${e.type}",\n` +
        `        location: "${e.location}",\n` +
        `        coords: { lat: ${e.coords.lat}, lon: ${e.coords.lon} },\n` +
        `        date: "${e.date}",\n` +
        `        maxSpeed: ${e.maxSpeed},\n` +
        `        minSpeed: ${e.minSpeed},\n` +
        `        acceleration: ${e.acceleration},\n` +
        `        addedAt: ${now},\n` +
        `      },`
    )
    .join("\n");
}

/** Insert a brand-new section block before the closing `];` of the
 *  `highlightSections` array. */
function insertNewSection(
  content: string,
  title: string,
  entries: HighlightEntry[],
  now: number
): string {
  const description = SECTION_DESCRIPTIONS[title] ?? "";
  const entriesCode = buildEntryCode(entries, now);
  const block =
    `  {\n` +
    `    title: "${title}",\n` +
    `    description:\n      "${description}",\n` +
    `    events: [\n${entriesCode}\n    ],\n` +
    `  },\n`;

  // Find the closing `];` of the top-level array. Search from the end.
  const closeIdx = content.lastIndexOf("];");
  if (closeIdx < 0) {
    console.warn(`  Could not find highlightSections close bracket; skipping new section "${title}"`);
    return content;
  }
  return content.slice(0, closeIdx) + block + content.slice(closeIdx);
}

function appendToHighlights(sectionNewEvents: Map<number, HighlightEntry[]>) {
  const hlPath = path.resolve(__dirname, "../src/lib/highlights.ts");
  let content = fs.readFileSync(hlPath, "utf-8");
  const now = Date.now();

  // For each section, find the last event entry and insert after it
  for (const [sectionIndex, entries] of sectionNewEvents) {
    if (entries.length === 0) continue;

    const section = SECTIONS[sectionIndex];
    const sectionTitle = section.name;
    const newCode = buildEntryCode(entries, now);

    // Find the events array closing for this section by looking for the
    // section title, then finding the next `    ],` which closes the events array.
    const titleIdx = content.indexOf(`title: "${sectionTitle}"`);
    if (titleIdx < 0) {
      // Section not present yet — create it (e.g., first run with VRU Close Calls).
      console.log(`  Creating new section "${sectionTitle}" with ${entries.length} events`);
      content = insertNewSection(content, sectionTitle, entries, now);
      continue;
    }

    // Find the closing `    ],` of the events array after this title
    const eventsStart = content.indexOf("events: [", titleIdx);
    if (eventsStart < 0) continue;

    // Find the matching close: look for `    ],` after the events array opening
    // We need to find the right `],` — count brackets
    let depth = 0;
    let closeIdx = -1;
    for (let i = eventsStart + "events: [".length; i < content.length; i++) {
      if (content[i] === "[") depth++;
      if (content[i] === "]") {
        if (depth === 0) {
          closeIdx = i;
          break;
        }
        depth--;
      }
    }

    if (closeIdx < 0) continue;

    // Insert before the closing ]
    content =
      content.slice(0, closeIdx) +
      "\n" +
      newCode +
      "\n    " +
      content.slice(closeIdx);
  }

  fs.writeFileSync(hlPath, content, "utf-8");
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main() {
  console.log(`\n🔍 Discovering highlights (last ${days} days, limit ${perSectionLimit}/section)${dryRun ? " [DRY RUN]" : ""}\n`);

  const endDate = new Date();
  const startDate = new Date();
  startDate.setDate(startDate.getDate() - days);
  const startStr = startDate.toISOString();
  const endStr = endDate.toISOString();

  // Fetch all event types in parallel
  console.log("Fetching events from Bee Maps...");
  const results = await Promise.all(
    TYPES.map(async (type) => {
      const events = await searchEvents(type, startStr, endStr);
      console.log(`  ${type}: ${events.length} events`);
      return { type, events };
    })
  );

  const byType: Record<string, ScoredEvent[]> = {};
  for (const { type, events } of results) {
    byType[type] = events.map((event) => {
      const { maxSpeed, minSpeed } = parseSpeedArray(event.metadata);
      const acceleration = getAcceleration(event.metadata);
      return {
        event,
        maxSpeed,
        minSpeed,
        acceleration,
        vruScore: 0,
        vruLabels: [],
      };
    });
  }

  // Get existing IDs to avoid duplicates
  const existingIds = getExistingIds();
  console.log(`\nExisting highlights: ${existingIds.size} events`);

  // Fetch VRU scores for all candidate events in one pass.
  const allIds = Object.values(byType).flat()
    .map((s) => s.event.id)
    .filter((id) => !existingIds.has(id));
  console.log(`\nFetching VRU metadata for ${allIds.length} candidate events...`);
  const vruMap = await fetchVruScores(allIds);
  console.log(`  got VRU data for ${vruMap.size} events`);
  for (const scored of Object.values(byType).flat()) {
    const v = vruMap.get(scored.event.id);
    if (v) {
      scored.vruScore = v.score;
      scored.vruLabels = v.labels;
    }
  }

  // Rank per section, excluding existing
  const sectionNewEvents = new Map<number, HighlightEntry[]>();
  const needsGeocoding = new Map<string, { lat: number; lon: number }>();

  for (const section of SECTIONS) {
    let pool: ScoredEvent[] = [];
    for (const t of section.types) {
      pool = pool.concat(byType[t] || []);
    }
    if (section.filter) pool = pool.filter(section.filter);

    // Remove existing
    pool = pool.filter((s) => !existingIds.has(s.event.id));

    // Sort and take top N (per-section limit overrides global)
    pool.sort(section.rank);
    const sectionMax = section.limit ?? perSectionLimit;
    const top = pool.slice(0, sectionMax);

    const entries: HighlightEntry[] = top.map((s) => ({
      id: s.event.id,
      type: s.event.type,
      location: "", // filled after geocoding
      coords: { lat: s.event.location.lat, lon: s.event.location.lon },
      date: new Date(s.event.timestamp).toLocaleDateString("en-US", {
        month: "short",
        day: "2-digit",
        year: "numeric",
      }),
      maxSpeed: Math.round(s.maxSpeed * 10) / 10,
      minSpeed: Math.round(s.minSpeed * 10) / 10,
      acceleration: Math.round(s.acceleration * 1000) / 1000,
    }));

    for (const e of entries) {
      needsGeocoding.set(e.id, e.coords);
    }

    sectionNewEvents.set(section.index, entries);
  }

  // Geocode all unique locations (rate-limited)
  const uniqueCoords = Array.from(needsGeocoding.entries());
  console.log(`\nGeocoding ${uniqueCoords.length} locations...`);
  const locationMap = new Map<string, string>();
  for (let i = 0; i < uniqueCoords.length; i++) {
    const [id, coords] = uniqueCoords[i];
    if (i > 0) await delay(1100); // Nominatim rate limit
    const loc = await reverseGeocode(coords.lat, coords.lon);
    locationMap.set(id, loc);
    process.stdout.write(`  ${i + 1}/${uniqueCoords.length} ${loc}\n`);
  }

  // Fill in location names
  for (const entries of sectionNewEvents.values()) {
    for (const e of entries) {
      e.location = locationMap.get(e.id) || `${e.coords.lat.toFixed(4)}, ${e.coords.lon.toFixed(4)}`;
    }
  }

  // Build a lookup from id → ScoredEvent for logging VRU info alongside entries.
  const scoredById = new Map<string, ScoredEvent>();
  for (const list of Object.values(byType)) {
    for (const s of list) scoredById.set(s.event.id, s);
  }

  // Report
  let totalNew = 0;
  console.log("\n--- Results ---\n");
  for (const section of SECTIONS) {
    const entries = sectionNewEvents.get(section.index) || [];
    console.log(`${section.name}: ${entries.length} new events`);
    for (const e of entries) {
      const speedDrop = Math.round(e.maxSpeed - e.minSpeed);
      const scored = scoredById.get(e.id);
      const vruStr = scored && scored.vruScore > 0
        ? ` | VRU: ${scored.vruScore.toFixed(2)} [${scored.vruLabels.join(",")}]`
        : "";
      console.log(
        `  ${e.id.slice(0, 8)}… | ${e.location} | ${e.date} | speed: ${e.maxSpeed}→${e.minSpeed} (Δ${speedDrop}) | accel: ${e.acceleration} m/s²${vruStr}`
      );
    }
    totalNew += entries.length;
  }

  if (totalNew === 0) {
    console.log("\nNo new events found. Highlights are up to date!");
    return;
  }

  console.log(`\nTotal: ${totalNew} new events`);

  if (dryRun) {
    console.log("\n[DRY RUN] No changes written. Remove --dry-run to update highlights.ts");
    return;
  }

  // Write to highlights.ts
  appendToHighlights(sectionNewEvents);
  console.log("\n✅ Updated src/lib/highlights.ts");
  console.log("Run `npx tsc --noEmit` to verify, then review the diff with `git diff`.");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});

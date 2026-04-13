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

const API_BASE_URL = "https://beemaps.com/api/developer/aievents";

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

const SECTIONS: Section[] = [
  {
    index: 0,
    name: "Extreme Braking",
    types: ["HARSH_BRAKING"],
    rank: (a, b) => (b.maxSpeed - b.minSpeed) - (a.maxSpeed - a.minSpeed),
    limit: 10,
  },
  {
    index: 1,
    name: "High Speed",
    types: ["HIGH_SPEED"],
    rank: (a, b) => b.maxSpeed - a.maxSpeed,
  },
  {
    index: 2,
    name: "Highest G-Force",
    types: ["HIGH_G_FORCE"],
    rank: (a, b) => b.acceleration - a.acceleration,
  },
  {
    index: 3,
    name: "Aggressive Acceleration",
    types: ["AGGRESSIVE_ACCELERATION"],
    rank: (a, b) => b.acceleration - a.acceleration,
  },
  {
    index: 4,
    name: "Swerving",
    types: ["SWERVING"],
    rank: (a, b) => b.acceleration - a.acceleration,
  },
  {
    index: 5,
    name: "International Highlights",
    types: [...TYPES],
    rank: (a, b) => {
      const sa = (a.maxSpeed - a.minSpeed) + a.acceleration * 30 + a.maxSpeed * 0.3;
      const sb = (b.maxSpeed - b.minSpeed) + b.acceleration * 30 + b.maxSpeed * 0.3;
      return sb - sa;
    },
    filter: (s) => !isUSCoords(s.event.location.lat, s.event.location.lon),
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

function appendToHighlights(sectionNewEvents: Map<number, HighlightEntry[]>) {
  const hlPath = path.resolve(__dirname, "../src/lib/highlights.ts");
  let content = fs.readFileSync(hlPath, "utf-8");

  // For each section, find the last event entry and insert after it
  for (const [sectionIndex, entries] of sectionNewEvents) {
    if (entries.length === 0) continue;

    const section = SECTIONS[sectionIndex];
    const sectionTitle = section.name;

    // Build the new entries as TypeScript code
    const now = Date.now();
    const newCode = entries
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

    // Find the events array closing for this section by looking for the
    // section title, then finding the next `    ],` which closes the events array.
    const titleIdx = content.indexOf(`title: "${sectionTitle}"`);
    if (titleIdx < 0) {
      console.warn(`  Could not find section "${sectionTitle}" in highlights.ts, skipping`);
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
      return { event, maxSpeed, minSpeed, acceleration };
    });
  }

  // Get existing IDs to avoid duplicates
  const existingIds = getExistingIds();
  console.log(`\nExisting highlights: ${existingIds.size} events`);

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

  // Report
  let totalNew = 0;
  console.log("\n--- Results ---\n");
  for (const section of SECTIONS) {
    const entries = sectionNewEvents.get(section.index) || [];
    console.log(`${section.name}: ${entries.length} new events`);
    for (const e of entries) {
      const speedDrop = Math.round(e.maxSpeed - e.minSpeed);
      console.log(
        `  ${e.id.slice(0, 8)}… | ${e.location} | ${e.date} | speed: ${e.maxSpeed}→${e.minSpeed} (Δ${speedDrop}) | accel: ${e.acceleration} m/s²`
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

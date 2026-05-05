#!/usr/bin/env npx tsx
/**
 * Seed the "VRU Close Calls" highlights section by finding HARSH_BRAKING /
 * SWERVING events with high-confidence pedestrian / bike / stroller / wheelchair
 * detections. Outputs HighlightEvent[] TypeScript code to stdout.
 *
 * Usage:
 *   npx tsx scripts/seed-vru-highlights.ts [--limit 20] [--min-conf 0.6]
 *
 * Then paste the output into src/lib/highlights.ts inside the VRU section.
 */

import * as fs from "fs";
import * as path from "path";
import { createClient } from "@libsql/client";

function loadEnv() {
  const envPath = path.resolve(__dirname, "../.env.local");
  if (!fs.existsSync(envPath)) return;
  for (const line of fs.readFileSync(envPath, "utf-8").split("\n")) {
    const m = line.match(/^([A-Z_]+)=(.*)$/);
    if (m && !process.env[m[1]]) process.env[m[1]] = m[2];
  }
}
loadEnv();

const BEEMAPS_KEY = process.env.BEEMAPS_API_KEY!;
const AUTH = BEEMAPS_KEY.startsWith("Basic ") ? BEEMAPS_KEY : `Basic ${BEEMAPS_KEY}`;
const TURSO_URL = process.env.TURSO_DATABASE_URL!;
const TURSO_TOKEN = process.env.TURSO_AUTH_TOKEN;

const args = process.argv.slice(2);
function arg(name: string) {
  const i = args.indexOf(name);
  return i >= 0 ? args[i + 1] : undefined;
}
const LIMIT = parseInt(arg("--limit") ?? "20", 10);
const MIN_CONF = parseFloat(arg("--min-conf") ?? "0.6");
const SKIP_EXISTING = !args.includes("--include-existing");

/** Collect ids already in highlights.ts so we don't duplicate. */
function getExistingIds(): Set<string> {
  const ids = new Set<string>();
  try {
    const content = fs.readFileSync(path.resolve(__dirname, "../src/lib/highlights.ts"), "utf-8");
    for (const m of content.matchAll(/id:\s*"([a-f0-9]{24})"/g)) ids.add(m[1]);
  } catch { /* ignore */ }
  return ids;
}

/** VRU label weights — approximate "vulnerability" scaling. Heavily rare labels
 *  (stroller, wheelchair) get a small bonus but not enough to drown out person
 *  and cyclist events, which are the most common real-world close calls. */
const WEIGHT: Record<string, number> = {
  stroller: 3.5,
  wheelchair: 3.5,
  child: 3.0,
  pedestrian: 2.2,
  person: 2.0,
  bicycle: 1.5,
  scooter: 1.3,
  skateboard: 1.3,
  motorcycle: 1.0,
};
const VRU_LABELS = Object.keys(WEIGHT);

interface EventDetail {
  id: string;
  type: string;
  timestamp: string;
  location: { lat: number; lon: number };
  metadata?: Record<string, unknown>;
}

function parseSpeed(md: Record<string, unknown> | undefined) {
  const arr = md?.SPEED_ARRAY as Array<number | { AVG_SPEED_MS: number }> | undefined;
  if (!Array.isArray(arr) || arr.length === 0) return { maxSpeed: 0, minSpeed: 0 };
  const ms = arr.map((s) => (typeof s === "number" ? s : s.AVG_SPEED_MS));
  const kmh = ms.map((s) => s * 3.6);
  return { maxSpeed: Math.max(...kmh), minSpeed: Math.min(...kmh) };
}
function getAccel(md: Record<string, unknown> | undefined): number {
  const v = md?.ACCELERATION_MS2;
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

async function fetchEvent(id: string): Promise<EventDetail | null> {
  for (let attempt = 0; attempt < 5; attempt++) {
    const res = await fetch(`https://beemaps.com/api/developer/aievents/${id}`, {
      headers: { Authorization: AUTH },
    });
    if (res.ok) return await res.json();
    if (res.status === 429 || res.status === 503) {
      await new Promise((r) => setTimeout(r, Math.min(500 * Math.pow(2, attempt), 8000)));
      continue;
    }
    return null;
  }
  return null;
}

async function reverseGeocode(lat: number, lon: number): Promise<string> {
  try {
    const res = await fetch(
      `https://nominatim.openstreetmap.org/reverse?format=json&lat=${lat}&lon=${lon}&zoom=10`,
      { headers: { "User-Agent": "ai-event-videos/vru-seeder" } }
    );
    if (!res.ok) return `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    const j = await res.json();
    const a = j.address ?? {};
    const name = a.city || a.town || a.village || a.county || a.state || `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
    return a.country ? `${name}, ${a.country}` : name;
  } catch {
    return `${lat.toFixed(4)}, ${lon.toFixed(4)}`;
  }
}

async function main() {
  console.error(`Finding top ${LIMIT} VRU close calls (conf ≥ ${MIN_CONF})...`);

  const db = createClient({ url: TURSO_URL, authToken: TURSO_TOKEN });

  // Best VRU per video (peak confidence across all segments).
  const placeholders = VRU_LABELS.map(() => "?").join(",");
  const vruRes = await db.execute({
    sql: `SELECT video_id, label, MAX(max_confidence) AS max_conf
          FROM video_detection_segments
          WHERE label IN (${placeholders}) AND max_confidence >= ?
          GROUP BY video_id, label`,
    args: [...VRU_LABELS, MIN_CONF],
  });

  const existingIds = SKIP_EXISTING ? getExistingIds() : new Set<string>();
  if (SKIP_EXISTING) console.error(`  skipping ${existingIds.size} ids already in highlights.ts`);

  // Per video, keep the highest-weighted VRU.
  const perVideo = new Map<string, { label: string; conf: number; weight: number }>();
  for (const row of vruRes.rows as unknown as Array<{
    video_id: string; label: string; max_conf: number;
  }>) {
    if (existingIds.has(row.video_id)) continue;
    const w = (WEIGHT[row.label] ?? 1.0) * row.max_conf;
    const prev = perVideo.get(row.video_id);
    if (!prev || w > prev.weight) {
      perVideo.set(row.video_id, { label: row.label, conf: row.max_conf, weight: w });
    }
  }
  console.error(`  ${perVideo.size} candidate videos with VRU ≥${MIN_CONF}`);

  // Sort by weight, fetch Bee Maps details sequentially with 250ms delay.
  const sorted = Array.from(perVideo.entries()).sort((a, b) => b[1].weight - a[1].weight);

  interface Seed {
    id: string;
    type: "HARSH_BRAKING" | "SWERVING" | "HIGH_G_FORCE";
    timestamp: string;
    lat: number; lon: number;
    maxSpeed: number; minSpeed: number; acceleration: number;
    vruLabel: string; vruConfidence: number;
    score: number;
  }
  const seeds: Seed[] = [];
  let fetched = 0;
  for (const [vid, v] of sorted) {
    if (seeds.length >= LIMIT * 3) break; // collect 3× then rank
    if (fetched > LIMIT * 20) break; // safety cap
    await new Promise((r) => setTimeout(r, 250));
    fetched++;
    const ev = await fetchEvent(vid);
    if (!ev) continue;
    // VRU close-call event types — anything where the driver reacted sharply.
    if (ev.type !== "HARSH_BRAKING" && ev.type !== "SWERVING" && ev.type !== "HIGH_G_FORCE") continue;
    const { maxSpeed, minSpeed } = parseSpeed(ev.metadata);
    const accel = getAccel(ev.metadata);
    const speedDrop = maxSpeed - minSpeed;
    let severity: number;
    if (ev.type === "HARSH_BRAKING") {
      severity = speedDrop + accel * 20 + maxSpeed * 0.2;
    } else if (ev.type === "SWERVING") {
      severity = accel * 40 + speedDrop * 0.5 + maxSpeed * 0.2;
    } else {
      // HIGH_G_FORCE — acceleration dominates
      severity = accel * 50 + speedDrop * 0.3 + maxSpeed * 0.15;
    }
    const score = severity * v.weight;
    seeds.push({
      id: ev.id,
      type: ev.type,
      timestamp: ev.timestamp,
      lat: ev.location.lat, lon: ev.location.lon,
      maxSpeed: Math.round(maxSpeed * 10) / 10,
      minSpeed: Math.round(minSpeed * 10) / 10,
      acceleration: Math.round(accel * 1000) / 1000,
      vruLabel: v.label,
      vruConfidence: Math.round(v.conf * 1000) / 1000,
      score,
    });
    if (seeds.length % 5 === 0) {
      console.error(`  collected ${seeds.length} matches (${fetched} fetched)`);
    }
  }
  console.error(`  collected ${seeds.length} total matches from ${fetched} fetches`);

  seeds.sort((a, b) => b.score - a.score);
  const top = seeds.slice(0, LIMIT);

  // Geocode (1 req/s Nominatim).
  console.error(`Geocoding ${top.length} locations...`);
  const locations = new Map<string, string>();
  for (let i = 0; i < top.length; i++) {
    if (i > 0) await new Promise((r) => setTimeout(r, 1100));
    const loc = await reverseGeocode(top[i].lat, top[i].lon);
    locations.set(top[i].id, loc);
    console.error(`  ${i + 1}/${top.length} ${loc}`);
  }

  // Print TS code.
  const now = Date.now();
  console.log("// ── paste into src/lib/highlights.ts VRU section ─────────────");
  for (const s of top) {
    const dateStr = new Date(s.timestamp).toLocaleDateString("en-US", {
      month: "short", day: "2-digit", year: "numeric",
    });
    console.log(`      {`);
    console.log(`        id: "${s.id}",`);
    console.log(`        type: "${s.type}",`);
    console.log(`        location: ${JSON.stringify(locations.get(s.id) ?? "")},`);
    console.log(`        coords: { lat: ${s.lat}, lon: ${s.lon} },`);
    console.log(`        date: "${dateStr}",`);
    console.log(`        maxSpeed: ${s.maxSpeed},`);
    console.log(`        minSpeed: ${s.minSpeed},`);
    console.log(`        acceleration: ${s.acceleration},`);
    console.log(`        vruLabel: "${s.vruLabel}",`);
    console.log(`        vruConfidence: ${s.vruConfidence},`);
    console.log(`        addedAt: ${now},`);
    console.log(`      },`);
  }
}

main().catch((e) => { console.error(e); process.exit(1); });

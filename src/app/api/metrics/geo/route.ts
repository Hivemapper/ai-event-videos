import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import fs from "fs";
import path from "path";

export const runtime = "nodejs";
const GEO_CACHE_TTL_MS = 10 * 60 * 1000;

interface GeoMetricsResponse {
  countries: Array<{ country: string; count: number; pct: number }>;
  total: number;
  resolved: number;
  unresolved: number;
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let queryFn: ((point: [number, number]) => Record<string, any> | null) | null =
  null;
let geoCache: { expiresAt: number; data: GeoMetricsResponse } | null = null;

async function ensureCountryLookup() {
  if (queryFn) return;
  const whichPolygon = (await import("which-polygon" as string)).default;
  const geojsonPath = path.join(process.cwd(), "public", "data", "countries-110m.json");
  const geojson = JSON.parse(fs.readFileSync(geojsonPath, "utf-8"));
  queryFn = whichPolygon(geojson);
}

function getCountry(lat: number, lon: number): string | null {
  if (!queryFn) return null;
  const result = queryFn([lon, lat]);
  return (result?.ADMIN as string) || null;
}

export async function GET() {
  try {
    if (geoCache && geoCache.expiresAt > Date.now()) {
      return NextResponse.json(geoCache.data, {
        headers: {
          "Cache-Control": "private, max-age=300, stale-while-revalidate=600",
          "X-Metrics-Cache": "hit",
        },
      });
    }

    await ensureCountryLookup();

    const db = await getDb();

    // Ensure triage_results table exists
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
      event_timestamp TEXT,
      lat REAL,
      lon REAL,
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )`);
    await db.exec(`
      CREATE INDEX IF NOT EXISTS idx_triage_results_signal_location
        ON triage_results (triage_result, lat, lon)
    `);

    const result = await db.query(
      `SELECT ROUND(lat, 2) AS lat, ROUND(lon, 2) AS lon, COUNT(*) AS count
       FROM triage_results
       WHERE triage_result = 'signal' AND lat IS NOT NULL AND lon IS NOT NULL
       GROUP BY ROUND(lat, 2), ROUND(lon, 2)`
    );

    const countryCounts: Record<string, number> = {};
    let resolved = 0;
    let unresolved = 0;

    for (const row of result.rows as Array<{ lat: number; lon: number; count: number }>) {
      const country = getCountry(row.lat, row.lon);
      const count = Number(row.count) || 0;
      if (country) {
        countryCounts[country] = (countryCounts[country] || 0) + count;
        resolved += count;
      } else {
        unresolved += count;
      }
    }

    const total = resolved + unresolved;
    const countries = Object.entries(countryCounts)
      .map(([country, count]) => ({
        country,
        count,
        pct: total > 0 ? Math.round((count / total) * 1000) / 10 : 0,
      }))
      .sort((a, b) => b.count - a.count);

    const data = { countries, total, resolved, unresolved };
    geoCache = { data, expiresAt: Date.now() + GEO_CACHE_TTL_MS };

    return NextResponse.json(data, {
      headers: {
        "Cache-Control": "private, max-age=300, stale-while-revalidate=600",
        "X-Metrics-Cache": "miss",
      },
    });
  } catch (error) {
    console.error("Geo metrics error:", error);
    return NextResponse.json(
      { error: String(error), countries: [], total: 0 },
      { status: 500 }
    );
  }
}

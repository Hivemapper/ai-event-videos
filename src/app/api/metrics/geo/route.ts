import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import fs from "fs";
import path from "path";

export const runtime = "nodejs";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
let queryFn: ((point: [number, number]) => Record<string, any> | null) | null =
  null;

async function ensureCountryLookup() {
  if (queryFn) return;
  // eslint-disable-next-line @typescript-eslint/no-require-imports
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

    const result = await db.query(
      `SELECT lat, lon FROM triage_results WHERE triage_result = 'signal' AND lat IS NOT NULL AND lon IS NOT NULL`
    );

    const countryCounts: Record<string, number> = {};
    let resolved = 0;
    let unresolved = 0;

    for (const row of result.rows as Array<{ lat: number; lon: number }>) {
      const country = getCountry(row.lat, row.lon);
      if (country) {
        countryCounts[country] = (countryCounts[country] || 0) + 1;
        resolved++;
      } else {
        unresolved++;
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

    return NextResponse.json({ countries, total, resolved, unresolved });
  } catch (error) {
    console.error("Geo metrics error:", error);
    return NextResponse.json(
      { error: String(error), countries: [], total: 0 },
      { status: 500 }
    );
  }
}

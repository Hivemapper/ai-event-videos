import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { getTimeOfDay } from "@/lib/sun";

export const runtime = "nodejs";

interface EnrichmentResult {
  intersection: { score: number; connectors?: number; features?: string[] } | null;
  vruDetections: Array<{
    label: string;
    segments: Array<{ startMs: number; endMs: number; maxConfidence: number }>;
  }>;
  weather: { value: string; confidence: number } | null;
  road: {
    type: string | null;
    label: string | null;
    name: string | null;
    speedLimit: { value: number; unit: string } | null;
  };
  summary: string | null;
  timeOfDay: string | null;
  location: { city: string | null; country: string | null } | null;
  timeline: Array<{ startSec: number; endSec: number; event: string; details: string }> | null;
}

/** Labels that are NOT VRU detections */
const NON_VRU_LABELS = new Set([
  "car", "truck", "bus",
  "stop sign", "traffic light", "crosswalk", "traffic signal", "yield sign",
  "traffic cone",
]);

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id: videoId } = await params;
    const db = await getDb();

    // Find the latest completed detection run for this video
    const runResult = await db.query(
      `SELECT id FROM detection_runs
       WHERE video_id = ? AND status = 'completed'
       ORDER BY created_at DESC LIMIT 1`,
      [videoId]
    );
    const runId = (runResult.rows[0] as { id: string } | undefined)?.id ?? null;

    // --- Scene attributes (weather + intersection) ---
    let weather: EnrichmentResult["weather"] = null;
    let intersection: EnrichmentResult["intersection"] = null;
    if (runId) {
      const sceneResult = await db.query(
        `SELECT attribute, value, confidence FROM scene_attributes
         WHERE video_id = ? AND run_id = ?`,
        [videoId, runId]
      );
      for (const row of sceneResult.rows as Array<{
        attribute: string;
        value: string;
        confidence: number | null;
      }>) {
        if (row.attribute === "weather" && row.confidence !== null) {
          weather = { value: row.value, confidence: row.confidence };
        }
        if (row.attribute === "intersection" && row.confidence !== null) {
          intersection = { score: row.confidence };
        }
      }
    }

    // --- VRU detection segments ---
    const vruDetections: EnrichmentResult["vruDetections"] = [];
    if (runId) {
      const segResult = await db.query(
        `SELECT label, start_ms, end_ms, max_confidence
         FROM video_detection_segments
         WHERE video_id = ? AND run_id = ?
         ORDER BY label ASC, start_ms ASC`,
        [videoId, runId]
      );
      const byLabel = new Map<
        string,
        Array<{ startMs: number; endMs: number; maxConfidence: number }>
      >();
      for (const row of segResult.rows as Array<{
        label: string;
        start_ms: number;
        end_ms: number;
        max_confidence: number;
      }>) {
        if (NON_VRU_LABELS.has(row.label)) continue;
        const arr = byLabel.get(row.label) ?? [];
        arr.push({
          startMs: row.start_ms,
          endMs: row.end_ms,
          maxConfidence: row.max_confidence,
        });
        byLabel.set(row.label, arr);
      }
      for (const [label, segments] of byLabel) {
        vruDetections.push({ label, segments });
      }
    }

    // --- Road type + name (from Mapbox, query internally) ---
    let roadType: string | null = null;
    let roadLabel: string | null = null;
    let roadName: string | null = null;

    const { searchParams } = new URL(request.url);
    const lat = searchParams.get("lat");
    const lon = searchParams.get("lon");

    const mapboxToken = searchParams.get("mapboxToken") ?? process.env.NEXT_PUBLIC_MAPBOX_TOKEN;

    if (lat && lon) {
      try {
        const roadUrl = mapboxToken
          ? `${request.nextUrl.origin}/api/road-type?lat=${lat}&lon=${lon}&token=${mapboxToken}`
          : `${request.nextUrl.origin}/api/road-type?lat=${lat}&lon=${lon}`;
        const roadResp = await fetch(roadUrl);
        if (roadResp.ok) {
          const roadData = await roadResp.json();
          roadType = roadData.class ?? null;
          roadLabel = roadData.classLabel ?? null;
          roadName = roadData.name ?? null;
        }
      } catch { /* ignore */ }
    }

    // --- Speed limit (from BeeMaps map-features) ---
    let speedLimit: { value: number; unit: string } | null = null;
    if (lat && lon) {
      try {
        const apiKey = request.headers.get("Authorization") ?? "";
        const featResp = await fetch(
          `${request.nextUrl.origin}/api/map-features?lat=${lat}&lon=${lon}&radius=200`,
          { headers: { Authorization: apiKey } }
        );
        if (featResp.ok) {
          const featData = await featResp.json();
          const speedSigns = (featData.features ?? []).filter(
            (f: { class: string; speedLimit?: number }) =>
              f.class === "speed-sign" && f.speedLimit !== undefined
          );
          if (speedSigns.length > 0) {
            speedLimit = {
              value: speedSigns[0].speedLimit,
              unit: speedSigns[0].unit || "mph",
            };
          }
        }
      } catch { /* ignore */ }
    }

    // --- Time of day ---
    let timeOfDay: string | null = null;
    const timestamp = searchParams.get("timestamp");
    if (timestamp && lat && lon) {
      try {
        const sunInfo = getTimeOfDay(timestamp, parseFloat(lat), parseFloat(lon));
        timeOfDay = sunInfo.timeOfDay;
      } catch { /* ignore */ }
    }

    // --- Location (city, country) via Mapbox geocoding ---
    let location: EnrichmentResult["location"] = null;
    if (lat && lon) {
      try {
        if (mapboxToken) {
          const geoResp = await fetch(
            `https://api.mapbox.com/geocoding/v5/mapbox.places/${lon},${lat}.json?types=place,country&access_token=${mapboxToken}`
          );
          if (geoResp.ok) {
            const geoData = await geoResp.json();
            const features = geoData.features ?? [];
            const city = features.find((f: { place_type: string[] }) =>
              f.place_type?.includes("place")
            )?.text ?? null;
            const country = features.find((f: { place_type: string[] }) =>
              f.place_type?.includes("country")
            )?.text ?? null;
            location = { city, country };
          }
        }
      } catch { /* ignore */ }
    }

    // --- Clip summary ---
    let summary: string | null = null;
    const summaryResult = await db.query(
      `SELECT summary FROM clip_summaries WHERE video_id = ?`,
      [videoId]
    );
    const summaryRow = summaryResult.rows[0] as { summary: string } | undefined;
    if (summaryRow) {
      summary = summaryRow.summary;
    }

    // --- Timeline ---
    let timeline: EnrichmentResult["timeline"] = null;
    if (runId) {
      const tlResult = await db.query(
        `SELECT timeline_json FROM clip_timelines WHERE video_id = ? AND run_id = ? ORDER BY created_at DESC LIMIT 1`,
        [videoId, runId]
      );
      const tlRow = tlResult.rows[0] as { timeline_json: string } | undefined;
      if (tlRow) {
        try {
          timeline = JSON.parse(tlRow.timeline_json);
        } catch { /* ignore */ }
      }
    }

    const enrichment: EnrichmentResult = {
      intersection,
      vruDetections,
      weather,
      road: {
        type: roadType,
        label: roadLabel,
        name: roadName,
        speedLimit,
      },
      summary,
      timeOfDay,
      location,
      timeline,
    };

    return NextResponse.json(enrichment, {
      headers: {
        "Cache-Control": "public, max-age=60, stale-while-revalidate=300",
      },
    });
  } catch (error) {
    console.error("Enrichment error:", error);
    return NextResponse.json(
      { error: error instanceof Error ? error.message : "Internal server error" },
      { status: 500 }
    );
  }
}

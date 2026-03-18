import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { cocoExportSchema } from "@/lib/schemas";

interface DbFrameDetectionRow {
  id: number;
  video_id: string;
  frame_ms: number;
  label: string;
  x_min: number;
  y_min: number;
  x_max: number;
  y_max: number;
  confidence: number;
  frame_width: number;
  frame_height: number;
  pipeline_version: string;
  created_at: string;
}

export function GET(request: NextRequest) {
  const params = cocoExportSchema.safeParse({
    day: request.nextUrl.searchParams.get("day"),
  });

  if (!params.success) {
    return NextResponse.json(
      { error: params.error.issues.map((i) => i.message).join(", ") },
      { status: 400 }
    );
  }

  const { day } = params.data;
  const db = getDb();

  const rows = db
    .prepare(
      `SELECT fd.*
       FROM frame_detections fd
       JOIN video_pipeline_state vps ON fd.video_id = vps.video_id
       WHERE vps.day = ?
         AND vps.status = 'processed'
       ORDER BY fd.video_id, fd.frame_ms, fd.id
       LIMIT 500000`
    )
    .all(day) as DbFrameDetectionRow[];

  // Build deterministic category map (sorted alphabetically)
  const categoryNames = [...new Set(rows.map((r) => r.label))].sort();
  const categoryIdMap = new Map<string, number>();
  categoryNames.forEach((name, idx) => {
    categoryIdMap.set(name, idx + 1);
  });

  // Build image list: one per unique (video_id, frame_ms) pair
  const imageKey = (videoId: string, frameMs: number) =>
    `${videoId}_${frameMs}`;
  const imageIdMap = new Map<string, number>();
  const images: Array<{
    id: number;
    file_name: string;
    width: number;
    height: number;
  }> = [];

  let nextImageId = 1;
  for (const row of rows) {
    const key = imageKey(row.video_id, row.frame_ms);
    if (!imageIdMap.has(key)) {
      const id = nextImageId++;
      imageIdMap.set(key, id);
      images.push({
        id,
        file_name: `${row.video_id}_frame_${row.frame_ms}.jpg`,
        width: row.frame_width,
        height: row.frame_height,
      });
    }
  }

  // Build annotations
  const annotations = rows.map((row, idx) => {
    const imgId = imageIdMap.get(imageKey(row.video_id, row.frame_ms))!;
    const catId = categoryIdMap.get(row.label)!;
    const x = row.x_min;
    const y = row.y_min;
    const w = row.x_max - row.x_min;
    const h = row.y_max - row.y_min;

    return {
      id: idx + 1,
      image_id: imgId,
      category_id: catId,
      bbox: [x, y, w, h],
      segmentation: [[x, y, x + w, y, x + w, y + h, x, y + h]],
      area: w * h,
      iscrowd: 0,
      score: row.confidence,
    };
  });

  // Build categories
  const supercategoryMap: Record<string, string> = {
    pedestrian: "vru",
    animal: "vru",
    motorcycle: "vru",
    bicycle: "vru",
    kids: "vru",
    wheelchair: "vru",
    scooter: "vru",
    skateboard: "vru",
    "work-zone-person": "vru",
    vehicle: "vehicle",
    traffic_light: "traffic_infrastructure",
    stop_sign: "traffic_infrastructure",
  };

  const categories = categoryNames.map((name) => ({
    id: categoryIdMap.get(name)!,
    name,
    supercategory: supercategoryMap[name] ?? "other",
  }));

  const coco = {
    info: {
      description: "Bee Maps AI Event Detections",
      date_created: new Date().toISOString(),
    },
    images,
    annotations,
    categories,
  };

  return NextResponse.json(coco);
}

import { NextRequest, NextResponse } from "next/server";
import { getDb } from "@/lib/db";
import { cocoExportSchema } from "@/lib/schemas";

export async function GET(request: NextRequest) {
  const params = cocoExportSchema.safeParse({
    day: request.nextUrl.searchParams.get("day"),
    model: request.nextUrl.searchParams.get("model") ?? undefined,
  });

  if (!params.success) {
    return NextResponse.json(
      { error: params.error.issues.map((i) => i.message).join(", ") },
      { status: 400 }
    );
  }

  const { day, model } = params.data;
  const db = await getDb();

  const result = model
    ? await db.execute({
        sql: `SELECT fd.*
              FROM frame_detections fd
              JOIN video_pipeline_state vps ON fd.video_id = vps.video_id
              WHERE vps.day = ?
                AND vps.status = 'processed'
                AND fd.model_name = ?
              ORDER BY fd.video_id, fd.frame_ms, fd.id
              LIMIT 500000`,
        args: [day, model],
      })
    : await db.execute({
        sql: `SELECT fd.*
              FROM frame_detections fd
              JOIN video_pipeline_state vps ON fd.video_id = vps.video_id
              WHERE vps.day = ?
                AND vps.status = 'processed'
              ORDER BY fd.video_id, fd.frame_ms, fd.id
              LIMIT 500000`,
        args: [day],
      });

  const rows = result.rows;

  // Build deterministic category map (sorted alphabetically)
  const categoryNames = [...new Set(rows.map((r) => String(r.label)))].sort();
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
    const key = imageKey(String(row.video_id), Number(row.frame_ms));
    if (!imageIdMap.has(key)) {
      const id = nextImageId++;
      imageIdMap.set(key, id);
      images.push({
        id,
        file_name: `${row.video_id}_frame_${row.frame_ms}.jpg`,
        width: Number(row.frame_width),
        height: Number(row.frame_height),
      });
    }
  }

  // Build annotations
  const annotations = rows.map((row, idx) => {
    const imgId = imageIdMap.get(imageKey(String(row.video_id), Number(row.frame_ms)))!;
    const catId = categoryIdMap.get(String(row.label))!;
    const x = Number(row.x_min);
    const y = Number(row.y_min);
    const w = Number(row.x_max) - x;
    const h = Number(row.y_max) - y;

    return {
      id: idx + 1,
      image_id: imgId,
      category_id: catId,
      bbox: [x, y, w, h],
      segmentation: [[x, y, x + w, y, x + w, y + h, x, y + h]],
      area: w * h,
      iscrowd: 0,
      score: Number(row.confidence),
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

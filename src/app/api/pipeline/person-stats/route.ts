import { NextResponse } from "next/server";
import { getDb } from "@/lib/db";

export const runtime = "nodejs";

export async function GET() {
  const db = await getDb();

  const result = await db.query(`
    SELECT
      (SELECT COUNT(DISTINCT video_id) FROM frame_detections
       WHERE label IN ('person', 'construction worker', 'pedestrian')
      ) as videos_with_persons,
      (SELECT COUNT(*) FROM frame_detections
       WHERE label IN ('person', 'construction worker', 'pedestrian')
      ) as total_person_detections,
      (SELECT COUNT(DISTINCT video_id) FROM frame_detections) as total_videos_with_detections
  `);

  const row = result.rows[0] as Record<string, number>;

  return NextResponse.json({
    videosWithPersons: Number(row.videos_with_persons ?? 0),
    totalPersonDetections: Number(row.total_person_detections ?? 0),
    totalVideosWithDetections: Number(row.total_videos_with_detections ?? 0),
  });
}

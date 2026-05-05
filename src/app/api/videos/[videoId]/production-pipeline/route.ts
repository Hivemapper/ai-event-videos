import { NextResponse } from "next/server";
import {
  enqueueCompletedVruForProduction,
  getProductionRunByVideoId,
} from "@/lib/pipeline-store";
import { syncClippedEventAssetsForProductionAws } from "@/lib/clipped-event-assets";

export const runtime = "nodejs";

export async function GET(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;

  try {
    const run = await getProductionRunByVideoId(videoId);
    return NextResponse.json({ run });
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return NextResponse.json({ error: message }, { status: 500 });
  }
}

export async function POST(
  _request: Request,
  { params }: { params: Promise<{ videoId: string }> }
) {
  const { videoId } = await params;
  let localAssetSync: Awaited<
    ReturnType<typeof syncClippedEventAssetsForProductionAws>
  > | null = null;

  try {
    localAssetSync = await syncClippedEventAssetsForProductionAws(videoId);
    if (localAssetSync.skippedReason === "no-production-hosts") {
      return NextResponse.json(
        {
          error:
            "Local clipped event assets exist, but no AWS production hosts were configured or discovered",
          localAssetSync,
        },
        { status: 500 }
      );
    }

    const result = await enqueueCompletedVruForProduction(videoId);
    return NextResponse.json(
      { ...result, localAssetSync },
      { status: result.created ? 201 : 200 }
    );
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    const status = message.includes("completed VRU") ? 400 : 500;
    return NextResponse.json({ error: message }, { status });
  }
}

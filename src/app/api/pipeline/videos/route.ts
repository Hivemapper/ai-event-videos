import { NextRequest, NextResponse } from "next/server";
import { fetchAllBeeMapsEventsForDay } from "@/lib/beemaps";
import {
  getActivePipelineRun,
  listPipelineRuns,
  listVideoPipelineStatesForDay,
  summarizeVideoStates,
} from "@/lib/pipeline-store";
import { PipelineVideoRow, VideoPipelineState } from "@/types/pipeline";

export const runtime = "nodejs";

function mapStateByVideo(states: VideoPipelineState[]) {
  return new Map(states.map((state) => [state.videoId, state]));
}

export async function GET(request: NextRequest) {
  const day = request.nextUrl.searchParams.get("day");
  const apiKey =
    request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY || "";

  if (!day) {
    return NextResponse.json({ error: "day is required" }, { status: 400 });
  }

  if (!apiKey) {
    return NextResponse.json(
      { error: "Bee Maps API key is required" },
      { status: 401 }
    );
  }

  try {
    const [events, states, runs] = await Promise.all([
      fetchAllBeeMapsEventsForDay({ apiKey, day }),
      Promise.resolve(listVideoPipelineStatesForDay(day)),
      Promise.resolve(listPipelineRuns(day)),
    ]);

    const stateMap = mapStateByVideo(states);
    const videos: PipelineVideoRow[] = events.map((event) => {
      const state = stateMap.get(event.id);
      return {
        videoId: event.id,
        timestamp: event.timestamp,
        type: event.type,
        videoUrl: event.videoUrl,
        status: state?.status ?? "unprocessed",
        labelsApplied: state?.labelsApplied ?? [],
        pipelineVersion: state?.pipelineVersion ?? null,
        modelName: state?.modelName ?? null,
        completedAt: state?.completedAt ?? null,
        lastError: state?.lastError ?? null,
      };
    });

    const stateSummary = summarizeVideoStates(
      videos.map((video) => ({
        videoId: video.videoId,
        day,
        status: video.status,
        pipelineVersion: video.pipelineVersion ?? "",
        modelName: video.modelName,
        labelsApplied: video.labelsApplied,
        queuedAt: null,
        startedAt: null,
        completedAt: video.completedAt,
        lastHeartbeatAt: null,
        lastError: video.lastError,
      }))
    );

    return NextResponse.json({
      day,
      videos,
      summary: {
        total: videos.length,
        processed: stateSummary.processed,
        failed: stateSummary.failed,
        stale: stateSummary.stale,
        running: stateSummary.running,
        queued: stateSummary.queued,
        remaining:
          stateSummary.unprocessed +
          stateSummary.queued +
          stateSummary.running +
          stateSummary.failed +
          stateSummary.stale,
      },
      latestRun: runs[0] ?? null,
      activeRun:
        runs.find((run) => ["queued", "running", "paused"].includes(run.status)) ??
        getActivePipelineRun(),
    });
  } catch (error) {
    return NextResponse.json(
      {
        error:
          error instanceof Error
            ? error.message
            : "Failed to load pipeline videos",
      },
      { status: 500 }
    );
  }
}

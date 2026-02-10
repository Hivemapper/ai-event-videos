import {
  ActorDetectionResult,
  ActorTrack,
  ActorTrackingResult,
  ActorObservation,
} from "@/types/actors";
import { haversineDistance } from "@/lib/geo-utils";

// Re-export for consumers that import from this module
export { haversineDistance } from "@/lib/geo-utils";

const TRACK_COLORS = [
  "#3b82f6", // blue
  "#ef4444", // red
  "#10b981", // emerald
  "#f59e0b", // amber
  "#8b5cf6", // violet
  "#ec4899", // pink
  "#06b6d4", // cyan
  "#f97316", // orange
  "#14b8a6", // teal
  "#6366f1", // indigo
];

const MAX_MATCH_DISTANCE_M = 30;
const LABEL_SIMILARITY_WEIGHT = 0.3;

/** Word-overlap similarity between two labels (0–1). */
export function labelSimilarity(a: string, b: string): number {
  const wordsA = new Set(a.toLowerCase().split(/\s+/));
  const wordsB = new Set(b.toLowerCase().split(/\s+/));
  if (wordsA.size === 0 && wordsB.size === 0) return 1;
  let overlap = 0;
  for (const w of wordsA) {
    if (wordsB.has(w)) overlap++;
  }
  const union = new Set([...wordsA, ...wordsB]).size;
  return union === 0 ? 0 : overlap / union;
}

interface OpenTrack {
  trackId: string;
  type: string;
  label: string;
  lastObservation: ActorObservation;
  observations: ActorObservation[];
}

/**
 * Build actor tracks from multi-frame detection results using greedy
 * bipartite matching across consecutive frames.
 */
export function buildTracks(
  frameResults: ActorDetectionResult[],
  eventId: string
): ActorTrackingResult {
  const sorted = [...frameResults].sort((a, b) => a.timestamp - b.timestamp);
  const openTracks: OpenTrack[] = [];
  const closedTracks: OpenTrack[] = [];
  let nextId = 1;

  for (const frame of sorted) {
    const observations: { actor: (typeof frame.actors)[0]; obs: ActorObservation }[] =
      frame.actors.map((actor) => ({
        actor,
        obs: {
          timestamp: frame.timestamp,
          worldPosition: actor.worldPosition,
          bbox: actor.bbox,
          confidence: actor.confidence,
          description: actor.description,
        },
      }));

    const matched = new Set<number>(); // indices into openTracks
    const usedObs = new Set<number>(); // indices into observations

    // Greedy matching: compute all costs, sort, assign greedily
    const candidates: { trackIdx: number; obsIdx: number; cost: number }[] = [];

    for (let ti = 0; ti < openTracks.length; ti++) {
      const track = openTracks[ti];
      for (let oi = 0; oi < observations.length; oi++) {
        const { actor, obs } = observations[oi];
        // Hard gate: same type
        if (actor.type !== track.type) continue;

        const dist = haversineDistance(
          track.lastObservation.worldPosition.lat,
          track.lastObservation.worldPosition.lon,
          obs.worldPosition.lat,
          obs.worldPosition.lon
        );
        if (dist > MAX_MATCH_DISTANCE_M) continue;

        // Cost: distance with label similarity bonus (lower = better match)
        const sim = labelSimilarity(track.label, actor.label);
        const cost = dist - sim * LABEL_SIMILARITY_WEIGHT * MAX_MATCH_DISTANCE_M;
        candidates.push({ trackIdx: ti, obsIdx: oi, cost });
      }
    }

    candidates.sort((a, b) => a.cost - b.cost);

    for (const { trackIdx, obsIdx } of candidates) {
      if (matched.has(trackIdx) || usedObs.has(obsIdx)) continue;
      matched.add(trackIdx);
      usedObs.add(obsIdx);

      const track = openTracks[trackIdx];
      const { obs } = observations[obsIdx];
      track.observations.push(obs);
      track.lastObservation = obs;
    }

    // Close unmatched tracks
    for (let ti = openTracks.length - 1; ti >= 0; ti--) {
      if (!matched.has(ti)) {
        closedTracks.push(openTracks[ti]);
        openTracks.splice(ti, 1);
      }
    }

    // Start new tracks for unmatched observations
    for (let oi = 0; oi < observations.length; oi++) {
      if (usedObs.has(oi)) continue;
      const { actor, obs } = observations[oi];
      openTracks.push({
        trackId: `track-${nextId++}`,
        type: actor.type,
        label: actor.label,
        lastObservation: obs,
        observations: [obs],
      });
    }
  }

  // Close remaining open tracks
  closedTracks.push(...openTracks);

  // Convert to ActorTrack with colors
  const tracks: ActorTrack[] = closedTracks.map((t, i) => ({
    trackId: t.trackId,
    type: t.type as ActorTrack["type"],
    label: t.label,
    color: TRACK_COLORS[i % TRACK_COLORS.length],
    observations: t.observations,
    firstSeen: t.observations[0].timestamp,
    lastSeen: t.observations[t.observations.length - 1].timestamp,
  }));

  // Sort by firstSeen
  tracks.sort((a, b) => a.firstSeen - b.firstSeen);

  return {
    tracks,
    keyframeTimestamps: sorted.map((f) => f.timestamp),
    frameResults: sorted,
    eventId,
  };
}

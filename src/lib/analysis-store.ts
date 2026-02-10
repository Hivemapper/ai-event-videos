import { VideoAnalysis } from "@/types/analysis";

const STORAGE_PREFIX = "video-analysis-";

interface CachedAnalysis {
  analysis: VideoAnalysis;
  eventId: string;
  analyzedAt: string;
  frameTimestamps: number[];
}

export function getCachedAnalysis(eventId: string): VideoAnalysis | null {
  if (typeof window === "undefined") return null;
  try {
    const stored = localStorage.getItem(`${STORAGE_PREFIX}${eventId}`);
    if (!stored) return null;
    const cached: CachedAnalysis = JSON.parse(stored);
    return cached.analysis;
  } catch {
    return null;
  }
}

export function setCachedAnalysis(
  eventId: string,
  data: CachedAnalysis
): void {
  if (typeof window === "undefined") return;
  try {
    localStorage.setItem(`${STORAGE_PREFIX}${eventId}`, JSON.stringify(data));
  } catch {
    // localStorage full
  }
}

export function getAllCachedAnalyses(): Record<string, VideoAnalysis> {
  if (typeof window === "undefined") return {};
  const result: Record<string, VideoAnalysis> = {};

  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i);
    if (!key?.startsWith(STORAGE_PREFIX)) continue;

    try {
      const stored = localStorage.getItem(key);
      if (!stored) continue;
      const cached: CachedAnalysis = JSON.parse(stored);
      result[cached.eventId] = cached.analysis;
    } catch {
      // Skip corrupt entries
    }
  }

  return result;
}

export interface AnalysisFilters {
  weather?: string[];
  lighting?: string[];
  visibility?: string[];
  setting?: string[];
  roadSurface?: string[];
  hazardSeverity?: string[];
  hasNearMiss?: boolean;
  hasPedestrians?: boolean;
  hasCyclists?: boolean;
  hasTrafficLight?: boolean;
  hasStopSign?: boolean;
  hasSpeedLimit?: boolean;
  hasVisibilityIssues?: boolean;
  minLanes?: number;
  drivingAssessment?: string[];
}

export function matchesAnalysisFilters(
  analysis: VideoAnalysis,
  filters: AnalysisFilters
): boolean {
  if (
    filters.weather?.length &&
    analysis.environment.weather &&
    !filters.weather.includes(analysis.environment.weather)
  ) {
    return false;
  }

  if (
    filters.lighting?.length &&
    analysis.environment.lighting &&
    !filters.lighting.includes(analysis.environment.lighting)
  ) {
    return false;
  }

  if (
    filters.visibility?.length &&
    analysis.environment.visibility &&
    !filters.visibility.includes(analysis.environment.visibility)
  ) {
    return false;
  }

  if (
    filters.setting?.length &&
    analysis.environment.setting &&
    !filters.setting.includes(analysis.environment.setting)
  ) {
    return false;
  }

  if (
    filters.roadSurface?.length &&
    analysis.road.surface &&
    !filters.roadSurface.includes(analysis.road.surface)
  ) {
    return false;
  }

  if (
    filters.hazardSeverity?.length &&
    !filters.hazardSeverity.includes(analysis.hazard.severity)
  ) {
    return false;
  }

  if (filters.hasNearMiss === true && !analysis.hazard.hasNearMiss) {
    return false;
  }

  if (filters.hasPedestrians === true) {
    const hasPed = analysis.objects.some((o) => o.type === "pedestrian");
    if (!hasPed) return false;
  }

  if (filters.hasCyclists === true) {
    const hasCyclist = analysis.objects.some((o) => o.type === "cyclist");
    if (!hasCyclist) return false;
  }

  if (
    filters.minLanes &&
    analysis.road.lanes !== null &&
    analysis.road.lanes < filters.minLanes
  ) {
    return false;
  }

  if (
    filters.drivingAssessment?.length &&
    !filters.drivingAssessment.includes(analysis.driving.assessment)
  ) {
    return false;
  }

  if (filters.hasTrafficLight === true) {
    const hasLight = (analysis.signage || []).some((s) => s.type === "traffic_light");
    if (!hasLight) return false;
  }

  if (filters.hasStopSign === true) {
    const hasStop = (analysis.signage || []).some((s) => s.type === "stop_sign");
    if (!hasStop) return false;
  }

  if (filters.hasSpeedLimit === true) {
    const hasLimit = (analysis.signage || []).some((s) => s.type === "speed_limit");
    if (!hasLimit) return false;
  }

  if (filters.hasVisibilityIssues === true) {
    if (!analysis.visibilityIssues || analysis.visibilityIssues.length === 0) return false;
  }

  return true;
}

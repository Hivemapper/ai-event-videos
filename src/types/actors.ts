export type ActorType =
  | "car"
  | "truck"
  | "suv"
  | "van"
  | "bus"
  | "motorcycle"
  | "bicycle"
  | "pedestrian"
  | "animal"
  | "scooter"
  | "other_vehicle"
  | "other";

export interface DetectedActor {
  type: ActorType;
  label: string;
  confidence: "high" | "medium" | "low";
  bbox: { x_min: number; y_min: number; x_max: number; y_max: number };
  estimatedDistanceMeters: number;
  moving: boolean | null;
  description: string;
  worldPosition: { lat: number; lon: number };
  bearingFromCamera: number;
}

export interface ActorDetectionResult {
  actors: DetectedActor[];
  timestamp: number;
  cameraPosition: { lat: number; lon: number };
  cameraBearing: number;
  fovDegrees: number;
  detectedAt: string;
}

// --- Tracking types ---

export interface ActorObservation {
  timestamp: number;
  worldPosition: { lat: number; lon: number };
  bbox: { x_min: number; y_min: number; x_max: number; y_max: number };
  confidence: "high" | "medium" | "low";
  description: string;
}

export interface ActorTrack {
  trackId: string;
  type: ActorType;
  label: string;
  color: string;
  observations: ActorObservation[];
  firstSeen: number;
  lastSeen: number;
}

export interface ActorTrackingResult {
  tracks: ActorTrack[];
  keyframeTimestamps: number[];
  frameResults: ActorDetectionResult[];
  eventId: string;
}

export interface TrackingProgress {
  currentFrame: number;
  totalFrames: number;
  status: "detecting" | "matching" | "done" | "error";
  message: string;
}

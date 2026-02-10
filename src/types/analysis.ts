export interface RoadCharacteristics {
  roadType: string | null;               // "residential street", "highway", "rural two-lane", "intersection"
  lanes: number | null;
  laneWidth: "narrow" | "standard" | "wide" | null;
  surface: "paved" | "gravel" | "dirt" | "wet" | "snow_covered" | null;
  markings: "clear" | "faded" | "absent" | null;
  curvature: "straight" | "gentle_curve" | "sharp_curve" | null;
  grade: "flat" | "uphill" | "downhill" | null;
  shoulder: boolean | null;
  median: boolean | null;
  crosswalk: boolean | null;
  intersection: boolean | null;
  intersectionType?: "signalized" | "stop_controlled" | "yield" | "uncontrolled" | "roundabout";
}

export interface RoadSign {
  type: "speed_limit" | "stop_sign" | "yield" | "traffic_light" | "warning" | "regulatory" | "guide" | "construction" | "other";
  state?: "red" | "yellow" | "green" | "flashing" | "off";   // for traffic lights
  value: string;           // full text: "EXIT 25", "Speed Limit 65", "I-280 South / San Jose", "STOP", etc.
  position: "ahead" | "left" | "right" | "overhead" | "roadside";
  estimatedDistance: "very_close" | "close" | "medium" | "far";
  description: string;
}

export interface VisibilityIssue {
  type: "sun_glare" | "rain" | "snow" | "fog" | "darkness" | "dirty_windshield" | "headlight_glare" | "spray" | "smoke" | "other";
  severity: "mild" | "moderate" | "severe";
  description: string;
}

export interface EnvironmentConditions {
  weather: "clear" | "cloudy" | "rainy" | "foggy" | "snowy" | "overcast" | null;
  lighting: "daylight" | "dawn" | "dusk" | "night_lit" | "night_dark" | null;
  visibility: "excellent" | "good" | "moderate" | "poor" | null;
  setting: "urban" | "suburban" | "rural" | "highway" | "industrial" | "parking" | null;
  glare: boolean | null;
}

export interface DetectedObject {
  type: "vehicle" | "pedestrian" | "cyclist" | "animal" | "debris" | "construction" | "other";
  subtype?: string;       // "sedan", "truck", "SUV", "pickup", "motorcycle", "bus", "child", "adult"
  position: "ahead" | "left" | "right" | "oncoming" | "crossing" | "adjacent" | "behind";
  estimatedDistance: "very_close" | "close" | "medium" | "far";
  relevance: "high" | "medium" | "low";
  moving: boolean | null;
  description: string;
}

export interface HazardAssessment {
  hasNearMiss: boolean;
  nearMissType?: string;
  severity: "none" | "low" | "moderate" | "high" | "critical";
  hazardType?: string;
  contributingFactors: string[];
}

export interface DrivingBehavior {
  assessment: "normal" | "cautious" | "aggressive" | "erratic" | "emergency";
  speedContext: string;
  brakingContext?: string;
  steeringContext?: string;
}

export interface VideoAnalysis {
  summary: string;
  road: RoadCharacteristics;
  environment: EnvironmentConditions;
  objects: DetectedObject[];
  signage: RoadSign[];
  visibilityIssues: VisibilityIssue[];
  hazard: HazardAssessment;
  driving: DrivingBehavior;
  confidence: "high" | "medium" | "low";
  frameNotes: string[];
}

export interface ChatMessage {
  role: "user" | "assistant";
  content: string;
}

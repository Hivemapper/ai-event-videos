import {
  AlertTriangle,
  Zap,
  RotateCcw,
  Gauge,
  Activity,
  Octagon,
  CircleDot,
  Car,
  Hand,
  HelpCircle,
  LucideIcon,
} from "lucide-react";
import { AIEventType } from "@/types/events";

export interface EventTypeConfig {
  label: string;
  color: string;
  bgColor: string;
  borderColor: string;
  icon: LucideIcon;
  gradient: string;
}

export const EVENT_TYPE_CONFIG: Record<AIEventType, EventTypeConfig> = {
  HARSH_BRAKING: {
    label: "Harsh Braking",
    color: "text-red-600",
    bgColor: "bg-red-100",
    borderColor: "border-red-200",
    icon: AlertTriangle,
    gradient: "from-red-500 to-red-700",
  },
  AGGRESSIVE_ACCELERATION: {
    label: "Aggressive Acceleration",
    color: "text-orange-600",
    bgColor: "bg-orange-100",
    borderColor: "border-orange-200",
    icon: Zap,
    gradient: "from-orange-500 to-orange-700",
  },
  SWERVING: {
    label: "Swerving",
    color: "text-yellow-600",
    bgColor: "bg-yellow-100",
    borderColor: "border-yellow-200",
    icon: RotateCcw,
    gradient: "from-yellow-500 to-yellow-700",
  },
  HIGH_SPEED: {
    label: "High Speed",
    color: "text-purple-600",
    bgColor: "bg-purple-100",
    borderColor: "border-purple-200",
    icon: Gauge,
    gradient: "from-purple-500 to-purple-700",
  },
  HIGH_G_FORCE: {
    label: "High G-Force",
    color: "text-pink-600",
    bgColor: "bg-pink-100",
    borderColor: "border-pink-200",
    icon: Activity,
    gradient: "from-pink-500 to-pink-700",
  },
  STOP_SIGN_VIOLATION: {
    label: "Stop Sign Violation",
    color: "text-red-700",
    bgColor: "bg-red-50",
    borderColor: "border-red-300",
    icon: Octagon,
    gradient: "from-red-600 to-red-800",
  },
  TRAFFIC_LIGHT_VIOLATION: {
    label: "Traffic Light Violation",
    color: "text-amber-600",
    bgColor: "bg-amber-100",
    borderColor: "border-amber-200",
    icon: CircleDot,
    gradient: "from-amber-500 to-amber-700",
  },
  TAILGATING: {
    label: "Tailgating",
    color: "text-blue-600",
    bgColor: "bg-blue-100",
    borderColor: "border-blue-200",
    icon: Car,
    gradient: "from-blue-500 to-blue-700",
  },
  MANUAL_REQUEST: {
    label: "Manual Request",
    color: "text-green-600",
    bgColor: "bg-green-100",
    borderColor: "border-green-200",
    icon: Hand,
    gradient: "from-green-500 to-green-700",
  },
  UNKNOWN: {
    label: "Unknown",
    color: "text-gray-600",
    bgColor: "bg-gray-100",
    borderColor: "border-gray-200",
    icon: HelpCircle,
    gradient: "from-gray-500 to-gray-700",
  },
};

export const ALL_EVENT_TYPES: AIEventType[] = [
  "HARSH_BRAKING",
  "AGGRESSIVE_ACCELERATION",
  "SWERVING",
  "HIGH_SPEED",
  "HIGH_G_FORCE",
  "STOP_SIGN_VIOLATION",
  "TRAFFIC_LIGHT_VIOLATION",
  "TAILGATING",
  "MANUAL_REQUEST",
  "UNKNOWN",
];

export const API_BASE_URL = "https://beemaps.com/api/developer/aievents";
export const MAX_DATE_RANGE_DAYS = 90;

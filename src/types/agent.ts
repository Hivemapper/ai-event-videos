import { AIEvent, AIEventType } from "./events";
import { TimeOfDay } from "@/lib/sun";

export interface AgentFilterResponse {
  startDate?: string;
  endDate?: string;
  types?: AIEventType[];
  timeOfDay?: TimeOfDay[];
  coordinates?: {
    lat: number;
    lon: number;
  };
  radius?: number;
  explanation: string;
}

export interface AgentRequest {
  query: string;
}

export type AgentApiResult =
  | { success: true; filters: AgentFilterResponse; events: AIEvent[]; totalCount: number }
  | { success: false; error: string };

export interface ScanMatch {
  eventId: string;
  match: boolean;
  confidence: "high" | "medium" | "low";
  reason: string;
}

export type ChatMessage =
  | { role: "user"; content: string }
  | { role: "assistant"; content: string; events: AIEvent[]; totalCount: number; filters?: AgentFilterResponse }
  | { role: "scan"; query: string; matches: ScanMatch[]; eventsScanned: number }
  | { role: "error"; content: string };

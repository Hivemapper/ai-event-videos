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

export type ChatMessage =
  | { role: "user"; content: string }
  | { role: "assistant"; content: string; events: AIEvent[]; totalCount: number }
  | { role: "error"; content: string };

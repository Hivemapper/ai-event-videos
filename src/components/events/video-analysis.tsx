"use client";

import { useState } from "react";
import {
  Brain,
  Loader2,
  Eye,
  Cloud,
  AlertTriangle,
  Gauge,
  Send,
  ChevronDown,
  ChevronUp,
  SignpostBig,
  EyeOff,
  Route,
  Users,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { useVideoAnalysis } from "@/hooks/use-video-analysis";
import {
  VideoAnalysis,
  DetectedObject,
  RoadSign,
  VisibilityIssue,
  ChatMessage,
} from "@/types/analysis";
import { cn } from "@/lib/utils";

const SEVERITY_COLORS: Record<string, string> = {
  none: "bg-green-100 text-green-700 border-green-200",
  low: "bg-yellow-100 text-yellow-700 border-yellow-200",
  moderate: "bg-orange-100 text-orange-700 border-orange-200",
  high: "bg-red-100 text-red-700 border-red-200",
  critical: "bg-red-200 text-red-800 border-red-300",
};

const CONFIDENCE_COLORS: Record<string, string> = {
  high: "bg-green-100 text-green-700",
  medium: "bg-yellow-100 text-yellow-700",
  low: "bg-red-100 text-red-700",
};

const ASSESSMENT_COLORS: Record<string, string> = {
  normal: "bg-green-100 text-green-700",
  cautious: "bg-blue-100 text-blue-700",
  aggressive: "bg-orange-100 text-orange-700",
  erratic: "bg-red-100 text-red-700",
  emergency: "bg-red-200 text-red-800",
};

const SIGN_COLORS: Record<string, string> = {
  traffic_light: "text-amber-700 bg-amber-50 border-amber-200",
  stop_sign: "text-red-700 bg-red-50 border-red-200",
  speed_limit: "text-blue-700 bg-blue-50 border-blue-200",
  yield: "text-orange-700 bg-orange-50 border-orange-200",
  warning: "text-yellow-700 bg-yellow-50 border-yellow-200",
  construction: "text-orange-700 bg-orange-50 border-orange-200",
  regulatory: "text-gray-700 bg-gray-50 border-gray-200",
  guide: "text-green-700 bg-green-50 border-green-200",
  other: "text-gray-700 bg-gray-50 border-gray-200",
};

const LIGHT_STATE_COLORS: Record<string, string> = {
  red: "bg-red-500",
  yellow: "bg-yellow-400",
  green: "bg-green-500",
  flashing: "bg-yellow-400 animate-pulse",
  off: "bg-gray-400",
};

const VISIBILITY_SEVERITY_COLORS: Record<string, string> = {
  mild: "text-yellow-700 bg-yellow-50 border-yellow-200",
  moderate: "text-orange-700 bg-orange-50 border-orange-200",
  severe: "text-red-700 bg-red-50 border-red-200",
};

// --- Section: Road ---
function RoadSection({ analysis }: { analysis: VideoAnalysis }) {
  const { road } = analysis;
  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium flex items-center gap-2">
        <Route className="w-4 h-4" />
        Road
      </h4>
      <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
        <div className="text-muted-foreground">Type</div>
        <div className="font-medium">{road.roadType || "—"}</div>
        <div className="text-muted-foreground">Lanes</div>
        <div className="font-medium">{road.lanes !== null ? road.lanes : "—"}</div>
        <div className="text-muted-foreground">Surface</div>
        <div className="font-medium capitalize">{road.surface?.replace("_", " ") || "—"}</div>
        <div className="text-muted-foreground">Markings</div>
        <div className="font-medium capitalize">{road.markings || "—"}</div>
      </div>
      <div className="flex flex-wrap gap-1.5 mt-1">
        {road.intersection && (
          <Badge variant="outline" className="text-xs">
            intersection{road.intersectionType ? ` (${road.intersectionType.replace("_", " ")})` : ""}
          </Badge>
        )}
        {road.crosswalk && (
          <Badge variant="outline" className="text-xs">
            crosswalk
          </Badge>
        )}
        {road.curvature && road.curvature !== "straight" && (
          <Badge variant="secondary" className="text-xs capitalize">
            {road.curvature.replace("_", " ")}
          </Badge>
        )}
        {road.grade && road.grade !== "flat" && (
          <Badge variant="secondary" className="text-xs capitalize">
            {road.grade}
          </Badge>
        )}
        {road.shoulder !== null && (
          <Badge variant="secondary" className="text-xs">
            {road.shoulder ? "shoulder" : "no shoulder"}
          </Badge>
        )}
        {road.median !== null && (
          <Badge variant="secondary" className="text-xs">
            {road.median ? "median" : "no median"}
          </Badge>
        )}
      </div>
    </div>
  );
}

// --- Section: Signage ---
function SignageSection({ signs }: { signs: RoadSign[] }) {
  if (signs.length === 0) return null;
  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium flex items-center gap-2">
        <SignpostBig className="w-4 h-4" />
        Signage ({signs.length})
      </h4>
      <div className="space-y-1.5">
        {signs.map((sign, i) => (
          <div
            key={i}
            className={cn(
              "flex items-center gap-2 p-2 rounded-lg border text-sm",
              SIGN_COLORS[sign.type] || SIGN_COLORS.other
            )}
          >
            {sign.type === "traffic_light" && sign.state && (
              <span
                className={cn("w-3 h-3 rounded-full shrink-0", LIGHT_STATE_COLORS[sign.state])}
                title={sign.state}
              />
            )}
            <div className="flex-1 min-w-0">
              <span className="font-medium capitalize">
                {sign.type.replace(/_/g, " ")}
              </span>
              {sign.type === "traffic_light" && sign.state && (
                <span className="ml-1.5 text-xs capitalize">({sign.state})</span>
              )}
              {sign.value && (
                <span className="ml-1.5 font-semibold">
                  {sign.value}
                </span>
              )}
              <p className="text-xs mt-0.5">{sign.description}</p>
            </div>
            <Badge variant="secondary" className="text-xs shrink-0">
              {sign.position}
            </Badge>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Section: Objects ---
function ObjectsSection({ objects }: { objects: DetectedObject[] }) {
  if (objects.length === 0) return null;

  const people = objects.filter((o) => o.type === "pedestrian" || o.type === "cyclist");
  const vehicles = objects.filter((o) => o.type === "vehicle");
  const other = objects.filter((o) => o.type !== "pedestrian" && o.type !== "cyclist" && o.type !== "vehicle");

  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium flex items-center gap-2">
        <Users className="w-4 h-4" />
        Objects & People ({objects.length})
      </h4>
      <div className="space-y-1.5">
        {people.map((obj, i) => (
          <ObjectRow key={`p${i}`} obj={obj} highlight />
        ))}
        {vehicles.map((obj, i) => (
          <ObjectRow key={`v${i}`} obj={obj} />
        ))}
        {other.map((obj, i) => (
          <ObjectRow key={`o${i}`} obj={obj} />
        ))}
      </div>
    </div>
  );
}

function ObjectRow({ obj, highlight }: { obj: DetectedObject; highlight?: boolean }) {
  return (
    <div
      className={cn(
        "flex items-start gap-2 p-2 rounded-lg border text-sm",
        highlight
          ? "bg-purple-50 border-purple-200 text-purple-900"
          : "bg-muted/30"
      )}
    >
      <div className="flex-1 min-w-0">
        <span className="font-medium capitalize">{obj.type}</span>
        {obj.subtype && (
          <span className="text-xs ml-1 text-muted-foreground">({obj.subtype})</span>
        )}
        {obj.moving !== null && (
          <Badge variant="secondary" className="text-xs ml-1.5">
            {obj.moving ? "moving" : "stationary"}
          </Badge>
        )}
        <p className="text-xs mt-0.5">{obj.description}</p>
      </div>
      <div className="flex flex-col items-end gap-0.5 shrink-0">
        <Badge variant="secondary" className="text-xs">
          {obj.position}
        </Badge>
        <span className="text-xs text-muted-foreground">
          {obj.estimatedDistance.replace("_", " ")}
        </span>
      </div>
    </div>
  );
}

// --- Section: Visibility Issues ---
function VisibilitySection({ issues }: { issues: VisibilityIssue[] }) {
  if (issues.length === 0) return null;
  return (
    <div className="space-y-2">
      <h4 className="text-sm font-medium flex items-center gap-2">
        <EyeOff className="w-4 h-4" />
        Visibility Issues ({issues.length})
      </h4>
      <div className="space-y-1.5">
        {issues.map((issue, i) => (
          <div
            key={i}
            className={cn(
              "p-2 rounded-lg border text-sm",
              VISIBILITY_SEVERITY_COLORS[issue.severity]
            )}
          >
            <span className="font-medium capitalize">
              {issue.type.replace("_", " ")}
            </span>
            <Badge variant="secondary" className="text-xs ml-2 capitalize">
              {issue.severity}
            </Badge>
            <p className="text-xs mt-0.5">{issue.description}</p>
          </div>
        ))}
      </div>
    </div>
  );
}

// --- Main Results ---
function AnalysisResults({ analysis }: { analysis: VideoAnalysis }) {
  const [showMore, setShowMore] = useState(false);

  return (
    <div className="space-y-4">
      {/* Summary */}
      <div className="space-y-2">
        <p className="text-sm leading-relaxed">{analysis.summary}</p>
        <div className="flex items-center gap-2 flex-wrap">
          <Badge className={cn("text-xs", CONFIDENCE_COLORS[analysis.confidence])}>
            {analysis.confidence} confidence
          </Badge>
          <Badge className={cn("text-xs", ASSESSMENT_COLORS[analysis.driving.assessment])}>
            {analysis.driving.assessment} driving
          </Badge>
          {analysis.environment.weather && (
            <Badge variant="secondary" className="text-xs capitalize">
              {analysis.environment.weather}
            </Badge>
          )}
          {analysis.environment.lighting && (
            <Badge variant="secondary" className="text-xs capitalize">
              {analysis.environment.lighting.replace("_", " ")}
            </Badge>
          )}
          {analysis.environment.setting && (
            <Badge variant="secondary" className="text-xs capitalize">
              {analysis.environment.setting}
            </Badge>
          )}
        </div>
      </div>

      {/* Hazard Assessment (if non-trivial) */}
      {analysis.hazard.severity !== "none" && (
        <div
          className={cn(
            "p-3 rounded-lg border",
            SEVERITY_COLORS[analysis.hazard.severity]
          )}
        >
          <div className="flex items-center gap-2 mb-1">
            <AlertTriangle className="w-4 h-4" />
            <span className="font-medium text-sm capitalize">
              {analysis.hazard.severity} severity
              {analysis.hazard.hazardType && ` — ${analysis.hazard.hazardType}`}
            </span>
          </div>
          {analysis.hazard.hasNearMiss && analysis.hazard.nearMissType && (
            <p className="text-sm ml-6">Near miss: {analysis.hazard.nearMissType}</p>
          )}
          {analysis.hazard.contributingFactors.length > 0 && (
            <p className="text-sm ml-6">
              Factors: {analysis.hazard.contributingFactors.join(", ")}
            </p>
          )}
        </div>
      )}

      {/* 5 key sections — always visible */}
      <RoadSection analysis={analysis} />
      <SignageSection signs={analysis.signage || []} />
      <ObjectsSection objects={analysis.objects} />
      <VisibilitySection issues={analysis.visibilityIssues || []} />

      {/* Expandable: driving behavior + frame notes */}
      <button
        onClick={() => setShowMore(!showMore)}
        className="flex items-center gap-1 text-sm text-muted-foreground hover:text-foreground transition-colors"
      >
        {showMore ? (
          <ChevronUp className="w-4 h-4" />
        ) : (
          <ChevronDown className="w-4 h-4" />
        )}
        {showMore ? "Less" : "More"} (driving, environment, frame notes)
      </button>

      {showMore && (
        <div className="space-y-4">
          {/* Driving Behavior */}
          <div className="space-y-2">
            <h4 className="text-sm font-medium flex items-center gap-2">
              <Gauge className="w-4 h-4" />
              Driving Behavior
            </h4>
            <div className="text-sm space-y-1">
              <p>{analysis.driving.speedContext}</p>
              {analysis.driving.brakingContext && (
                <p className="text-muted-foreground">
                  Braking: {analysis.driving.brakingContext}
                </p>
              )}
              {analysis.driving.steeringContext && (
                <p className="text-muted-foreground">
                  Steering: {analysis.driving.steeringContext}
                </p>
              )}
            </div>
          </div>

          {/* Environment */}
          <div className="space-y-2">
            <h4 className="text-sm font-medium flex items-center gap-2">
              <Cloud className="w-4 h-4" />
              Environment
            </h4>
            <div className="grid grid-cols-2 gap-x-4 gap-y-1 text-sm">
              <div className="text-muted-foreground">Weather</div>
              <div className="capitalize">{analysis.environment.weather || "—"}</div>
              <div className="text-muted-foreground">Lighting</div>
              <div className="capitalize">{analysis.environment.lighting?.replace("_", " ") || "—"}</div>
              <div className="text-muted-foreground">Visibility</div>
              <div className="capitalize">{analysis.environment.visibility || "—"}</div>
              <div className="text-muted-foreground">Setting</div>
              <div className="capitalize">{analysis.environment.setting || "—"}</div>
              <div className="text-muted-foreground">Glare</div>
              <div>{analysis.environment.glare ? "Yes" : analysis.environment.glare === false ? "No" : "—"}</div>
            </div>
          </div>

          {/* Frame Notes */}
          {analysis.frameNotes.length > 0 && (
            <div className="space-y-2">
              <h4 className="text-sm font-medium flex items-center gap-2">
                <Eye className="w-4 h-4" />
                Frame Notes
              </h4>
              <ol className="list-decimal list-inside space-y-1 text-sm text-muted-foreground">
                {analysis.frameNotes.map((note, i) => (
                  <li key={i}>{note}</li>
                ))}
              </ol>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ChatSection({
  chatHistory,
  onSend,
  isLoading,
}: {
  chatHistory: ChatMessage[];
  onSend: (msg: string) => void;
  isLoading: boolean;
}) {
  const [input, setInput] = useState("");

  const handleSend = () => {
    if (!input.trim() || isLoading) return;
    const msg = input.trim();
    setInput("");
    onSend(msg);
  };

  return (
    <div className="space-y-3 border-t pt-3">
      {chatHistory.length > 0 && (
        <div className="space-y-2 max-h-60 overflow-y-auto">
          {chatHistory.map((msg, i) => (
            <div
              key={i}
              className={cn(
                "text-sm p-2 rounded-lg",
                msg.role === "user"
                  ? "bg-primary/10 ml-8"
                  : "bg-muted mr-8"
              )}
            >
              <p className="whitespace-pre-wrap">{msg.content}</p>
            </div>
          ))}
          {isLoading && (
            <div className="bg-muted mr-8 p-2 rounded-lg">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
            </div>
          )}
        </div>
      )}

      <div className="flex gap-2">
        <Input
          placeholder="Ask about this event..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              handleSend();
            }
          }}
          disabled={isLoading}
          className="text-sm"
        />
        <Button
          size="icon"
          variant="ghost"
          onClick={handleSend}
          disabled={!input.trim() || isLoading}
        >
          <Send className="w-4 h-4" />
        </Button>
      </div>
    </div>
  );
}

interface VideoAnalysisCardProps {
  eventId: string;
}

export function VideoAnalysisCard({ eventId }: VideoAnalysisCardProps) {
  const {
    analysis,
    analyzedAt,
    isLoading,
    error,
    analyze,
    chatHistory,
    askFollowUp,
    isChatLoading,
  } = useVideoAnalysis(eventId);

  const handleChatSend = (msg: string) => {
    askFollowUp(msg).catch(() => {
      // Error is handled in the hook by removing the user message
    });
  };

  return (
    <Card>
      <CardHeader>
        <CardTitle className="text-lg flex items-center gap-2">
          <Brain className="w-5 h-5" />
          Scene Analysis
          {analyzedAt && (
            <span className="text-xs font-normal text-muted-foreground ml-auto">
              {new Date(analyzedAt).toLocaleString()}
            </span>
          )}
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">
        {!analysis && !isLoading && !error && (
          <div className="text-center py-4">
            <p className="text-sm text-muted-foreground mb-3">
              Analyze the scene — road type, lanes, signage, objects, and visibility.
            </p>
            <Button onClick={analyze}>
              <Brain className="w-4 h-4 mr-2" />
              Analyze Video
            </Button>
          </div>
        )}

        {isLoading && (
          <div className="flex flex-col items-center py-6 gap-3">
            <Loader2 className="w-8 h-8 animate-spin text-primary" />
            <div className="text-center">
              <p className="text-sm font-medium">Analyzing video...</p>
              <p className="text-xs text-muted-foreground mt-1">
                Extracting frames, analyzing video (~15-20s)
              </p>
            </div>
          </div>
        )}

        {error && (
          <div className="space-y-3">
            <div className="text-sm text-red-600 bg-red-50 p-3 rounded-lg">
              {error === "NO_API_KEY"
                ? "Anthropic API key required. Add it in Settings."
                : error}
            </div>
            <Button onClick={analyze} variant="outline" size="sm">
              Retry
            </Button>
          </div>
        )}

        {analysis && <AnalysisResults analysis={analysis} />}

        {analysis && (
          <ChatSection
            chatHistory={chatHistory}
            onSend={handleChatSend}
            isLoading={isChatLoading}
          />
        )}
      </CardContent>
    </Card>
  );
}

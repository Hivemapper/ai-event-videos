"use client";

import { useState, useRef, useEffect } from "react";
import Link from "next/link";
import {
  Send,
  Loader2,
  AlertCircle,
  Key,
  Check,
  Trash2,
  Scan,
  ExternalLink,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ChatMessage, AgentApiResult, AgentFilterResponse, ScanMatch } from "@/types/agent";
import { AIEvent } from "@/types/events";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getAnthropicKey, setAnthropicKey, getApiKey, getMapboxToken } from "@/lib/api";
import { createCirclePolygon } from "@/lib/geo-utils";
import { cn } from "@/lib/utils";

const STORAGE_KEY = "agent-chat-history";

const EXAMPLE_QUERIES = [
  "Harsh braking in London last week",
  "Nighttime swerving events",
  "High speed near San Francisco",
  "Stop sign violations past 3 days",
];

const SCAN_EXAMPLES = [
  "Likely pedestrian areas",
  "Bicycle-friendly roads",
  "Near school zones",
  "Construction areas",
];

const CONFIDENCE_STYLES: Record<string, { label: string; className: string }> = {
  high: { label: "high", className: "bg-green-500/15 text-green-700 dark:text-green-400 border-green-500/30" },
  medium: { label: "med", className: "bg-yellow-500/15 text-yellow-700 dark:text-yellow-400 border-yellow-500/30" },
  low: { label: "low", className: "bg-orange-500/15 text-orange-700 dark:text-orange-400 border-orange-500/30" },
};

function formatRelativeTime(timestamp: string): string {
  const date = new Date(timestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / 60000);
  const diffHours = Math.floor(diffMins / 60);
  const diffDays = Math.floor(diffHours / 24);
  if (diffDays > 30) return date.toLocaleDateString();
  if (diffDays > 0) return `${diffDays}d ago`;
  if (diffHours > 0) return `${diffHours}h ago`;
  if (diffMins > 0) return `${diffMins}m ago`;
  return "Just now";
}

function EventResult({ event }: { event: AIEvent }) {
  const config = EVENT_TYPE_CONFIG[event.type] || EVENT_TYPE_CONFIG.UNKNOWN;

  return (
    <Link
      href={`/event/${event.id}`}
      target="_blank"
      className="flex items-center gap-3 p-2.5 rounded-lg hover:bg-muted/50 transition-colors"
    >
      <Badge
        className={cn(
          "shrink-0",
          config.bgColor,
          config.color,
          config.borderColor,
          "border text-xs"
        )}
        variant="outline"
      >
        {config.label}
      </Badge>
      <span className="text-xs text-muted-foreground">
        {formatRelativeTime(event.timestamp)}
      </span>
      <span className="text-xs text-muted-foreground hidden sm:inline">
        {event.location.lat.toFixed(2)}, {event.location.lon.toFixed(2)}
      </span>
    </Link>
  );
}

function ScanResultRow({ match }: { match: ScanMatch }) {
  const config = CONFIDENCE_STYLES[match.confidence];
  return (
    <Link
      href={`/event/${match.eventId}`}
      target="_blank"
      className="flex items-start gap-2.5 p-2.5 rounded-lg hover:bg-muted/50 transition-colors group"
    >
      <Badge
        variant="outline"
        className={cn("shrink-0 text-[10px] px-1.5 py-0 mt-0.5", config.className)}
      >
        {config.label}
      </Badge>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5">
          <span className="text-xs font-mono text-muted-foreground">
            {match.eventId.slice(0, 12)}...
          </span>
          <ExternalLink className="h-3 w-3 text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
        </div>
        <p className="text-xs text-muted-foreground mt-0.5 line-clamp-2">
          {match.reason}
        </p>
      </div>
    </Link>
  );
}

export function AgentView() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>(() => {
    if (typeof window === "undefined") return [];
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      return stored ? JSON.parse(stored) : [];
    } catch {
      return [];
    }
  });
  const [isLoading, setIsLoading] = useState(false);
  const [isScanning, setIsScanning] = useState(false);
  const [loadingMore, setLoadingMore] = useState<number | null>(null);
  const [needsKey, setNeedsKey] = useState(false);
  const [keyInput, setKeyInput] = useState("");
  const [keySaved, setKeySaved] = useState(false);
  const [pendingQuery, setPendingQuery] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // Persist messages to localStorage
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(messages));
    } catch { /* storage full or unavailable */ }
  }, [messages]);

  const clearHistory = () => {
    setMessages([]);
  };

  const loadMore = async (msgIndex: number) => {
    const msg = messages[msgIndex];
    if (msg.role !== "assistant" || !msg.filters) return;

    setLoadingMore(msgIndex);
    const beemapsKey = getApiKey();
    const filters = msg.filters;

    try {
      const defaults = (() => {
        const end = new Date();
        const start = new Date();
        start.setDate(start.getDate() - 31);
        return { startDate: start.toISOString(), endDate: end.toISOString() };
      })();

      const toISO = (date: string, end = false): string => {
        if (date.includes("T")) return date;
        return end ? `${date}T23:59:59.999Z` : `${date}T00:00:00.000Z`;
      };

      const body: Record<string, unknown> = {
        startDate: filters.startDate ? toISO(filters.startDate) : defaults.startDate,
        endDate: filters.endDate ? toISO(filters.endDate, true) : defaults.endDate,
        types: filters.types,
        limit: 20,
        offset: msg.events.length,
      };

      if (filters.coordinates && filters.radius) {
        body.polygon = createCirclePolygon(
          filters.coordinates.lat,
          filters.coordinates.lon,
          filters.radius
        );
      }

      const res = await fetch("/api/events", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          ...(beemapsKey ? { Authorization: beemapsKey } : {}),
        },
        body: JSON.stringify(body),
      });

      if (res.ok) {
        const data = await res.json();
        const newEvents: AIEvent[] = data.events || [];
        setMessages((prev) =>
          prev.map((m, i) =>
            i === msgIndex && m.role === "assistant"
              ? { ...m, events: [...m.events, ...newEvents] }
              : m
          )
        );
      } else {
        console.error("Load more failed:", res.status, await res.text());
      }
    } catch (err) {
      console.error("Load more error:", err);
    } finally {
      setLoadingMore(null);
    }
  };

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isLoading, isScanning]);

  const submitQuery = async (q: string) => {
    if (!q.trim() || isLoading) return;

    setMessages((prev) => [...prev, { role: "user", content: q }]);
    setQuery("");
    setIsLoading(true);
    setNeedsKey(false);

    const anthropicKey = getAnthropicKey();
    const beemapsKey = getApiKey();

    try {
      const res = await fetch("/api/agent", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: q,
          ...(anthropicKey ? { apiKey: anthropicKey } : {}),
          ...(beemapsKey ? { beemapsApiKey: beemapsKey } : {}),
        }),
      });

      const data: AgentApiResult = await res.json();

      if (data.success) {
        setMessages((prev) => [
          ...prev,
          {
            role: "assistant",
            content: data.filters.explanation,
            events: data.events,
            totalCount: data.totalCount,
            filters: data.filters,
          },
        ]);
      } else if (data.error === "NO_API_KEY") {
        setNeedsKey(true);
        setPendingQuery(q);
        setMessages((prev) => prev.slice(0, -1));
      } else {
        setMessages((prev) => [
          ...prev,
          { role: "error", content: data.error },
        ]);
      }
    } catch {
      setMessages((prev) => [
        ...prev,
        {
          role: "error",
          content: "Failed to connect to the agent. Please try again.",
        },
      ]);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSubmit = (text?: string) => {
    submitQuery(text ?? query);
  };

  const handleSaveKey = () => {
    if (!keyInput.trim()) return;
    setAnthropicKey(keyInput.trim());
    setKeySaved(true);
    setNeedsKey(false);
    setTimeout(() => setKeySaved(false), 2000);
    if (pendingQuery) {
      submitQuery(pendingQuery);
      setPendingQuery(null);
    }
  };

  const runScan = async (events: AIEvent[], userQuery: string) => {
    if (isScanning || events.length === 0) return;
    setIsScanning(true);

    try {
      const scanEvents = events.slice(0, 50).map((e) => ({
        eventId: e.id,
        lat: e.location.lat,
        lon: e.location.lon,
        eventType: e.type,
      }));

      const res = await fetch("/api/vision-scan", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          query: userQuery,
          events: scanEvents,
          model: "haiku" as const,
          anthropicApiKey: getAnthropicKey() || undefined,
          mapboxToken: getMapboxToken() || undefined,
        }),
      });

      const data = await res.json();

      if (!res.ok) {
        if (data.error === "NO_API_KEY") {
          setNeedsKey(true);
        } else {
          setMessages((prev) => [
            ...prev,
            { role: "error", content: data.error || "Scan failed" },
          ]);
        }
        return;
      }

      const matches = (data.matches as ScanMatch[])
        .filter((m) => m.match)
        .sort((a, b) => {
          const order = { high: 0, medium: 1, low: 2 };
          return order[a.confidence] - order[b.confidence];
        });

      setMessages((prev) => [
        ...prev,
        {
          role: "scan" as const,
          query: userQuery,
          matches,
          eventsScanned: data.eventsScanned,
        },
      ]);
    } catch {
      setMessages((prev) => [
        ...prev,
        { role: "error", content: "Vision scan request failed." },
      ]);
    } finally {
      setIsScanning(false);
    }
  };

  const hasMessages = messages.length > 0;

  return (
    <div className="flex flex-col h-[calc(100vh-3.5rem)]">
      {/* Chat area */}
      <div
        ref={scrollRef}
        className="flex-1 overflow-y-auto"
      >
        <div className="max-w-2xl mx-auto px-4 py-6 space-y-4">
          {/* Empty state */}
          {!hasMessages && !isLoading && !needsKey && (
            <div className="flex flex-col items-center justify-center min-h-[50vh] space-y-6 text-center">
              <div className="space-y-2">
                <p className="text-lg font-medium">Ask me about events</p>
                <p className="text-sm text-muted-foreground">
                  Describe what you&apos;re looking for and I&apos;ll find matching events.
                </p>
              </div>
              <div className="flex flex-wrap gap-2 justify-center max-w-md">
                {EXAMPLE_QUERIES.map((example) => (
                  <button
                    key={example}
                    type="button"
                    onClick={() => handleSubmit(example)}
                    className="text-sm px-4 py-2 rounded-full border bg-muted/50 hover:bg-muted transition-colors"
                  >
                    {example}
                  </button>
                ))}
              </div>
              <p className="text-xs text-muted-foreground pt-2">
                After results load, use <strong>Scan locations</strong> to analyze road context:
              </p>
              <div className="flex flex-wrap gap-2 justify-center max-w-md">
                {SCAN_EXAMPLES.map((example) => (
                  <span
                    key={example}
                    className="text-xs px-3 py-1.5 rounded-full border border-dashed text-muted-foreground"
                  >
                    {example}
                  </span>
                ))}
              </div>
            </div>
          )}

          {/* Messages */}
          {messages.map((msg, i) => (
            <div key={i}>
              {msg.role === "user" && (
                <div className="flex justify-end">
                  <div className="bg-primary text-primary-foreground rounded-2xl rounded-br-sm px-4 py-2.5 max-w-[85%] text-sm">
                    {msg.content}
                  </div>
                </div>
              )}

              {msg.role === "assistant" && (
                <div className="space-y-3">
                  <div className="bg-muted rounded-2xl rounded-bl-sm px-4 py-2.5 max-w-[85%] text-sm">
                    {msg.content}
                  </div>
                  {msg.events.length > 0 ? (
                    <div className="border rounded-lg divide-y">
                      {msg.events.map((event: AIEvent) => (
                        <EventResult key={event.id} event={event} />
                      ))}
                    </div>
                  ) : (
                    <p className="text-sm text-muted-foreground px-1">
                      No events matched these filters. Try broadening your search.
                    </p>
                  )}
                  <div className="flex items-center gap-3 px-1">
                    {msg.totalCount > msg.events.length &&
                      (msg.filters ? (
                        <button
                          type="button"
                          onClick={() => loadMore(i)}
                          disabled={loadingMore === i}
                          className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50"
                        >
                          {loadingMore === i ? (
                            <>
                              <Loader2 className="w-3 h-3 animate-spin" />
                              Loading...
                            </>
                          ) : (
                            <>
                              Showing {msg.events.length} of {msg.totalCount} — Show more
                            </>
                          )}
                        </button>
                      ) : (
                        <span className="text-xs text-muted-foreground">
                          Showing {msg.events.length} of {msg.totalCount}
                        </span>
                      ))}
                    {msg.events.length > 0 && (
                      <button
                        type="button"
                        onClick={() => {
                          // Find the user query that preceded this assistant message
                          const userMsg = messages.slice(0, i).reverse().find((m) => m.role === "user");
                          const scanQuery = userMsg ? userMsg.content : "";
                          runScan(msg.events, scanQuery);
                        }}
                        disabled={isScanning}
                        className="flex items-center gap-1.5 text-xs text-muted-foreground hover:text-foreground transition-colors disabled:opacity-50 ml-auto"
                      >
                        {isScanning ? (
                          <>
                            <Loader2 className="w-3 h-3 animate-spin" />
                            Scanning...
                          </>
                        ) : (
                          <>
                            <Scan className="w-3 h-3" />
                            Scan locations
                          </>
                        )}
                      </button>
                    )}
                  </div>
                </div>
              )}

              {msg.role === "scan" && (
                <div className="space-y-2">
                  <div className="bg-muted rounded-2xl rounded-bl-sm px-4 py-2.5 max-w-[85%] text-sm flex items-center gap-2">
                    <Scan className="w-4 h-4 shrink-0" />
                    {msg.matches.length > 0 ? (
                      <span>
                        Location scan: <strong>{msg.matches.length}</strong> of {msg.eventsScanned} events likely match &ldquo;{msg.query}&rdquo;
                      </span>
                    ) : (
                      <span>No events matched &ldquo;{msg.query}&rdquo; based on location context.</span>
                    )}
                  </div>
                  {msg.matches.length > 0 && (
                    <div className="border rounded-lg divide-y">
                      {msg.matches.map((match) => (
                        <ScanResultRow key={match.eventId} match={match} />
                      ))}
                    </div>
                  )}
                </div>
              )}

              {msg.role === "error" && (
                <div className="flex items-center gap-2 p-3 bg-destructive/10 text-destructive rounded-lg text-sm">
                  <AlertCircle className="w-4 h-4 shrink-0" />
                  {msg.content}
                </div>
              )}
            </div>
          ))}

          {/* Loading */}
          {isLoading && (
            <div className="flex items-center gap-2">
              <div className="bg-muted rounded-2xl rounded-bl-sm px-4 py-3 flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="w-4 h-4 animate-spin" />
                Searching events...
              </div>
            </div>
          )}

          {/* Scanning */}
          {isScanning && (
            <div className="flex items-center gap-2">
              <div className="bg-muted rounded-2xl rounded-bl-sm px-4 py-3 flex items-center gap-2 text-sm text-muted-foreground">
                <Loader2 className="w-4 h-4 animate-spin" />
                Scanning event locations...
              </div>
            </div>
          )}

          {/* No API key prompt */}
          {needsKey && (
            <div className="space-y-3 p-4 border rounded-lg bg-muted/30 max-w-md">
              <div className="flex items-center gap-2 text-sm font-medium">
                <Key className="w-4 h-4" />
                Anthropic API Key Required
              </div>
              <p className="text-sm text-muted-foreground">
                Enter your key below or add it in{" "}
                <span className="font-medium text-foreground">Settings</span>.
              </p>
              <div className="flex gap-2">
                <Input
                  type="password"
                  placeholder="sk-ant-..."
                  value={keyInput}
                  onChange={(e) => setKeyInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleSaveKey();
                  }}
                />
                <Button onClick={handleSaveKey} disabled={!keyInput.trim()}>
                  {keySaved ? (
                    <>
                      <Check className="w-4 h-4 mr-1" />
                      Saved
                    </>
                  ) : (
                    "Save"
                  )}
                </Button>
              </div>
              <p className="text-xs text-muted-foreground">
                Get your key from the{" "}
                <a
                  href="https://console.anthropic.com/settings/keys"
                  target="_blank"
                  rel="noopener noreferrer"
                  className="text-primary hover:underline"
                >
                  Anthropic Console
                </a>
                . Stored locally in your browser.
              </p>
            </div>
          )}
        </div>
      </div>

      {/* Input bar */}
      <div className="shrink-0 border-t bg-background">
        <div className="max-w-2xl mx-auto px-4 py-4">
          <div className="flex gap-2">
            {hasMessages && (
              <Button
                size="icon"
                variant="ghost"
                onClick={clearHistory}
                disabled={isLoading}
                title="Clear chat"
              >
                <Trash2 className="w-4 h-4" />
              </Button>
            )}
            <Input
              ref={inputRef}
              placeholder="Ask about events..."
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => {
                if (e.key === "Enter") handleSubmit();
              }}
              disabled={isLoading}
            />
            <Button
              size="icon"
              onClick={() => handleSubmit()}
              disabled={!query.trim() || isLoading}
            >
              {isLoading ? (
                <Loader2 className="w-4 h-4 animate-spin" />
              ) : (
                <Send className="w-4 h-4" />
              )}
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

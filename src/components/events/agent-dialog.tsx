"use client";

import { useState, useRef, useEffect } from "react";
import Link from "next/link";
import {
  Sparkles,
  Send,
  Loader2,
  AlertCircle,
  Key,
  Check,
  ExternalLink,
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Badge } from "@/components/ui/badge";
import { ChatMessage, AgentApiResult } from "@/types/agent";
import { AIEvent } from "@/types/events";
import { EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getAnthropicKey, setAnthropicKey, getApiKey } from "@/lib/api";
import { cn } from "@/lib/utils";

const EXAMPLE_QUERIES = [
  "Harsh braking in London last week",
  "Nighttime swerving events",
  "High speed near San Francisco",
  "Stop sign violations past 3 days",
];

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
  const Icon = config.icon;

  return (
    <Link
      href={`/event/${event.id}`}
      className="flex items-center gap-3 p-2.5 rounded-lg hover:bg-muted/50 transition-colors group"
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
        <Icon className="w-3 h-3 mr-1" />
        {config.label}
      </Badge>
      <span className="text-xs text-muted-foreground">
        {formatRelativeTime(event.timestamp)}
      </span>
      <span className="text-xs text-muted-foreground hidden sm:inline">
        {event.location.lat.toFixed(2)}, {event.location.lon.toFixed(2)}
      </span>
      <ExternalLink className="w-3 h-3 ml-auto text-muted-foreground opacity-0 group-hover:opacity-100 transition-opacity shrink-0" />
    </Link>
  );
}

export function AgentView() {
  const [query, setQuery] = useState("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [needsKey, setNeedsKey] = useState(false);
  const [keyInput, setKeyInput] = useState("");
  const [keySaved, setKeySaved] = useState(false);
  const [pendingQuery, setPendingQuery] = useState<string | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  // Auto-scroll to bottom when messages change
  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages, isLoading]);

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
              <div className="w-16 h-16 rounded-full bg-muted flex items-center justify-center">
                <Sparkles className="w-8 h-8 text-muted-foreground" />
              </div>
              <div className="space-y-2">
                <p className="text-lg font-medium">Ask me about events</p>
                <p className="text-sm text-muted-foreground">
                  Describe what you&apos;re looking for and I&apos;ll find matching dashcam events.
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
                  {msg.totalCount > msg.events.length && (
                    <p className="text-xs text-muted-foreground px-1">
                      Showing {msg.events.length} of {msg.totalCount} results
                    </p>
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

"use client";

import { Suspense, useState } from "react";
import Link from "next/link";
import useSWR from "swr";
import { ChevronLeft, ChevronRight, Copy, FileQuestion, Ghost, Route, VideoOff, Zap } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import { cn } from "@/lib/utils";

const PAGE_SIZE = 50;
const fetcher = (url: string) => fetch(url).then((r) => r.json());

interface TriageResult {
  id: string;
  event_type: string;
  triage_result: "missing_video" | "missing_metadata" | "ghost" | "open_road" | "signal" | "duplicate";
  rules_triggered: string;
  speed_min: number | null;
  speed_max: number | null;
  speed_mean: number | null;
  speed_stddev: number | null;
  gnss_displacement_m: number | null;
  event_timestamp: string | null;
  created_at: string;
}

const RESULT_CONFIG = {
  missing_video: { label: "Missing Video", color: "bg-blue-50 text-blue-700 border-blue-200", icon: VideoOff },
  missing_metadata: { label: "Missing Metadata", color: "bg-violet-50 text-violet-700 border-violet-200", icon: FileQuestion },
  ghost: { label: "Ghost", color: "bg-red-50 text-red-700 border-red-200", icon: Ghost },
  open_road: { label: "Open Road", color: "bg-amber-50 text-amber-700 border-amber-200", icon: Route },
  signal: { label: "Signal", color: "bg-green-50 text-green-700 border-green-200", icon: Zap },
  duplicate: { label: "Duplicate", color: "bg-orange-50 text-orange-700 border-orange-200", icon: Copy },
} as const;

const FILTER_OPTIONS = [
  { value: null, label: "All" },
  { value: "missing_video", label: "Missing Video" },
  { value: "missing_metadata", label: "Missing Metadata" },
  { value: "ghost", label: "Ghost" },
  { value: "open_road", label: "Open Road" },
  { value: "duplicate", label: "Duplicate" },
  { value: "signal", label: "Signal" },
] as const;

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr.endsWith("Z") ? dateStr : dateStr + "Z");
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatRules(rulesJson: string): string[] {
  try {
    return JSON.parse(rulesJson);
  } catch {
    return [];
  }
}

function TriageTable() {
  const [offset, setOffset] = useState(0);
  const [filter, setFilter] = useState<string | null>(null);

  const url = `/api/triage?limit=${PAGE_SIZE}&offset=${offset}${filter ? `&filter=${filter}` : ""}`;
  const { data, isLoading } = useSWR<{
    results: TriageResult[];
    total: number;
    summary: Record<string, number>;
  }>(url, fetcher, { revalidateOnFocus: false });

  const results = data?.results ?? [];
  const total = data?.total ?? 0;
  const summary = data?.summary ?? {};
  const page = Math.floor(offset / PAGE_SIZE) + 1;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  // Reset offset when filter changes
  const handleFilter = (f: string | null) => {
    setFilter(f);
    setOffset(0);
  };

  return (
    <div>
      {/* Summary cards */}
      <div className="grid grid-cols-7 gap-2 mb-6">
        {FILTER_OPTIONS.map(({ value, label }) => {
          const count = value ? (summary[value] ?? 0) : Object.values(summary).reduce((a, b) => a + b, 0);
          const grandTotal = Object.values(summary).reduce((a, b) => a + b, 0);
          const pct = grandTotal > 0 ? ((count / grandTotal) * 100).toFixed(1) : "0.0";
          const isActive = filter === value;
          const config = value ? RESULT_CONFIG[value as keyof typeof RESULT_CONFIG] : null;
          return (
            <button
              key={label}
              onClick={() => handleFilter(value)}
              className={cn(
                "rounded-lg border px-3 py-2 text-left transition-all",
                isActive ? "ring-2 ring-primary border-primary" : "hover:border-foreground/20",
                value === "signal" && "bg-green-50 border-green-200"
              )}
            >
              <p className="text-xl font-semibold tabular-nums">{count} <span className="text-xs font-normal text-muted-foreground">{pct}%</span></p>
              <p className="text-xs text-muted-foreground mt-0.5">{label}</p>
            </button>
          );
        })}
      </div>

      {isLoading ? (
        <div className="space-y-3">
          {Array.from({ length: 10 }).map((_, i) => (
            <Skeleton key={i} className="h-12 w-full" />
          ))}
        </div>
      ) : results.length === 0 ? (
        <div className="text-center py-16 text-muted-foreground">
          {filter ? `No ${filter.replace("_", " ")} events found.` : "No triage results yet. Run: python3 scripts/run-triage.py 500"}
        </div>
      ) : (
        <>
          <div className="rounded-lg border">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b bg-muted/40">
                  <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">#</th>
                  <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Event</th>
                  <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Type</th>
                  <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Classification</th>
                  <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Rules</th>
                  <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Speed</th>
                  <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Date</th>
                </tr>
              </thead>
              <tbody>
                {results.map((r, i) => {
                  const config = RESULT_CONFIG[r.triage_result];
                  const rules = formatRules(r.rules_triggered);
                  const Icon = config.icon;
                  return (
                    <tr key={r.id} className="border-b last:border-0 hover:bg-muted/20 transition-colors">
                      <td className="px-4 py-2.5 text-muted-foreground tabular-nums">
                        {offset + i + 1}
                      </td>
                      <td className="px-4 py-2.5">
                        <Link
                          href={`/event/${r.id}`}
                          className="font-mono text-primary hover:underline"
                        >
                          {r.id.slice(0, 16)}…
                        </Link>
                      </td>
                      <td className="px-4 py-2.5">
                        <span className="text-xs text-muted-foreground">
                          {r.event_type.replace(/_/g, " ")}
                        </span>
                      </td>
                      <td className="px-4 py-2.5">
                        <Badge variant="outline" className={cn("gap-1", config.color)}>
                          <Icon className="w-3 h-3" />
                          {config.label}
                        </Badge>
                      </td>
                      <td className="px-4 py-2.5">
                        <div className="flex flex-wrap gap-1">
                          {rules.map((rule) => (
                            <span
                              key={rule}
                              className="text-[11px] px-1.5 py-0.5 rounded bg-muted text-muted-foreground"
                            >
                              {rule.replace(/_/g, " ")}
                            </span>
                          ))}
                        </div>
                      </td>
                      <td className="px-4 py-2.5 text-right tabular-nums text-muted-foreground">
                        {r.speed_min !== null && r.speed_max !== null
                          ? `${r.speed_min}–${r.speed_max} mph`
                          : "—"}
                      </td>
                      <td className="px-4 py-2.5 text-right text-muted-foreground">
                        {formatDate(r.event_timestamp)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          {totalPages > 1 && (
            <div className="flex items-center justify-between mt-4">
              <p className="text-sm text-muted-foreground">
                {total} results
              </p>
              <div className="flex items-center gap-2">
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset === 0}
                  onClick={() => setOffset(Math.max(0, offset - PAGE_SIZE))}
                >
                  <ChevronLeft className="w-4 h-4 mr-1" />
                  Prev
                </Button>
                <span className="text-sm text-muted-foreground">
                  Page {page} of {totalPages}
                </span>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={offset + PAGE_SIZE >= total}
                  onClick={() => setOffset(offset + PAGE_SIZE)}
                >
                  Next
                  <ChevronRight className="w-4 h-4 ml-1" />
                </Button>
              </div>
            </div>
          )}
        </>
      )}
    </div>
  );
}

export default function TriagePage() {
  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="mb-6">
          <h1 className="text-xl font-semibold">Triage</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Phase 0 classification — Ghost, Open Road, or Signal
          </p>
        </div>
        <Suspense fallback={<Skeleton className="h-96 w-full" />}>
          <TriageTable />
        </Suspense>
      </main>
    </>
  );
}

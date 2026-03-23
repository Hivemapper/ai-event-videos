"use client";

import { Suspense, useState } from "react";
import Link from "next/link";
import useSWR from "swr";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { Header } from "@/components/layout/header";
import type { DetectionRun } from "@/types/pipeline";

const PAGE_SIZE = 50;
const fetcher = (url: string) => fetch(url).then((r) => r.json());

function formatDate(dateStr: string | null): string {
  if (!dateStr) return "—";
  const d = new Date(dateStr.endsWith("Z") ? dateStr : dateStr + "Z");
  return d.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
  });
}

function formatDuration(startedAt: string | null, completedAt: string | null): string {
  if (!startedAt || !completedAt) return "—";
  const start = new Date(startedAt.endsWith("Z") ? startedAt : startedAt + "Z");
  const end = new Date(completedAt.endsWith("Z") ? completedAt : completedAt + "Z");
  const sec = Math.round((end.getTime() - start.getTime()) / 1000);
  if (sec < 60) return `${sec}s`;
  const m = Math.floor(sec / 60);
  const s = sec % 60;
  return `${m}m ${s}s`;
}

function PipelineTable() {
  const [offset, setOffset] = useState(0);

  const { data, isLoading } = useSWR<{ runs: DetectionRun[]; total: number }>(
    `/api/detection-runs?limit=${PAGE_SIZE}&offset=${offset}`,
    fetcher,
    { revalidateOnFocus: false }
  );

  const runs = data?.runs ?? [];
  const total = data?.total ?? 0;
  const page = Math.floor(offset / PAGE_SIZE) + 1;
  const totalPages = Math.ceil(total / PAGE_SIZE);

  if (isLoading) {
    return (
      <div className="space-y-3">
        {Array.from({ length: 10 }).map((_, i) => (
          <Skeleton key={i} className="h-12 w-full" />
        ))}
      </div>
    );
  }

  if (runs.length === 0) {
    return (
      <div className="text-center py-16 text-muted-foreground">
        No completed detection runs yet.
      </div>
    );
  }

  return (
    <div>
      <div className="rounded-lg border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b bg-muted/40">
              <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">#</th>
              <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Event</th>
              <th className="text-left font-medium px-4 py-2.5 text-muted-foreground">Model</th>
              <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Detections</th>
              <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Duration</th>
              <th className="text-right font-medium px-4 py-2.5 text-muted-foreground">Completed</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run, i) => (
              <tr key={run.id} className="border-b last:border-0 hover:bg-muted/20 transition-colors">
                <td className="px-4 py-2.5 text-muted-foreground tabular-nums">
                  {offset + i + 1}
                </td>
                <td className="px-4 py-2.5">
                  <Link
                    href={`/event/${run.videoId}`}
                    className="font-mono text-primary hover:underline"
                  >
                    {run.videoId.slice(0, 16)}…
                  </Link>
                </td>
                <td className="px-4 py-2.5">
                  <Badge variant="outline" className="font-normal">
                    {run.modelName}
                  </Badge>
                </td>
                <td className="px-4 py-2.5 text-right tabular-nums">
                  {run.detectionCount ?? "—"}
                </td>
                <td className="px-4 py-2.5 text-right text-muted-foreground tabular-nums">
                  {formatDuration(run.startedAt, run.completedAt)}
                </td>
                <td className="px-4 py-2.5 text-right text-muted-foreground">
                  {formatDate(run.completedAt)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {totalPages > 1 && (
        <div className="flex items-center justify-between mt-4">
          <p className="text-sm text-muted-foreground">
            {total} completed runs
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
    </div>
  );
}

export default function PipelinePage() {
  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="mb-6">
          <h1 className="text-xl font-semibold">Pipeline</h1>
          <p className="text-sm text-muted-foreground mt-1">
            Completed detection runs across all events
          </p>
        </div>
        <Suspense fallback={<Skeleton className="h-96 w-full" />}>
          <PipelineTable />
        </Suspense>
      </main>
    </>
  );
}

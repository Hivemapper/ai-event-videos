"use client";

import { Suspense, useState, useEffect, useCallback } from "react";
import { Loader2, TrendingUp, Calendar, BarChart3, Clock, RefreshCw } from "lucide-react";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Header } from "@/components/layout/header";
import { ALL_EVENT_TYPES, EVENT_TYPE_CONFIG } from "@/lib/constants";
import { getApiKey } from "@/lib/api";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

interface DailyEntry {
  date: string;
  day: string;
  total: number;
}

interface PeriodMetrics {
  total: number;
  byType: Record<string, number>;
}

interface MetricsResponse {
  "60d": PeriodMetrics;
  "30d": PeriodMetrics;
  "7d": PeriodMetrics;
  "24h": PeriodMetrics;
}

function MetricsContent() {
  const [data, setData] = useState<MetricsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const [dailyData, setDailyData] = useState<DailyEntry[] | null>(null);
  const [dailyLoading, setDailyLoading] = useState(true);
  const [dailyError, setDailyError] = useState<string | null>(null);

  const fetchDaily = useCallback(async () => {
    setDailyLoading(true);
    setDailyError(null);
    try {
      const apiKey = getApiKey();
      const headers: Record<string, string> = {};
      if (apiKey) headers["Authorization"] = apiKey;
      const res = await fetch("/api/metrics/daily", { headers });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Unknown error" }));
        throw new Error(err.error || `API error: ${res.status}`);
      }
      setDailyData(await res.json());
    } catch (err) {
      setDailyError(err instanceof Error ? err.message : "Failed to load daily metrics");
    } finally {
      setDailyLoading(false);
    }
  }, []);

  useEffect(() => {
    async function load() {
      try {
        const apiKey = getApiKey();
        const headers: Record<string, string> = {};
        if (apiKey) headers["Authorization"] = apiKey;

        const res = await fetch("/api/metrics", { headers });
        if (!res.ok) {
          const err = await res.json().catch(() => ({ error: "Unknown error" }));
          throw new Error(err.error || `API error: ${res.status}`);
        }
        setData(await res.json());
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load metrics");
      } finally {
        setLoading(false);
      }
    }
    load();
    fetchDaily();
  }, [fetchDaily]);

  if (loading) {
    return (
      <div className="flex items-center justify-center py-24">
        <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
        <span className="ml-2 text-muted-foreground">Loading metrics...</span>
      </div>
    );
  }

  if (error) {
    return (
      <div className="text-center py-24 text-destructive">
        <p>{error}</p>
      </div>
    );
  }

  if (!data) return null;

  const periods = [
    { label: "60 Days", key: "60d" as const, metrics: data["60d"], icon: Calendar },
    { label: "30 Days", key: "30d" as const, metrics: data["30d"], icon: TrendingUp },
    { label: "7 Days", key: "7d" as const, metrics: data["7d"], icon: BarChart3 },
    { label: "24 Hours", key: "24h" as const, metrics: data["24h"], icon: Clock },
  ];

  const activeTypes = ALL_EVENT_TYPES.filter(
    (type) =>
      data["60d"].byType[type] > 0 ||
      data["30d"].byType[type] > 0 ||
      data["7d"].byType[type] > 0 ||
      data["24h"].byType[type] > 0
  );

  return (
    <div className="container mx-auto px-4 py-6 space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
        {periods.map((period) => {
          const Icon = period.icon;
          return (
            <Card key={period.key}>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  Past {period.label}
                </CardTitle>
                <Icon className="w-4 h-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-bold">
                  {period.metrics.total.toLocaleString()}
                </div>
                <p className="text-xs text-muted-foreground mt-1">total events</p>
              </CardContent>
            </Card>
          );
        })}
      </div>

      {/* Daily Events (Last 7 Days) */}
      <Card className="max-w-sm">
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-lg">Daily Events (Last 7 Days)</CardTitle>
          <Button
            variant="ghost"
            size="icon"
            onClick={fetchDaily}
            disabled={dailyLoading}
            className="h-8 w-8"
          >
            <RefreshCw className={cn("w-4 h-4", dailyLoading && "animate-spin")} />
          </Button>
        </CardHeader>
        <CardContent>
          {dailyError ? (
            <p className="text-sm text-destructive">{dailyError}</p>
          ) : dailyLoading && !dailyData ? (
            <div className="flex items-center gap-2 py-4">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Loading daily data...</span>
            </div>
          ) : dailyData ? (
            (() => {
              const today = dailyData[dailyData.length - 1]?.date;
              return (
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2 font-medium text-muted-foreground">Date</th>
                      <th className="text-right py-2 font-medium text-muted-foreground">Events</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dailyData.map((entry) => {
                      const isToday = entry.date === today;
                      const d = new Date(entry.date + "T12:00:00");
                      const formatted = d.toLocaleDateString("en-US", { weekday: "short", month: "long", day: "numeric" });
                      return (
                        <tr
                          key={entry.date}
                          className={cn("border-b last:border-0", isToday && "font-bold")}
                        >
                          <td className="py-2">{formatted}</td>
                          <td className="text-right py-2 tabular-nums">
                            {entry.total.toLocaleString()}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              );
            })()
          ) : null}
        </CardContent>
      </Card>

      {/* Per-Type Breakdown */}
      <Card>
        <CardHeader>
          <CardTitle className="text-lg">Events by Category</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b">
                  <th className="text-left py-3 pr-4 font-medium text-muted-foreground">
                    Category
                  </th>
                  {periods.map((p) => (
                    <th
                      key={p.key}
                      className="text-right py-3 px-4 font-medium text-muted-foreground"
                    >
                      {p.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {activeTypes.map((type) => {
                  const config = EVENT_TYPE_CONFIG[type];
                  const Icon = config.icon;
                  return (
                    <tr key={type} className="border-b last:border-0">
                      <td className="py-3 pr-4">
                        <div className="flex items-center gap-2">
                          <div className={cn("p-1 rounded", config.bgColor)}>
                            <Icon className={cn("w-3.5 h-3.5", config.color)} />
                          </div>
                          <span className="font-medium">{config.label}</span>
                        </div>
                      </td>
                      {periods.map((p) => (
                        <td
                          key={p.key}
                          className="text-right py-3 px-4 tabular-nums"
                        >
                          {(p.metrics.byType[type] || 0).toLocaleString()}
                        </td>
                      ))}
                    </tr>
                  );
                })}
                <tr className="border-t-2 font-bold">
                  <td className="py-3 pr-4">Total</td>
                  {periods.map((p) => (
                    <td
                      key={p.key}
                      className="text-right py-3 px-4 tabular-nums"
                    >
                      {p.metrics.total.toLocaleString()}
                    </td>
                  ))}
                </tr>
              </tbody>
            </table>
          </div>
        </CardContent>
      </Card>
    </div>
  );
}

export default function MetricsPage() {
  return (
    <div className="min-h-screen bg-background">
      <Header />
      <Suspense
        fallback={
          <div className="flex items-center justify-center py-24">
            <Loader2 className="w-6 h-6 animate-spin text-muted-foreground" />
          </div>
        }
      >
        <MetricsContent />
      </Suspense>
    </div>
  );
}

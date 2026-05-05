"use client";

import { Suspense, useState, useEffect, useCallback } from "react";
import { Loader2, TrendingUp, Calendar, BarChart3, Clock, RefreshCw, Globe, Download, Infinity as InfinityIcon } from "lucide-react";
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

interface MonthlyEntry {
  key: string;
  label: string;
  startDate: string;
  endDate: string;
  total: number;
  partial: boolean;
}

interface MonthlyResponse {
  months: MonthlyEntry[];
  partial: boolean;
}

interface GeoEntry {
  country: string;
  count: number;
  pct: number;
}

interface GeoResponse {
  countries: GeoEntry[];
  total: number;
  resolved: number;
  unresolved: number;
}

interface PeriodMetrics {
  total: number;
  byType: Record<string, number>;
  partial?: boolean;
}

interface MetricsResponse {
  all: PeriodMetrics;
  "60d": PeriodMetrics;
  "30d": PeriodMetrics;
  "7d": PeriodMetrics;
  "24h": PeriodMetrics;
}

function MetricsContent() {
  const [summaryData, setSummaryData] = useState<MetricsResponse | null>(null);
  const [summaryLoading, setSummaryLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [breakdownData, setBreakdownData] = useState<MetricsResponse | null>(null);
  const [breakdownLoading, setBreakdownLoading] = useState(true);
  const [breakdownError, setBreakdownError] = useState<string | null>(null);

  const [dailyData, setDailyData] = useState<DailyEntry[] | null>(null);
  const [dailyLoading, setDailyLoading] = useState(true);
  const [dailyError, setDailyError] = useState<string | null>(null);

  const [monthlyData, setMonthlyData] = useState<MonthlyResponse | null>(null);
  const [monthlyLoading, setMonthlyLoading] = useState(true);
  const [monthlyError, setMonthlyError] = useState<string | null>(null);

  const [geoData, setGeoData] = useState<GeoResponse | null>(null);
  const [geoLoading, setGeoLoading] = useState(true);

  const getMetricsHeaders = useCallback(() => {
    const apiKey = getApiKey();
    const headers: Record<string, string> = {};
    if (apiKey) headers.Authorization = apiKey;
    return headers;
  }, []);

  const fetchMonthly = useCallback(async () => {
    setMonthlyLoading(true);
    setMonthlyError(null);
    try {
      const res = await fetch("/api/metrics/monthly", {
        headers: getMetricsHeaders(),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Unknown error" }));
        throw new Error(err.error || `API error: ${res.status}`);
      }
      setMonthlyData(await res.json());
    } catch (err) {
      setMonthlyError(err instanceof Error ? err.message : "Failed to load monthly metrics");
    } finally {
      setMonthlyLoading(false);
    }
  }, [getMetricsHeaders]);

  const fetchDaily = useCallback(async () => {
    setDailyLoading(true);
    setDailyError(null);
    try {
      const res = await fetch("/api/metrics/daily", {
        headers: getMetricsHeaders(),
      });
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
  }, [getMetricsHeaders]);

  const fetchBreakdown = useCallback(async () => {
    setBreakdownLoading(true);
    setBreakdownError(null);
    try {
      const res = await fetch("/api/metrics?mode=breakdown", {
        headers: getMetricsHeaders(),
      });
      if (!res.ok) {
        const err = await res.json().catch(() => ({ error: "Unknown error" }));
        throw new Error(err.error || `API error: ${res.status}`);
      }
      setBreakdownData(await res.json());
    } catch (err) {
      setBreakdownError(
        err instanceof Error ? err.message : "Failed to load category breakdown"
      );
    } finally {
      setBreakdownLoading(false);
    }
  }, [getMetricsHeaders]);

  useEffect(() => {
    async function load() {
      try {
        const res = await fetch("/api/metrics?mode=summary", {
          headers: getMetricsHeaders(),
        });
        if (!res.ok) {
          const err = await res.json().catch(() => ({ error: "Unknown error" }));
          throw new Error(err.error || `API error: ${res.status}`);
        }
        setSummaryData(await res.json());
      } catch (err) {
        setError(err instanceof Error ? err.message : "Failed to load metrics");
      } finally {
        setSummaryLoading(false);
        void fetchBreakdown();
        void fetchDaily();
        void fetchMonthly();
        fetch("/api/metrics/geo")
          .then((r) => r.json())
          .then((d) => setGeoData(d))
          .finally(() => setGeoLoading(false));
      }
    }
    void load();
  }, [fetchBreakdown, fetchDaily, fetchMonthly, getMetricsHeaders]);

  if (summaryLoading && !summaryData) {
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

  if (!summaryData) return null;

  const periods = [
    { label: "All", sublabel: "since Feb 1, 2025", key: "all" as const, metrics: summaryData.all, icon: InfinityIcon },
    { label: "60 Days", key: "60d" as const, metrics: summaryData["60d"], icon: Calendar },
    { label: "30 Days", key: "30d" as const, metrics: summaryData["30d"], icon: TrendingUp },
    { label: "7 Days", key: "7d" as const, metrics: summaryData["7d"], icon: BarChart3 },
    { label: "24 Hours", key: "24h" as const, metrics: summaryData["24h"], icon: Clock },
  ];

  const breakdownPeriods = breakdownData
    ? [
        { label: "All", key: "all" as const, metrics: breakdownData.all },
        { label: "60 Days", key: "60d" as const, metrics: breakdownData["60d"] },
        { label: "30 Days", key: "30d" as const, metrics: breakdownData["30d"] },
        { label: "7 Days", key: "7d" as const, metrics: breakdownData["7d"] },
        { label: "24 Hours", key: "24h" as const, metrics: breakdownData["24h"] },
      ]
    : [];

  const activeTypes = breakdownData
    ? ALL_EVENT_TYPES.filter(
        (type) =>
          breakdownData.all.byType[type] > 0 ||
          breakdownData["60d"].byType[type] > 0 ||
          breakdownData["30d"].byType[type] > 0 ||
          breakdownData["7d"].byType[type] > 0 ||
          breakdownData["24h"].byType[type] > 0
      )
    : [];
  const breakdownPartial = breakdownPeriods.some((period) => period.metrics.partial);

  return (
    <div className="container mx-auto px-4 py-6 space-y-6">
      {/* Summary Cards */}
      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
        {periods.map((period) => {
          const Icon = period.icon;
          const isAll = period.key === "all";
          return (
            <Card key={period.key}>
              <CardHeader className="flex flex-row items-center justify-between pb-2">
                <CardTitle className="text-sm font-medium text-muted-foreground">
                  {isAll ? period.label : `Past ${period.label}`}
                </CardTitle>
                <Icon className="w-4 h-4 text-muted-foreground" />
              </CardHeader>
              <CardContent>
                <div className="text-3xl font-bold">
                  {period.metrics.total.toLocaleString()}
                </div>
                <p className="text-xs text-muted-foreground mt-1">
                  {period.sublabel ?? "total events"}
                </p>
                {period.metrics.partial && (
                  <p className="text-xs text-amber-600 mt-1">
                    Partial — some chunks rate-limited; refresh to retry
                  </p>
                )}
              </CardContent>
            </Card>
          );
        })}
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        {/* Daily Events (Last 7 Days) */}
        <Card>
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

        {/* Monthly Events (Since Jan 2026) */}
        <Card>
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle className="text-lg">Monthly Events (Since Jan 2026)</CardTitle>
            <Button
              variant="ghost"
              size="icon"
              onClick={fetchMonthly}
              disabled={monthlyLoading}
              className="h-8 w-8"
            >
              <RefreshCw className={cn("w-4 h-4", monthlyLoading && "animate-spin")} />
            </Button>
          </CardHeader>
          <CardContent>
            {monthlyError ? (
              <p className="text-sm text-destructive">{monthlyError}</p>
            ) : monthlyLoading && !monthlyData ? (
              <div className="flex items-center gap-2 py-4">
                <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
                <span className="text-sm text-muted-foreground">Loading monthly data...</span>
              </div>
            ) : monthlyData && monthlyData.months.length > 0 ? (
              <>
                <table className="w-full text-sm">
                  <thead>
                    <tr className="border-b">
                      <th className="text-left py-2 font-medium text-muted-foreground">Month</th>
                      <th className="text-right py-2 font-medium text-muted-foreground">Events</th>
                    </tr>
                  </thead>
                  <tbody>
                    {monthlyData.months.map((entry, i) => {
                      const isCurrent = i === monthlyData.months.length - 1;
                      return (
                        <tr
                          key={entry.key}
                          className={cn("border-b last:border-0", isCurrent && "font-bold")}
                        >
                          <td className="py-2">
                            {entry.label}
                            {isCurrent && (
                              <span className="ml-2 text-xs font-normal text-muted-foreground">
                                (in progress)
                              </span>
                            )}
                          </td>
                          <td className="text-right py-2 tabular-nums">
                            {entry.total.toLocaleString()}
                            {entry.partial && (
                              <span className="ml-2 text-xs text-amber-600">partial</span>
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
                {monthlyData.partial && (
                  <p className="text-xs text-amber-600 mt-3">
                    Some months rate-limited by Bee Maps; refresh to retry.
                  </p>
                )}
              </>
            ) : (
              <p className="text-sm text-muted-foreground py-4">No monthly data available.</p>
            )}
          </CardContent>
        </Card>
      </div>

      {/* Signal Events by Country */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <div className="flex items-center gap-2">
            <Globe className="w-4 h-4 text-muted-foreground" />
            <CardTitle className="text-lg">Signal Events by Country</CardTitle>
          </div>
          <div className="flex items-center gap-3">
            {geoData && geoData.resolved != null && (
              <span className="text-xs text-muted-foreground">
                {geoData.resolved.toLocaleString()} of {geoData.total.toLocaleString()} with location data
              </span>
            )}
            {geoData && geoData.countries.length > 0 && (
              <Button
                variant="outline"
                size="sm"
                className="h-7 text-xs gap-1"
                onClick={() => {
                  const header = "Country,Count,Percent\n";
                  const rows = geoData.countries
                    .map((c) => `"${c.country}",${c.count},${c.pct}`)
                    .join("\n");
                  const blob = new Blob([header + rows], { type: "text/csv" });
                  const url = URL.createObjectURL(blob);
                  const a = document.createElement("a");
                  a.href = url;
                  a.download = "signal-events-by-country.csv";
                  a.click();
                  URL.revokeObjectURL(url);
                }}
              >
                <Download className="w-3 h-3" />
                CSV
              </Button>
            )}
          </div>
        </CardHeader>
        <CardContent>
          {geoLoading ? (
            <div className="flex items-center gap-2 py-4">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Resolving countries...</span>
            </div>
          ) : geoData && geoData.countries.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-2 pr-4 font-medium text-muted-foreground">#</th>
                    <th className="text-left py-2 pr-4 font-medium text-muted-foreground">Country</th>
                    <th className="text-right py-2 px-4 font-medium text-muted-foreground">Count</th>
                    <th className="text-right py-2 pl-4 font-medium text-muted-foreground">%</th>
                  </tr>
                </thead>
                <tbody>
                  {geoData.countries.map((entry, i) => (
                    <tr key={entry.country} className="border-b last:border-0">
                      <td className="py-2 pr-4 text-muted-foreground tabular-nums">{i + 1}</td>
                      <td className="py-2 pr-4 font-medium">{entry.country}</td>
                      <td className="py-2 px-4 text-right tabular-nums">{entry.count.toLocaleString()}</td>
                      <td className="py-2 pl-4 text-right tabular-nums text-muted-foreground">{entry.pct}%</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <p className="text-sm text-muted-foreground py-4">No geo data available.</p>
          )}
        </CardContent>
      </Card>

      {/* Per-Type Breakdown */}
      <Card>
        <CardHeader className="flex flex-row items-center justify-between">
          <CardTitle className="text-lg">Events by Category</CardTitle>
          {breakdownLoading && (
            <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
              <Loader2 className="w-3 h-3 animate-spin" />
              Loading breakdown
            </span>
          )}
        </CardHeader>
        <CardContent>
          {breakdownError ? (
            <p className="text-sm text-destructive">{breakdownError}</p>
          ) : breakdownLoading && !breakdownData ? (
            <div className="flex items-center gap-2 py-4">
              <Loader2 className="w-4 h-4 animate-spin text-muted-foreground" />
              <span className="text-sm text-muted-foreground">Loading category breakdown...</span>
            </div>
          ) : breakdownData ? (
          <>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b">
                    <th className="text-left py-3 pr-4 font-medium text-muted-foreground">
                      Category
                    </th>
                    {breakdownPeriods.map((p) => (
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
                        {breakdownPeriods.map((p) => (
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
                    {breakdownPeriods.map((p) => (
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
            {breakdownPartial && (
              <p className="text-xs text-amber-600 mt-3">
                Some category counts are partial because Bee Maps rate-limited a
                count chunk; refresh to retry.
              </p>
            )}
          </>
          ) : null}
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

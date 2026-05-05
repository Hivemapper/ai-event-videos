"use client";

import { useState, useMemo } from "react";
import { ArrowUpDown, ArrowUp, ArrowDown, Copy, Check } from "lucide-react";

interface SpeedDataPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

function formatSpeed(speedMs: number): string {
  const mph = speedMs * 2.237;
  return `${mph.toFixed(1)} mph`;
}

function formatTimestamp(timestamp: number): string {
  const date = new Date(timestamp);
  return date.toLocaleTimeString();
}

type SortDirection = "asc" | "desc" | null;
type SortColumn = "speed" | "timestamp" | null;

interface MetadataTableProps {
  metadata: Record<string, unknown>;
  eventId?: string;
}

function SortIcon({
  column,
  sortColumn,
  sortDirection,
}: {
  column: SortColumn;
  sortColumn: SortColumn;
  sortDirection: SortDirection;
}) {
  if (sortColumn !== column) {
    return <ArrowUpDown className="w-3 h-3 ml-1 opacity-50" />;
  }
  if (sortDirection === "asc") {
    return <ArrowUp className="w-3 h-3 ml-1" />;
  }
  return <ArrowDown className="w-3 h-3 ml-1" />;
}

export function MetadataTable({ metadata, eventId }: MetadataTableProps) {
  const [sortColumn, setSortColumn] = useState<SortColumn>(null);
  const [copiedId, setCopiedId] = useState(false);
  const [sortDirection, setSortDirection] = useState<SortDirection>(null);

  const speedData = metadata?.SPEED_ARRAY as SpeedDataPoint[] | undefined;

  const sortedSpeedData = useMemo(() => {
    if (!speedData) return [];
    if (!sortColumn || !sortDirection) return speedData;

    return [...speedData].sort((a, b) => {
      let comparison = 0;
      if (sortColumn === "speed") {
        comparison = a.AVG_SPEED_MS - b.AVG_SPEED_MS;
      } else if (sortColumn === "timestamp") {
        comparison = a.TIMESTAMP - b.TIMESTAMP;
      }
      return sortDirection === "asc" ? comparison : -comparison;
    });
  }, [speedData, sortColumn, sortDirection]);

  const handleSort = (column: SortColumn) => {
    if (sortColumn === column) {
      if (sortDirection === "asc") {
        setSortDirection("desc");
      } else if (sortDirection === "desc") {
        setSortColumn(null);
        setSortDirection(null);
      }
    } else {
      setSortColumn(column);
      setSortDirection("asc");
    }
  };

  // Get other metadata fields (not SPEED_ARRAY)
  const otherFields = Object.entries(metadata).filter(
    ([key]) => key !== "SPEED_ARRAY"
  );

  return (
    <div className="space-y-4">
      {/* Other metadata fields */}
      {otherFields.length > 0 && (
        <div className="overflow-hidden rounded-lg border">
          <table className="w-full text-sm">
            <thead className="bg-muted/50">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Field</th>
                <th className="px-4 py-2 text-left font-medium">Value</th>
              </tr>
            </thead>
            <tbody>
              {eventId && (
                <tr className="border-t">
                  <td className="px-4 py-2 font-mono text-muted-foreground">
                    EVENT_ID
                  </td>
                  <td className="px-4 py-2">
                    <span className="inline-flex items-center gap-1.5">
                      <span className="font-mono text-xs">{eventId}</span>
                      <button
                        onClick={async () => {
                          await navigator.clipboard.writeText(eventId);
                          setCopiedId(true);
                          setTimeout(() => setCopiedId(false), 2000);
                        }}
                        className="inline-flex items-center justify-center h-5 w-5 rounded hover:bg-muted transition-colors text-muted-foreground hover:text-foreground"
                        title="Copy Event ID"
                      >
                        {copiedId ? <Check className="w-3 h-3 text-green-600" /> : <Copy className="w-3 h-3" />}
                      </button>
                    </span>
                  </td>
                </tr>
              )}
              {otherFields.map(([key, value]) => (
                <tr key={key} className="border-t">
                  <td className="px-4 py-2 font-mono text-muted-foreground">
                    {key}
                  </td>
                  <td className="px-4 py-2">
                    {typeof value === "number"
                      ? key.includes("SPEED")
                        ? formatSpeed(value)
                        : key.includes("ACCELERATION")
                        ? `${value.toFixed(3)} m/s²`
                        : value.toFixed(4)
                      : String(value)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {/* Speed array table */}
      {sortedSpeedData.length > 0 && (
        <div>
          <h4 className="text-sm font-medium mb-2">Speed Data ({sortedSpeedData.length} points)</h4>
          <div className="overflow-hidden rounded-lg border max-h-64 overflow-y-auto">
            <table className="w-full text-sm">
              <thead className="bg-muted/50 sticky top-0">
                <tr>
                  <th className="px-4 py-2 text-left font-medium">#</th>
                  <th
                    className="px-4 py-2 text-left font-medium cursor-pointer hover:bg-muted/70 select-none"
                    onClick={() => handleSort("speed")}
                  >
                    <span className="flex items-center">
                      Speed
                      <SortIcon column="speed" sortColumn={sortColumn} sortDirection={sortDirection} />
                    </span>
                  </th>
                  <th
                    className="px-4 py-2 text-left font-medium cursor-pointer hover:bg-muted/70 select-none"
                    onClick={() => handleSort("timestamp")}
                  >
                    <span className="flex items-center">
                      Timestamp
                      <SortIcon column="timestamp" sortColumn={sortColumn} sortDirection={sortDirection} />
                    </span>
                  </th>
                </tr>
              </thead>
              <tbody>
                {sortedSpeedData.map((point, index) => (
                  <tr key={index} className="border-t hover:bg-muted/30">
                    <td className="px-4 py-2 text-muted-foreground">{index + 1}</td>
                    <td className="px-4 py-2 font-mono">
                      {formatSpeed(point.AVG_SPEED_MS)}
                    </td>
                    <td className="px-4 py-2 font-mono text-muted-foreground">
                      {formatTimestamp(point.TIMESTAMP)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

import { revalidatePath } from "next/cache";
import { notFound } from "next/navigation";
import { Header } from "@/components/layout/header";
import { CustomerPicker } from "@/components/customers/customer-picker";
import {
  getCustomerEventsReadDb,
  getCustomerEventsWriteDb,
} from "@/lib/customer-events-read-db";
import {
  loadCustomerEventList,
  removeCustomerEvent,
  type CustomerEventRow,
  type CustomerSortDir,
  type CustomerSortKey,
} from "@/lib/customer-events-store";
import { cn } from "@/lib/utils";

export const dynamic = "force-dynamic";
export const revalidate = 0;

const SORT_LABELS: Record<CustomerSortKey, string> = {
  position: "#",
  date: "Date",
  bitrate: "Bitrate",
  fps: "FPS QC",
  vru: "VRU",
  production: "Production",
};

interface CustomerPageProps {
  params: Promise<{ slug: string }>;
  searchParams: Promise<{ sort?: string; dir?: string }>;
}

export default async function CustomerPage({
  params,
  searchParams,
}: CustomerPageProps) {
  const [{ slug }, { sort, dir }] = await Promise.all([params, searchParams]);
  const db = getCustomerEventsReadDb();
  const list = await loadCustomerEventList(db, slug, sort, dir);

  if (!list) notFound();

  const summary = summarizeRows(list.rows);

  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="mb-5 flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
          <div>
            <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
              Customers
            </p>
            <h1 className="mt-1 text-2xl font-semibold">{list.customer.name}</h1>
            <p className="mt-1 text-sm text-muted-foreground">
              {list.rows.length} videos · VRU complete {summary.vruComplete} ·
              Production complete {summary.productionComplete}
            </p>
          </div>
          <CustomerPicker customers={list.customers} slug={list.customer.slug} />
        </div>

        <div className="overflow-x-auto rounded-md border bg-background">
          <table className="w-full min-w-[1040px] text-sm">
            <thead className="border-b bg-muted/40 text-xs uppercase tracking-wide text-muted-foreground">
              <tr>
                <th className="w-12 px-2 py-2 text-center font-medium">X</th>
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="position"
                  activeSort={list.sort}
                  dir={list.dir}
                  className="w-16"
                />
                <th className="px-3 py-2 text-left font-medium">Event</th>
                <th className="px-3 py-2 text-left font-medium">Type</th>
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="vru"
                  activeSort={list.sort}
                  dir={list.dir}
                />
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="production"
                  activeSort={list.sort}
                  dir={list.dir}
                />
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="fps"
                  activeSort={list.sort}
                  dir={list.dir}
                />
                <th className="px-3 py-2 text-right font-medium">Late %</th>
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="bitrate"
                  activeSort={list.sort}
                  dir={list.dir}
                  align="right"
                />
                <SortableHeader
                  customerSlug={list.customer.slug}
                  sortKey="date"
                  activeSort={list.sort}
                  dir={list.dir}
                />
              </tr>
            </thead>
            <tbody>
              {list.rows.map((row) => (
                <CustomerEventTableRow
                  key={row.eventId}
                  customerSlug={list.customer.slug}
                  row={row}
                />
              ))}
            </tbody>
          </table>
        </div>
      </main>
    </>
  );
}

async function removeCustomerEventAction(formData: FormData) {
  "use server";

  const customerSlug = String(formData.get("customerSlug") ?? "").trim();
  const eventId = String(formData.get("eventId") ?? "").trim();
  if (!customerSlug || !eventId) return;

  await removeCustomerEvent(getCustomerEventsWriteDb(), customerSlug, eventId);
  revalidatePath(`/customers/${customerSlug}`);
}

function SortableHeader({
  customerSlug,
  sortKey,
  activeSort,
  dir,
  align = "left",
  className,
}: {
  customerSlug: string;
  sortKey: CustomerSortKey;
  activeSort: CustomerSortKey;
  dir: CustomerSortDir;
  align?: "left" | "right";
  className?: string;
}) {
  const active = activeSort === sortKey;
  const nextDir = active && dir === "asc" ? "desc" : "asc";

  return (
    <th
      className={cn(
        "px-3 py-2 font-medium",
        align === "right" ? "text-right" : "text-left",
        className
      )}
    >
      <a
        className={cn(
          "inline-flex items-center gap-1 rounded px-1 py-0.5 hover:bg-muted",
          active && "text-foreground"
        )}
        href={`/customers/${customerSlug}?sort=${sortKey}&dir=${nextDir}`}
      >
        {SORT_LABELS[sortKey]}
        <span aria-hidden="true" className="text-muted-foreground">
          {active ? (dir === "asc" ? "^" : "v") : "<>"}
        </span>
      </a>
    </th>
  );
}

function CustomerEventTableRow({
  customerSlug,
  row,
}: {
  customerSlug: string;
  row: CustomerEventRow;
}) {
  return (
    <tr className="border-b last:border-b-0 hover:bg-muted/30">
      <td className="px-2 py-2 text-center">
        <form action={removeCustomerEventAction}>
          <input type="hidden" name="customerSlug" value={customerSlug} />
          <input type="hidden" name="eventId" value={row.eventId} />
          <button
            type="submit"
            aria-label={`Remove ${row.eventId} from ${customerSlug}`}
            className="inline-flex h-6 w-6 items-center justify-center rounded text-xs font-medium text-muted-foreground hover:bg-destructive/10 hover:text-destructive"
          >
            X
          </button>
        </form>
      </td>
      <td className="px-3 py-2 font-mono text-xs text-muted-foreground">
        {row.position}
      </td>
      <td className="px-3 py-2">
        <a
          className="font-mono text-xs text-primary underline-offset-2 hover:underline"
          href={`/event/${row.eventId}`}
        >
          {row.eventId}
        </a>
      </td>
      <td className="px-3 py-2">
        <StatusPill value={formatEventType(row.eventType)} tone="neutral" />
      </td>
      <td className="px-3 py-2">
        <div className="flex items-center gap-2">
          <StatusPill value={formatStatus(row.vruStatus)} tone={vruTone(row.vruStatus)} />
          {row.vruLabel && (
            <span className="text-xs text-muted-foreground">
              {row.vruLabel}
              {row.vruConfidence !== null
                ? ` ${Math.round(row.vruConfidence * 100)}%`
                : ""}
            </span>
          )}
        </div>
      </td>
      <td className="px-3 py-2">
        <StatusPill
          value={formatStatus(row.productionStatus)}
          tone={productionTone(row.productionStatus)}
        />
      </td>
      <td className="px-3 py-2">
        <StatusPill value={formatFpsQc(row.fpsQc)} tone={fpsTone(row.fpsQc)} />
      </td>
      <td className="px-3 py-2 text-right font-mono text-xs text-muted-foreground">
        {formatPct(row.lateFramePct)}
      </td>
      <td className="px-3 py-2 text-right font-mono text-xs text-muted-foreground">
        {formatBitrate(row.bitrateBps)}
      </td>
      <td className="px-3 py-2 text-muted-foreground">
        {formatDate(row.eventTimestamp)}
      </td>
    </tr>
  );
}

function StatusPill({
  value,
  tone,
}: {
  value: string;
  tone: "neutral" | "good" | "warn" | "bad" | "info";
}) {
  return (
    <span
      className={cn(
        "inline-flex whitespace-nowrap rounded-full border px-2 py-0.5 text-xs font-medium",
        tone === "good" && "border-emerald-200 bg-emerald-50 text-emerald-700",
        tone === "warn" && "border-amber-200 bg-amber-50 text-amber-700",
        tone === "bad" && "border-rose-200 bg-rose-50 text-rose-700",
        tone === "info" && "border-sky-200 bg-sky-50 text-sky-700",
        tone === "neutral" && "border-border bg-muted text-muted-foreground"
      )}
    >
      {value}
    </span>
  );
}

function summarizeRows(rows: CustomerEventRow[]) {
  return rows.reduce(
    (summary, row) => ({
      vruComplete: summary.vruComplete + (row.vruStatus === "completed" ? 1 : 0),
      productionComplete:
        summary.productionComplete +
        (row.productionStatus === "completed" ? 1 : 0),
    }),
    { vruComplete: 0, productionComplete: 0 }
  );
}

function formatEventType(value: string | null): string {
  if (!value) return "Unknown";
  return value
    .toLowerCase()
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatStatus(value: string): string {
  return value
    .replace(/_/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatFpsQc(value: string | null): string {
  return value ? formatStatus(value) : "-";
}

function formatPct(value: number | null): string {
  return value === null ? "-" : `${value.toFixed(2)}%`;
}

function formatBitrate(value: number | null): string {
  return value === null || value <= 0 ? "-" : (value / 1_000_000).toFixed(2);
}

function formatDate(value: string | null): string {
  if (!value) return "-";
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return value;
  return date.toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  });
}

function vruTone(status: string): "neutral" | "good" | "warn" | "bad" | "info" {
  if (status === "completed") return "good";
  if (status === "running") return "info";
  if (status === "queued") return "warn";
  if (status === "failed" || status === "cancelled") return "bad";
  return "neutral";
}

function productionTone(status: string): "neutral" | "good" | "warn" | "bad" | "info" {
  if (status === "completed") return "good";
  if (status === "processing") return "info";
  if (status === "queued") return "warn";
  if (status === "failed") return "bad";
  return "neutral";
}

function fpsTone(value: string | null): "neutral" | "good" | "warn" | "bad" | "info" {
  if (value === "perfect") return "good";
  if (value === "ok") return "info";
  if (value === "filter_out") return "bad";
  return "neutral";
}

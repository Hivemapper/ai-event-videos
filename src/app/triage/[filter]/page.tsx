import { redirect } from "next/navigation";
import { ALL_EVENT_TYPES } from "@/lib/constants";

type SearchValue = string | string[] | undefined;

const VALID_FILTERS = new Set([
  "all",
  "filtered",
  "signal",
  "missing_video",
  "missing_metadata",
  "ghost",
  "open_road",
  "duplicate",
  "non_linear",
  "privacy",
  "skipped_firmware",
]);

function first(value: SearchValue): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

export default async function TriageFilterRedirect({
  params,
  searchParams,
}: {
  params: Promise<{ filter: string }>;
  searchParams: Promise<Record<string, SearchValue>>;
}) {
  const [{ filter }, resolvedSearchParams] = await Promise.all([params, searchParams]);
  const status = VALID_FILTERS.has(filter) ? filter : "all";
  const next = new URLSearchParams({
    stage: "triage",
    status,
  });
  const period = first(resolvedSearchParams.period);
  const sort = first(resolvedSearchParams.sort);
  const dir = first(resolvedSearchParams.dir);
  const excludeTypes = first(resolvedSearchParams.excludeTypes);

  if (period) next.set("period", period);
  if (sort === "fps_qc") next.set("sort", "fps_qc");
  if (sort === "event_type") next.set("sort", "event_type");
  if (dir === "asc" || dir === "desc") next.set("dir", dir);

  if (excludeTypes) {
    const excluded = new Set(excludeTypes.split(",").map((type) => type.trim()).filter(Boolean));
    const included = ALL_EVENT_TYPES.filter((type) => !excluded.has(type));
    if (included.length > 0 && included.length < ALL_EVENT_TYPES.length) {
      next.set("eventTypes", included.join(","));
    }
  }

  redirect(`/pipeline?${next.toString()}`);
}

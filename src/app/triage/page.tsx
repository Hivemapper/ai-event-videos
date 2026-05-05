import { redirect } from "next/navigation";
import { ALL_EVENT_TYPES } from "@/lib/constants";

type SearchValue = string | string[] | undefined;

function first(value: SearchValue): string | undefined {
  return Array.isArray(value) ? value[0] : value;
}

function buildTriageRedirect(searchParams: Record<string, SearchValue>, status = "all") {
  const next = new URLSearchParams({
    stage: "triage",
    status,
  });
  const period = first(searchParams.period);
  const sort = first(searchParams.sort);
  const dir = first(searchParams.dir);
  const excludeTypes = first(searchParams.excludeTypes);

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

export default async function TriageRedirect({
  searchParams,
}: {
  searchParams: Promise<Record<string, SearchValue>>;
}) {
  buildTriageRedirect(await searchParams);
}

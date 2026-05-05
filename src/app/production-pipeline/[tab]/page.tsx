import { redirect } from "next/navigation";

const VALID_TABS = new Set(["queued", "processing", "completed", "failed"]);

export default async function ProductionPipelineTabRedirect({
  params,
}: {
  params: Promise<{ tab: string }>;
}) {
  const { tab } = await params;
  const status = VALID_TABS.has(tab) ? tab : "queued";
  redirect(`/pipeline?stage=production&status=${status}`);
}

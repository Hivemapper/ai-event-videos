import { notFound } from "next/navigation";
import { PipelineDayDetail } from "@/components/pipeline/pipeline-day-detail";

function isValidDay(value: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

export default async function PipelineDayPage({
  params,
}: {
  params: Promise<{ day: string }>;
}) {
  const { day } = await params;

  if (!isValidDay(day)) {
    notFound();
  }

  return <PipelineDayDetail day={day} />;
}

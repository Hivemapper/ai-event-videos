import { redirect } from "next/navigation";

export default function ProductionPipelineRedirect() {
  redirect("/production-pipeline/queued");
}

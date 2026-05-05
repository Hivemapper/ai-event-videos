import { redirect } from "next/navigation";

export default function ProductionPipelineRedirect() {
  redirect("/pipeline?stage=production&status=queued");
}

import { redirect } from "next/navigation";

export default function PipelineRedirect() {
  redirect("/pipeline/queued");
}

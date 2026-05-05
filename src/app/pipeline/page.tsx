import { Suspense } from "react";
import { Header } from "@/components/layout/header";
import { Skeleton } from "@/components/ui/skeleton";
import { PipelineOverview } from "@/components/pipeline/pipeline-overview";

export default function PipelinePage() {
  return (
    <>
      <Header />
      <main className="container mx-auto px-4 py-6">
        <div className="mb-5">
          <h1 className="text-xl font-semibold">Pipeline</h1>
          <p className="text-sm text-muted-foreground">
            Triage, VRU detection, and production review in one workflow.
          </p>
        </div>
        <Suspense fallback={<Skeleton className="h-96 w-full" />}>
          <PipelineOverview />
        </Suspense>
      </main>
    </>
  );
}

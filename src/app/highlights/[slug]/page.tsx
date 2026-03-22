"use client";

import { use, Suspense } from "react";
import { redirect } from "next/navigation";
import {
  HighlightsContent,
  HighlightsSkeleton,
  SLUG_TO_TAB,
  DEFAULT_SLUG,
} from "../page";

export default function HighlightSlugPage({
  params,
}: {
  params: Promise<{ slug: string }>;
}) {
  const { slug } = use(params);
  const tabId = SLUG_TO_TAB[slug];

  if (!tabId) {
    redirect(`/highlights/${DEFAULT_SLUG}`);
  }

  return (
    <Suspense fallback={<HighlightsSkeleton />}>
      <HighlightsContent initialTab={tabId} />
    </Suspense>
  );
}

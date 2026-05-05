"use client";

import { useEffect, useState } from "react";

export function useRetainedValue<T>(value: T | undefined): T | undefined {
  const [retained, setRetained] = useState<T | undefined>(value);

  useEffect(() => {
    if (value !== undefined) {
      queueMicrotask(() => setRetained(value));
    }
  }, [value]);

  return value ?? retained;
}

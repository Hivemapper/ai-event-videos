export function parseSpeedArray(metadata: Record<string, unknown> | undefined): {
  maxSpeed: number;
  minSpeed: number;
} {
  if (!metadata?.SPEED_ARRAY) return { maxSpeed: 0, minSpeed: 0 };
  try {
    const speeds = metadata.SPEED_ARRAY as number[];
    if (!Array.isArray(speeds) || speeds.length === 0)
      return { maxSpeed: 0, minSpeed: 0 };
    const kmhSpeeds = speeds.map((s) => s * 3.6);
    return {
      maxSpeed: Math.max(...kmhSpeeds),
      minSpeed: Math.min(...kmhSpeeds),
    };
  } catch {
    return { maxSpeed: 0, minSpeed: 0 };
  }
}

export function getAcceleration(
  metadata: Record<string, unknown> | undefined
): number {
  if (!metadata?.ACCELERATION_MS2) return 0;
  const val = Number(metadata.ACCELERATION_MS2);
  return isNaN(val) ? 0 : val;
}

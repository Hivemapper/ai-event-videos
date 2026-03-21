interface SpeedArrayPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

export function parseSpeedArray(metadata: Record<string, unknown> | undefined): {
  maxSpeed: number;
  minSpeed: number;
  /** Peak deceleration rate in m/s² (positive = braking harder) */
  peakDeceleration: number;
  /** Duration of the speed event in seconds */
  durationSeconds: number;
} {
  const empty = { maxSpeed: 0, minSpeed: 0, peakDeceleration: 0, durationSeconds: 0 };
  if (!metadata?.SPEED_ARRAY) return empty;

  try {
    const raw = metadata.SPEED_ARRAY;
    if (!Array.isArray(raw) || raw.length === 0) return empty;

    // SPEED_ARRAY can be either { AVG_SPEED_MS, TIMESTAMP }[] or plain number[]
    let speedsMs: number[];
    let timestamps: number[] | null = null;

    if (typeof raw[0] === "object" && raw[0] !== null && "AVG_SPEED_MS" in raw[0]) {
      const points = raw as SpeedArrayPoint[];
      speedsMs = points.map((p) => p.AVG_SPEED_MS);
      timestamps = points.map((p) => p.TIMESTAMP);
    } else if (typeof raw[0] === "number") {
      speedsMs = raw as number[];
    } else {
      return empty;
    }

    const kmhSpeeds = speedsMs.map((s) => s * 3.6);
    const maxSpeed = Math.max(...kmhSpeeds);
    const minSpeed = Math.min(...kmhSpeeds);

    // Compute peak deceleration (biggest speed drop per second between consecutive samples)
    let peakDeceleration = 0;
    for (let i = 1; i < speedsMs.length; i++) {
      const dv = speedsMs[i - 1] - speedsMs[i]; // positive = decelerating
      if (dv <= 0) continue;

      let dt: number;
      if (timestamps && timestamps[i] !== timestamps[i - 1]) {
        dt = (timestamps[i] - timestamps[i - 1]) / 1000; // ms to seconds
      } else {
        dt = 0.25; // default ~4Hz sampling
      }
      if (dt <= 0) dt = 0.25;

      const decel = dv / dt;
      if (decel > peakDeceleration) peakDeceleration = decel;
    }

    let durationSeconds = 0;
    if (timestamps && timestamps.length >= 2) {
      durationSeconds = (timestamps[timestamps.length - 1] - timestamps[0]) / 1000;
    }

    return { maxSpeed, minSpeed, peakDeceleration, durationSeconds };
  } catch {
    return { maxSpeed: 0, minSpeed: 0, peakDeceleration: 0, durationSeconds: 0 };
  }
}

export function getAcceleration(
  metadata: Record<string, unknown> | undefined
): number {
  if (!metadata?.ACCELERATION_MS2) return 0;
  const val = Number(metadata.ACCELERATION_MS2);
  return isNaN(val) ? 0 : val;
}

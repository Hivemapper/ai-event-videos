import { GnssDataPoint, ImuDataPoint } from "@/types/events";

export interface SpeedPoint {
  /** Normalized time 0..1 within the event duration */
  t: number;
  /** Speed in m/s */
  speed: number;
}

export interface AccelPoint {
  /** Normalized time 0..1 within the event duration */
  t: number;
  /** Net acceleration magnitude in m/s² (gravity removed) */
  accel: number;
}

export interface SpeedProfile {
  speedPoints: SpeedPoint[];
  accelPoints: AccelPoint[];
  maxSpeed: number;
  maxAccel: number;
}

const EARTH_RADIUS = 6371000; // meters

function haversineDistance(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const dLat = ((lat2 - lat1) * Math.PI) / 180;
  const dLon = ((lon2 - lon1) * Math.PI) / 180;
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos((lat1 * Math.PI) / 180) *
      Math.cos((lat2 * Math.PI) / 180) *
      Math.sin(dLon / 2) ** 2;
  return EARTH_RADIUS * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

export function computeGnssSpeed(gnssData: GnssDataPoint[]): SpeedPoint[] {
  if (gnssData.length < 2) return [];

  const startTime = gnssData[0].timestamp;
  const endTime = gnssData[gnssData.length - 1].timestamp;
  const totalDuration = endTime - startTime;
  if (totalDuration <= 0) return [];

  const result: SpeedPoint[] = [];
  for (let i = 1; i < gnssData.length; i++) {
    const prev = gnssData[i - 1];
    const curr = gnssData[i];
    const dt = (curr.timestamp - prev.timestamp) / 1000; // ms → s
    if (dt <= 0) continue;
    const dist = haversineDistance(prev.lat, prev.lon, curr.lat, curr.lon);
    result.push({
      t: (curr.timestamp - startTime) / totalDuration,
      speed: dist / dt,
    });
  }
  return result;
}

const GRAVITY = 9.81;

export function computeAccelerationProfile(
  imuData: ImuDataPoint[]
): AccelPoint[] {
  if (imuData.length === 0) return [];

  const withTimestamp = imuData.filter((p) => p.timestamp && p.accelerometer);
  if (withTimestamp.length === 0) return [];

  const startTime = withTimestamp[0].timestamp;
  const endTime = withTimestamp[withTimestamp.length - 1].timestamp;
  const totalDuration = endTime - startTime;
  if (totalDuration <= 0) return [];

  return withTimestamp.map((point) => {
    const { x, y, z } = point.accelerometer!;
    // Net acceleration magnitude minus gravity (approximation)
    const magnitude = Math.sqrt(x * x + y * y + z * z);
    const netAccel = Math.abs(magnitude - GRAVITY);
    return {
      t: (point.timestamp - startTime) / totalDuration,
      accel: netAccel,
    };
  });
}

interface SpeedArrayPoint {
  AVG_SPEED_MS: number;
  TIMESTAMP: number;
}

export function generateSpeedProfile(
  gnssData?: GnssDataPoint[],
  imuData?: ImuDataPoint[],
  speedArray?: SpeedArrayPoint[]
): SpeedProfile {
  // Speed: prefer SPEED_ARRAY metadata, fallback to GNSS-derived
  let speedPoints: SpeedPoint[];

  if (speedArray && speedArray.length > 0) {
    const startTime = speedArray[0].TIMESTAMP;
    const endTime = speedArray[speedArray.length - 1].TIMESTAMP;
    const totalDuration = endTime - startTime;
    if (totalDuration > 0) {
      speedPoints = speedArray.map((p) => ({
        t: (p.TIMESTAMP - startTime) / totalDuration,
        speed: p.AVG_SPEED_MS,
      }));
    } else {
      // All same timestamp — distribute evenly
      speedPoints = speedArray.map((p, i) => ({
        t: speedArray.length > 1 ? i / (speedArray.length - 1) : 0.5,
        speed: p.AVG_SPEED_MS,
      }));
    }
  } else if (gnssData && gnssData.length >= 2) {
    speedPoints = computeGnssSpeed(gnssData);
  } else {
    speedPoints = [];
  }

  // Acceleration from IMU
  const accelPoints =
    imuData && imuData.length > 0
      ? computeAccelerationProfile(imuData)
      : [];

  const maxSpeed =
    speedPoints.length > 0
      ? Math.max(...speedPoints.map((p) => p.speed))
      : 0;
  const maxAccel =
    accelPoints.length > 0
      ? Math.max(...accelPoints.map((p) => p.accel))
      : 0;

  return { speedPoints, accelPoints, maxSpeed, maxAccel };
}

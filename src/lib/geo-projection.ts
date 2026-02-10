const EARTH_RADIUS = 6371000; // meters

function toRad(deg: number): number {
  return (deg * Math.PI) / 180;
}

function toDeg(rad: number): number {
  return (rad * 180) / Math.PI;
}

/** Bearing between two points in degrees (0=north, 90=east). */
export function calculateBearing(
  lat1: number,
  lon1: number,
  lat2: number,
  lon2: number
): number {
  const dLon = toRad(lon2 - lon1);
  const y = Math.sin(dLon) * Math.cos(toRad(lat2));
  const x =
    Math.cos(toRad(lat1)) * Math.sin(toRad(lat2)) -
    Math.sin(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.cos(dLon);
  const bearing = toDeg(Math.atan2(y, x));
  return (bearing + 360) % 360;
}

export interface CameraIntrinsics {
  focal: number;
  k1: number;
  k2: number;
}

/**
 * Undistort a normalized pixel coordinate using the Brown-Conrady radial distortion model.
 * Input: distorted normalized coords (relative to image center, divided by half-width).
 * Output: undistorted normalized coords.
 * Uses iterative inversion: r_d = r_u * (1 + k1*r_u² + k2*r_u⁴)
 */
function undistortPoint(
  xd: number,
  yd: number,
  k1: number,
  k2: number
): { x: number; y: number } {
  let xu = xd;
  let yu = yd;

  for (let i = 0; i < 5; i++) {
    const r2 = xu * xu + yu * yu;
    const r4 = r2 * r2;
    const scale = 1 + k1 * r2 + k2 * r4;
    xu = xd / scale;
    yu = yd / scale;
  }

  return { x: xu, y: yu };
}

/**
 * Convert a pixel X coordinate to a bearing offset from center of frame.
 * Uses the rectilinear (pinhole) camera model: angle = atan(x / f).
 * Optionally applies radial undistortion if camera intrinsics are provided.
 */
export function pixelToBearingOffset(
  pixelX: number,
  imageWidth: number,
  fovDegrees: number,
  intrinsics?: CameraIntrinsics
): number {
  // Normalized coordinate: -0.5 at left edge, +0.5 at right edge
  const normalizedX = (pixelX / imageWidth) - 0.5;

  if (intrinsics) {
    // Undistort the pixel coordinate
    const undistorted = undistortPoint(normalizedX, 0, intrinsics.k1, intrinsics.k2);
    // Rectilinear model: bearing = atan(x / focal)
    return toDeg(Math.atan(undistorted.x / intrinsics.focal));
  }

  // Without intrinsics, use rectilinear model with FOV-derived focal length
  // FOV = 2 * atan(0.5 / focal) → focal = 0.5 / tan(FOV/2)
  const halfFovRad = toRad(fovDegrees / 2);
  const focalFromFov = 0.5 / Math.tan(halfFovRad);
  return toDeg(Math.atan(normalizedX / focalFromFov));
}

/** Haversine forward projection: given a start point, bearing, and distance, compute the destination. */
export function projectPoint(
  lat: number,
  lon: number,
  bearingDeg: number,
  distanceMeters: number
): { lat: number; lon: number } {
  const d = distanceMeters / EARTH_RADIUS;
  const brng = toRad(bearingDeg);
  const lat1 = toRad(lat);
  const lon1 = toRad(lon);

  const lat2 = Math.asin(
    Math.sin(lat1) * Math.cos(d) +
    Math.cos(lat1) * Math.sin(d) * Math.cos(brng)
  );
  const lon2 =
    lon1 +
    Math.atan2(
      Math.sin(brng) * Math.sin(d) * Math.cos(lat1),
      Math.cos(d) - Math.sin(lat1) * Math.sin(lat2)
    );

  return { lat: toDeg(lat2), lon: toDeg(lon2) };
}

/** Project a detected actor's bounding box to a world position using camera geometry. */
export function projectActorToWorld(
  bbox: { x_min: number; x_max: number },
  distanceMeters: number,
  cameraLat: number,
  cameraLon: number,
  cameraBearing: number,
  imageWidth: number,
  fovDegrees: number,
  intrinsics?: CameraIntrinsics
): { lat: number; lon: number; bearing: number } {
  const bboxCenterX = (bbox.x_min + bbox.x_max) / 2;
  const bearingOffset = pixelToBearingOffset(bboxCenterX, imageWidth, fovDegrees, intrinsics);
  const absoluteBearing = (cameraBearing + bearingOffset + 360) % 360;
  const pos = projectPoint(cameraLat, cameraLon, absoluteBearing, distanceMeters);
  return { ...pos, bearing: absoluteBearing };
}

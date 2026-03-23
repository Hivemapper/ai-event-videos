import { NextRequest, NextResponse } from "next/server";
import { API_BASE_URL } from "@/lib/constants";
import { fetchWithRetry } from "@/lib/fetch-retry";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const apiKey = request.headers.get("Authorization") || process.env.BEEMAPS_API_KEY;

    if (!apiKey) {
      return NextResponse.json(
        { error: "API key is required" },
        { status: 401 }
      );
    }

    // Use Basic auth for base64-encoded API keys
    const authHeader = apiKey.startsWith("Basic ") ? apiKey : `Basic ${apiKey}`;

    // Build URL with optional query parameters for GNSS and IMU data
    const url = new URL(`${API_BASE_URL}/${id}`);
    const searchParams = request.nextUrl.searchParams;

    if (searchParams.get("includeGnssData") === "true") {
      url.searchParams.set("includeGnssData", "true");
    }
    if (searchParams.get("includeImuData") === "true") {
      url.searchParams.set("includeImuData", "true");
    }

    const response = await fetchWithRetry(url.toString(), {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Authorization: authHeader,
      },
    });

    if (!response.ok) {
      const errorText = await response.text();
      let errorMessage = `API error: ${response.status}`;

      try {
        const errorJson = JSON.parse(errorText);
        if (typeof errorJson === "string") {
          errorMessage = errorJson;
        } else if (errorJson.message) {
          errorMessage =
            typeof errorJson.message === "string"
              ? errorJson.message
              : JSON.stringify(errorJson.message);
        } else if (errorJson.error) {
          errorMessage =
            typeof errorJson.error === "string"
              ? errorJson.error
              : JSON.stringify(errorJson.error);
        }
      } catch {
        // errorText may be raw HTML (e.g. Cloudflare challenge) — don't pass it through
        if (errorText && errorText.length < 500 && !errorText.includes("<")) {
          errorMessage = errorText;
        }
      }

      return NextResponse.json(
        { error: errorMessage },
        { status: response.status }
      );
    }

    const data = await response.json();

    // Normalize IMU data: the Bee Maps API returns flat fields (acc_x, gyro_x, etc.)
    // instead of nested { accelerometer: {x,y,z}, gyroscope: {x,y,z} } objects.
    if (data.imuData && Array.isArray(data.imuData) && data.imuData.length > 0) {
      const sample = data.imuData[0];

      // If the sample already has the expected nested structure, no transform needed
      if (!sample.accelerometer && !sample.gyroscope) {
        data.imuData = data.imuData.map((point: Record<string, unknown>) => {
          const normalized: Record<string, unknown> = {
            timestamp: point.timestamp ?? point.TIMESTAMP ?? point.time,
          };

          // Bee Maps API uses acc_x/acc_y/acc_z
          const ax = point.acc_x ?? point.accel_x ?? point.accelX ?? point.ACCEL_X;
          const ay = point.acc_y ?? point.accel_y ?? point.accelY ?? point.ACCEL_Y;
          const az = point.acc_z ?? point.accel_z ?? point.accelZ ?? point.ACCEL_Z;

          if (ax !== undefined && ay !== undefined && az !== undefined) {
            normalized.accelerometer = {
              x: Number(ax),
              y: Number(ay),
              z: Number(az),
            };
          }

          // Bee Maps API uses gyro_x/gyro_y/gyro_z
          const gx = point.gyro_x ?? point.gyroX ?? point.GYRO_X;
          const gy = point.gyro_y ?? point.gyroY ?? point.GYRO_Y;
          const gz = point.gyro_z ?? point.gyroZ ?? point.GYRO_Z;

          if (gx !== undefined && gy !== undefined && gz !== undefined) {
            normalized.gyroscope = {
              x: Number(gx),
              y: Number(gy),
              z: Number(gz),
            };
          }

          return normalized;
        });
      }
    }

    // Append enrichment data (our computed fields) to the BeeMaps response
    try {
      const lat = data.location?.lat;
      const lon = data.location?.lon;
      const enrichParams = new URLSearchParams();
      if (lat !== undefined) enrichParams.set("lat", String(lat));
      if (lon !== undefined) enrichParams.set("lon", String(lon));
      if (data.timestamp) enrichParams.set("timestamp", data.timestamp);
      const mapboxToken = searchParams.get("mapboxToken");
      if (mapboxToken) enrichParams.set("mapboxToken", mapboxToken);

      const enrichUrl = new URL(
        `/api/events/${id}/enrichment?${enrichParams.toString()}`,
        request.nextUrl.origin
      );
      const enrichResp = await fetch(enrichUrl.toString(), {
        headers: {
          Authorization: authHeader,
        },
      });
      if (enrichResp.ok) {
        data.enrichment = await enrichResp.json();
      }
    } catch {
      // Enrichment is optional — don't fail the response if it errors
    }

    return NextResponse.json(data, {
      headers: {
        "Cache-Control": "public, max-age=3600, stale-while-revalidate=86400",
      },
    });
  } catch (error) {
    console.error("API proxy error:", error);
    return NextResponse.json(
      {
        error: error instanceof Error ? error.message : "Internal server error",
      },
      { status: 500 }
    );
  }
}

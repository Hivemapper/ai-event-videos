#!/usr/bin/env python3
"""
Mount-quality audit: fetch recent events, score IMU for mount-induced jitter
vs normal road vibration, and list the worst offenders.

Heuristic: at ~10 Hz IMU, real driving maneuvers are smooth over 100-200ms.
A loose/bad mount produces high-frequency flip-flop in the gyroscope
(rotational jitter) that real road vibration rarely causes — road bumps show
up mostly as Z-axis accel spikes, not sustained gyro noise.

Score components (per event):
  - gyro_jitter: RMS of sample-to-sample gyro deltas / RMS of gyro magnitude
    (loose mount → close to or >1; tight mount → much less than 1)
  - gyro_hf_rms: raw high-frequency gyro energy (deg/s) — catches vibration
    that swamps real motion
  - accel_hf_z: std of sample-to-sample accel_z deltas after detrending — a
    very noisy vertical accel channel when not on a rough road hints at mount
  - gyro_at_lowspeed: gyro RMS during GNSS-reported speed < 2 m/s (idle or
    near-stationary) — road can't cause gyro noise when you're barely moving
"""
import json
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from statistics import mean, stdev
from math import sqrt

BASE = "http://localhost:3000"


def curl_json(url, data=None):
    cmd = ["curl", "-s", "--max-time", "30", url]
    if data is not None:
        cmd = ["curl", "-s", "--max-time", "30", "-X", "POST",
               "-H", "Content-Type: application/json",
               "-d", json.dumps(data), url]
    out = subprocess.run(cmd, capture_output=True, text=True)
    if out.returncode != 0 or not out.stdout:
        return None
    try:
        return json.loads(out.stdout)
    except json.JSONDecodeError:
        return None


def rms(xs):
    if not xs:
        return 0.0
    return sqrt(sum(x * x for x in xs) / len(xs))


def diffs(xs):
    return [xs[i + 1] - xs[i] for i in range(len(xs) - 1)]


def interp_speed(gnss, ts):
    """Interpolate speed (m/s) at timestamp ts from successive GNSS points."""
    if not gnss or len(gnss) < 2:
        return None
    # find bracketing points
    for i in range(len(gnss) - 1):
        t0 = gnss[i]["timestamp"]
        t1 = gnss[i + 1]["timestamp"]
        if t0 <= ts <= t1:
            # compute speed from haversine / dt
            import math
            lat1, lon1 = gnss[i]["lat"], gnss[i]["lon"]
            lat2, lon2 = gnss[i + 1]["lat"], gnss[i + 1]["lon"]
            R = 6371000.0
            p1 = math.radians(lat1)
            p2 = math.radians(lat2)
            dp = math.radians(lat2 - lat1)
            dl = math.radians(lon2 - lon1)
            a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
            d = 2 * R * math.asin(sqrt(a))
            dt = (t1 - t0) / 1000.0
            if dt <= 0:
                return None
            return d / dt
    return None


def score_event(event_id):
    data = curl_json(f"{BASE}/api/events/{event_id}?includeImuData=true&includeGnssData=true")
    if not data:
        return None
    imu = data.get("imuData") or []
    gnss = data.get("gnssData") or []
    if len(imu) < 20:
        return None

    gx = [p["gyroscope"]["x"] for p in imu if p.get("gyroscope")]
    gy = [p["gyroscope"]["y"] for p in imu if p.get("gyroscope")]
    gz = [p["gyroscope"]["z"] for p in imu if p.get("gyroscope")]
    az = [p["accelerometer"]["z"] for p in imu if p.get("accelerometer")]
    ts = [p["timestamp"] for p in imu if p.get("gyroscope")]

    if len(gx) < 20:
        return None

    # Sample-to-sample deltas — high-frequency jitter
    dgx, dgy, dgz = diffs(gx), diffs(gy), diffs(gz)
    daz = diffs(az)

    gyro_mag_rms = sqrt(rms(gx) ** 2 + rms(gy) ** 2 + rms(gz) ** 2)
    gyro_delta_rms = sqrt(rms(dgx) ** 2 + rms(dgy) ** 2 + rms(dgz) ** 2)

    # Jitter ratio — bounded to 10 to avoid dominating on quiet events
    gyro_jitter = gyro_delta_rms / max(gyro_mag_rms, 0.5)

    # Raw HF gyro energy (deg/s of noise)
    gyro_hf_rms = gyro_delta_rms  # deg/s between consecutive samples

    # Accel Z detrended HF: remove rolling 5-sample mean, take std of residual
    w = 5
    az_detrended = []
    for i in range(len(az)):
        lo = max(0, i - w)
        hi = min(len(az), i + w + 1)
        window = az[lo:hi]
        az_detrended.append(az[i] - mean(window))
    accel_hf_z = stdev(az_detrended) if len(az_detrended) > 1 else 0.0

    # Low-speed gyro RMS: samples when speed < 2 m/s (~4.5 mph)
    low_speed_gyros = []
    for i in range(len(ts)):
        sp = interp_speed(gnss, ts[i])
        if sp is not None and sp < 2.0:
            low_speed_gyros.append(sqrt(gx[i] ** 2 + gy[i] ** 2 + gz[i] ** 2))
    gyro_at_lowspeed = rms(low_speed_gyros) if low_speed_gyros else None

    # Mount-vs-road ratio: a loose mount rotates without a matching vertical
    # bump. Road bumps produce accel_z spikes proportional to gyro. High
    # gyro_hf / accel_hf_z → mount; balanced → road.
    mount_vs_road = gyro_hf_rms / max(accel_hf_z * 1000, 5.0)  # normalized

    # Composite mount-suspicion score (higher = more suspicious)
    score = gyro_jitter * 1.5 + min(gyro_hf_rms / 20.0, 5.0) + min(mount_vs_road / 0.5, 3.0)
    if gyro_at_lowspeed is not None:
        score += min(gyro_at_lowspeed / 5.0, 3.0)

    # Coarse bucket: round lat/lon to 0.5 deg to group events from same vehicle/area
    lat = data.get("location", {}).get("lat") or 0
    lon = data.get("location", {}).get("lon") or 0
    bucket = f"{round(lat*2)/2:+.1f},{round(lon*2)/2:+.1f}"

    return {
        "id": event_id,
        "type": data.get("type"),
        "score": round(score, 2),
        "gyro_jitter": round(gyro_jitter, 2),
        "gyro_hf_rms": round(gyro_hf_rms, 1),
        "accel_hf_z": round(accel_hf_z, 3),
        "mount_vs_road": round(mount_vs_road, 2),
        "gyro_lowspeed": round(gyro_at_lowspeed, 1) if gyro_at_lowspeed is not None else None,
        "imu_samples": len(gx),
        "lowspeed_samples": len(low_speed_gyros),
        "bucket": bucket,
        "lat": lat,
        "lon": lon,
    }


def main():
    # Fetch 100 recent events
    search = curl_json(f"{BASE}/api/events", data={
        "startDate": "2026-04-15T00:00:00.000Z",
        "endDate": "2026-04-17T23:59:59.999Z",
        "limit": 100,
        "offset": 0,
    })
    if not search or not search.get("events"):
        print("Failed to fetch events", file=sys.stderr)
        sys.exit(1)

    events = search["events"]
    print(f"Scoring {len(events)} events...", file=sys.stderr)

    results = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(score_event, e["id"]): e["id"] for e in events}
        done = 0
        for f in as_completed(futures):
            done += 1
            if done % 10 == 0:
                print(f"  {done}/{len(events)}", file=sys.stderr)
            r = f.result()
            if r:
                results.append(r)

    results.sort(key=lambda r: r["score"], reverse=True)

    print(f"\n{'score':>6}  {'jitter':>6}  {'gyroHf':>7}  {'azHf':>6}  {'m/r':>5}  {'type':<22}  url")
    print("-" * 140)
    for r in results[:25]:
        lo = r["gyro_lowspeed"] if r["gyro_lowspeed"] is not None else "—"
        url = f"http://localhost:3000/event/{r['id']}"
        print(f"{r['score']:>6}  {r['gyro_jitter']:>6}  {r['gyro_hf_rms']:>7}  {r['accel_hf_z']:>6}  {r['mount_vs_road']:>5}  {r['type']:<22}  {url}")

    # "Definitely road, not mount": strong vertical bumps + low m/r ratio
    # Require real vibration (gyroHf > 15) AND accel_z bumps (azHf > 0.03)
    # AND m/r ratio < 0.7 — gyro noise is matched by vertical bumps.
    road_candidates = [
        r for r in results
        if r["gyro_hf_rms"] > 15
        and r["accel_hf_z"] > 0.03
        and r["mount_vs_road"] < 0.7
    ]
    # Rank by a "road confidence" score: strong vibration × low m/r
    for r in road_candidates:
        r["road_confidence"] = round(
            (r["gyro_hf_rms"] + r["accel_hf_z"] * 500) * (1.0 - min(r["mount_vs_road"], 0.9)), 2
        )
    road_candidates.sort(key=lambda r: r["road_confidence"], reverse=True)

    print(f"\n=== DEFINITELY ROAD (not mount) — high bumps, m/r < 0.7 ===")
    print(f"{'conf':>5}  {'gyroHf':>7}  {'azHf':>6}  {'m/r':>5}  {'type':<22}  url")
    print("-" * 140)
    for r in road_candidates[:20]:
        url = f"http://localhost:3000/event/{r['id']}"
        print(f"{r['road_confidence']:>5}  {r['gyro_hf_rms']:>7}  {r['accel_hf_z']:>6}  {r['mount_vs_road']:>5}  {r['type']:<22}  {r['bucket']:<14}  {url}")

    # Group by bucket (likely same vehicle cluster)
    print("\n--- grouped by 0.5° bucket (likely same vehicle) ---")
    from collections import defaultdict
    buckets = defaultdict(list)
    for r in results:
        buckets[r["bucket"]].append(r)
    rows = []
    for b, rs in buckets.items():
        mean_score = sum(x["score"] for x in rs) / len(rs)
        max_score = max(x["score"] for x in rs)
        rows.append((b, len(rs), round(mean_score, 2), round(max_score, 2)))
    rows.sort(key=lambda x: x[2], reverse=True)
    print(f"{'bucket':<14}  {'n':>3}  {'meanSc':>7}  {'maxSc':>6}")
    for b, n, ms, xs in rows[:12]:
        print(f"{b:<14}  {n:>3}  {ms:>7}  {xs:>6}")

    # Stats
    if results:
        scores = [r["score"] for r in results]
        print(f"\n{len(results)} scored  |  mean {mean(scores):.2f}  median {sorted(scores)[len(scores)//2]:.2f}  max {max(scores):.2f}", file=sys.stderr)


if __name__ == "__main__":
    main()

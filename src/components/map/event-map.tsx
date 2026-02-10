"use client";

import { useEffect, useRef, useState } from "react";
import mapboxgl from "mapbox-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { getMapboxToken, getApiKey } from "@/lib/api";
import { DetectedActor } from "@/types/actors";
import { ActorTrack } from "@/types/actors";
import { calculateBearing } from "@/lib/geo-projection";
import { MAKI_ICONS, getActorIcon } from "@/lib/actor-icons";

interface PathPoint {
  lat: number;
  lon: number;
  timestamp?: number;
}

interface MapFeature {
  // Beemaps format
  class?: string;
  speedLimit?: number;
  unit?: string;
  position?: {
    lon: number;
    lat: number;
    azimuth?: number;
    alt?: number;
  };
  properties?: {
    type?: string;
    speedLimit?: number;
    unit?: string;
  };
  // GeoJSON format
  type?: string;
  geometry?: {
    type: string;
    coordinates: [number, number];
  };
}

interface EventMapProps {
  location: {
    lat: number;
    lon: number;
  };
  path?: PathPoint[];
  currentTime?: number;
  videoDuration?: number;
  className?: string;
  style?: React.CSSProperties;
  showMapFeatures?: boolean;
  detectedActors?: DetectedActor[];
  actorTracks?: ActorTrack[];
  onSeek?: (time: number) => void;
}

const MAPBOX_STYLE = "mapbox://styles/arielseidman/clyf7l1at00u001r1eyc63yyy";

/** Project query point onto the nearest position along the path, returning a fractional index. */
function findNearestFractionalIndex(
  queryLng: number,
  queryLat: number,
  path: PathPoint[]
): number {
  let bestFrac = 0;
  let bestDist = Infinity;
  const cosLat = Math.cos((queryLat * Math.PI) / 180);

  for (let i = 0; i < path.length - 1; i++) {
    const ax = path[i].lon * cosLat;
    const ay = path[i].lat;
    const bx = path[i + 1].lon * cosLat;
    const by = path[i + 1].lat;
    const px = queryLng * cosLat;
    const py = queryLat;

    const dx = bx - ax;
    const dy = by - ay;
    const lenSq = dx * dx + dy * dy;
    const t = lenSq > 0 ? Math.max(0, Math.min(1, ((px - ax) * dx + (py - ay) * dy) / lenSq)) : 0;

    const projX = ax + t * dx;
    const projY = ay + t * dy;
    const dist = (px - projX) ** 2 + (py - projY) ** 2;

    if (dist < bestDist) {
      bestDist = dist;
      bestFrac = i + t;
    }
  }

  return bestFrac;
}

/** Convert fractional path index to video time. */
function fractionalIndexToTime(fracIdx: number, pathLength: number, duration: number): number {
  return (fracIdx / (pathLength - 1)) * duration;
}

/** Interpolate [lon, lat] at a fractional path index. */
function interpolatePathPosition(path: PathPoint[], fracIdx: number): [number, number] {
  const lower = Math.floor(fracIdx);
  const upper = Math.min(lower + 1, path.length - 1);
  const t = fracIdx - lower;
  return [
    path[lower].lon + (path[upper].lon - path[lower].lon) * t,
    path[lower].lat + (path[upper].lat - path[lower].lat) * t,
  ];
}

export function EventMap({ location, path, currentTime, videoDuration, className = "", style, showMapFeatures = true, detectedActors, actorTracks, onSeek }: EventMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);
  const movingMarkerRef = useRef<mapboxgl.Marker | null>(null);
  const trackMarkersRef = useRef<Map<string, mapboxgl.Marker>>(new Map());
  const isDraggingRef = useRef(false);
  const onSeekRef = useRef(onSeek);
  const videoDurationRef = useRef(videoDuration);
  const [token, setToken] = useState<string | null>(null);
  const [tokenChecked, setTokenChecked] = useState(false);
  const [mapFeatures, setMapFeatures] = useState<MapFeature[]>([]);
  const [mapLoaded, setMapLoaded] = useState(false);

  // Check for token on mount
  useEffect(() => {
    const mapboxToken = getMapboxToken();
    setToken(mapboxToken);
    setTokenChecked(true);
  }, []);

  // Keep refs current for use in drag handlers
  useEffect(() => { onSeekRef.current = onSeek; }, [onSeek]);
  useEffect(() => { videoDurationRef.current = videoDuration; }, [videoDuration]);

  // Fetch map features (stop signs, speed limits)
  useEffect(() => {
    if (!showMapFeatures) return;

    const fetchMapFeatures = async () => {
      const apiKey = getApiKey();
      if (!apiKey) return;

      try {
        const response = await fetch(
          `/api/map-features?lat=${location.lat}&lon=${location.lon}&radius=200`,
          {
            headers: {
              Authorization: apiKey,
            },
          }
        );

        if (response.ok) {
          const data = await response.json();
          console.log("Map features API response:", data);
          if (data.features) {
            setMapFeatures(data.features);
          }
        } else {
          const errorData = await response.json().catch(() => ({}));
          console.error("Map features API error:", response.status, errorData);
        }
      } catch (error) {
        console.error("Failed to fetch map features:", error);
      }
    };

    fetchMapFeatures();
  }, [location, showMapFeatures]);

  useEffect(() => {
    if (!mapContainer.current || !token || !tokenChecked) return;

    mapboxgl.accessToken = token;

    map.current = new mapboxgl.Map({
      container: mapContainer.current,
      style: MAPBOX_STYLE,
      center: [location.lon, location.lat],
      zoom: 15,
    });

    const mapInstance = map.current;

    mapInstance.on("load", () => {
      // Add navigation controls
      mapInstance.addControl(new mapboxgl.NavigationControl(), "top-right");

      // If we have a path, draw it
      if (path && path.length > 1) {
        const coordinates = path.map((p) => [p.lon, p.lat] as [number, number]);

        mapInstance.addSource("route", {
          type: "geojson",
          data: {
            type: "Feature",
            properties: {},
            geometry: {
              type: "LineString",
              coordinates,
            },
          },
        });

        mapInstance.addLayer({
          id: "route",
          type: "line",
          source: "route",
          layout: {
            "line-join": "round",
            "line-cap": "round",
          },
          paint: {
            "line-color": "#ef4444",
            "line-width": 4,
            "line-opacity": 0.8,
          },
        });

        // Fit map to path bounds
        const bounds = new mapboxgl.LngLatBounds();
        coordinates.forEach((coord) => bounds.extend(coord));
        mapInstance.fitBounds(bounds, { padding: 50 });
      }

      // Add moving marker (red with white border)
      const markerEl = document.createElement("div");
      markerEl.className = "event-marker";
      markerEl.innerHTML = `
        <div style="
          width: 24px;
          height: 24px;
          background: #ef4444;
          border: 3px solid white;
          border-radius: 50%;
          box-shadow: 0 2px 8px rgba(0,0,0,0.3);
        "></div>
      `;

      const marker = new mapboxgl.Marker(markerEl)
        .setLngLat([location.lon, location.lat])
        .addTo(mapInstance);

      movingMarkerRef.current = marker;

      // --- Drag-to-seek interaction ---
      if (path && path.length > 1) {
        const handleDragMove = (clientX: number, clientY: number) => {
          const rect = mapInstance.getCanvas().getBoundingClientRect();
          const point = new mapboxgl.Point(clientX - rect.left, clientY - rect.top);
          const lngLat = mapInstance.unproject(point);

          const fracIdx = findNearestFractionalIndex(lngLat.lng, lngLat.lat, path);
          const [lon, lat] = interpolatePathPosition(path, fracIdx);
          marker.setLngLat([lon, lat]);

          const dur = videoDurationRef.current;
          if (dur && dur > 0 && onSeekRef.current) {
            onSeekRef.current(fractionalIndexToTime(fracIdx, path.length, dur));
          }
        };

        // Mouse events
        const onMouseMove = (e: MouseEvent) => { e.preventDefault(); handleDragMove(e.clientX, e.clientY); };
        const onMouseUp = () => {
          isDraggingRef.current = false;
          mapInstance.dragPan.enable();
          document.removeEventListener("mousemove", onMouseMove);
          document.removeEventListener("mouseup", onMouseUp);
        };

        markerEl.addEventListener("mousedown", (e: MouseEvent) => {
          e.stopPropagation();
          isDraggingRef.current = true;
          mapInstance.dragPan.disable();
          document.addEventListener("mousemove", onMouseMove);
          document.addEventListener("mouseup", onMouseUp);
        });

        // Touch events
        const onTouchMove = (e: TouchEvent) => {
          e.preventDefault();
          const touch = e.touches[0];
          handleDragMove(touch.clientX, touch.clientY);
        };
        const onTouchEnd = () => {
          isDraggingRef.current = false;
          mapInstance.dragPan.enable();
          document.removeEventListener("touchmove", onTouchMove);
          document.removeEventListener("touchend", onTouchEnd);
        };

        markerEl.addEventListener("touchstart", (e: TouchEvent) => {
          e.stopPropagation();
          isDraggingRef.current = true;
          mapInstance.dragPan.disable();
          document.addEventListener("touchmove", onTouchMove, { passive: false });
          document.addEventListener("touchend", onTouchEnd);
        });
      }

      // Mark map as loaded
      setMapLoaded(true);
    });

    return () => {
      mapInstance.remove();
      setMapLoaded(false);
    };
  }, [location, path, token, tokenChecked]);

  // Add map feature markers when features are loaded and map is ready
  useEffect(() => {
    const mapInstance = map.current;
    if (!mapInstance || !mapLoaded || mapFeatures.length === 0) return;

    console.log("Adding map features:", mapFeatures.length, "features");
    console.log("Feature structure sample:", JSON.stringify(mapFeatures[0], null, 2));

    const markers: mapboxgl.Marker[] = [];

    mapFeatures.forEach((feature) => {
      // Handle the Beemaps API format
      // Features have: class, position: { lon, lat }, properties: { speedLimit, type, unit }
      let lon: number, lat: number;
      let featureClass: string | undefined;

      if (feature.position?.lon !== undefined && feature.position?.lat !== undefined) {
        // Beemaps format
        lon = feature.position.lon;
        lat = feature.position.lat;
        featureClass = feature.class;
      } else if (feature.geometry?.coordinates) {
        // GeoJSON format
        [lon, lat] = feature.geometry.coordinates;
        featureClass = feature.properties?.type || feature.class;
      } else {
        console.log("Unknown feature format:", feature);
        return;
      }

      const markerEl = document.createElement("div");
      markerEl.className = "map-feature-marker";

      if (featureClass === "stop-sign") {
        markerEl.innerHTML = `
          <svg width="28" height="28" viewBox="0 0 1296 1296" fill="none" xmlns="http://www.w3.org/2000/svg" style="filter: drop-shadow(0 2px 4px rgba(0,0,0,0.3));">
            <path d="M379.599 1296.1L-0.000585936 916.499L-0.100586 379.599L379.499 -0.000585936L916.399 -0.100586L1296.1 379.499V916.399L916.499 1296.1H379.599Z" fill="white"/>
            <path d="M390.8 1269L27 905.2V390.8L390.8 27H905.2L1269 390.8V905.2L905.2 1269H390.8Z" fill="#BF311A"/>
            <path d="M521.9 864H459.6V489.6H380.5V432H600.9V489.6H521.8V864H521.9Z" fill="white"/>
            <path d="M632 647.1C632 420.9 736.9 424.5 760.2 424.5C783.5 424.5 887 421.001 887 647.201C887 873.401 783.5 871.5 760.2 871.5C736.9 871.5 632 875.1 632 647.1ZM759.5 485.101C747.2 485.101 691.5 482.601 691.5 647.401C691.5 812.201 747.2 810.901 759.5 810.901C771.8 810.901 827.4 813.401 827.4 647.401C827.4 481.401 771.8 485.101 759.5 485.101Z" fill="white"/>
            <path d="M1000.8 674.5V864H938.8V432H1067.4C1135.5 432 1180.8 480.7 1180.8 553.3C1180.8 625.9 1139.9 674.5 1067.4 674.5H1000.8ZM1000.8 615.4H1064.3C1100.6 615.4 1120.3 592.7 1120.3 553.3C1120.3 513.9 1097.4 492.7 1064.4 492.7H1000.8V615.4Z" fill="white"/>
            <path d="M297.8 745.7C297.8 653.7 136.5 654.3 136.5 537.2C136.5 420.1 185.2 424.4 247.6 424.4C310 424.4 355.7 471.3 357.2 536.6H297.7C296.2 506.3 274.9 486.6 247.5 486.6C220.1 486.6 195.8 507.8 195.8 536.5C195.8 565.2 207.6 569.9 233.7 586.2C293.3 621.1 357.1 666 357.1 745.8C357.1 825.6 303.8 871.6 236.8 871.6C169.8 871.6 115.1 807.2 115.1 736.6H176.1C176.1 773.4 194.3 811 236.9 811C270.4 811 297.8 780.6 297.8 745.7Z" fill="white"/>
          </svg>
        `;
      } else if (featureClass === "speed-sign") {
        const speedLimit = feature.speedLimit || feature.properties?.speedLimit || "?";
        markerEl.innerHTML = `
          <svg width="24" height="30" viewBox="0 0 1728 2160" fill="none" xmlns="http://www.w3.org/2000/svg" style="filter: drop-shadow(0 2px 4px rgba(0,0,0,0.3));">
            <path d="M1620 -0.0999756H108C48.298 -0.0999756 -0.0999756 48.298 -0.0999756 108V2052C-0.0999756 2111.7 48.298 2160.1 108 2160.1H1620C1679.7 2160.1 1728.1 2111.7 1728.1 2052V108C1728.1 48.298 1679.7 -0.0999756 1620 -0.0999756Z" fill="white"/>
            <path d="M1701 2052L1701 108C1701 63.2649 1664.74 27 1620 27L108 27C63.2649 27 27 63.2649 27 108L27 2052C27 2096.74 63.2649 2133 108 2133H1620C1664.74 2133 1701 2096.74 1701 2052Z" fill="#231F20"/>
            <path d="M1656 2052L1656 108C1656 88.1177 1639.88 72 1620 72L108 72C88.1178 72 72 88.1177 72 108L72 2052C72 2071.88 88.1178 2088 108 2088H1620C1639.88 2088 1656 2071.88 1656 2052Z" fill="white"/>
            <path d="M379.8 561.6C359.5 573.9 333.3 581.1 306.8 581.1C257.1 581.1 219.5 562.7 187 524.8L228.6 492C249.9 517.6 273.2 528.9 305.7 528.9C338.2 528.9 368.6 511.7 368.6 491C368.6 470.3 339.4 459.8 258.8 437.1C218.3 425.7 200.1 394.9 200.1 363.9C200.1 315.7 240.7 283 299.6 283C358.5 283 377.8 297.3 407.2 328L368.6 362.8C350.3 343.3 327 334.1 299.6 334.1C272.2 334.1 252.9 346.9 252.9 363.5C252.9 395.4 292.1 388.4 363.7 413.6C414.3 431.5 420.3 474.7 420.3 492.9C420.3 522.6 407.1 545.2 379.7 561.6H379.8Z" fill="#231F20"/>
            <path d="M530.6 462.8V576H478V288H618.9C679.6 288 711.4 334.2 711.4 375.4C711.4 416.6 682.9 462.8 621.1 462.8H530.5H530.6ZM530.6 339.6V411.3H617.6C642.8 411.3 660 397.2 660 375.9C660 354.6 643.2 339.5 617.5 339.5H530.6V339.6Z" fill="#231F20"/>
            <path d="M760.4 288H971.3V339.6H813.1V393.1H899.3V445.7H813.1V524.5H976.4V576.1H760.4V288.1V288Z" fill="#231F20"/>
            <path d="M1034 288H1244.9V339.6H1086.7V393.1H1172.9V445.7H1086.7V524.5H1250V576.1H1034V288.1V288Z" fill="#231F20"/>
            <path d="M1307.6 576V288H1402.8C1494.5 288 1541 357.5 1541 431.5C1541 505.5 1489.7 576 1403.1 576H1307.6ZM1400.4 339.6H1360.1V524.5H1400C1446.1 524.5 1489.5 494 1489.5 431.5C1489.5 369 1453.1 339.6 1400.4 339.6Z" fill="#231F20"/>
            <path d="M336.9 720H388.6V963.5H552.9V1008H336.9V720Z" fill="#231F20"/>
            <path d="M601.9 720H653.7V1008H601.9V720Z" fill="#231F20"/>
            <path d="M948.7 832.2L863.5 1008L779.3 832.2V1008H728.6V720H779.3L863.5 890.8L948.7 720H999.4V1008H948.7V832.2Z" fill="#231F20"/>
            <path d="M1074.3 720H1126.1V1008H1074.3V720Z" fill="#231F20"/>
            <path d="M1308.4 1008H1257.9V770.5H1175.1V720H1391.1V770.5H1308.3V1008H1308.4Z" fill="#231F20"/>
            <text x="864" y="1620" text-anchor="middle" dominant-baseline="middle" fill="#231F20" font-size="680" font-weight="bold" font-family="Arial Black, sans-serif">${speedLimit}</text>
          </svg>
        `;
      } else {
        console.log("Unknown feature class:", featureClass);
        return; // Skip unknown feature types
      }

      const marker = new mapboxgl.Marker(markerEl)
        .setLngLat([lon, lat])
        .addTo(mapInstance);

      markers.push(marker);
    });

    return () => {
      markers.forEach((marker) => marker.remove());
    };
  }, [mapFeatures, mapLoaded]);

  // Add detected actor markers
  useEffect(() => {
    const mapInstance = map.current;
    if (!mapInstance || !mapLoaded || !detectedActors || detectedActors.length === 0) return;

    const markers: mapboxgl.Marker[] = [];

    detectedActors.forEach((actor) => {
      const { path: iconPath, color } = getActorIcon(actor.type);

      const markerEl = document.createElement("div");
      markerEl.className = "actor-marker";
      markerEl.innerHTML = `
        <div style="
          display: flex;
          flex-direction: column;
          align-items: center;
          cursor: pointer;
          filter: drop-shadow(0 1px 3px rgba(0,0,0,0.4));
        ">
          <div style="
            width: 32px;
            height: 32px;
            background: ${color};
            border-radius: 50% 50% 50% 0;
            transform: rotate(-45deg);
            display: flex;
            align-items: center;
            justify-content: center;
          ">
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 15 15" style="transform: rotate(45deg); fill: white;">
              <path d="${iconPath}"/>
            </svg>
          </div>
        </div>
      `;

      const popup = new mapboxgl.Popup({
        offset: 24,
        closeButton: false,
        closeOnClick: false,
      }).setHTML(`
        <div style="font-size: 13px; max-width: 200px;">
          <strong>${actor.label}</strong>
          <div style="color: #666; margin-top: 2px;">
            ${Math.round(actor.estimatedDistanceMeters)}m away
          </div>
          <div style="color: #666; margin-top: 2px; font-size: 12px;">
            ${actor.description}
          </div>
        </div>
      `);

      const marker = new mapboxgl.Marker({ element: markerEl, anchor: "bottom-left" })
        .setLngLat([actor.worldPosition.lon, actor.worldPosition.lat])
        .setPopup(popup)
        .addTo(mapInstance);

      // Show popup on hover
      markerEl.addEventListener("mouseenter", () => marker.togglePopup());
      markerEl.addEventListener("mouseleave", () => marker.togglePopup());

      markers.push(marker);
    });

    return () => {
      markers.forEach((marker) => marker.remove());
    };
  }, [detectedActors, mapLoaded]);

  // Create/destroy track markers
  useEffect(() => {
    const mapInstance = map.current;
    if (!mapInstance || !mapLoaded) {
      return;
    }

    const existing = trackMarkersRef.current;
    const currentTrackIds = new Set((actorTracks ?? []).map((t) => t.trackId));

    // Remove markers for tracks that no longer exist
    for (const [trackId, marker] of existing) {
      if (!currentTrackIds.has(trackId)) {
        marker.remove();
        existing.delete(trackId);
      }
    }

    // Create markers for new tracks
    for (const track of actorTracks ?? []) {
      if (existing.has(track.trackId)) continue;

      const { path: iconPath } = getActorIcon(track.type);
      const markerEl = document.createElement("div");
      markerEl.className = "track-marker";
      markerEl.innerHTML = `
        <div style="
          display: flex;
          flex-direction: column;
          align-items: center;
          cursor: pointer;
          filter: drop-shadow(0 1px 3px rgba(0,0,0,0.4));
          transition: opacity 0.5s ease;
        ">
          <div style="
            width: 32px;
            height: 32px;
            background: ${track.color};
            border-radius: 50% 50% 50% 0;
            transform: rotate(-45deg);
            display: flex;
            align-items: center;
            justify-content: center;
          ">
            <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 15 15" style="transform: rotate(45deg); fill: white;">
              <path d="${iconPath}"/>
            </svg>
          </div>
          <div style="
            font-size: 10px;
            font-weight: 600;
            color: ${track.color};
            white-space: nowrap;
            margin-top: 2px;
            text-shadow: 0 0 3px white, 0 0 3px white;
          ">${track.label}</div>
        </div>
      `;

      const popup = new mapboxgl.Popup({
        offset: 24,
        closeButton: false,
        closeOnClick: false,
      }).setHTML(`
        <div style="font-size: 13px; max-width: 200px;">
          <strong style="color: ${track.color};">${track.label}</strong>
          <div style="color: #666; margin-top: 2px;">
            ${track.observations.length} observation${track.observations.length !== 1 ? "s" : ""}
          </div>
          <div style="color: #666; margin-top: 2px; font-size: 12px;">
            ${track.firstSeen.toFixed(1)}s – ${track.lastSeen.toFixed(1)}s
          </div>
        </div>
      `);

      // Start at first observation position
      const firstObs = track.observations[0];
      const marker = new mapboxgl.Marker({ element: markerEl, anchor: "bottom-left" })
        .setLngLat([firstObs.worldPosition.lon, firstObs.worldPosition.lat])
        .setPopup(popup)
        .addTo(mapInstance);

      markerEl.addEventListener("mouseenter", () => marker.togglePopup());
      markerEl.addEventListener("mouseleave", () => marker.togglePopup());

      existing.set(track.trackId, marker);
    }

    return () => {
      // Full cleanup when actorTracks changes to undefined/null
      if (!actorTracks || actorTracks.length === 0) {
        for (const [, marker] of existing) {
          marker.remove();
        }
        existing.clear();
      }
    };
  }, [actorTracks, mapLoaded]);

  // Animate track marker positions based on video time
  useEffect(() => {
    if (!mapLoaded || !actorTracks || actorTracks.length === 0 || currentTime === undefined) return;

    const existing = trackMarkersRef.current;
    const INACTIVE_OPACITY = 0.35;
    const fadeMargin = 1.5; // seconds to fade between inactive and active

    for (const track of actorTracks) {
      const marker = existing.get(track.trackId);
      if (!marker) continue;

      const el = marker.getElement();

      // Compute opacity: active (1.0) within time range, fading to inactive outside
      let opacity: number;
      if (currentTime >= track.firstSeen && currentTime <= track.lastSeen) {
        opacity = 1;
      } else if (currentTime < track.firstSeen) {
        const gap = track.firstSeen - currentTime;
        opacity = gap < fadeMargin ? INACTIVE_OPACITY + (1 - INACTIVE_OPACITY) * (1 - gap / fadeMargin) : INACTIVE_OPACITY;
      } else {
        const gap = currentTime - track.lastSeen;
        opacity = gap < fadeMargin ? INACTIVE_OPACITY + (1 - INACTIVE_OPACITY) * (1 - gap / fadeMargin) : INACTIVE_OPACITY;
      }
      el.style.opacity = String(opacity);

      // Interpolate position between nearest observations
      const obs = track.observations;
      if (obs.length === 1) {
        marker.setLngLat([obs[0].worldPosition.lon, obs[0].worldPosition.lat]);
        continue;
      }

      // Find the two bracketing observations and interpolate
      if (currentTime <= obs[0].timestamp) {
        marker.setLngLat([obs[0].worldPosition.lon, obs[0].worldPosition.lat]);
      } else if (currentTime >= obs[obs.length - 1].timestamp) {
        const last = obs[obs.length - 1];
        marker.setLngLat([last.worldPosition.lon, last.worldPosition.lat]);
      } else {
        for (let i = 0; i < obs.length - 1; i++) {
          if (currentTime >= obs[i].timestamp && currentTime <= obs[i + 1].timestamp) {
            const dt = obs[i + 1].timestamp - obs[i].timestamp;
            const t = dt > 0 ? (currentTime - obs[i].timestamp) / dt : 0;
            const lat = obs[i].worldPosition.lat + (obs[i + 1].worldPosition.lat - obs[i].worldPosition.lat) * t;
            const lon = obs[i].worldPosition.lon + (obs[i + 1].worldPosition.lon - obs[i].worldPosition.lon) * t;
            marker.setLngLat([lon, lat]);
            break;
          }
        }
      }
    }
  }, [currentTime, actorTracks, mapLoaded]);

  // Calculate smoothed bearing by looking ahead a few points
  const calculateSmoothedBearing = (pathData: PathPoint[], currentIndex: number): number => {
    const lookAhead = 3; // Average over a few points for smoother rotation
    const endIndex = Math.min(currentIndex + lookAhead, pathData.length - 1);

    if (endIndex <= currentIndex) {
      return calculateBearing(
        pathData[Math.max(0, currentIndex - 1)].lat,
        pathData[Math.max(0, currentIndex - 1)].lon,
        pathData[currentIndex].lat,
        pathData[currentIndex].lon
      );
    }

    return calculateBearing(
      pathData[currentIndex].lat,
      pathData[currentIndex].lon,
      pathData[endIndex].lat,
      pathData[endIndex].lon
    );
  };

  // Update marker position based on video time
  useEffect(() => {
    if (isDraggingRef.current) return;

    const marker = movingMarkerRef.current;
    const mapInstance = map.current;
    if (!marker || !mapInstance || !mapLoaded || !path || path.length < 2 || currentTime === undefined || !videoDuration) {
      return;
    }

    // Calculate progress through the video (0 to 1)
    const progress = Math.max(0, Math.min(1, currentTime / videoDuration));

    // Find the position along the path based on progress
    // We'll interpolate between path points based on the video progress
    const pathIndex = progress * (path.length - 1);
    const lowerIndex = Math.floor(pathIndex);
    const upperIndex = Math.min(lowerIndex + 1, path.length - 1);
    const t = pathIndex - lowerIndex;

    // Interpolate between the two points
    const p1 = path[lowerIndex];
    const p2 = path[upperIndex];
    const lat = p1.lat + (p2.lat - p1.lat) * t;
    const lon = p1.lon + (p2.lon - p1.lon) * t;

    marker.setLngLat([lon, lat]);

    // Calculate smoothed bearing to orient map so vehicle moves toward top
    const bearing = calculateSmoothedBearing(path, lowerIndex);

    // Rotate map so vehicle heading points up
    // Map bearing is the rotation of the map - if vehicle heads east (90°),
    // we rotate map by -90° so east points up
    mapInstance.easeTo({
      center: [lon, lat],
      bearing: -bearing,
      duration: 300,
      easing: (t) => t, // Linear easing for smooth continuous motion
    });
  }, [currentTime, videoDuration, path, mapLoaded]);

  if (!tokenChecked) {
    return (
      <div
        className={`flex items-center justify-center bg-muted text-muted-foreground ${className}`}
      >
        <p>Loading map...</p>
      </div>
    );
  }

  if (!token) {
    return (
      <div
        className={`flex items-center justify-center bg-muted text-muted-foreground ${className}`}
      >
        <p>Mapbox token not configured. Add it in Settings.</p>
      </div>
    );
  }

  return <div ref={mapContainer} className={className} style={style} />;
}

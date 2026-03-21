"use client";

import { useEffect, useRef, useState, useCallback } from "react";
import mapboxgl from "mapbox-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { getMapboxToken } from "@/lib/api";
import { AIEvent } from "@/types/events";

interface EventsMapProps {
  events: AIEvent[];
  onEventClick: (event: AIEvent) => void;
  className?: string;
}

const MAPBOX_STYLE = "mapbox://styles/arielseidman/clyf7l1at00u001r1eyc63yyy";
const SOURCE_ID = "events";
const CLUSTER_THRESHOLD = 25;

function eventsToGeoJSON(events: AIEvent[]): GeoJSON.FeatureCollection {
  return {
    type: "FeatureCollection",
    features: events.map((e) => ({
      type: "Feature" as const,
      geometry: { type: "Point" as const, coordinates: [e.location.lon, e.location.lat] },
      properties: { id: e.id },
    })),
  };
}

export function EventsMap({ events, onEventClick, className = "" }: EventsMapProps) {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);
  const eventsLookupRef = useRef<Map<string, AIEvent>>(new Map());
  const hasFittedRef = useRef(false);
  const [token, setToken] = useState<string | null>(null);
  const [tokenChecked, setTokenChecked] = useState(false);
  const [mapReady, setMapReady] = useState(false);

  // Keep a lookup map of events by id for click handling
  useEffect(() => {
    const lookup = new Map<string, AIEvent>();
    for (const e of events) lookup.set(e.id, e);
    eventsLookupRef.current = lookup;
  }, [events]);

  useEffect(() => {
    const mapboxToken = getMapboxToken();
    setToken(mapboxToken);
    setTokenChecked(true);
  }, []);

  // Initialize map with clustering layers
  useEffect(() => {
    if (!mapContainer.current || !token || !tokenChecked) return;

    mapboxgl.accessToken = token;

    const m = new mapboxgl.Map({
      container: mapContainer.current,
      style: MAPBOX_STYLE,
      center: [-98.5795, 39.8283],
      zoom: 3,
    });
    map.current = m;

    m.on("load", () => {
      m.addControl(new mapboxgl.NavigationControl(), "top-right");

      // Add clustered GeoJSON source
      m.addSource(SOURCE_ID, {
        type: "geojson",
        data: { type: "FeatureCollection", features: [] },
        cluster: true,
        clusterMaxZoom: 14,
        clusterRadius: 60,
        clusterMinPoints: CLUSTER_THRESHOLD,
      });

      // Cluster circle layer
      m.addLayer({
        id: "clusters",
        type: "circle",
        source: SOURCE_ID,
        filter: ["has", "point_count"],
        paint: {
          "circle-color": [
            "step", ["get", "point_count"],
            "#8b5cf6",   // < 100: purple
            100, "#7c3aed", // 100+: darker purple
            500, "#6d28d9", // 500+: even darker
          ],
          "circle-radius": [
            "step", ["get", "point_count"],
            20,      // < 100
            100, 28, // 100+
            500, 36, // 500+
          ],
          "circle-stroke-width": 2,
          "circle-stroke-color": "#ffffff",
        },
      });

      // Cluster count label
      m.addLayer({
        id: "cluster-count",
        type: "symbol",
        source: SOURCE_ID,
        filter: ["has", "point_count"],
        layout: {
          "text-field": "{point_count_abbreviated}",
          "text-font": ["DIN Pro Medium", "Arial Unicode MS Bold"],
          "text-size": 13,
        },
        paint: {
          "text-color": "#ffffff",
        },
      });

      // Unclustered individual points
      m.addLayer({
        id: "unclustered-point",
        type: "circle",
        source: SOURCE_ID,
        filter: ["!", ["has", "point_count"]],
        paint: {
          "circle-color": "#8b5cf6",
          "circle-radius": 7,
          "circle-stroke-width": 2,
          "circle-stroke-color": "#ffffff",
        },
      });

      // Click on cluster → zoom in
      m.on("click", "clusters", (e) => {
        const features = m.queryRenderedFeatures(e.point, { layers: ["clusters"] });
        if (!features.length) return;
        const clusterId = features[0].properties?.cluster_id;
        const source = m.getSource(SOURCE_ID) as mapboxgl.GeoJSONSource;
        source.getClusterExpansionZoom(clusterId, (err, zoom) => {
          if (err) return;
          const coords = (features[0].geometry as GeoJSON.Point).coordinates as [number, number];
          m.easeTo({ center: coords, zoom: zoom ?? 10 });
        });
      });

      // Click on individual point → navigate to event
      m.on("click", "unclustered-point", (e) => {
        const feature = e.features?.[0];
        if (!feature) return;
        const eventId = feature.properties?.id;
        const event = eventsLookupRef.current.get(eventId);
        if (event) onEventClick(event);
      });

      // Cursor styling
      m.on("mouseenter", "clusters", () => { m.getCanvas().style.cursor = "pointer"; });
      m.on("mouseleave", "clusters", () => { m.getCanvas().style.cursor = ""; });
      m.on("mouseenter", "unclustered-point", () => { m.getCanvas().style.cursor = "pointer"; });
      m.on("mouseleave", "unclustered-point", () => { m.getCanvas().style.cursor = ""; });

      setMapReady(true);
    });

    return () => {
      m.remove();
      setMapReady(false);
      hasFittedRef.current = false;
    };
  }, [token, tokenChecked, onEventClick]);

  // Update source data when events change (no fitBounds except first load)
  useEffect(() => {
    const m = map.current;
    if (!m || !mapReady) return;

    const source = m.getSource(SOURCE_ID) as mapboxgl.GeoJSONSource | undefined;
    if (!source) return;

    source.setData(eventsToGeoJSON(events));

    // Only fit bounds once on the first batch that has events
    if (!hasFittedRef.current && events.length > 0) {
      hasFittedRef.current = true;

      if (events.length === 1) {
        m.flyTo({
          center: [events[0].location.lon, events[0].location.lat],
          zoom: 14,
        });
      } else {
        const bounds = new mapboxgl.LngLatBounds();
        for (const e of events) bounds.extend([e.location.lon, e.location.lat]);
        m.fitBounds(bounds, { padding: 50 });
      }
    }
  }, [events, mapReady]);

  if (!tokenChecked) {
    return (
      <div className={`flex items-center justify-center bg-muted text-muted-foreground ${className}`}>
        <p>Loading map...</p>
      </div>
    );
  }

  if (!token) {
    return (
      <div className={`flex items-center justify-center bg-muted text-muted-foreground ${className}`}>
        <p>Mapbox token not configured. Add it in Settings.</p>
      </div>
    );
  }

  return <div ref={mapContainer} className={className} />;
}

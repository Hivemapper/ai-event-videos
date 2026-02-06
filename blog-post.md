# AI Event Videos: Exploring Dashcam Driving Events with the Bee Maps API

**[github.com/Hivemapper/ai-event-videos](https://github.com/Hivemapper/ai-event-videos)**

AI Event Videos is an open-source Next.js application for browsing, analyzing, and extracting training data from AI-detected driving events captured by Hivemapper's global dashcam network. It's built on the [Bee Maps AI Events API](https://beemaps.com/api/developer/docs#tag/aievents/POST/aievents/search), which provides access to tens of thousands of real-world driving incidents — each with video, GPS telemetry, speed profiles, and nearby map features.

This post walks through what the app does, how it integrates with the API, and how you can use it to explore driving behavior data at scale.

---

## What You Get

The app provides a complete workflow for working with dashcam event data:

- **Gallery view** — Browse events as a grid of video cards or as pins on a map, filtered by date, type, time of day, country, and geographic radius
- **Event detail pages** — Watch the video synchronized with a live GPS track on the map, inspect speed profiles with speed-limit violation highlighting, and drill into raw GNSS and IMU sensor data
- **Frame labeling** — Extract individual video frames at any timestamp, pair them with nearby map features (stop signs, speed signs), and export labeled training data for ML models
- **AI-powered search** — Ask natural language questions like "harsh braking in London last week" and let Claude translate them into structured API filters
- **Curated highlights** — A showcase of the most extreme events: 120+ km/h braking drops, 1.8g peak forces, incidents from dozens of countries

---

## The Gallery

The home page loads the last 7 days of events by default and displays them as a responsive card grid. Each card shows a video thumbnail, event type badge, timestamp, and location.

### Filtering

The filter bar supports multi-dimensional search:

- **Date range** — Any window up to 31 days (the API's maximum per request)
- **Event types** — 9 categories: `HARSH_BRAKING`, `AGGRESSIVE_ACCELERATION`, `SWERVING`, `HIGH_SPEED`, `HIGH_G_FORCE`, `STOP_SIGN_VIOLATION`, `TRAFFIC_LIGHT_VIOLATION`, `TAILGATING`, `MANUAL_REQUEST`
- **Time of day** — Day, Dawn, Dusk, Night — calculated per-event using the [suncalc](https://github.com/mourner/suncalc) library with each event's GPS coordinates and timestamp
- **Country** — Multi-select from countries present in results
- **Geographic radius** — Enter coordinates and a radius (100m–5km) to search a specific area

Under the hood, the geographic filter generates a 32-point polygon approximating a circle and passes it to the API's `polygon` parameter:

```typescript
// From src/app/api/events/route.ts — circle polygon for radius search
function createCirclePolygon(lat: number, lon: number, radiusMeters: number, numPoints = 32) {
  const coords: [number, number][] = [];
  const earthRadius = 6371000;
  for (let i = 0; i <= numPoints; i++) {
    const angle = (i / numPoints) * 2 * Math.PI;
    const dLat = (radiusMeters / earthRadius) * Math.cos(angle);
    const dLon = (radiusMeters / (earthRadius * Math.cos((lat * Math.PI) / 180))) * Math.sin(angle);
    coords.push([lon + (dLon * 180) / Math.PI, lat + (dLat * 180) / Math.PI]);
  }
  return coords;
}
```

### Map View

Toggle from grid to map view to see all filtered events plotted on a Mapbox GL map with clustering. Click any marker to navigate to the event detail page.

---

## Event Detail Page

Clicking an event opens a rich detail page with two columns of synchronized data.

### Video + Speed Profile (Left Column)

The video plays through a proxy endpoint (`/api/video`) that handles CORS and streaming from the pre-signed S3 URLs. Below the video:

- **Event metadata** — Timestamp, coordinates (click to copy), road type (from Mapbox), event type badge, max speed, peak acceleration, country, and a link to Google Maps
- **Speed profile chart** — A bar chart of speed over time from the event's `SPEED_ARRAY`. Bars are color-coded: red when speed exceeds the nearest posted speed limit, blue otherwise. The speed limit is fetched from the Bee Maps map features endpoint by querying a 200m radius around the event location for `speed-sign` features

### Interactive Map + Sensor Data (Right Column)

- **Map** — Shows the event location marker. If GNSS data is available, the full GPS track is drawn as a polyline, and a marker follows along the path in sync with video playback. Nearby map features (stop signs, speed signs, turn restriction signs) are displayed as icons
- **GNSS tab** — Raw positioning data table: latitude, longitude, altitude, timestamp at ~30Hz resolution
- **IMU tab** — Accelerometer (X, Y, Z) and gyroscope readings with timestamps, lazy-loaded on demand

The GNSS and IMU data come from the event detail endpoint with query parameters:

```
GET /api/events/{id}?includeGnssData=true&includeImuData=true
```

Which proxies to:

```
GET https://beemaps.com/api/developer/aievents/{id}?includeGnssData=true&includeImuData=true
```

### Camera Intrinsics

The detail page also displays camera calibration data (focal length, horizontal FOV, radial distortion coefficients) fetched from the Bee Maps `/devices` endpoint. These follow the OpenCV camera model and are useful for undistorting video frames.

---

## Frame Labeling for ML Training Data

One of the most powerful features is the frame labeling tool on the event detail page. It lets you:

1. **Scrub to any timestamp** in the video using a slider
2. **Extract a frame** — calls `/api/frames` which uses FFmpeg server-side to pull the exact frame as a JPEG
3. **View nearby map features** — stop signs, speed limit signs, and turn restriction signs within a configurable radius, each with GPS position, confidence score, and detection image
4. **Export a labeled pair** — downloads three files:
   - The extracted frame (JPEG)
   - `labels.json` — nearby features with class, position, speed limit values, and distances
   - `metadata.json` — event context including coordinates, timestamp, speed, and acceleration

This workflow produces ready-to-use training data for object detection, scene understanding, or driving behavior models. The global coverage of the dashcam network provides diversity across road types, signage conventions, weather conditions, and driving cultures.

---

## AI Agent: Natural Language Search

The app integrates Claude (Sonnet 4.5) to translate natural language queries into structured API filters. The agent tab provides a chat interface where you can type queries like:

- "Harsh braking in London last week"
- "Nighttime swerving events near San Francisco"
- "High G-force events in the past 3 days"
- "Stop sign violations in Texas"

Behind the scenes, the `/api/agent` endpoint sends your query to Claude with a tool-use schema that includes all available filter parameters. Claude interprets the query and calls a `set_filters` tool with structured output:

```typescript
// Claude's tool response becomes structured filters
{
  startDate: "2026-01-29",
  endDate: "2026-02-05",
  types: ["HARSH_BRAKING"],
  coordinates: { lat: 51.5074, lon: -0.1278 },
  radius: 5000,
  explanation: "Searching for harsh braking events in London over the past week..."
}
```

The agent then optionally queries the Bee Maps API with these filters and returns matching events directly in the chat.

---

## Highlights Page

The `/highlights` page showcases curated examples of the most extreme events found in the API data, organized into three sections:

### Extreme Braking

Events with the largest speed drops — over 90 km/h of deceleration captured on dashcam. The most dramatic example is a vehicle in Bailey County, TX decelerating from 123.8 km/h to 1.1 km/h. Other highlights include a 147.9 → 30.5 km/h drop in Mooskirchen, Austria and a 113.7 → 5.1 km/h drop in Camarillo, CA.

| Speed Drop | From → To (km/h) | Location | Event ID |
|-----------|-------------------|----------|----------|
| 122.7 | 123.8 → 1.1 | Bailey County, TX | `68693232d2b06edd1cd1ed9d` |
| 117.4 | 147.9 → 30.5 | Mooskirchen, Austria | `6867ff149abbc70fa1f2e3ab` |
| 108.6 | 113.7 → 5.1 | Camarillo, CA | `69581dad62cb7e369e720878` |
| 105.6 | 105.6 → 0.0 | Randall County, TX | `68bb0935716411932b9feb6d` |
| 101.3 | 107.9 → 6.6 | Cleveland, TX | `690a7281957cb58b9d79a392` |

### Highest G-Force

The most intense acceleration events, peaking at 1.859 m/s² in New Orleans, LA. These capture sudden directional changes and hard braking at moderate speeds.

### International Highlights

A sample of events from around the world: Mexico, Australia, Taiwan, Portugal, demonstrating the global coverage of the Hivemapper dashcam network.

Each event in the highlights links directly to its detail page in the app.

---

## API Integration Architecture

The app uses Next.js API routes as a backend proxy layer between the browser and the Bee Maps API. This architecture keeps the API key server-side and handles concerns like CORS, video streaming, and frame extraction.

### Endpoints the App Uses

| App Route | Bee Maps Endpoint | Purpose |
|-----------|-------------------|---------|
| `POST /api/events` | `POST /aievents/search` | Search events by date, type, polygon |
| `GET /api/events/[id]` | `GET /aievents/{id}` | Event detail with GNSS/IMU data |
| `GET /api/map-features` | `POST /map-data` | Stop signs, speed signs near a location |
| `GET /api/labeled-frame` | Multiple | Frame extraction + map feature labeling |
| `GET /api/video` | S3 pre-signed URL | Video streaming proxy |
| `GET /api/frames` | S3 + FFmpeg | Frame extraction at timestamp |
| `GET /api/thumbnail` | S3 + FFmpeg | Video thumbnail generation |
| `GET /api/road-type` | Mapbox Tilequery | Road classification at coordinates |
| `POST /api/agent` | Anthropic + Bee Maps | Natural language → filters → results |

### Authentication

The Bee Maps API uses Basic authentication with a base64-encoded API key:

```
Authorization: Basic <base64-encoded-api-key>
```

API keys are entered through the in-app settings dialog and stored in the browser's localStorage. The Next.js API routes read the key from the request's Authorization header and forward it to Bee Maps.

### Key API Details

- **Date format**: ISO 8601 datetime strings required (e.g., `"2026-01-05T00:00:00.000Z"`). Plain date strings are rejected.
- **Date window**: Maximum 31-day span per search request. The app enforces this in the filter UI.
- **Speed data**: `AVG_SPEED_MS` is in meters per second. The app multiplies by 3.6 for km/h display.
- **Video URLs**: Pre-signed S3 links valid for 24 hours. The proxy endpoint streams these to avoid CORS issues.
- **GNSS resolution**: ~30Hz, producing 500–1000 points per event.
- **Pagination**: `limit` and `offset` parameters. The app uses infinite scroll with a "Load More" button.

---

## Scale of the Data

The Bee Maps API serves data from a global dashcam network. In a recent 31-day window:

- **43,560 total events** across all types
- **11,725 high G-force events**
- **1,114 harsh braking events**
- Events from **dozens of countries** including USA, Canada, Mexico, Brazil, UK, Germany, Poland, Austria, Slovenia, Portugal, Australia, Taiwan, Japan

---

## Tech Stack

- **Next.js 16** with React 19 and App Router
- **TypeScript 5** throughout
- **Tailwind CSS 4** for styling
- **shadcn/ui** + Radix UI for accessible components
- **Mapbox GL JS** for interactive maps, GNSS track rendering, and map feature display
- **Anthropic SDK** for Claude integration in the AI agent
- **FFmpeg** (server-side) for video frame extraction
- **suncalc** for time-of-day classification based on sun position

---

## Getting Started

```bash
git clone https://github.com/Hivemapper/ai-event-videos.git
cd ai-event-videos
npm install
```

Create `.env.local` with your API keys:

```
NEXT_PUBLIC_MAPBOX_TOKEN=your_mapbox_token
```

The Bee Maps and Anthropic API keys are configured in the app's settings dialog at runtime — no need to put them in env files.

```bash
npm run dev
# Open http://localhost:3000
```

Get a Bee Maps API key at [beemaps.com/developers](https://beemaps.com/developers). Mapbox token from [mapbox.com](https://account.mapbox.com/). Anthropic key (optional, for AI search) from [console.anthropic.com](https://console.anthropic.com/).

---

## Use Cases

**AI model training** — Use the frame labeling tool to extract video frames paired with nearby map features (stop signs, speed signs) as labeled training data. The global coverage provides diversity across road types, signage styles, and driving conditions.

**Driving behavior analysis** — Filter events by type and geography, then drill into speed profiles and IMU data to study braking patterns, acceleration profiles, and lateral forces.

**Fleet safety monitoring** — Use geographic radius search to monitor specific routes or regions. The highlights page demonstrates how to identify the most extreme events across the network.

**Infrastructure auditing** — Cross-reference event locations with map features to find places where braking events cluster near missing or obscured signage.

**Video dataset construction** — Bulk browse events, download videos with full metadata, and build driving video datasets with GPS coordinates, speed telemetry, and timestamps attached to every clip.

---

## Links

- **App repository**: [github.com/Hivemapper/ai-event-videos](https://github.com/Hivemapper/ai-event-videos)
- **Bee Maps API docs**: [beemaps.com/api/developer/docs](https://beemaps.com/api/developer/docs#tag/aievents/POST/aievents/search)
- **Get a Bee Maps API key**: [beemaps.com/developers](https://beemaps.com/developers)

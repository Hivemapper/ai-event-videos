# AI Event Videos

A Next.js web application for browsing, filtering, and analyzing AI-detected driving events videos captured by Hivemapper dashcams. 
View event videos, inspect speed profiles, explore GNSS/IMU sensor data, and see nearby map features — all on an interactive map.

## Features

- **Event Gallery** — Browse events in a grid view with video thumbnails, or switch to an interactive map view
- **Filtering** — Filter by date range, event type, time of day (day/dawn/dusk/night), country, and geographic coordinates with radius
- **Event Detail View** — Play event videos with synchronized map playback showing the GNSS track
- **Speed Profile** — Visualize speed data over the duration of an event, with speed limit violation highlighting
- **Positioning Data** — Inspect raw GNSS (lat/lon/altitude) and IMU (accelerometer/gyroscope) sensor data
- **Nearby Map Features** — See stop signs, speed signs, and other map features near an event location
- **Frame Labeling** — Extract individual video frames at any timestamp and pair them with nearby map feature labels for training data export
- **Road Classification** — Automatic road type identification (highway, primary, residential, etc.)

## Event Types

| Type | Description |
|------|-------------|
| Harsh Braking | Sudden deceleration detected |
| Aggressive Acceleration | Rapid acceleration detected |
| Swerving | Lateral movement deviation |
| High Speed | Speed exceeding threshold |
| High G-Force | Elevated g-force detected |
| Stop Sign Violation | Failure to stop at stop sign |
| Traffic Light Violation | Running a red light |
| Tailgating | Following too closely |
| Manual Request | Driver-triggered recording |

## External APIs

This application uses the following external APIs:

### BeeMaps API

Base URL: [Bee Maps AI Event Videos](https://beemaps.com/api/developer/docs#tag/aievents/POST/aievents/search)

| Endpoint | Method | Description |
|----------|--------|-------------|
| [/aievents/search](https://beemaps.com/api/developer/docs#tag/aievents/POST/aievents/search) | POST | Search for AI events by date range, event type, and geographic polygon |
| [/aievents/{id}](https://beemaps.com/api/developer/docs#tag/ai-events/GET/aievents/{id}) | GET | Get a single event by ID, optionally including GNSS and IMU data |
| [/map-data](https://beemaps.com/api/developer/docs#POST/mapFeatures/poly) | POST | Query map features (stop signs, speed signs, etc.) within a geographic polygon |
| [/devices](https://beemaps.com/api/developer/docs#GET/devices) | GET | Get camera intrinsic parameters (focal length, distortion coefficients) for Hivemapper Bee devices |

Authentication: BeeMaps API key.  Get API Key https://beemaps.com/developers

### Mapbox APIs

| API | Description |
|-----|-------------|
| [Map GL JS](https://docs.mapbox.com/mapbox-gl-js/) | Interactive map rendering for event locations and GNSS tracks |
| [Tilequery API](https://docs.mapbox.com/api/maps/tilequery/) | Reverse lookup of road classification at a given coordinate |
| [Geocoding API](https://docs.mapbox.com/api/search/geocoding/) | Reverse geocoding to resolve country names from coordinates |

Authentication: Mapbox access token.

### FFmpeg (local)

Used server-side to extract video frames and generate thumbnails. Must be installed on the host machine.

## Getting Started

### Prerequisites

- Node.js 18+
- FFmpeg installed and available on `PATH`
- A [BeeMaps API key](https://beemaps.com)
- A [Mapbox access token](https://account.mapbox.com)

### Setup

1. Clone the repository:

```bash
git clone https://github.com/Hivemapper/ai-event-videos.git
cd ai-event-videos
```

2. Install dependencies:

```bash
npm install
```

3. Configure environment variables:

```bash
cp .env.example .env.local
```

Edit `.env.local` and add your Mapbox token:

```
NEXT_PUBLIC_MAPBOX_TOKEN=your_mapbox_token_here
```

The BeeMaps API key can be set via the `.env.local` file or configured through the settings dialog in the UI.

4. Start the development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) to view the app.

## Project Structure

```
src/
├── app/
│   ├── api/
│   │   ├── events/          # Proxy for BeeMaps AI events API
│   │   ├── frames/          # FFmpeg frame extraction at specific timestamps
│   │   ├── labeled-frame/   # Frame extraction + nearby map feature labels
│   │   ├── map-features/    # Proxy for BeeMaps map data API
│   │   ├── road-type/       # Mapbox road classification lookup
│   │   ├── thumbnail/       # FFmpeg thumbnail generation
│   │   └── video/           # Video proxy for CORS-free playback
│   ├── event/[id]/          # Event detail page
│   └── page.tsx             # Home page (event gallery)
├── components/
│   ├── events/              # Event grid, cards, filters, settings
│   ├── map/                 # Mapbox map components
│   └── ui/                  # Reusable UI components (shadcn/ui)
├── hooks/                   # React hooks for data fetching
├── lib/                     # Utilities, constants, API helpers
└── types/                   # TypeScript type definitions
```

## Tech Stack

- **Framework**: [Next.js](https://nextjs.org) 16 (App Router)
- **Language**: TypeScript
- **Styling**: [Tailwind CSS](https://tailwindcss.com) 4
- **UI Components**: [shadcn/ui](https://ui.shadcn.com) + [Radix UI](https://www.radix-ui.com)
- **Maps**: [Mapbox GL JS](https://docs.mapbox.com/mapbox-gl-js/)
- **Icons**: [Lucide React](https://lucide.dev)

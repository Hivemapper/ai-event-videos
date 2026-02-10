# AI Event Videos — Project Guide

## Overview

Next.js 16 (App Router) web app for browsing, filtering, and analyzing AI-detected driving events from Hivemapper dashcams. Uses the Bee Maps API for event data and Mapbox for maps.

## Tech Stack

- **Next.js 16** with App Router, TypeScript, React 19
- **Tailwind CSS 4** + **shadcn/ui** (Radix UI primitives)
- **Mapbox GL JS** for interactive maps
- **FFmpeg** for server-side video frame extraction and thumbnails
- **Anthropic Claude API** (Sonnet 4.5) for AI Filter Agent

## Key Architecture

### API Routes (all under `src/app/api/`)

- `events/` — Proxy to Bee Maps `/aievents/search` (POST) and `/aievents/{id}` (GET). Uses Basic auth.
- `agent/` — AI Filter Agent. Sends natural language queries to Claude, which extracts structured filters via tool use.
- `video/` — Video proxy with Range request support for seeking/scrubbing.
- `frames/` — FFmpeg frame extraction at specific timestamps. Caches to temp dir.
- `thumbnail/` — FFmpeg thumbnail generation (320px wide at 1s). Caches to temp dir.
- `labeled-frame/` — Combines frame extraction with nearby map feature labels from Bee Maps.
- `map-features/` — Proxy to Bee Maps `/map-data` for querying map features within a polygon.
- `road-type/` — Mapbox Tilequery API for road classification at a coordinate.

### Pages

- `/` — Event gallery with grid/map views, filters, and Agent toggle
- `/event/[id]` — Event detail: video player with speed overlay, map with GNSS track, speed profile, GNSS/IMU data, frame labeling
- `/highlights` — Curated tables of extreme events (braking, speed, g-force, acceleration, swerving, international)

### External APIs

- **Bee Maps API** (`https://beemaps.com/api/developer`): Events, map features, device intrinsics. Auth: `Basic {base64_key}`
- **Mapbox**: Map rendering, Tilequery (road type), Geocoding (country names). Auth: access token.
- **Anthropic Messages API**: Powers the AI Filter Agent with Claude Sonnet 4.5.

### API Keys

Stored in browser localStorage. Keys: `beemaps-api-key`, `mapbox-token`, `anthropic-api-key`. Can also be set via `.env.local` (see `.env.example`).

## Conventions

- UI components in `src/components/ui/` are shadcn/ui — do not modify directly, add new ones with `npx shadcn@latest add`.
- Event types are defined in `src/types/events.ts`. Config (labels, colors, icons) in `src/lib/constants.ts`.
- Highlights data is in `src/lib/highlights.ts`. The highlights page (`src/app/highlights/page.tsx`) renders sections by index.
- Speed values from the API are in m/s. Convert to mph (* 2.237) for display. The speed overlay on the video player uses mph.
- The Bee Maps search API expects ISO datetime strings (e.g. `2026-01-06T00:00:00.000Z`), not plain date strings.

## Common Tasks

- **Add a UI component**: `npx shadcn@latest add <component>`
- **Type check**: `npx tsc --noEmit`
- **Dev server**: `npm run dev`
- **Build**: `npm run build`
- **Lint**: `npm run lint`

## Gotchas

- The video proxy must forward `Range` headers for scrubbing to work.
- Bee Maps API returns 400 if dates aren't full ISO datetime format.
- The `.gitignore` blocks all `.env*` files. Use `-f` flag to force-add `.env.example`.
- FFmpeg must be installed on the host machine for frame/thumbnail extraction.

# Customer Video Lists

Customer pages live under `/customers/[slug]`.

## Turso tables

Customer lists are stored in Turso:

- `customers`: one row per customer, keyed by `slug`.
- `customer_events`: one row per customer video, keyed by
  `(customer_slug, event_id)` with a `list_position` for the customer order.

The page joins `customer_events` to existing Turso tables for display fields:
`triage_results`, `video_frame_timing_qc`, `detection_runs`,
`video_detection_segments`, and `production_runs`.

## Seeding

The Nvidia list is seeded by:

```bash
npx tsx scripts/seed-customer-events.ts
```

That script creates the customer tables in Turso, replaces the seeded Nvidia
membership list, and prints the resulting customer counts.

## Page controls

Each row has an `X` control that removes that event from the selected customer
list in Turso and compacts the remaining list positions. Re-running the seed
script replaces the seeded Nvidia membership list.

## Page performance

`/customers/[slug]` is a server-rendered table. It performs one small Turso
list query for the selected customer and does not call Bee Maps, generate
thumbnails, proxy videos, or read the local SQLite replica during page load.
Production status and priority are read from live `production_runs` rows at
render time so worker progress is not blocked by stale `customer_events`
snapshots. The page also refreshes itself while visible so open customer tabs
pick up queue, processing, completed, and failed status changes.

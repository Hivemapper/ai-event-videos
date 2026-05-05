# Pipeline Dashboards

`/pipeline` is the shared operations dashboard for the workflow stages:
Triage, VRU, and Production. The active stage, status, filters, and sort order
live in the URL (`stage`, `status`, `period`, `fpsQc`, `eventTypes`,
`vruLabels`, `sort`, and `dir`) so filtered views are shareable.

Legacy dashboard routes redirect into the unified page:

- `/triage` and `/triage/[filter]` -> `/pipeline?stage=triage`
- `/pipeline/[tab]` -> `/pipeline?stage=vru`
- `/production-pipeline/[tab]` -> `/pipeline?stage=production`

Rows are served by `/api/pipeline/overview` and counts by
`/api/pipeline/overview/counts`. Both use server-side filtering and pagination
over `triage_results`, `video_frame_timing_qc`, `detection_runs`,
`frame_detections`, and `production_runs`.

For the Triage stage, the `Awaiting` card is computed from the Bee Maps source
event total for the selected period minus the number of source IDs already
present in `triage_results`. It is only shown for supported triage periods and
requires `BEEMAPS_API_KEY` in the Next.js process.

When a supported period has more than 200 awaiting events, the counts endpoint
starts one detached `scripts/run-triage.py` pass for that period. It checks for
an existing local `run-triage.py` process before spawning so dashboard refreshes
do not duplicate triage workers.

The period selector shows each period's date range. During refreshes or
transient API failures, the dashboard keeps the last valid row response mounted
instead of replacing the table with an empty state.

The dashboard keeps the table compact and consistent across stages. It uses
stored `triage_results.bitrate_bps` when available; the Mbps column only probes
the source video after the user clicks `Probe` for rows without stored bitrate.
VRU label filtering uses saved `frame_detections.label`, so selecting labels
only returns rows with matching detections.

The client keeps previous rows mounted while it refreshes, but the polling
cadence is intentionally moderate because the rows, counts, and stage stats
touch separate aggregate queries. Active VRU and Production queues refresh more
often than completed/failed or Triage views; Triage counts are slower because
the `Awaiting` card may need the Bee Maps source total.

The rows and counts endpoints also keep short in-process response caches keyed
by the URL query. Triage and terminal statuses cache for 30 seconds; active VRU
and Production views cache for 10 seconds. This keeps dashboard polling from
re-running the same aggregate SQL on every client refresh.

The row endpoint depends on confidence-ordered lookups for saved frame
detections and status lookups for production runs. The schema includes matching
indexes on `frame_detections(video_id, confidence, created_at)` and
`production_runs(video_id, status)` so the dashboard can paginate without
scanning those tables for each visible row.

The v1 UI keeps row-level production actions such as push, prioritize, and
requeue. Bulk or fleet-level controls remain outside the unified dashboard.

# Run Triage

`scripts/run-triage.py` classifies Bee Maps AI events into triage buckets and writes rows to `triage_results`.

Triage is supported for Period 4 and newer. The triage page's "All Periods" view means Periods 4+, and the runner only accepts `--period 4`, `--period 5`, `--period 6`, or `--period 7`. Date-window runs with `--days` are clamped to the Period 4 start.

For period-specific runs, use `--period` instead of approximating with `--days`. Period 7 uses the source-level firmware gate and only processes events whose firmware is eligible for that period.

```bash
python3 scripts/run-triage.py 15000 --period 7
```

To triage only a specific firmware release within the selected date window, add
`--firmware`. The match is exact against Bee Maps search metadata:

```bash
python3 scripts/run-triage.py 500 --period 7 --firmware 7.7.6
```

The event-search phase skips already-triaged IDs as Bee Maps pages arrive and de-dupes candidate IDs across pages. Pages are fetched concurrently, with a default of 4 workers. To tune that without editing code:

```bash
TRIAGE_FETCH_CONCURRENCY=6 python3 scripts/run-triage.py 15000 --period 7
```

Keep concurrency modest because Bee Maps can rate-limit with HTTP 403. The runner backs off and retries when that happens.

Period 7 firmware-ineligible events are recorded as `skipped_firmware` so later runs do not fetch and reject the same source rows again. The runner uses the firmware included in the Bee Maps search response to skip known-ineligible events before full detail, video probing, frame QC, or geocoding work. If the search response is missing firmware, the runner fetches event detail before deciding.

Within a run, video URL checks and frame-timing probes are cached by video URL. This avoids repeating the same Range request and remote `ffprobe` when multiple event IDs refer to the same clip.

The Bee Maps search response can omit `videoUrl` even when the event-detail endpoint has a playable video. The runner now fetches event detail before writing `missing_video` for a missing search URL, and retries video validation with the detail URL if the search URL fails.

New `missing_video` rows include `detail_confirmed_missing_video` in `rules_triggered`, along with the specific reason such as `no_video_url_after_detail`, `video_unreachable_after_detail`, or `file_too_small_<bytes>B`. This distinguishes confirmed missing videos from older rows that were classified from incomplete search metadata.

To audit and repair older Period 7 rows that were falsely classified as `missing_video`, run:

```bash
python3 scripts/audit-period7-missing-videos.py --fix
```

Without `--fix`, the audit script runs in dry-run mode and reports what it would update. Use `--workers N` or `TRIAGE_PROCESS_WORKERS=N` to overlap event-detail fetches and video probes.

Bee Maps throttling may arrive as Cloudflare HTTP 403 or API HTTP 429. Both are treated as rate limits and retried with backoff. Transient HTTP 502/503/504 responses are retried a few times before the event is reported as a fetch error.

## Triage Page Counts

`/api/triage` returns the existing triaged buckets plus `awaitingTriageTotal` for the selected period. The awaiting count is the Bee Maps source event total for the period minus event IDs already present in `triage_results`; the source total is cached briefly server-side and requires `BEEMAPS_API_KEY` to be available to the Next.js process.

The unified Pipeline counts endpoint uses the same source-total definition for
the Triage stage's `Awaiting` card. For supported period-specific views, if the
awaiting count is greater than 200, `/api/pipeline/overview/counts` starts a
detached `scripts/run-triage.py <awaiting> --period <period>` pass and writes
logs under `logs/auto-triage-period*.log`. It will not start another pass while
a local `scripts/run-triage.py` process is already running.

Per-event processing is serial by default so local runs preserve the historical behavior. To overlap event-detail fetches, video checks, frame-QC probes, and geocoding while keeping database writes in the main thread:

```bash
TRIAGE_PROCESS_WORKERS=4 python3 scripts/run-triage.py 15000 --period 7
```

The expensive signal duplicate clustering pass is opt-in for large runs:

```bash
python3 scripts/run-triage.py 15000 --period 7 --dedupe
```

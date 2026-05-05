# Metrics Performance

The `/metrics` page loads high-level totals before the expensive category
breakdown.

- `/api/metrics?mode=summary` fetches only total counts for the dashboard cards.
  It decomposes overlapping periods into non-overlapping time windows and shares
  cached Bee Maps count chunks with the daily and monthly endpoints.
- `/api/metrics?mode=breakdown` preserves the event-type breakdown used by the
  category table, but the page loads it in the background after summary cards
  render.
- `/api/metrics/daily` and `/api/metrics/monthly` use the same count cache, so
  overlapping ranges do not refetch the same Bee Maps count chunks within the
  cache TTL. They also keep short route-level caches for repeat page visits.
- `/api/metrics/geo` groups signal rows by rounded coordinate buckets in SQL
  before resolving countries, which avoids transferring and processing every
  signal row on each request. The country response is cached briefly in memory.

The default `/api/metrics` behavior remains the full category breakdown for
callers that do not pass `mode=summary`.

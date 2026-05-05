# Event List Performance

The home event list uses `/api/events` as a Bee Maps search proxy. Bee Maps
searches are split into 31-day chunks when the requested date range is longer
than the upstream range limit.

For multi-chunk ranges, pagination is planned newest-to-oldest:

- request a cheap `limit: 1` page for each chunk to read its total;
- skip whole chunks until the requested global offset is reached;
- fetch only the chunk page segments needed for the requested `limit`;
- return the summed chunk total and the requested page.

This avoids the old behavior where the proxy fetched only the first 500 events
from every chunk, merged that partial set, and sliced it locally.

The home-page map view has been removed. `/?view=map` now renders the normal
event list, so the app no longer auto-loads every event just to populate map
markers. Event-detail maps and Mapbox-backed road/location helpers remain.

The gallery keeps the last successful page of event results in an in-memory
cache keyed by the server-side search inputs. Returning to the gallery or
refreshing an already-loaded query keeps those rows visible while the fresh page
loads in the background. The `Show X new videos` action uses the same retained
state: it disables the banner and loads the new first page behind the current
grid, then swaps in the new event list after the response succeeds.

Top Hits list rows come from the Turso-backed `/api/top-hits` summary response.
That response includes event type, timestamp, bitrate, FPS QC, VRU summary, and
pipeline status from local tables, so the initial Top Hits table does not call
Bee Maps once per saved event. Video preview mode can still lazily fetch an
individual event when a preview card scrolls into view.

The Top Hits route performs table/index setup once per server process and keeps
a short 15-second in-memory cache for GET responses. POST and DELETE mutations
invalidate the cache before returning the refreshed list.

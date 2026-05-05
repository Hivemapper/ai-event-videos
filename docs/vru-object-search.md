# VRU And Object Search

The event gallery can filter results by VRU or detected object labels with the
`vruLabels` filter. The value is carried in the page URL and in `/api/events`
POST bodies as a comma-separated URL parameter or string array.

The filter is backed by local `frame_detections` rows joined to
`triage_results`, then the matching event IDs are fetched from Bee Maps for the
normal gallery cards. Date, event type, and coordinate/polygon search filters
are applied before pagination.

Label options and aliases live in `src/lib/vru-labels.ts` as
`VRU_OBJECT_FILTER_OPTIONS`. Keep `isVruDetectionLabel` VRU-only; object-search
options may include non-VRU labels such as cars, trucks, buses, or crosswalks
without exposing those labels in the event detail VRU overlay.

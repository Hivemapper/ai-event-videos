#!/bin/bash
# Find 12 harsh braking events (to 0 mph) across 3 timeframes, 4 per group

API_KEY=$(grep BEEMAPS_API_KEY /Users/as/Documents/Projects/ai-event-videos/.env.local | head -1 | cut -d= -f2-)
API_URL="http://localhost:3000/api/events"
TARGET_PER_GROUP=4

JQ_FILTER='
.events[]?
| select(.metadata.SPEED_ARRAY != null)
| .metadata.SPEED_ARRAY as $speeds
| ([$speeds[].AVG_SPEED_MS] | max * 2.237) as $maxMph
| ([$speeds[].AVG_SPEED_MS] | min * 2.237) as $minMph
| select($maxMph >= 40 and $minMph <= 0.5)
| "\(.id)\t\($maxMph * 10 | round / 10)\t\($minMph * 10 | round / 10)\t\(.location.lat),\(.location.lon)\t\(.timestamp)"
'

search_group() {
  local group_name="$1"
  local start_date="$2"
  local end_date="$3"
  local results_file=$(mktemp)
  local found=0

  echo "" >&2
  echo "=== Group $group_name: $start_date to $end_date ===" >&2

  for OFFSET in 0 500 1000 1500 2000 2500 3000 3500 4000; do
    echo "  Fetching offset $OFFSET..." >&2
    RESP=$(curl -s "$API_URL" \
      -X POST \
      -H "Content-Type: application/json" \
      -H "Authorization: Basic $API_KEY" \
      -d "{\"startDate\":\"$start_date\",\"endDate\":\"$end_date\",\"types\":[\"HARSH_BRAKING\"],\"limit\":500,\"offset\":$OFFSET}")

    COUNT=$(echo "$RESP" | jq '.events | length' 2>/dev/null)
    if [ "$COUNT" = "0" ] || [ "$COUNT" = "null" ] || [ -z "$COUNT" ]; then
      echo "  No more events at offset $OFFSET" >&2
      break
    fi
    echo "  Got $COUNT events" >&2

    echo "$RESP" | jq -r "$JQ_FILTER" >> "$results_file" 2>/dev/null
    found=$(wc -l < "$results_file" | tr -d ' ')

    if [ "$found" -ge "$TARGET_PER_GROUP" ]; then
      echo "  Found $found matches (need $TARGET_PER_GROUP)" >&2
      break
    fi
  done

  # Output top results (sorted by max speed descending), limited to TARGET_PER_GROUP
  sort -t$'\t' -k2 -rn "$results_file" | head -n "$TARGET_PER_GROUP" | while IFS= read -r line; do
    echo "$group_name	$line"
  done

  local total=$(wc -l < "$results_file" | tr -d ' ')
  echo "  Total matches for group $group_name: $total (using top $TARGET_PER_GROUP)" >&2
  rm "$results_file"
}

echo ""
echo "Group	ID	MaxMPH	MinMPH	Location	Timestamp"
echo "-----	--	------	------	--------	---------"

search_group "A" "2026-01-13T00:00:00.000Z" "2026-02-13T23:59:59.999Z"
search_group "B" "2026-02-15T00:00:00.000Z" "2026-03-15T23:59:59.999Z"
search_group "C" "2026-03-17T00:00:00.000Z" "2026-03-24T23:59:59.999Z"

echo ""
echo "Done. View any event at: http://localhost:3001/event/{ID}"

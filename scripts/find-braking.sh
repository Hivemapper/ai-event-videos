#!/bin/bash
# Find harsh braking events: 40+ mph max to 0 mph min, last 100 days

API_KEY=$(grep BEEMAPS_API_KEY /Users/as/Documents/Projects/ai-event-video/.env.local | head -1 | cut -d= -f2-)
START=$(date -v-100d -u +"%Y-%m-%dT00:00:00.000Z")
END=$(date -u +"%Y-%m-%dT23:59:59.999Z")

JQ_FILTER='
.events[]?
| select(.metadata.SPEED_ARRAY != null)
| .metadata.SPEED_ARRAY as $speeds
| ([$speeds[].AVG_SPEED_MS] | max * 2.237) as $maxMph
| ([$speeds[].AVG_SPEED_MS] | min * 2.237) as $minMph
| select($maxMph >= 40 and $minMph <= 0.5)
| "\(.id)\t\($maxMph * 10 | round / 10)\t\($minMph * 10 | round / 10)\t\(.location.lat),\(.location.lon)\t\(.timestamp)"
'

RESULTS_FILE=$(mktemp)

for OFFSET in 0 500 1000 1500 2000 2500 3000 3500 4000 4500 5000 5500 6000 6500 7000 7500 8000 8500 9000 9500 10000; do
  echo "Fetching offset $OFFSET..." >&2
  RESP=$(curl -s "http://localhost:3001/api/events" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $API_KEY" \
    -d "{\"startDate\":\"$START\",\"endDate\":\"$END\",\"types\":[\"HARSH_BRAKING\"],\"limit\":500,\"offset\":$OFFSET}")

  COUNT=$(echo "$RESP" | jq '.events | length' 2>/dev/null)
  if [ "$COUNT" = "0" ] || [ "$COUNT" = "null" ] || [ -z "$COUNT" ]; then
    echo "No more events at offset $OFFSET" >&2
    break
  fi

  echo "$RESP" | jq -r "$JQ_FILTER" >> "$RESULTS_FILE" 2>/dev/null
  echo "  Got $COUNT events" >&2
done

echo ""
echo "=== RESULTS (sorted by max speed, descending) ==="
echo "ID	MaxMPH	MinMPH	Location	Timestamp"
sort -t$'\t' -k2 -rn "$RESULTS_FILE"
echo ""
echo "Total matches: $(wc -l < "$RESULTS_FILE" | tr -d ' ')"
rm "$RESULTS_FILE"

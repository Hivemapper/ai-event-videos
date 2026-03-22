#!/bin/bash
# Find harsh braking events at intersections (≤25 mph to 0)
# Uses: Mapbox tilequery (multiple road segments = intersection) + Bee Maps (stop signs/traffic lights)

API_KEY=$(grep BEEMAPS_API_KEY /Users/as/Documents/Projects/ai-event-video/.env.local | head -1 | cut -d= -f2-)
MAPBOX_TOKEN=$(grep NEXT_PUBLIC_MAPBOX_TOKEN /Users/as/Documents/Projects/ai-event-video/.env.local | head -1 | cut -d= -f2-)
START=$(date -v-100d -u +"%Y-%m-%dT00:00:00.000Z")
END=$(date -u +"%Y-%m-%dT23:59:59.999Z")

SPEED_FILTER_FILE=$(mktemp)
cat > "$SPEED_FILTER_FILE" << 'JQEOF'
.events[]?
| select(.metadata.SPEED_ARRAY != null)
| .metadata.SPEED_ARRAY as $speeds
| ([$speeds[].AVG_SPEED_MS] | max * 2.237) as $maxMph
| ([$speeds[].AVG_SPEED_MS] | min * 2.237) as $minMph
| select($maxMph <= 25 and $minMph <= 0.5)
| "\(.id)\t\($maxMph * 10 | round / 10)\t\($minMph * 10 | round / 10)\t\(.location.lat)\t\(.location.lon)\t\(.timestamp)"
JQEOF

echo "=== Step 1: Finding harsh braking events (≤25 mph → 0) ===" >&2
CANDIDATES=$(mktemp)
for OFFSET in 0 500 1000 1500 2000 2500 3000 3500 4000; do
  echo "  Fetching offset $OFFSET..." >&2
  RESP=$(curl -s "http://localhost:3001/api/events" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $API_KEY" \
    -d "{\"startDate\":\"$START\",\"endDate\":\"$END\",\"types\":[\"HARSH_BRAKING\"],\"limit\":500,\"offset\":$OFFSET}")
  COUNT=$(echo "$RESP" | jq '.events | length' 2>/dev/null)
  [ "$COUNT" = "0" ] || [ "$COUNT" = "null" ] || [ -z "$COUNT" ] && break
  echo "$RESP" | jq -rf "$SPEED_FILTER_FILE" >> "$CANDIDATES" 2>/dev/null
  echo "  $COUNT events scanned" >&2
done
rm "$SPEED_FILTER_FILE"

TOTAL=$(wc -l < "$CANDIDATES" | tr -d ' ')
echo "  Found $TOTAL candidate events" >&2

echo ""
echo "=== Step 2: Checking each for intersection signals ===" >&2

RESULTS=$(mktemp)

while IFS=$'\t' read -r ID MAX_MPH MIN_MPH LAT LON TIMESTAMP; do
  SIGNALS=""
  SIGNAL_COUNT=0

  # Signal 1: Mapbox tilequery — count distinct road features within 30m
  # Multiple roads at the same point = intersection
  ROADS=$(curl -s "https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${LON},${LAT}.json?layers=road&radius=30&limit=5&access_token=${MAPBOX_TOKEN}" \
    | jq '[.features[]? | .properties.class] | unique | length' 2>/dev/null)

  ROAD_COUNT=$(curl -s "https://api.mapbox.com/v4/mapbox.mapbox-streets-v8/tilequery/${LON},${LAT}.json?layers=road&radius=30&limit=10&access_token=${MAPBOX_TOKEN}" \
    | jq '[.features[]?] | length' 2>/dev/null)

  if [ "$ROAD_COUNT" -ge 2 ] 2>/dev/null; then
    SIGNALS="${SIGNALS}roads:${ROAD_COUNT},"
    SIGNAL_COUNT=$((SIGNAL_COUNT + 1))
  fi

  # Signal 2: Bee Maps map features — stop signs or traffic lights within 50m
  FEATURES=$(curl -s "http://localhost:3001/api/map-features?lat=${LAT}&lon=${LON}&radius=50" \
    -H "Authorization: Basic $API_KEY" \
    | jq -r '[.features[]? | .class] | join(",")' 2>/dev/null)

  if echo "$FEATURES" | grep -qi "stop"; then
    SIGNALS="${SIGNALS}stop-sign,"
    SIGNAL_COUNT=$((SIGNAL_COUNT + 2))
  fi
  if echo "$FEATURES" | grep -qi "light\|signal\|traffic"; then
    SIGNALS="${SIGNALS}traffic-light,"
    SIGNAL_COUNT=$((SIGNAL_COUNT + 2))
  fi
  if echo "$FEATURES" | grep -qi "speed"; then
    SIGNALS="${SIGNALS}speed-sign,"
    SIGNAL_COUNT=$((SIGNAL_COUNT + 1))
  fi

  # Keep events with at least 2 intersection signals (or a stop sign/light which is definitive)
  if [ "$SIGNAL_COUNT" -ge 2 ] 2>/dev/null; then
    SIGNALS=$(echo "$SIGNALS" | sed 's/,$//')
    echo -e "${ID}\t${MAX_MPH}\t${MIN_MPH}\t${LAT},${LON}\t${TIMESTAMP}\t${SIGNALS}"
    echo -e "${ID}\t${MAX_MPH}\t${MIN_MPH}\t${LAT},${LON}\t${TIMESTAMP}\t${SIGNALS}" >> "$RESULTS"
    echo "  ✓ $ID — ${MAX_MPH}→${MIN_MPH} mph — $SIGNALS" >&2
  else
    echo "  ✗ $ID — no intersection signal (roads:${ROAD_COUNT:-0}, features:${FEATURES:-none})" >&2
  fi

done < "$CANDIDATES"
rm "$CANDIDATES"

echo ""
echo "=== INTERSECTION BRAKING EVENTS ==="
echo "Total: $(wc -l < "$RESULTS" | tr -d ' ') events at intersections"
echo ""
echo "ID	MaxMPH	MinMPH	Location	Timestamp	Signals"
sort -t$'\t' -k2 -rn "$RESULTS"
rm "$RESULTS"

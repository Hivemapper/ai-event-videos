#!/bin/bash
# Find harsh braking events at intersections using Overture Maps connector density

API_KEY=$(grep BEEMAPS_API_KEY /Users/as/Documents/Projects/ai-event-video/.env.local | head -1 | cut -d= -f2-)
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
  echo "  offset $OFFSET..." >&2
  RESP=$(curl -s "http://localhost:3001/api/events" \
    -X POST \
    -H "Content-Type: application/json" \
    -H "Authorization: Basic $API_KEY" \
    -d "{\"startDate\":\"$START\",\"endDate\":\"$END\",\"types\":[\"HARSH_BRAKING\"],\"limit\":500,\"offset\":$OFFSET}")
  COUNT=$(echo "$RESP" | jq '.events | length' 2>/dev/null)
  [ "$COUNT" = "0" ] || [ "$COUNT" = "null" ] || [ -z "$COUNT" ] && break
  echo "$RESP" | jq -rf "$SPEED_FILTER_FILE" >> "$CANDIDATES" 2>/dev/null
  echo "  $COUNT events" >&2
done
rm "$SPEED_FILTER_FILE"

TOTAL=$(wc -l < "$CANDIDATES" | tr -d ' ')
echo "  Found $TOTAL candidates" >&2

echo "" >&2
echo "=== Step 2: Checking Overture connector density (3+ connectors within 20m = intersection) ===" >&2

RESULTS=$(mktemp)
CHECKED=0

while IFS=$'\t' read -r ID MAX_MPH MIN_MPH LAT LON TIMESTAMP; do
  CHECKED=$((CHECKED + 1))
  # ~20m bbox: 0.0002 degrees
  LAT_MIN=$(echo "$LAT - 0.0002" | bc -l)
  LAT_MAX=$(echo "$LAT + 0.0002" | bc -l)
  LON_MIN=$(echo "$LON - 0.0002" | bc -l)
  LON_MAX=$(echo "$LON + 0.0002" | bc -l)

  CONN_COUNT=$(duckdb -csv -noheader -c "
    LOAD spatial;
    LOAD httpfs;
    SET s3_region='us-west-2';
    SELECT COUNT(*) FROM read_parquet(
      's3://overturemaps-us-west-2/release/2026-03-18.0/theme=transportation/type=connector/*',
      filename=true, hive_partitioning=1)
    WHERE bbox.xmin > $LON_MIN AND bbox.xmax < $LON_MAX
      AND bbox.ymin > $LAT_MIN AND bbox.ymax < $LAT_MAX;
  " 2>/dev/null)

  if [ "$CONN_COUNT" -ge 3 ] 2>/dev/null; then
    echo -e "${ID}\t${MAX_MPH}\t${MIN_MPH}\t${LAT},${LON}\t${TIMESTAMP}\tconnectors:${CONN_COUNT}"
    echo -e "${ID}\t${MAX_MPH}\t${MIN_MPH}\t${LAT},${LON}\t${TIMESTAMP}\tconnectors:${CONN_COUNT}" >> "$RESULTS"
    echo "  [$CHECKED/$TOTAL] ✓ $ID — ${MAX_MPH}→${MIN_MPH} mph — $CONN_COUNT connectors" >&2
  else
    echo "  [$CHECKED/$TOTAL] ✗ $ID — $CONN_COUNT connectors" >&2
  fi
done < "$CANDIDATES"
rm "$CANDIDATES"

echo "" >&2
echo "=== RESULTS: Harsh braking at intersections (Overture connectors ≥ 3) ===" >&2
MATCH_COUNT=$(wc -l < "$RESULTS" | tr -d ' ')
echo "Total: $MATCH_COUNT intersection events out of $TOTAL candidates" >&2
echo ""
echo "ID	MaxMPH	MinMPH	Location	Timestamp	Connectors"
sort -t$'\t' -k2 -rn "$RESULTS"
rm "$RESULTS"

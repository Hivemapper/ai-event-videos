#!/usr/bin/env python3
"""Check 500 most recent harsh braking events against Overture Maps connector data."""

import json
import subprocess
import sys
import tempfile
import os

EVENTS_FILE = sys.argv[1] if len(sys.argv) > 1 else None

if not EVENTS_FILE:
    print("Usage: python3 intersection-check.py <events_tsv_file>")
    sys.exit(1)

# Parse events
events = []
with open(EVENTS_FILE) as f:
    for line in f:
        parts = line.strip().split('\t')
        if len(parts) >= 6:
            events.append({
                'id': parts[0],
                'max_mph': float(parts[1]),
                'min_mph': float(parts[2]),
                'lat': float(parts[3]),
                'lon': float(parts[4]),
                'ts': parts[5],
            })

print(f"Loaded {len(events)} events", file=sys.stderr)

# Group events by region (~1 degree tiles) so we can query connectors per region
from collections import defaultdict
regions = defaultdict(list)
for e in events:
    key = (round(e['lat']), round(e['lon']))
    regions[key].append(e)

print(f"Events span {len(regions)} geographic regions", file=sys.stderr)

# Build SQL: load connectors per region bbox, then join
values_rows = []
for e in events:
    values_rows.append(f"('{e['id']}', {e['max_mph']}, {e['min_mph']}, {e['lat']}, {e['lon']}, '{e['ts']}')")
values_sql = ',\n'.join(values_rows)

# Build UNION ALL of connector queries per region (each with tight bbox = fast parquet scan)
connector_unions = []
for (rlat, rlon), region_events in regions.items():
    min_lat = min(e['lat'] for e in region_events) - 0.001
    max_lat = max(e['lat'] for e in region_events) + 0.001
    min_lon = min(e['lon'] for e in region_events) - 0.001
    max_lon = max(e['lon'] for e in region_events) + 0.001
    connector_unions.append(f"""
  SELECT ST_X(geometry) as clon, ST_Y(geometry) as clat
  FROM read_parquet('s3://overturemaps-us-west-2/release/2026-03-18.0/theme=transportation/type=connector/*',
    filename=true, hive_partitioning=1)
  WHERE bbox.xmin > {min_lon} AND bbox.xmax < {max_lon}
    AND bbox.ymin > {min_lat} AND bbox.ymax < {max_lat}""")

connectors_sql = '\nUNION ALL\n'.join(connector_unions)

sql = f"""
LOAD spatial;
LOAD httpfs;
SET s3_region='us-west-2';

CREATE TEMP TABLE events(id VARCHAR, max_mph DOUBLE, min_mph DOUBLE, lat DOUBLE, lon DOUBLE, ts VARCHAR);
INSERT INTO events VALUES
{values_sql};

CREATE TEMP TABLE connectors AS
{connectors_sql};

SELECT
  e.id,
  ROUND(e.max_mph, 1) as max_mph,
  ROUND(e.min_mph, 1) as min_mph,
  e.lat,
  e.lon,
  e.ts,
  COUNT(*) as connectors
FROM events e
JOIN connectors c
  ON c.clon BETWEEN e.lon - 0.0002 AND e.lon + 0.0002
  AND c.clat BETWEEN e.lat - 0.0002 AND e.lat + 0.0002
GROUP BY e.id, e.max_mph, e.min_mph, e.lat, e.lon, e.ts
HAVING connectors >= 3
ORDER BY connectors DESC, max_mph DESC;
"""

sql_file = tempfile.NamedTemporaryFile(mode='w', suffix='.sql', delete=False)
sql_file.write(sql)
sql_file.close()

print(f"Running DuckDB query (this may take 1-2 min for S3 parquet scan)...", file=sys.stderr)

with open(sql_file.name) as f:
    sql_content = f.read()
os.unlink(sql_file.name)

result = subprocess.run(
    ['duckdb', '-csv', '-header'],
    input=sql_content,
    capture_output=True, text=True, timeout=600
)

if result.returncode != 0:
    print(f"DuckDB error: {result.stderr}", file=sys.stderr)
    sys.exit(1)

# Parse and display results
lines = result.stdout.strip().split('\n')
if len(lines) <= 1:
    print("No intersection events found.", file=sys.stderr)
    sys.exit(0)

header = lines[0]
rows = lines[1:]
print(f"\nFound {len(rows)} events at intersections (3+ Overture connectors within 20m)\n", file=sys.stderr)

print(f"{'#':<4} {'ID':<28} {'Max→Min mph':<16} {'Connectors':<12} {'Location':<30} {'Timestamp'}")
print('-' * 120)
for i, row in enumerate(rows):
    parts = row.split(',')
    eid, max_mph, min_mph, lat, lon, ts, conns = parts
    ts_short = ts.split('T')[0] if 'T' in ts else ts
    print(f"{i+1:<4} {eid:<28} {max_mph:>5} → {min_mph:<8} {conns:>5}          {lat},{lon:<20} {ts_short}")

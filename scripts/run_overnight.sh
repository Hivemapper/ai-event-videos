#!/bin/bash
# Overnight intersection braking pipeline runner
# Runs multiple passes with increasing date ranges to find as many intersection events as possible.

set -euo pipefail
cd "$(dirname "$0")/.."

export BEEMAPS_API_KEY="NjhkYjIzZmQ1YjY5YmQ1MDY5NTJlZGU4OjYyOTA4MWYyLWRkOTEtNGYyMy1hNzEwLWYzOTJhMWQ4OTBhNg=="
OUTPUT_DIR="scripts/data"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
RESULTS_FILE="${OUTPUT_DIR}/intersection_braking_results_${TIMESTAMP}.tsv"
LOG_FILE="${OUTPUT_DIR}/pipeline_${TIMESTAMP}.log"

echo "=== Intersection Braking Pipeline - Overnight Run ===" | tee "$LOG_FILE"
echo "Started: $(date)" | tee -a "$LOG_FILE"
echo "Results: ${RESULTS_FILE}" | tee -a "$LOG_FILE"
echo "" | tee -a "$LOG_FILE"

# Run: 1000 events from last 90 days
echo "--- Pass 1: Last 90 days, 1000 events ---" | tee -a "$LOG_FILE"
python3 scripts/intersection_braking_pipeline.py \
    --days 90 \
    --max-events 1000 \
    --output "$RESULTS_FILE" \
    2>&1 | tee -a "$LOG_FILE"

echo "" | tee -a "$LOG_FILE"
echo "Completed: $(date)" | tee -a "$LOG_FILE"
echo "Results saved to: ${RESULTS_FILE}" | tee -a "$LOG_FILE"

# Count results
if [ -f "$RESULTS_FILE" ]; then
    TOTAL=$(tail -n +2 "$RESULTS_FILE" | wc -l | tr -d ' ')
    echo "Total intersection events found: ${TOTAL}" | tee -a "$LOG_FILE"
fi

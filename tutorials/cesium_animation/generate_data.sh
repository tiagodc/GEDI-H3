#!/bin/bash
# Generate data assets for CesiumJS GEDI animation
# Produces individual GEDI shots and H3 aggregated hexagons at 5 levels (3-7)
set -euo pipefail

CONDA_ENV="/gpfs/data1/vclgp/decontot/environments/gh3_dev"
DB="/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_merged"
OUTDIR="$(cd "$(dirname "$0")" && pwd)/data"
WORKERS=16
QUERY="wsci_quality_flag_l4c == 1 and rh_098_l2a > 0 and rh_098_l2a < 60"

# Activate conda environment
eval "$(conda shell.bash hook)"
conda activate "$CONDA_ENV"

echo "=== Output directory: $OUTDIR ==="

# Step 1: Individual GEDI shots over Maryland
echo ""
echo "=== Extracting individual GEDI shots over Maryland ==="
gh3_extract -d "$DB" \
  -r="-79.5,37.9,-75.0,39.7" \
  -l2a rh_098 \
  -q "$QUERY" \
  -g \
  -o "$OUTDIR/shots_maryland" \
  -N $WORKERS -v

# Step 2: H3 aggregated hexagons at 5 levels with expanding regions

echo ""
echo "=== Aggregating H3 level 7 - Maryland (~2km hexagons) ==="
gh3_aggregate -d "$DB" \
  -r="-79.5,37.9,-75.0,39.7" \
  -l2a rh_098 \
  -h3 7 -a mean \
  -q "$QUERY" \
  -o "$OUTDIR/h3_07_maryland" \
  -N $WORKERS -v

echo ""
echo "=== Aggregating H3 level 6 - Mid-Atlantic (~7km hexagons) ==="
gh3_aggregate -d "$DB" \
  -r="-82,36,-73,42" \
  -l2a rh_098 \
  -h3 6 -a mean \
  -q "$QUERY" \
  -o "$OUTDIR/h3_06_midatlantic" \
  -N $WORKERS -v

echo ""
echo "=== Aggregating H3 level 5 - Eastern US (~16km hexagons) ==="
gh3_aggregate -d "$DB" \
  -r="-90,30,-70,45" \
  -l2a rh_098 \
  -h3 5 -a mean \
  -q "$QUERY" \
  -o "$OUTDIR/h3_05_eastern" \
  -N $WORKERS -v

echo ""
echo "=== Aggregating H3 level 4 - East+Central US (~40km hexagons) ==="
gh3_aggregate -d "$DB" \
  -r="-105,25,-65,50" \
  -l2a rh_098 \
  -h3 4 -a mean \
  -q "$QUERY" \
  -o "$OUTDIR/h3_04_central" \
  -N $WORKERS -v

echo ""
echo "=== Aggregating H3 level 3 - CONUS (~110km hexagons) ==="
gh3_aggregate -d "$DB" \
  -r="-125,24,-66,50" \
  -l2a rh_098 \
  -h3 3 -a mean \
  -q "$QUERY" \
  -o "$OUTDIR/h3_03_conus" \
  -N $WORKERS -v

echo ""
echo "=== Data generation complete ==="
echo "Now run: python prepare_data.py"

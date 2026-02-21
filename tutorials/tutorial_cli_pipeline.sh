#!/bin/bash
# =============================================================================
# gedih3 CLI Tutorial: Complete GEDI Data Processing Pipeline
# =============================================================================
#
# This tutorial demonstrates the complete workflow for processing GEDI satellite
# LiDAR data using the gedih3 command-line tools. By the end, you will have:
#
# 1. Downloaded GEDI data from NASA's DAAC
# 2. Built an H3-indexed database for fast spatial queries
# 3. Extracted and filtered GEDI shots
# 4. Aggregated data to coarser resolutions
# 5. Created GeoTIFF raster maps
#
# Prerequisites:
# - conda environment with gedih3 installed (see environment.yml)
# - NASA Earthdata account credentials in ~/.netrc
# - ~10GB disk space for example data
#
# Author: gedih3 team
# =============================================================================

set -e  # Exit on error

echo "=============================================="
echo " gedih3 CLI Tutorial"
echo "=============================================="
echo ""

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Define your study area (example: small region in Amazon rainforest)
# Format: "West,South,East,North" (decimal degrees)
REGION="-51,0,-50,1"

# Temporal range
DATE_START="2020-01-01"
DATE_END="2020-03-31"

# Output directories (customize these paths)
BASE_DIR="../tmp/gedih3_tutorial"
TMP_DIR="${BASE_DIR}/tmp"             # Temporary files
SOC_DIR="${BASE_DIR}/soc_data"        # Downloaded HDF5 files
H3_DIR="${BASE_DIR}/h3_database"      # H3-indexed parquet database
EXTRACT_DIR="${BASE_DIR}/extracted"   # Extracted data
AGG_DIR="${BASE_DIR}/aggregated"      # Aggregated data
RASTER_DIR="${BASE_DIR}/rasters"      # Output rasters

# H3 resolution settings
H3_RESOLUTION=12  # Index level (~9m, matches GEDI footprint)
H3_PARTITION=3    # Partition level (~100km tiles for file organization)

# Dask settings (adjust based on your system)
N_WORKERS=3       # Number of parallel workers
MEMORY_GB=4       # Memory per worker in GB

# Skip flags (set via environment to skip long-running steps)
SKIP_DOWNLOAD=${SKIP_DOWNLOAD:-false}
SKIP_BUILD=${SKIP_BUILD:-false}

# Create directories
mkdir -p "$SOC_DIR" "$H3_DIR" "$EXTRACT_DIR" "$AGG_DIR" "$RASTER_DIR"

echo "Study area: $REGION"
echo "Date range: $DATE_START to $DATE_END"
echo "Output directory: $BASE_DIR"
echo ""

# -----------------------------------------------------------------------------
# Step 1: Download GEDI Data
# -----------------------------------------------------------------------------

echo ""
echo "=============================================="
echo "Step 1: Downloading GEDI Data"
echo "=============================================="
echo ""

if [ "$SKIP_DOWNLOAD" = "false" ]; then

    # Download L2A (heights) and L4A (biomass) products
    # Use 'default' for the standard variable set, or list specific variables

    echo "Downloading GEDI L2A and L4A data..."
    echo "This may take several minutes depending on data availability..."
    echo ""

    gh3_download \
        -r="$REGION" \
        -d0 "$DATE_START" \
        -d1 "$DATE_END" \
        -l2a default \
        -l4a agbd \
        -o "$SOC_DIR" \
        -N "$N_WORKERS" \
        -M "$MEMORY_GB" \
        -v

    echo ""
    echo "Download complete! Files saved to: $SOC_DIR"
    echo "Downloaded files:"
    find "$SOC_DIR" -name "*.h5" | head -10
    echo ""

else
    echo "Skipping download (SKIP_DOWNLOAD=true)"
    echo ""
fi

# -----------------------------------------------------------------------------
# Step 2: Build H3-Indexed Database
# -----------------------------------------------------------------------------

echo "=============================================="
echo "Step 2: Building H3-Indexed Database"
echo "=============================================="
echo ""

if [ "$SKIP_BUILD" = "false" ]; then

    echo "Building H3 database with:"
    echo "  - Index resolution: $H3_RESOLUTION (~9m hexagons)"
    echo "  - Partition level: $H3_PARTITION (~100km tiles)"
    echo ""

    # Build the database from downloaded HDF5 files
    # -i points to the SOC directory from Step 1
    # Variables must match what was downloaded (Step 1 used: -l2a default -l4a agbd)
    gh3_build \
        -r="$REGION" \
        -d0 "$DATE_START" \
        -d1 "$DATE_END" \
        -l2a default \
        -l4a agbd \
        -i "$SOC_DIR" \
        -h3r "$H3_RESOLUTION" \
        -h3p "$H3_PARTITION" \
        -t "$TMP_DIR" \
        -o "$H3_DIR" \
        -N "$N_WORKERS" \
        -M "$MEMORY_GB" \
        -vv

    echo ""
    echo "Database built! Location: $H3_DIR"
    echo ""

    # View database metadata
    echo "Database metadata:"
    cat "$H3_DIR/gedih3_build_log.json" | python -m json.tool | head -30

    echo ""
    echo "Database structure:"
    ls -la "$H3_DIR" | head -20
    echo ""

else
    echo "Skipping build (SKIP_BUILD=true)"
    echo ""
fi

# -----------------------------------------------------------------------------
# Verify H3 database exists before proceeding
# -----------------------------------------------------------------------------

if [ ! -f "$H3_DIR/gedih3_build_log.json" ]; then
    echo "H3 database not found. Run build step first."
    exit 1
fi

# -----------------------------------------------------------------------------
# Step 3: Extract Data with Filters
# -----------------------------------------------------------------------------

echo "=============================================="
echo "Step 3: Extracting Filtered Data"
echo "=============================================="
echo ""

# Extract specific variables with quality filtering
echo "Extracting high-quality GEDI shots..."

gh3_extract \
    -d "$H3_DIR" \
    -r="$REGION" \
    -l2a rh_098 rh_050 \
    -l4a agbd \
    -y \
    -o "$EXTRACT_DIR" \
    -N "$N_WORKERS" \
    -vv

echo ""
echo "Extraction complete! Files saved to: $EXTRACT_DIR"
echo "Output files:"
find "$EXTRACT_DIR" -name "*.parquet" | head -10
echo ""

# -----------------------------------------------------------------------------
# Step 4: Aggregate Data
# -----------------------------------------------------------------------------

echo "=============================================="
echo "Step 4: Aggregating Data"
echo "=============================================="
echo ""

# ----- Option A: H3 Hexagonal Aggregation -----
echo "4A: Aggregating to H3 level 6 (~36 km2 hexagons)..."

H3_AGG_DIR="${AGG_DIR}/h3_level6"
mkdir -p "$H3_AGG_DIR"

gh3_aggregate \
    -d "$H3_DIR" \
    -h3 6 \
    -l4a agbd \
    -a mean \
    -y \
    -o "$H3_AGG_DIR" \
    -N "$N_WORKERS" \
    -vv

echo "H3 aggregation complete!"
echo ""

# ----- Option B: EGI Square Pixel Aggregation -----
echo "4B: Aggregating to EGI level 6 (~1km square pixels)..."

EGI_AGG_DIR="${AGG_DIR}/egi_level6"
mkdir -p "$EGI_AGG_DIR"

gh3_aggregate \
    -d "$H3_DIR" \
    -egi 6 \
    -l4a agbd \
    -a mean \
    -y \
    -o "$EGI_AGG_DIR" \
    -N "$N_WORKERS" \
    -vv

echo "EGI aggregation complete!"
echo ""

# -----------------------------------------------------------------------------
# Step 5: Rasterize to GeoTIFF
# -----------------------------------------------------------------------------
#
# Two approaches are available for rasterization:
#
# APPROACH A: Aggregate + Rasterize in one step (gh3_aggregate --rasterize)
#   - Use when you need raster (GeoTIFF) output directly from aggregation
#   - Efficient: aggregation and rasterization in a single pass
#
# APPROACH B: Rasterize pre-aggregated data (gh3_rasterize)
#   - Use when you already have aggregated data from Step 4
#   - Useful for converting existing datasets to rasters
#   - Allows filtering/selecting specific variables from aggregated data
#
# -----------------------------------------------------------------------------

echo "=============================================="
echo "Step 5: Rasterizing to GeoTIFF"
echo "=============================================="
echo ""

# ----- Option A: Aggregate + Rasterize in one step -----
echo "5A: Aggregating to EGI level 6 with rasterization..."

EGI_RASTER_DIR="${AGG_DIR}/egi_level6_with_rasters"
mkdir -p "$EGI_RASTER_DIR"

gh3_aggregate \
    -d "$H3_DIR" \
    -egi 6 \
    -l4a agbd \
    -a mean \
    -y \
    -R \
    --compress LZW \
    -o "$EGI_RASTER_DIR" \
    -N "$N_WORKERS" \
    -vv

echo "Raster output saved to: $EGI_RASTER_DIR"
echo "  - Raster files: $EGI_RASTER_DIR/*.tif"
echo ""

# ----- Option B: Rasterize from aggregated dataset -----
echo "5B: Rasterizing pre-aggregated EGI data (tiled output)..."

RASTER_TILES="${RASTER_DIR}/tiles"
mkdir -p "$RASTER_TILES"

# Note: gh3_rasterize takes the OUTPUT from gh3_aggregate (Step 4B)
# Column names keep original names when using a single aggregation function
gh3_rasterize \
    -d "$EGI_AGG_DIR" \
    -l agbd_l4a \
    -o "$RASTER_TILES" \
    --compress LZW \
    -N "$N_WORKERS" \
    -vv

echo "Tiled rasters saved to: $RASTER_TILES"
echo ""

# ----- Option C: Merged raster from aggregated data -----
echo "5C: Creating merged raster (single file)..."

MERGED_RASTER="${RASTER_DIR}/agbd_merged.tif"

gh3_rasterize \
    -d "$EGI_AGG_DIR" \
    -l agbd_l4a \
    -m \
    -o "$MERGED_RASTER" \
    --compress LZW \
    -N "$N_WORKERS" \
    -vv

echo "Merged raster saved to: $MERGED_RASTER"
echo ""

# ----- Option D: Time-series aggregate then rasterize -----
echo "5D: Time-series aggregation then rasterization..."

# Step 1: Create time-series aggregate (generates subdirectory per window)
gh3_aggregate \
    -d "$H3_DIR" \
    -egi 6 \
    -l4a agbd \
    -a mean \
    -ti 1 -tu years \
    -t0 "$DATE_START" -t1 "$DATE_END" \
    -o "$AGG_DIR/egi_timeseries" \
    -N "$N_WORKERS" \
    -v

echo "Time-series aggregated data saved to: $AGG_DIR/egi_timeseries"
echo ""

# Step 2: gh3_rasterize auto-detects time-series subdirectories
gh3_rasterize \
    -d "$AGG_DIR/egi_timeseries" \
    -m \
    -o "$RASTER_DIR/timeseries" \
    --compress LZW \
    -N "$N_WORKERS" \
    -v

echo "Time-series rasters saved to: $RASTER_DIR/timeseries"
echo ""

# -----------------------------------------------------------------------------
# Summary
# -----------------------------------------------------------------------------

echo "=============================================="
echo " Tutorial Complete!"
echo "=============================================="
echo ""
echo "Output Summary:"
echo "  - Downloaded HDF5 files: $SOC_DIR"
echo "  - H3 database: $H3_DIR"
echo "  - Extracted data: $EXTRACT_DIR"
echo "  - Aggregated data: $AGG_DIR"
echo "  - Raster outputs: $RASTER_DIR"
echo ""
echo "Next Steps:"
echo "  1. Open rasters in QGIS or ArcGIS"
echo "  2. Use extracted parquet files in Python/R"
echo "  3. Run analysis on aggregated data"
echo ""
echo "For more options, use --help with any tool:"
echo "  gh3_download --help"
echo "  gh3_build --help"
echo "  gh3_extract --help"
echo "  gh3_aggregate --help"
echo "  gh3_rasterize --help"
echo ""

# -----------------------------------------------------------------------------
# Bonus: Read Schema of Output Files
# -----------------------------------------------------------------------------

echo "=============================================="
echo " Bonus: Inspecting Output Schemas"
echo "=============================================="
echo ""

echo "H3 Database schema:"
FIRST_PARQUET=$(find "$H3_DIR" -name "*.parquet" | head -1)
if [ -n "$FIRST_PARQUET" ]; then
    gh3_read_schema "$FIRST_PARQUET" | head -20
fi

echo ""
echo "Aggregated data schema:"
FIRST_AGG=$(find "$EGI_AGG_DIR" -name "*.parquet" | head -1)
if [ -n "$FIRST_AGG" ]; then
    gh3_read_schema "$FIRST_AGG" | head -20
fi

echo ""
echo "Tutorial completed successfully!"

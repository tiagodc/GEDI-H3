#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

"""
GEDI Vector Polygon Join Tool

Spatially join polygon attributes (e.g., ecoregion names, administrative
boundaries, land cover classes) to GEDI shot locations from an H3 database
or simplified dataset.
"""

import os
import re
import sys
import argparse


def get_cmd_args():
    """Parse command line arguments for vector polygon join tool."""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_storage_args

    p = argparse.ArgumentParser(
        description="Spatially join polygon attributes to GEDI shot locations"
    )

    # Vector input
    p.add_argument("-i", "--input", dest="input", required=True, type=str,
                   help="path to vector file (.shp, .gpkg, .geojson) or directory")
    p.add_argument("-if", "--input-format", dest="input_format", type=str, default='*',
                   help="file extension for directory globbing [default=* (all supported)]")

    # Column configuration
    p.add_argument("-c", "--columns", dest="columns", nargs='+', type=str, default=None,
                   help="polygon attribute columns to include (default: all)")
    p.add_argument("-x", "--prefix", dest="prefix", type=str, default=None,
                   help="prefix for polygon column names (avoids conflicts)")

    # Join configuration
    p.add_argument("-p", "--predicate", dest="predicate", type=str, default='within',
                   choices=['within', 'intersects'],
                   help="spatial join predicate [default=within]")

    # Data source
    p.add_argument("-d", "--database", dest="database", type=str, default=None,
                   help="path to H3 database or simplified dataset directory")

    # Output
    p.add_argument("-o", "--output", dest="output", required=True, type=str,
                   help="output directory")
    p.add_argument("-f", "--format", dest="format", type=str, default='parquet',
                   help="output format [default=parquet]")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")

    # Spatial/quality filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="additional spatial filter: vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-y", "--quality", dest="quality", action='store_true',
                   help="apply quality filtering")
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering")
    p.add_argument("-b", "--beam-type", dest="beam_type", type=str, default=None,
                   choices=["power", "coverage"],
                   help="filter by beam type: 'power' (full-power beams) or 'coverage' (coverage beams)")

    # Temporal filtering
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")

    # Output options
    p.add_argument("-g", "--geo", dest="geo", action='store_true',
                   help="include geometry in output")
    p.add_argument("--dropna", dest="dropna", action='store_true',
                   help="drop shots with no polygon match (inner join)")
    p.add_argument("--resume", dest="resume", action='store_true',
                   help="skip already-processed partitions")

    # Dask, storage, and verbosity
    add_dask_args(p)
    add_storage_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def main():
    args = get_cmd_args()

    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        import glob
        import geopandas as gpd
        from dask.distributed import Client

        import gedih3.gh3driver as gh3
        from gedih3.cliutils import (
            setup_logging, print_banner, print_success,
            configure_database_path, parse_region, parse_dask_args,
            h3_col_name, get_dataset_index_info, build_query_string,
            setup_storage
        )
        from gedih3.vecutils import (
            resolve_vector_source, get_vector_info, load_vector,
            join_polygons_to_points, _compute_join_meta, _detect_spatial_cols
        )

        # Setup
        logger = setup_logging(args, __name__)
        setup_storage(args, logger=logger)
        print_banner("GEDI Vector Polygon Join Tool", logger=logger)

        # Resolve vector source
        logger.info(f"Vector source: {args.input}")
        vector_path, file_count = resolve_vector_source(args.input, args.input_format)
        if file_count > 1:
            logger.info(f"  Found {file_count} vector files, using: {os.path.basename(vector_path)}")

        # Read vector metadata
        vec_info = get_vector_info(vector_path)
        logger.info(f"  CRS: {vec_info['crs']}")
        logger.info(f"  Bounds (WGS84): {vec_info['bounds_wgs84']}")
        logger.info(f"  Features: {vec_info['feature_count']}")
        logger.info(f"  Geometry type: {vec_info['geometry_type']}")
        logger.info(f"  Columns: {vec_info['columns']}")

        # Validate user-specified columns exist
        if args.columns:
            missing = [c for c in args.columns if c not in vec_info['columns']]
            if missing:
                from gedih3.exceptions import GediSpatialJoinError
                raise GediSpatialJoinError(
                    f"Columns not found in vector file: {missing}. "
                    f"Available: {vec_info['columns']}"
                )
            join_columns = args.columns
            logger.info(f"  Selected columns: {join_columns}")
        else:
            join_columns = vec_info['columns']

        # Get polygon dtypes for meta computation
        # Load a sample to detect dtypes (rows=0 still gives schema)
        sample_gdf = gpd.read_file(vector_path, rows=1)
        polygon_dtypes = {c: str(sample_gdf[c].dtype) for c in join_columns if c in sample_gdf.columns}

        # Join configuration
        how = 'inner' if args.dropna else 'left'
        logger.info(f"  Join predicate: {args.predicate}")
        logger.info(f"  Join type: {how}")
        if args.prefix:
            logger.info(f"  Column prefix: {args.prefix}")

        # Configure database
        configure_database_path(args, logger=logger)

        from gedih3.utils import smart_exists
        if not smart_exists(args.database):
            logger.error(f"Database directory not found: {args.database}")
            sys.exit(1)

        # Detect data source type
        from gedih3.config import BUILD_LOG_FILENAME, DATASET_META_FILENAME
        from gedih3.utils import smart_join
        build_log = smart_join(args.database, BUILD_LOG_FILENAME)
        dataset_meta = smart_join(args.database, DATASET_META_FILENAME)
        is_h3_database = smart_exists(build_log)
        is_simplified_dataset = smart_exists(dataset_meta)

        if not is_h3_database and not is_simplified_dataset:
            raise FileNotFoundError(
                f"No database metadata found in {args.database}. "
                f"Expected {BUILD_LOG_FILENAME} (H3 DB) or {DATASET_META_FILENAME} (simplified dataset)."
            )

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Build query string
        query_str = None
        if is_h3_database:
            query_str = build_query_string(args)
        else:
            query_str = args.query
        if query_str:
            logger.info(f"Query filter: {query_str}")

        # Dask
        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            # Load GEDI data based on source type
            if is_h3_database:
                # Mode 1: H3 database — ROI = polygon bounds ∩ user region
                from shapely.geometry import box as shapely_box

                vec_bounds = vec_info['bounds_wgs84']
                vec_geom = shapely_box(*vec_bounds)
                vec_gdf = gpd.GeoDataFrame(geometry=[vec_geom], crs='EPSG:4326')

                if region is not None:
                    roi = gpd.overlay(vec_gdf, region.to_crs('EPSG:4326'), how='intersection')
                    if roi.empty:
                        raise ValueError("Vector bounds do not overlap with specified region")
                else:
                    roi = vec_gdf

                logger.info("Loading GEDI data from H3 database...")
                logger.info(f"  ROI: vector bounds ∩ region")

                columns = ['geometry']
                ddf = gh3.gh3_load(
                    columns=columns,
                    region=roi,
                    query=query_str,
                    source=args.database
                )

                part_level = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=args.database)
                partition_col = h3_col_name(part_level)
                index_type = 'h3'
                index_level = part_level

            else:
                # Mode 2: Simplified dataset — load all tiles
                logger.info("Loading GEDI data from simplified dataset...")
                logger.info("  ROI: entire dataset (all tiles)")

                # Detect index type before loading to route to correct loader
                ds_info = get_dataset_index_info(args.database)
                index_type = ds_info.get('index_type', 'h3')
                index_level = ds_info.get('index_level')

                if index_type == 'egi':
                    ddf = gh3.egi_load(args.database)
                else:
                    ddf = gh3.gh3_load(args.database)

                if query_str:
                    ddf = ddf.query(query_str)

                if index_type == 'egi':
                    from gedih3.egi.config import egi_col_name
                    part_level = ds_info.get('partition_level') or ds_info.get('egi_partition_level')
                    partition_col = egi_col_name(part_level) if part_level else None
                else:
                    part_level = ds_info.get('partition_level') or ds_info.get('h3_partition_level')
                    partition_col = h3_col_name(part_level) if part_level else None

            # Fallback partition detection: scan columns if metadata didn't provide it
            if partition_col is None:
                h3_cols = sorted([c for c in ddf.columns if re.match(r'^h3_\d{2}$', c)])
                egi_cols = sorted([c for c in ddf.columns if re.match(r'^egi\d{2}$', str(c))])
                if h3_cols:
                    partition_col = h3_cols[0]
                    part_level = int(partition_col.replace('h3_', ''))
                    index_type = index_type or 'h3'
                    logger.info(f"  Detected partition column from data: {partition_col}")
                elif egi_cols:
                    partition_col = egi_cols[0]
                    part_level = int(partition_col.replace('egi', ''))
                    index_type = index_type or 'egi'
                    logger.info(f"  Detected partition column from data: {partition_col}")

            # Validate geometry
            if 'geometry' not in ddf.columns:
                raise ValueError(
                    "Input data must contain geometry column for spatial join. "
                    "For simplified datasets, re-extract with the -g flag."
                )

            logger.info(f"  Loaded {ddf.npartitions} partitions")
            if partition_col:
                logger.info(f"  Partition column: {partition_col}")

            # Resume: filter out existing partitions
            os.makedirs(args.output, exist_ok=True)
            if args.resume:
                existing = glob.glob(os.path.join(args.output, f'*.{args.format}'))
                if existing:
                    existing_ids = {os.path.splitext(os.path.basename(f))[0] for f in existing}
                    logger.info(f"  Resume: skipping {len(existing_ids)} existing partitions")
                    if partition_col and partition_col in ddf.columns:
                        ddf = ddf[~ddf[partition_col].isin(existing_ids)]
                    elif partition_col and ddf.index.name == partition_col:
                        ddf = ddf[~ddf.index.isin(existing_ids)]

            # Detect spatial columns from loaded data for schema
            spatial_cols = _detect_spatial_cols(ddf)
            if not spatial_cols and partition_col:
                spatial_cols = {partition_col: 'object'}

            # Compute join meta for Dask
            meta = _compute_join_meta(
                join_columns, polygon_dtypes, args.prefix, args.geo,
                partition_col, spatial_cols=spatial_cols
            )

            # Apply spatial join via map_partitions
            logger.info("Joining polygon attributes to GEDI shot locations...")
            joined = ddf.map_partitions(
                join_polygons_to_points,
                vector_path=vector_path,
                join_columns=join_columns,
                predicate=args.predicate,
                how=how,
                prefix=args.prefix,
                partition_col=partition_col,
                geo=args.geo,
                meta=meta
            )

            # Export
            logger.info("Exporting data...")

            meta_kwargs = {
                'query_filter': query_str,
                'vector_source': os.path.abspath(vector_path),
                'vector_crs': vec_info['crs'],
                'vector_columns': join_columns,
                'join_predicate': args.predicate,
                'join_type': how,
            }
            if args.prefix:
                meta_kwargs['column_prefix'] = args.prefix
            if index_type == 'h3' and part_level is not None:
                meta_kwargs['h3_partition_level'] = part_level
            elif index_type == 'egi' and part_level is not None:
                meta_kwargs['egi_partition_level'] = part_level
                if index_level is not None:
                    meta_kwargs['egi_index_level'] = index_level

            gh3.gh3_export(
                joined, output=args.output, fmt=args.format, merge=args.merge,
                show_progress=not getattr(args, 'quiet', False),
                drop_internal=False,
                source_database=args.database, tool='gh3_from_polygon',
                **meta_kwargs
            )

            print_success(f"Vector polygon join complete → {args.output}", logger=logger)


if __name__ == '__main__':
    main()

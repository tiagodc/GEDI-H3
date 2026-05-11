#! python

# Copyright (C) 2025, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
GEDI H3/EGI Data Aggregation Tool

Aggregate GEDI shots from H3-indexed parquet database to coarser spatial
resolutions. Supports H3 hexagonal aggregation or EGI (EASE Grid Index)
square pixel aggregation for GEDI L4B compatibility.
"""

import os
import sys
import argparse

TIME_UNITS = ['years', 'months', 'weeks', 'days']

def get_cmd_args():
    """Parse command line arguments for GEDI data aggregation"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args, add_storage_args, parse_egi_levels

    p = argparse.ArgumentParser(
        description="Aggregate GEDI shots to H3 hexagons or EGI square pixels",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Database/output configuration
    p.add_argument("-d", "--database", dest="database", type=str, default=None,
                   help="path to H3 database or simplified dataset directory")
    p.add_argument("-o", "--output", dest="output", required=True, type=str,
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", type=str, default='parquet',
                   help="output format [default=parquet]")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")

    # Rasterization option
    p.add_argument("-R", "--rasterize", dest="rasterize", action='store_true',
                   help="also export data as GeoTIFF rasters after aggregation")
    p.add_argument("--compress", dest="compress", type=str, default='LZW',
                   choices=['LZW', 'ZSTD', 'DEFLATE', 'PACKBITS', 'NONE'],
                   help="GeoTIFF compression [default=LZW]")

    # Aggregation options
    p.add_argument("-h3", "--h3-level", dest="h3_level", type=int, default=None,
                   help="aggregate to H3 level [0-15]")
    p.add_argument("-egi", "--egi", dest="egi", type=parse_egi_levels, default=None,
                   nargs='?', const=(6, 12),
                   help="EGI aggregation: bare flag defaults to 6:12, or 'level[:partition]' e.g., '6' or '6:12'")
    p.add_argument("-a", "--aggregate", dest="aggregate", type=str, default="mean",
                   help="aggregation spec: function name, list, column dict, or file path.\n"
                        "  Inline: 'mean', \"['mean','std','count']\",\n"
                        "          \"{'agbd_l4a':['mean','count'], 'rh_098_l2a':'mean'}\"\n"
                        "  File:   agg.json (dict/list), agg.txt (one function per line)\n"
                        "  [default=mean]")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")
    p.add_argument("-ti", "--time-interval", dest="time_interval", type=int, default=0,
                   help="generate time-series outputs at interval")
    p.add_argument("-tu", "--time-units", dest="time_units", type=str, default='years',
                   choices=TIME_UNITS, help="time interval units [default=years]")

    # Variable selection
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to aggregate (space-separated, file path, or wildcards like 'agbd_*_l4a')")
    add_product_args(p, include_detail_level=False)

    # Filtering
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering")
    p.add_argument("-y", "--quality", dest="quality", action='store_true',
                   help="apply quality filtering")
    p.add_argument("-b", "--beam-type", dest="beam_type", type=str, default=None,
                   choices=["power", "coverage"],
                   help="filter by beam type: 'power' (full-power beams) or 'coverage' (coverage beams)")

    # Dask, storage, and verbosity
    add_dask_args(p)
    add_storage_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def _aggregate_data(ddf, *, use_egi, is_database, args, agg, agg_is_dict,
                    egi_agg_level, egi_partition_level, source_info,
                    columns, region, query_str, logger):
    """
    Run the EGI or H3 aggregation pipeline on loaded data.

    For EGI + database source, uses direct loading (no shuffle).
    For EGI + dataset source or H3, aggregates the provided ddf.

    Returns (aggdf, part_col, export_func).
    """
    import gedih3.gh3driver as gh3
    from gedih3.cliutils import get_numeric_columns, h3_col_name, filter_data_columns

    # Determine which columns to aggregate: only user-requested data variables,
    # NOT query-only columns that were loaded just for filtering.
    if agg_is_dict:
        agg_columns = list(agg.keys())
    else:
        # columns from collect_columns = user vars + geometry + datetime
        # filter_data_columns strips internal cols (h3_XX, egiXX, etc.) and geometry
        user_data_cols = [c for c in filter_data_columns(columns) if c != 'datetime']
        if user_data_cols:
            agg_columns = user_data_cols
        elif ddf is not None:
            agg_columns = get_numeric_columns(ddf)
        else:
            agg_columns = None  # let the function auto-detect

    if use_egi:
        from gedih3 import egi

        if is_database:
            # Direct loading into EGI partitions (no shuffle), then aggregate
            logger.info("Loading data into EGI partitions (no shuffle)...")
            ddf = gh3.egi_load(
                columns=columns,
                region=region,
                query=query_str,
                source=args.database,
                index_level=egi_agg_level,
                partition_level=egi_partition_level
            )
            logger.info(f"  Loaded {ddf.npartitions} EGI tiles")

        # Unified aggregation (detects EGI-indexed input automatically)
        logger.info("Aggregating to EGI...")
        aggdf = gh3.egi_aggregate(
            ddf,
            target_level=egi_agg_level,
            agg=agg,
            columns=agg_columns,
            add_geometry=True,
            partition_level=egi_partition_level,
            repartition=not args.merge
        )

        logger.info(f"  Result: {aggdf.npartitions} EGI partitions")
        part_col = egi.egi_col_name(egi_partition_level if not args.merge else egi_agg_level)
        export_func = gh3.egi_export_part
    else:
        # H3 (hexagon) aggregation
        h3_part_level = source_info.get('partition_level') or args.h3_level
        part_col = h3_col_name(h3_part_level)

        logger.info("Aggregating data...")
        logger.info(f"  Target: H3 level {args.h3_level}")

        aggdf = gh3.gh3_aggregate(
            ddf,
            target_res=args.h3_level,
            agg=agg,
            columns=agg_columns,
            add_geometry=True,
            repartition=not args.merge,
            partition_level=h3_part_level
        )
        export_func = gh3.gh3_export_part

    return aggdf, part_col, export_func


def _export_data(aggdf, *, export_func, part_col, output_dir, args,
                 use_egi, egi_agg_level, egi_partition_level, agg,
                 logger, h3_part_level=None):
    """
    Drop internal columns, persist, and export aggregated data.

    Handles raster, merge, and simplified flat file export modes.
    """
    import glob as globmod
    import pandas as pd

    import gedih3.gh3driver as gh3
    from gedih3.cliutils import is_internal_column, print_success

    # Drop spatial indexing columns from output (keep only data + geometry)
    # Preserve the partition column (part_col) — needed for output file naming
    drop_cols = [c for c in aggdf.columns if is_internal_column(c) and c != part_col]
    if drop_cols:
        logger.info(f"  Dropping internal columns: {drop_cols}")
        aggdf = aggdf.drop(columns=drop_cols)

    # No persist() here — let gh3_export() handle compute/persist internally.
    # Without SetIndex calls in the aggregation graph, partitions can be
    # processed and exported on-the-fly without holding all data in memory.

    # Export
    if args.rasterize:
        # Raster-only export (no vector files)
        logger.info("Rasterizing aggregated data to GeoTIFF...")

        from gedih3 import raster

        if use_egi:
            from gedih3 import egi
            rasterize_func = egi.rasterize_partition
        else:
            rasterize_func = raster.rasterize_h3_partition

        if args.merge:
            # Merged raster output (single .tif file)
            merged_path = output_dir if output_dir.endswith('.tif') else f"{output_dir}.tif"
            os.makedirs(os.path.dirname(os.path.abspath(merged_path)), exist_ok=True)

            raster.merge_and_export_rasters(
                aggdf, merged_path, rasterize_func,
                columns=None,
                compress=args.compress,
                show_progress=not args.quiet
            )
            print_success(f"Merged raster exported to {merged_path}", logger=logger)

        else:
            # Tiled raster output (directory of .tif files)
            os.makedirs(output_dir, exist_ok=True)

            if hasattr(aggdf, 'compute'):
                raster.rasterize_and_export_partitions(
                    aggdf, output_dir, rasterize_func,
                    columns=None,
                    compress=args.compress,
                    show_progress=not args.quiet
                )
            else:
                xras = rasterize_func(aggdf, columns=None)
                if isinstance(xras, pd.Series) and len(xras) > 0:
                    for i, tile_xras in enumerate(xras):
                        if hasattr(tile_xras, 'data_vars') and len(tile_xras.data_vars) > 0:
                            raster.export_raster(tile_xras, os.path.join(output_dir, f'tile_{i}.tif'), compress=args.compress)
                elif hasattr(xras, 'data_vars') and len(xras.data_vars) > 0:
                    raster.export_raster(xras, os.path.join(output_dir, 'merged.tif'), compress=args.compress)

            raster_files = globmod.glob(f"{output_dir}/*.tif")
            if len(raster_files) > 1:
                vrt_path = os.path.join(output_dir, 'mosaic.vrt')
                raster.build_vrt(raster_files, vrt_path)
                logger.info(f"  VRT mosaic: {vrt_path}")
            print_success(f"{len(raster_files)} raster files exported to {output_dir}", logger=logger)

    else:
        # Simplified flat file export (merge or tiled) via gh3_export()
        logger.info("Exporting data...")

        meta_kwargs = {'aggregation': str(agg)}
        if use_egi:
            meta_kwargs['egi_aggregation_level'] = egi_agg_level
            meta_kwargs['egi_partition_level'] = egi_partition_level

        gh3.gh3_export(
            aggdf, output=output_dir, fmt=args.format, merge=args.merge,
            show_progress=not args.quiet, drop_internal=False,
            source_database=args.database, tool='gh3_aggregate',
            h3_partition_level=h3_part_level,
            **meta_kwargs
        )
        print_success(f"Data exported to {output_dir}", logger=logger)


def main():
    args = get_cmd_args()

    # Validate aggregation level arguments
    if args.h3_level is None and args.egi is None:
        print("ERROR: Must specify either -h3/--h3-level or -egi for aggregation target")
        sys.exit(1)
    if args.h3_level is not None and args.egi is not None:
        print("ERROR: Cannot specify both -h3/--h3-level and -egi. Choose one.")
        sys.exit(1)

    use_egi = args.egi is not None
    if use_egi:
        egi_agg_level, egi_partition_level = args.egi
    else:
        egi_agg_level, egi_partition_level = None, None

    # Validate time-series arguments
    time_series = args.time_interval > 0
    if time_series:
        if not args.time_start or not args.time_end:
            from gedih3.exceptions import GediValidationError
            raise GediValidationError(
                "Time-series mode (-ti) requires both -t0 (start date) and -t1 (end date)."
            )

    # Import cli_exception_handler early for wrapping the main logic
    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        from dask.distributed import Client, progress

        from gedih3.cliutils import (collect_columns, build_query_string, parse_region,
                                     parse_dask_args, parse_file_format, setup_logging,
                                     print_banner, print_success, configure_database_path,
                                     load_data_from_source,
                                     get_dataset_index_info, parse_aggregation,
                                     setup_storage)

        # Setup logging and print banner
        logger = setup_logging(args, __name__)
        setup_storage(args, logger=logger)
        title = "GEDI EGI Data Aggregation Tool" if use_egi else "GEDI H3 Data Aggregation Tool"
        print_banner(title, logger=logger)

        # Configure database path
        configure_database_path(args, logger=logger)

        # Verify database exists
        from gedih3.utils import smart_exists
        if not smart_exists(args.database):
            logger.error(f"Database directory not found: {args.database}")
            sys.exit(1)

        # Parse format
        args.format = parse_file_format(args)

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Detect source type (H3 database vs simplified dataset)
        source_info = get_dataset_index_info(args.database)
        is_database = source_info['source_type'] == 'h3_database'
        if is_database:
            available_columns = source_info.get('h3_columns')
        else:
            available_columns = source_info.get('columns')
            logger.info(f"  Source: {source_info['source_type']} ({source_info.get('index_type', 'unknown')} index)")

            # Validate: cannot do H3 aggregation on EGI-indexed data
            if not use_egi and source_info.get('index_type') == 'egi':
                from gedih3.exceptions import GediValidationError
                raise GediValidationError(
                    "Cannot aggregate EGI-indexed dataset to H3 resolution. "
                    "Use -egi to aggregate to an EGI level instead, or provide an H3-indexed source."
                )

        # Collect columns
        logger.info("Collecting variables...")
        columns = collect_columns(args, available_columns=available_columns)

        # Time-series mode needs datetime column for per-window filtering
        if time_series and 'datetime' not in columns:
            columns.append('datetime')

        # EGI needs geometry for coordinate access
        if use_egi and 'geometry' not in columns:
            columns.append('geometry')

        if len(columns) == 0:
            raise ValueError("No variables selected. Use -l/--list or product options.")
        logger.info(f"  Total variables: {len(columns)}")

        # Build query
        query_str = build_query_string(args, available_columns=available_columns)
        if query_str:
            logger.info(f"Query filter: {query_str}")

        # Parse aggregation spec (string, list, or dict)
        agg = parse_aggregation(args.aggregate)
        # When agg is a dict, columns are implicit in the dict keys
        agg_is_dict = isinstance(agg, dict)
        if agg_is_dict:
            logger.info(f"Aggregation: column-specific {agg}")
        elif isinstance(agg, list):
            logger.info(f"Aggregation: {agg}")

        dask_kwargs = parse_dask_args(args)

        # Shared kwargs for _aggregate_data calls
        agg_kwargs = dict(
            use_egi=use_egi, is_database=is_database, args=args,
            agg=agg, agg_is_dict=agg_is_dict,
            egi_agg_level=egi_agg_level, egi_partition_level=egi_partition_level,
            source_info=source_info, columns=columns, region=region,
            logger=logger,
        )

        # Shared kwargs for _export_data calls
        h3_part_level = source_info.get('partition_level') if not use_egi else None
        export_kwargs = dict(
            args=args, use_egi=use_egi,
            egi_agg_level=egi_agg_level, egi_partition_level=egi_partition_level,
            agg=agg, logger=logger, h3_part_level=h3_part_level,
        )

        with Client(**dask_kwargs) as client:
            logger.info(f"Dask dashboard: {client.dashboard_link}")

            if use_egi:
                from gedih3 import egi
                target_res = egi.get_resolution(egi_agg_level)
                partition_res = egi.get_resolution(egi_partition_level)
                logger.info(f"  Target: EGI level {egi_agg_level} (~{target_res:.0f}m pixels)")
                if egi_partition_level != egi_agg_level:
                    logger.info(f"  Partition: EGI level {egi_partition_level} (~{partition_res:.0f}m)")

            if time_series:
                # ── Time-series mode ──
                from gedih3.raster.timeseries import generate_time_windows, build_temporal_query

                logger.info(f"Time-series mode: {args.time_interval} {args.time_units} "
                            f"from {args.time_start} to {args.time_end}")

                # Load data lazily (for non-database EGI or any H3 path).
                # Each time window re-reads from parquet with temporal filtering,
                # avoiding holding the full dataset in memory.
                if not (use_egi and is_database):
                    logger.info("Loading data...")
                    ddf = load_data_from_source(args.database, columns, region, query_str, logger)
                    logger.info(f"  Loaded {ddf.npartitions} partitions")
                else:
                    ddf = None  # EGI+database path loads per-window via egi_load + egi_aggregate

                # Materialize the generator so the progress bar can show a
                # total — window counts are typically small (dozens at most).
                from gedih3.cliutils import progress_iter
                windows = list(generate_time_windows(
                    args.time_start, args.time_end,
                    args.time_interval, args.time_units
                ))

                window_count = 0
                with progress_iter(windows, desc="Time-series windows",
                                   args=args, unit="win") as bar:
                    for t0, t1, suffix in bar:
                        logger.info(f"── Window: {suffix} ──")
                        if args.rasterize and args.merge:
                            window_dir = os.path.join(args.output, f"{suffix}.tif")
                        else:
                            window_dir = os.path.join(args.output, suffix)

                        if use_egi and is_database:
                            # Direct EGI loading per window: append temporal filter to query
                            time_query = build_temporal_query(
                                start_date=t0.strftime('%Y-%m-%d'),
                                end_date=t1.strftime('%Y-%m-%d')
                            )
                            window_query = f"({query_str}) & ({time_query})" if query_str else time_query

                            aggdf, part_col, export_func = _aggregate_data(
                                None, query_str=window_query, **agg_kwargs
                            )
                        else:
                            # Filter persisted data for this time window
                            time_query = build_temporal_query(
                                start_date=t0.strftime('%Y-%m-%d'),
                                end_date=t1.strftime('%Y-%m-%d')
                            )
                            window_ddf = ddf.query(time_query)

                            # Check if window has data (cheap: just check partition count stays > 0)
                            # The actual emptiness check happens during aggregation/export
                            aggdf, part_col, export_func = _aggregate_data(
                                window_ddf, query_str=None, **agg_kwargs
                            )

                        _export_data(
                            aggdf, export_func=export_func, part_col=part_col,
                            output_dir=window_dir, **export_kwargs
                        )
                        window_count += 1

                print_success(f"Time-series complete: {window_count} windows exported to {args.output}", logger=logger)

            else:
                # ── Single aggregation (original behavior) ──
                if not (use_egi and is_database):
                    logger.info("Loading data...")
                    ddf = load_data_from_source(args.database, columns, region, query_str, logger)
                    logger.info(f"  Loaded {ddf.npartitions} partitions")
                else:
                    ddf = None

                aggdf, part_col, export_func = _aggregate_data(
                    ddf, query_str=query_str, **agg_kwargs
                )

                _export_data(
                    aggdf, export_func=export_func, part_col=part_col,
                    output_dir=args.output, **export_kwargs
                )


if __name__ == '__main__':
    main()

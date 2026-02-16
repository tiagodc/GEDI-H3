#! python
DEBUG = False

"""
GEDI Raster Sampling Tool

Sample raster pixel values at GEDI shot locations from an H3 database or
simplified dataset. Supports single files, VRT mosaics, and tile directories.
Optional moving-window statistics (sum, mean, median, mode).

Author: Tiago de Conto
Package: gedih3
"""

import os
import re
import sys
import argparse


def get_cmd_args():
    """Parse command line arguments for raster sampling tool."""
    from gedih3.cliutils import add_dask_args, add_verbosity_args

    p = argparse.ArgumentParser(
        description="Sample raster pixel values at GEDI shot locations"
    )

    # Image input
    p.add_argument("-i", "--image", dest="image", required=not DEBUG, type=str,
                   help="path to raster file, VRT, or tile directory")
    p.add_argument("-if", "--image-format", dest="image_format", type=str, default='tif',
                   help="file extension for tile directory globbing [default=tif]")

    # Band configuration
    p.add_argument("-b", "--band-names", dest="band_names", nargs='+', type=str, default=None,
                   help="custom band names for output columns")
    p.add_argument("-B", "--bands", dest="bands", nargs='+', type=int, default=None,
                   help="select specific bands by 0-based index (default: all bands)")

    # Data source
    p.add_argument("-d", "--database", dest="database", type=str, default=None,
                   help="path to H3 database or simplified dataset directory")

    # Output
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str,
                   help="output directory")
    p.add_argument("-f", "--format", dest="format", type=str, default='parquet',
                   help="output format [default=parquet]")
    p.add_argument("-m", "--merge", dest="merge", action='store_true',
                   help="merge all partitions into single file")

    # Window operations
    p.add_argument("-w", "--window", dest="window", nargs='+', type=str, default=None,
                   help="window ops in 3-digit format: band(0-9) size(1-9,odd) op(0=sum,1=mean,2=median,3=mode)")

    # Spatial/quality filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="additional spatial filter: vector file, bbox 'W,S,E,N', or ISO3 code")
    p.add_argument("-y", "--quality", dest="quality", action='store_true',
                   help="apply quality filtering")
    p.add_argument("-q", "--query", dest="query", type=str, default=None,
                   help="pandas query string for filtering")

    # Raster handling
    p.add_argument("-g", "--geo", dest="geo", action='store_true',
                   help="include geometry in output")
    p.add_argument("-l", "--fillna", dest="fillna", type=float, default=None,
                   help="fill raster NaN/NoData with this value")
    p.add_argument("--dropna", dest="dropna", action='store_true',
                   help="drop rows where all band columns are NaN")
    p.add_argument("--resume", dest="resume", action='store_true',
                   help="skip already-processed partitions")

    # Dask and verbosity
    add_dask_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def main():
    if DEBUG:
        sys.path.insert(0, os.path.abspath('./src/'))

    args = get_cmd_args()

    if DEBUG:
        args.image = '/gpfs/data1/vclgp/decontot/data/raster/nasa_dem/'
        args.database = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database_world_merged/'
        args.output = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/from_img_test'
        args.region = '/gpfs/data1/vclgp/decontot/data/vector/other_boundaries/RO_UF_2022.shp'
        args.cores = 10
        args.port = 9994

    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        import glob
        import geopandas as gpd
        from dask.distributed import Client

        import gedih3.gh3driver as gh3
        from gedih3.cliutils import (
            setup_logging, print_banner, print_success,
            configure_database_path, parse_region, parse_dask_args,
            h3_col_name, get_dataset_index_info, build_query_string
        )
        from gedih3.imgutils import (
            resolve_raster_source, get_raster_info, parse_window_specs,
            sample_raster_at_points, _compute_sampling_meta
        )

        # Setup
        logger = setup_logging(args, __name__)
        print_banner("GEDI Raster Sampling Tool", logger=logger)

        # Resolve raster source
        logger.info(f"Image source: {args.image}")
        raster_path, is_vrt, tile_count = resolve_raster_source(args.image, args.image_format, odir=args.output)
        if tile_count > 1:
            logger.info(f"  Built VRT mosaic from {tile_count} tiles")
        raster_info = get_raster_info(raster_path)
        logger.info(f"  CRS: {raster_info['crs']}")
        logger.info(f"  Bounds (WGS84): {raster_info['bounds_wgs84']}")
        logger.info(f"  Bands: {raster_info['band_count']} {raster_info['band_names']}")
        logger.info(f"  Resolution: {raster_info['resolution']}")

        # Parse window specs (before band selection so we can auto-include window bands)
        window_ops = parse_window_specs(args.window)
        if window_ops:
            # Validate window band indices against full raster band count
            for wop in window_ops:
                if wop['band'] >= raster_info['band_count']:
                    raise ValueError(
                        f"Window spec references band {wop['band']} but raster "
                        f"only has {raster_info['band_count']} bands (0-indexed)"
                    )

        # Band selection (-B flag)
        all_band_names = raster_info['band_names']  # Full raster band names
        band_indices = args.bands  # None means all bands
        if band_indices is not None:
            # Validate band indices against raster
            for bi in band_indices:
                if bi < 0 or bi >= raster_info['band_count']:
                    raise ValueError(
                        f"Band index {bi} out of range for raster with "
                        f"{raster_info['band_count']} bands (0-indexed)"
                    )

            # Auto-include bands referenced by window ops
            if window_ops:
                window_band_set = {wop['band'] for wop in window_ops}
                extra_bands = window_band_set - set(band_indices)
                if extra_bands:
                    logger.info(f"  Auto-including bands {sorted(extra_bands)} for window operations")
                    # Note: extra bands are loaded for window ops but not output as columns

            # Band names for selected bands
            if args.band_names:
                band_names = args.band_names
                if len(band_names) != len(band_indices):
                    raise ValueError(
                        f"Number of band names ({len(band_names)}) doesn't match "
                        f"number of selected bands ({len(band_indices)})"
                    )
            else:
                band_names = [all_band_names[i] for i in band_indices]

            logger.info(f"  Selected bands: {band_indices} → {band_names}")
        else:
            # All bands
            if args.band_names:
                band_names = args.band_names
                if len(band_names) != raster_info['band_count']:
                    raise ValueError(
                        f"Number of band names ({len(band_names)}) doesn't match "
                        f"raster band count ({raster_info['band_count']})"
                    )
            else:
                band_names = all_band_names
            all_band_names = band_names  # No selection, all_band_names == band_names

        if window_ops:
            logger.info(f"Window operations: {[w['name'] for w in window_ops]}")

        # Configure database
        configure_database_path(args, logger=logger)

        if not os.path.exists(args.database):
            logger.error(f"Database directory not found: {args.database}")
            sys.exit(1)

        # Detect data source type
        build_log = os.path.join(args.database, "gedih3_build_log.json")
        dataset_meta = os.path.join(args.database, "gedih3_dataset.json")
        is_h3_database = os.path.exists(build_log)
        is_simplified_dataset = os.path.exists(dataset_meta)

        if not is_h3_database and not is_simplified_dataset:
            raise FileNotFoundError(
                f"No database metadata found in {args.database}. "
                "Expected gedih3_build_log.json (H3 DB) or gedih3_dataset.json (simplified dataset)."
            )

        # Parse region
        region = None
        if args.region:
            logger.info(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Build query string
        # For H3 database, use build_query_string with quality flags
        # For simplified dataset, use raw query only
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
                # Mode 1: H3 database — ROI = image bounds ∩ user region
                from shapely.geometry import box as shapely_box

                img_bounds = raster_info['bounds_wgs84']
                img_geom = shapely_box(*img_bounds)
                img_gdf = gpd.GeoDataFrame(geometry=[img_geom], crs='EPSG:4326')

                if region is not None:
                    # Intersect image bounds with user region
                    roi = gpd.overlay(img_gdf, region.to_crs('EPSG:4326'), how='intersection')
                    if roi.empty:
                        raise ValueError("Image bounds do not overlap with specified region")
                else:
                    roi = img_gdf

                logger.info("Loading GEDI data from H3 database...")
                logger.info(f"  ROI: image bounds ∩ region")

                # Always load geometry for coordinate extraction
                columns = ['geometry']
                ddf = gh3.gh3_load(
                    columns=columns,
                    region=roi,
                    query=query_str,
                    gh3_dir=args.database
                )

                # Get partition column
                part_level = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=args.database)
                partition_col = h3_col_name(part_level)
                index_type = 'h3'
                index_level = part_level

            else:
                # Mode 2: Simplified dataset — load all tiles
                logger.info("Loading GEDI data from simplified dataset...")
                logger.info("  ROI: entire dataset (all tiles)")
                ddf = gh3.gh3_load_dataset_lazy(args.database)
                if query_str:
                    ddf = ddf.query(query_str)

                # Get partition column from dataset metadata
                ds_info = get_dataset_index_info(args.database)
                index_type = ds_info.get('index_type', 'h3')
                index_level = ds_info.get('index_level')

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
                    "Input data must contain geometry column for coordinate extraction. "
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
                    # Filter out partitions whose ID is already exported
                    if partition_col and partition_col in ddf.columns:
                        ddf = ddf[~ddf[partition_col].isin(existing_ids)]
                    elif partition_col and ddf.index.name == partition_col:
                        ddf = ddf[~ddf.index.isin(existing_ids)]

            # Detect spatial columns from loaded data for schema
            from gedih3.imgutils import _detect_spatial_cols
            spatial_cols = _detect_spatial_cols(ddf)
            if not spatial_cols and partition_col:
                spatial_cols = {partition_col: 'object'}

            # Compute sampling meta for Dask
            meta = _compute_sampling_meta(band_names, window_ops, args.geo, partition_col,
                                          spatial_cols=spatial_cols,
                                          all_band_names=all_band_names if band_indices else None)

            # Apply sampling via map_partitions
            logger.info("Sampling raster at GEDI shot locations...")
            sampled = ddf.map_partitions(
                sample_raster_at_points,
                raster_path=raster_path,
                band_names=band_names,
                window_ops=window_ops,
                fillna=args.fillna,
                dropna=args.dropna,
                geo=args.geo,
                partition_col=partition_col,
                band_indices=band_indices,
                all_band_names=all_band_names if band_indices else None,
                meta=meta
            )

            # Export
            logger.info("Exporting data...")

            # Build image-specific metadata
            from gedih3.imgutils import _resolve_window_col_name
            window_col_names = [_resolve_window_col_name(w, all_band_names) for w in (window_ops or [])]

            meta_kwargs = {
                'query_filter': query_str,
                'image_source': args.image if args.image.startswith(('http://', 'https://', 's3://', '/vsicurl/', '/vsis3/')) else os.path.abspath(args.image),
                'raster_crs': str(raster_info['crs']),
                'raster_resolution': list(raster_info['resolution']),
                'raster_bands': band_names,
            }
            if band_indices is not None:
                meta_kwargs['raster_band_indices'] = band_indices
            if index_type == 'h3' and part_level is not None:
                meta_kwargs['h3_partition_level'] = part_level
            elif index_type == 'egi' and part_level is not None:
                meta_kwargs['egi_partition_level'] = part_level
                if index_level is not None:
                    meta_kwargs['egi_index_level'] = index_level
            if window_ops:
                meta_kwargs['window_operations'] = window_col_names

            gh3.gh3_export(
                sampled, output=args.output, fmt=args.format, merge=args.merge,
                show_progress=not getattr(args, 'quiet', False),
                drop_internal=False,
                source_database=args.database, tool='gh3_from_img',
                **meta_kwargs
            )

            print_success(f"Raster sampling complete → {args.output}", logger=logger)


if __name__ == '__main__':
    main()

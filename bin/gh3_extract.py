#! python
"""
GEDI H3 Data Extraction Tool

Extract and filter GEDI shots from H3-indexed parquet database with spatial,
temporal, and quality filters. Supports multiple products (L2A, L2B, L4A, L4C)
and flexible output formats.

Author: Tiago de Conto
Package: gedih3
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from typing import Optional, List, Dict

def get_cmd_args():
    """Parse command line arguments for GEDI data extraction"""
    p = argparse.ArgumentParser(
        description="Extract and filter spatially indexed GEDI shots from H3 parquet database"
    )

    # Output configuration
    p.add_argument("-o", "--output", dest="output", required=True, type=str,
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", required=False, type=str,
                   default='parquet', help="output file format [default = parquet]")
    p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true',
                   help="merge all partitions and export to single file")

    # Spatial filtering
    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="path to vector (.shp, .gpkg, .kml, etc.) or raster (.tif) file with ROI, "
                        "or bounding box as 'W,S,E,N', or ISO3 country code")

    # Variable selection by product
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None,
                   help="GEDI L2A variables to export [space-separated list]")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None,
                   help="GEDI L2B variables to export [space-separated list]")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None,
                   help="GEDI L4A variables to export [space-separated list]")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None,
                   help="GEDI L4C variables to export [space-separated list]")

    # Temporal filtering
    p.add_argument("-t0", "--time_start", dest="time_start", type=str, default=None,
                   help="start date to filter shots [YYYY-MM-DD]")
    p.add_argument("-t1", "--time_end", dest="time_end", type=str, default=None,
                   help="end date to filter shots [YYYY-MM-DD]")
    p.add_argument("-t", "--time", dest="add_datetime", required=False, action='store_true',
                   help="add human-readable 'datetime' column to output")

    # Data filtering
    p.add_argument("-q", "--query", dest="query", required=False, type=str, default=None,
                   help="pandas query string for filtering - e.g. 'quality_flag_l2a == 1 & agbd_l4a > 50'")
    p.add_argument("-y", "--quality", dest="quality", required=False, action='store_true',
                   help="apply quality filtering (quality_flag_l2a == 1)")

    # Geometry options
    p.add_argument("-g", "--geo", dest="geo", required=False, action='store_true',
                   help="export as georeferenced points (requires lat/lon columns)")
    p.add_argument("-c", "--clip", dest="clip", required=False, action='store_true',
                   help="clip geometries to region boundary (requires -g and -r)")

    # Aggregation
    p.add_argument("-a", "--aggregate", dest="aggregate", required=False, type=int, default=None,
                   help="aggregate to target H3 resolution level [0-15, lower = coarser]")
    p.add_argument("-A", "--agg_func", dest="agg_func", required=False, type=str, default='mean',
                   help="aggregation function: 'mean', 'sum', 'median', 'count', 'std' [default = mean]")

    # Database configuration
    p.add_argument("-d", "--database", dest="database", required=False, type=str, default=None,
                   help="path to H3 database directory [default from config or environment]")

    # Computation settings
    n_default = max(1, os.cpu_count() // 4)
    p.add_argument("-n", "--cores", dest="cores", required=False, type=int, default=n_default,
                   help=f"number of CPU cores to use [default = {n_default}]")
    p.add_argument("-s", "--threads", dest="threads", required=False, type=int, default=1,
                   help="number of threads per CPU core [default = 1]")
    p.add_argument("-M", "--memory", dest="memory", required=False, type=int, default=4,
                   help="memory limit per worker in GB [default = 4]")
    p.add_argument("-p", "--port", dest="port", required=False, type=int, default=8787,
                   help="port for Dask dashboard [default = 8787]")

    # Debug options
    p.add_argument("-D", "--debug", dest="debug", required=False, action='store_true',
                   help="enable debug logging")
    p.add_argument("-v", "--verbose", dest="verbose", required=False, action='store_true',
                   help="verbose output")

    return p.parse_args()


def setup_logging(debug: bool = False, verbose: bool = False):
    """Configure logging level"""
    if debug:
        level = logging.DEBUG
    elif verbose:
        level = logging.INFO
    else:
        level = logging.WARNING

    logging.basicConfig(
        format='%(asctime)s - %(levelname)s - %(message)s',
        level=level,
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def parse_region(region_str: Optional[str]):
    """Parse region argument into GeoDataFrame or bbox"""
    if region_str is None:
        return None

    import geopandas as gpd
    from shapely.geometry import box
    from gedih3.utils import parse_spatial

    # Try as bounding box: "W,S,E,N"
    if ',' in region_str:
        try:
            coords = [float(x.strip()) for x in region_str.split(',')]
            if len(coords) == 4:
                return gpd.GeoDataFrame(geometry=[box(*coords)], crs=4326)
        except ValueError:
            pass

    # Try as file path
    if os.path.isfile(region_str):
        return parse_spatial(region_str)

    # Try as ISO3 country code
    region_upper = region_str.upper()
    if len(region_upper) == 3 and region_upper.isalpha():
        logging.warning(f"ISO3 country code '{region_upper}' provided, but database query not available. "
                       "Please provide a vector file instead.")
        return None

    raise ValueError(f"Could not parse region: {region_str}")


def collect_columns(args, available_cols: List[str]) -> tuple[Optional[List[str]], Dict[str, List[str]]]:
    """
    Collect all requested variables from command line arguments and validate against available columns.
    Returns: (column_list, product_map)
    """
    columns = []
    product_map = {'L2A': [], 'L2B': [], 'L4A': [], 'L4C': []}

    # Map command line args to products
    prod_args = {
        'L2A': args.l2a,
        'L2B': args.l2b,
        'L4A': args.l4a,
        'L4C': args.l4c
    }

    for prod, vars_list in prod_args.items():
        if vars_list is None:
            continue

        # Add product suffix if not present
        prod_suffix = f"_{prod.lower()}"
        for var in vars_list:
            if not var.endswith(prod_suffix):
                col_name = f"{var}{prod_suffix}"
            else:
                col_name = var

            # Validate column exists
            if col_name not in available_cols and not col_name.startswith('h3_'):
                logging.warning(f"Column '{col_name}' not found in database. Skipping.")
                continue

            columns.append(col_name)
            product_map[prod].append(col_name)

    # Add essential columns if geometry requested
    if args.geo:
        essential = ['lon_lowestmode_l2a', 'lat_lowestmode_l2a']
        for col in essential:
            if col not in columns and col in available_cols:
                columns.append(col)

    # Add time column if needed
    if args.add_datetime or args.time_start or args.time_end:
        time_col = 'delta_time_l2a'
        if time_col not in columns and time_col in available_cols:
            columns.append(time_col)

    return columns if columns else None, product_map


def build_query_string(args) -> Optional[str]:
    """Build pandas query string from arguments"""
    queries = []

    # Quality filter
    if args.quality:
        queries.append("quality_flag_l2a == 1")

    # Temporal filters
    if args.time_start or args.time_end:
        from gedih3.config import GEDI_START_DATE
        time_col = 'delta_time_l2a'

        if args.time_start:
            t0 = datetime.strptime(args.time_start, '%Y-%m-%d')
            t0_delta = (t0 - GEDI_START_DATE).total_seconds()
            queries.append(f"{time_col} >= {t0_delta}")

        if args.time_end:
            t1 = datetime.strptime(args.time_end, '%Y-%m-%d')
            t1_delta = (t1 - GEDI_START_DATE).total_seconds()
            queries.append(f"{time_col} <= {t1_delta}")

    # Custom query
    if args.query:
        queries.append(f"({args.query})")

    return " & ".join(queries) if queries else None


def add_datetime_column(ddf, time_col: str = 'delta_time_l2a'):
    """Add datetime column from delta_time"""
    from gedih3.config import GEDI_START_DATE
    import dask.dataframe as dd

    if time_col not in ddf.columns:
        logging.warning(f"Time column '{time_col}' not found. Cannot add datetime.")
        return ddf

    gedi_start_ts = GEDI_START_DATE.timestamp()
    ddf['datetime'] = dd.to_datetime(ddf[time_col] + gedi_start_ts, unit='s')
    return ddf


def create_geometries(ddf, x_col: str = 'lon_lowestmode_l2a', y_col: str = 'lat_lowestmode_l2a'):
    """Convert DataFrame to GeoDataFrame with point geometries"""
    import dask_geopandas as dkg

    if x_col not in ddf.columns or y_col not in ddf.columns:
        logging.error(f"Coordinate columns '{x_col}' and '{y_col}' not found in data.")
        return ddf

    ddf = ddf.assign(geometry=dkg.points_from_xy(ddf, x=x_col, y=y_col, crs=4326))
    ddf = dkg.from_dask_dataframe(ddf)
    return ddf


def export_data(ddf, output_path: str, format: str = 'parquet', merge: bool = False):
    """Export data to specified format"""
    import geopandas as gpd

    # Ensure output directory exists
    output_dir = os.path.dirname(output_path) if not os.path.isdir(output_path) else output_path
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    is_geodf = hasattr(ddf._meta, 'geometry')

    if merge:
        # Compute entire dataset into memory
        logging.info("Computing full dataset (this may take time)...")
        result = ddf.compute()

        # Ensure single file output
        if os.path.isdir(output_path):
            output_path = os.path.join(output_path, f'gedi_extract.{format}')
        elif not output_path.endswith(f'.{format}'):
            output_path += f'.{format}'

        # Export based on format
        if format in ['parquet', 'parq', 'pq']:
            result.to_parquet(output_path, engine='pyarrow', compression='zstd')
        elif format in ['gpkg', 'geopackage'] and is_geodf:
            result.to_file(output_path, driver='GPKG')
        elif format in ['shp', 'shapefile'] and is_geodf:
            result.to_file(output_path, driver='ESRI Shapefile')
        elif format == 'csv':
            result.to_csv(output_path, index=False)
        elif format == 'geojson' and is_geodf:
            result.to_file(output_path, driver='GeoJSON')
        else:
            logging.error(f"Unsupported format '{format}' for data type")
            return None

        logging.info(f"Exported {len(result):,} records to {output_path}")
        return output_path

    else:
        # Export partitioned data
        if not os.path.isdir(output_path):
            output_path = os.path.dirname(output_path) or '.'

        os.makedirs(output_path, exist_ok=True)

        if format in ['parquet', 'parq', 'pq']:
            # Dask native parquet export
            out_path = os.path.join(output_path, 'gedi_extract_*.parquet')
            ddf.to_parquet(out_path, engine='pyarrow', compression='zstd')
            logging.info(f"Exported partitioned data to {output_path}")
        else:
            # For other formats, we need to compute partitions
            logging.warning(f"Format '{format}' requires computing all partitions. Consider using --merge flag.")
            partitions = ddf.to_delayed()

            for i, part in enumerate(partitions):
                result = part.compute()
                if len(result) == 0:
                    continue

                part_file = os.path.join(output_path, f'gedi_extract_part{i:04d}.{format}')

                if format == 'csv':
                    result.to_csv(part_file, index=False)
                elif is_geodf:
                    if format in ['gpkg', 'geopackage']:
                        result.to_file(part_file, driver='GPKG')
                    elif format in ['shp', 'shapefile']:
                        result.to_file(part_file, driver='ESRI Shapefile')
                    elif format == 'geojson':
                        result.to_file(part_file, driver='GeoJSON')

            logging.info(f"Exported {len(partitions)} partition files to {output_path}")

        return output_path


def main():
    """Main execution function"""
    args = get_cmd_args()
    setup_logging(debug=args.debug, verbose=args.verbose)

    # Print header
    print("\n" + "="*70)
    print(" GEDI H3 Data Extraction Tool".center(70))
    print(" gedih3 v0.0.1".center(70))
    print("="*70 + "\n")

    try:
        # Import heavy dependencies after arg parsing
        import gedih3 as gh3
        import dask
        from dask.distributed import Client, progress

        # Configure database path
        if args.database:
            gh3.gh3driver.gh3_set_db_path(args.database)
            db_path = args.database
        else:
            db_path = gh3.config.GH3_DEFAULT_H3_DIR

        print(f"Database: {db_path}")

        # Verify database exists
        if not os.path.exists(db_path):
            print(f"\nERROR: Database directory not found: {db_path}")
            print("Please specify a valid database path with -d/--database")
            sys.exit(1)

        # Read metadata
        print("Reading database metadata...")
        available_cols = gh3.gh3driver.gh3_read_meta('h3_columns', gh3_root_dir=db_path)
        h3_res = gh3.gh3driver.gh3_read_meta('h3_resolution_level', gh3_root_dir=db_path)
        h3_part_level = gh3.gh3driver.gh3_read_meta('h3_partition_level', gh3_root_dir=db_path)

        if available_cols is None:
            print("\nERROR: Could not read database metadata. Invalid database?")
            sys.exit(1)

        print(f"  H3 resolution: {h3_res}")
        print(f"  Partition level: {h3_part_level}")
        print(f"  Available columns: {len(available_cols)}")

        # Setup Dask client
        print(f"\nInitializing Dask cluster...")
        print(f"  Workers: {args.cores}")
        print(f"  Threads per worker: {args.threads}")
        print(f"  Memory per worker: {args.memory}GB")

        client = Client(
            n_workers=args.cores,
            threads_per_worker=args.threads,
            memory_limit=f'{args.memory}GB',
            dashboard_address=f':{args.port}',
            silence_logs=logging.ERROR if not args.debug else logging.DEBUG
        )
        print(f"  Dashboard: {client.dashboard_link}")

        # Parse region
        region = None
        if args.region:
            print(f"\nParsing region: {args.region}")
            region = parse_region(args.region)
            if region is not None:
                print(f"  Region parsed successfully: {region.total_bounds}")

        # Collect columns
        print("\nCollecting variables...")
        columns, product_map = collect_columns(args, available_cols)

        if columns:
            print(f"  Total variables: {len(columns)}")
            for prod, cols in product_map.items():
                if cols:
                    print(f"    {prod}: {len(cols)} variables")
        else:
            print("  No specific variables requested - loading all columns")

        # Build query
        query_str = build_query_string(args)
        if query_str:
            print(f"\nQuery filter: {query_str}")

        # Load data
        print("\nLoading data from H3 database...")
        ddf = gh3.gh3driver.gh3_load(
            columns=columns,
            region=region,
            query=query_str,
            gh3_dir=db_path
        )

        print(f"  Loaded {ddf.npartitions} partitions")

        # Add datetime if requested
        if args.add_datetime:
            print("Adding datetime column...")
            ddf = add_datetime_column(ddf)

        # Create geometries if requested
        if args.geo:
            print("Creating point geometries...")
            ddf = create_geometries(ddf)

            # Clip to region if requested
            if args.clip and region is not None:
                print("Clipping geometries to region boundary...")
                ddf = ddf.clip(region.to_crs(4326))

        # Aggregate if requested
        if args.aggregate is not None:
            print(f"\nAggregating to H3 resolution {args.aggregate} using '{args.agg_func}'...")

            # Build aggregation dict
            agg_dict = {}
            if columns:
                # Aggregate numeric columns only
                for col in columns:
                    if not col.startswith('h3_') and col != 'shot_number':
                        agg_dict[col] = args.agg_func

            if not agg_dict:
                agg_dict = args.agg_func  # Use function for all numeric columns

            ddf = gh3.gh3driver.gh3_aggregate(
                ddf,
                target_res=args.aggregate,
                agg=agg_dict,
                add_geometry=args.geo
            )

            print(f"  Aggregated to H3_{args.aggregate:02d}")

        # Export
        print("\nExporting data...")
        output_file = export_data(ddf, args.output, format=args.format, merge=args.merge)

        if output_file:
            print(f"\n{'='*70}")
            print(f" SUCCESS: Data exported to {output_file}")
            print(f"{'='*70}\n")

        # Cleanup
        client.close()

    except KeyboardInterrupt:
        print("\n\nOperation cancelled by user.")
        sys.exit(130)

    except Exception as e:
        print(f"\n\nERROR: {type(e).__name__}: {e}")
        if args.debug:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()

#! python
DEBUG=False

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

TIME_UNITS = ['years', 'months', 'weeks', 'days']

def get_cmd_args():
    """Parse command line arguments for GEDI data extraction"""
    p = argparse.ArgumentParser(
        description="Extract and filter spatially indexed GEDI shots from H3 parquet database",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Database configuration
    p.add_argument("-d", "--database", dest="database", required=False, type=str, default=None,
                   help="path to H3 database directory [default from config or environment]")

    # Output configuration
    p.add_argument("-o", "--output", dest="output", required=not DEBUG, type=str, 
                   help="output directory or file path")
    p.add_argument("-f", "--format", dest="format", required=False, type=str, default='parquet', 
                   help="output file format [default = parquet]")
    p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true', 
                   help="merge all partitions and export to single file")

    # Aggregation options
    p.add_argument("-h3", "--h3-level", dest="h3_level", type=int, required=not DEBUG,
                   help="aggregate to target H3 resolution level [0-15, lower = coarser]")
    p.add_argument("-a", "--aggregate", dest="aggregate", required=not DEBUG, type=str, default="mean",
        help=(
            "aggregation spec for pandas GroupBy.agg. Accepts any valid pandas aggregator, e.g.\n"
            "  - 'mean' (single function)\n"
            "  - ['mean', 'std'] (list of functions)\n"
            "  - {'var1': 'mean', 'var2': ['min', 'max']} (per-column mapping)\n"
            "  - callable(s) when used programmatically\n"
            "[default = mean]"
        ),
    )

    # Spatial filtering
    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None,
                   help="path to vector (.shp, .gpkg, .kml, etc.) or raster (.tif) file with ROI, or bounding box as 'W,S,E,N', or ISO3 country code")

    # Variable selection by product
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="flat list (space-separated) or file path of variables to export from the GEDI H3 database (need to specify product suffix, e.g. '_l2a')")

    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None,
                   help="GEDI L1B variables to export [space-separated list]")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None,
                   help="GEDI L2A variables to export [space-separated list]")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None,
                   help="GEDI L2B variables to export [space-separated list]")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None,
                   help="GEDI L4A variables to export [space-separated list]")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None,
                   help="GEDI L4C variables to export [space-separated list]")

    # Temporal filtering
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date to filter shots [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date to filter shots [YYYY-MM-DD]")
    p.add_argument("-ti", "--time_interval", dest="time_interval", type=int, default=0, required=False, 
                   help="generate outputs in the given time interval")
    p.add_argument("-tu", "--time_units", dest="time_units", type=str, default='years', required=False, 
                   choices=TIME_UNITS, help="time interval units")

    # Data filtering
    p.add_argument("-q", "--query", dest="query", required=False, type=str, default=None,
                   help="pandas query string for filtering - e.g. 'quality_flag_l2a == 1 & agbd_l4a > 50'")
    p.add_argument("-y", "--quality", dest="quality", required=False, action='store_true',
                   help="apply quality filtering (quality_flag_l2a == 1)")

    # Computation settings
    p.add_argument("-s", "--dask-scheduler", dest="dask_scheduler", required=False, type=str, default=None,
                   help=f"dask scheduler address (overrides local cluster settings) [default = None]")

    from gedih3.utils import get_system_resources
    cpus, ram, storage = get_system_resources()
    n = max(1, cpus // 4)
    m = int(max(1, ram / n))
    
    p.add_argument("-N", "--cores", dest="cores", required=False, type=int, default=n,
                   help=f"number of CPU cores to use [default = {n}]")
    p.add_argument("-T", "--threads", dest="threads", required=False, type=int, default=1,
                   help="number of threads per CPU core [default = 1]")
    p.add_argument("-M", "--memory", dest="memory", required=False, type=int, default=m,
                   help=f"memory limit per worker in GB [default = {m}]")
    p.add_argument("-P", "--port", dest="port", required=False, type=int, default=8787,
                   help="port for Dask dashboard [default = 8787]")

    # Debug options
    p.add_argument("-D", "--debug", dest="debug", required=False, action='store_true',
                   help="enable debug logging")
    p.add_argument("-Q", "--quiet", dest="quiet", required=False, action='store_true',
                   help="quiet output")

    return p.parse_args()

def main():
    if DEBUG:
        sys.path.insert(0, os.path.abspath('./src/'))

    args = get_cmd_args()
    
    if DEBUG:
        args.output = '/gpfs/data1/vclgp/decontot/repos/gedih3/tmp/tmp/maryland'
        args.region = '/gpfs/data1/vclgp/decontot/data/vector/other_boundaries/md.shp'
        args.l2a = ['rh_098']
        args.l2b = ['pai_z_000']
        args.l4a = ['agbd']
        args.l4c = ['wsci']
        args.add_datetime = True
        args.quality = True
        args.database = '/gpfs/data1/vclgp/data/iss_gedi/h3_mock/database'
        args.cores = 20
        args.port = 9994
    
    try:    
        import glob
        import pandas as pd
        from dask.distributed import Client, progress
        
        from gedih3 import __version__ as _gh3_version
        import gedih3.gh3driver as gh3
        from gedih3.cliutils import collect_columns, build_query_string, parse_region, parse_dask_args
        from gedih3.config import GH3_DEFAULT_H3_DIR
        
        if not args.quiet:
            print("\n" + "="*70)
            print(" GEDI H3 Data Aggregation Tool".center(70))

            print(f" gedih3 v{_gh3_version}".center(70))
            print("="*70 + "\n")

        # Configure database path
        if args.database:
            gh3.gh3_set_db_path(args.database)
        else:
            args.database = GH3_DEFAULT_H3_DIR

        if not args.quiet:
            print(f"Database: {args.database}")

        # Verify database exists
        if not os.path.exists(args.database):
            print(f"ERROR: Database directory not found: {args.database}")
            print("Please specify a valid database path with -d/--database")
            sys.exit(1)

        # Parse region
        region = None
        if args.region:
            if not args.quiet:
                print(f"Parsing region: {args.region}")
            region = parse_region(args.region)

        # Collect columns
        if not args.quiet:
            print("Collecting variables...")
        columns = collect_columns(args)

        if len(columns) > 0:
            if not args.quiet:
                print(f"  Total variables: {len(columns)}")
        else:
            raise ValueError("No variables selected for extraction. Please specify variables with -l/--list or product-specific options.")

        # Build query
        query_str = build_query_string(args)
        if query_str:
            if not args.quiet:
                print(f"Query filter: {query_str}")

        dask_kwargs = parse_dask_args(args)

        with Client(**dask_kwargs) as client:
            if not args.quiet:
                print("Dask dashboard available at:", client.dashboard_link)

            # Load data
            if not args.quiet:
                print("Loading data from H3 dataset...")

            ddf = gh3.gh3_load(
                columns=columns,
                region=region,
                query=query_str,
                gh3_dir=args.database
            )

            if not args.quiet:
                print(f"  Loaded {ddf.npartitions} partitions")

            if not args.quiet:
                print("Aggregating data...")

            numeric_columns = [col for col in ddf.columns if ddf[col].dtype.kind in 'biufc']
            aggdf = gh3.gh3_aggregate(ddf, 
                                      target_res=args.h3_level,
                                      agg=args.aggregate,
                                      columns=numeric_columns,
                                      add_geometry=True,
                                      repartition=not args.merge
                                      )

            # Export
            if not args.quiet:
                print("Exporting data...")

            part = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=args.database)
            h3_col = f'h3_{part:02d}'
            
            print("... testing exporter")
            write_task = aggdf.groupby(h3_col, observed=True).apply(gh3.gh3_export_part,
                                        odir=args.output,
                                        fmt=args.format,
                                        include_groups=False,
                                        meta=pd.Series(dtype=str),
                                        )
            
            # write_task = aggdf.to_parquet(args.output,
            #                             write_metadata_file=True,
            #                             write_index=True,
            #                             overwrite=True,
            #                             compression='zstd',
            #                             partition_on=[h3_col],
            #                             compute=False
            #                             )
            
            write_task = write_task.persist()
            progress(write_task)
            
            ofiles = glob.glob(f"{args.output}/**/*.parquet", recursive=True)
            
            if len(ofiles) == 0:
                raise RuntimeError("No output files were created.")
            
            if not args.quiet:
                print(f"\n{'='*70}")
                print(f" SUCCESS: Data exported to {args.output}")
                print(f"{'='*70}\n")

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
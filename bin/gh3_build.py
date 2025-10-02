#! python
DEBUG=True

def getCmdArgs():
    p = argparse.ArgumentParser(description = "Download GEDI data from NASA's SOC")    
   
    p.add_argument("-s", "--spatial", dest="spatial", required=False, type=str, default=None, help="path to vector (.shp, .gpkg, .kml etc.) file with region of interest")
    p.add_argument("-b", "--box", dest="box", required=False, type=int, default=None, nargs=4, help="region of interest extent (in degrees) to intersect data (xmin ymin xmax ymax)")
    p.add_argument("-d0", "--date-start", dest="date_start", required=False, type=str, default=None, help="start search date in YYYY-MM-DD format")
    p.add_argument("-d1", "--date-end", dest="date_end", required=False, type=str, default=None, help="end search date in YYYY-MM-DD format")    
    
    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None, required=False, help="GEDI L1B variables to download")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to download")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None, required=False, help="GEDI L2B variables to download")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None, required=False, help="GEDI L4A variables to download")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to download")

    p.add_argument("-S", "--skip-download", dest="skip_download", action='store_true', help="skip downloading and build from local SOC database")
    p.add_argument("-r", "--resume", dest="resume", action='store_true', help="resume interrupted downloads")    
    p.add_argument("-u", "--update", dest="update", action='store_true', help="update existing SOC files")    
    p.add_argument("-o", "--outdir", dest="outdir", required=False, type=str, default=None, help="output directory for downloaded files (bypass GH3 default path)")
        
    p.add_argument("-D", "--dask-scheduler", dest="dask_scheduler", type=str, default=None, required=False, help="existing dask scheduler address, e.g. tcp://localhost:8786")

    n = max(1, os.cpu_count() // 2)
    p.add_argument("-N", "--n-cpus", dest="n_cpus", required=False, type=int, default=n, help=f"number of cpu cores to use [default = {n}]")
    p.add_argument("-T", "--threads", dest="threads", required=False, type=int, default=1, help="number of threads per cpu [default = 1]")
    p.add_argument("-R", "--ram", dest="ram", required=False, type=int, default=None, help="maximum RAM usage per cpu - in Giga Bytes")
    p.add_argument("-P", "--port", dest="port", required=False, type=int, default=None, help="port where to open dask dashboard")
    
    cmdargs = p.parse_args()
    return cmdargs

if __name__ == "__main__":
    import argparse, os
    args = getCmdArgs()
    
    if DEBUG:
        args.box = [-50.5,0.5,-50,1]
        args.date_start = '2020-01-01'
        args.date_end = '2020-07-01'
        # args.l1b = ['minimal']
        args.l2a = ['minimal']
        args.l2b = ['minimal']
        args.l4a = ['minimal'] 
        args.l4c = ['*']
        args.n_cpus = os.cpu_count() // 2
        args.port = 9997
        args.skip_download = True
        # args.dask_scheduler = 'tcp://localhost:8786'
        import sys
        sys.path.insert(0, os.path.abspath('./src/'))

    if args.outdir is not None:
        os.environ['GH3_DEFAULT_DOWNLOAD_DIR'] = os.path.abspath(args.outdir)
        from gedih3.config import configure_environment
        configure_environment()
        print("Overriding GH3 default output directory - new path is", os.environ['GH3_DEFAULT_DOWNLOAD_DIR'])

    import warnings
    from gedih3.utils import parse_gedi_args, parse_dask_args
    from gedih3.gh3builder import gh3_build_all
    from gedih3.logger import H3BuildLogger
    from dask.distributed import Client
    
    product_vars = parse_gedi_args(args)
    if len(product_vars) == 0:
        raise ValueError("No GEDI product selected for download - please select at least one of --l1b, --l2a, --l2b, --l4a, --l4c")    
    
    spatial = args.spatial if args.spatial is not None else args.box
    if spatial is None:
        warnings.warn("No spatial filter provided - downloading global data", UserWarning)
    
    if args.date_start or args.date_end:
        temporal = (args.date_start, args.date_end)
    else:
        temporal = None
        warnings.warn("No temporal filter provided - downloading all data", UserWarning)
    
    build_logger = H3BuildLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        resume=args.resume,
        update=args.update,
        db_type='h3' if args.skip_download else 'both'
    )

    dask_kwargs = parse_dask_args(args)
    with Client(**dask_kwargs) as client:
        print("Dask client available at", client.dashboard_link)

        h3_files = gh3_build_all(
            product_vars=build_logger.product_vars,
            spatial=build_logger.spatial,
            temporal=build_logger.temporal,
            direct_access=False,
            skip_download=args.skip_download,
            resume=args.resume,
            update=args.update,
            dask_client=client,
            build_logger=build_logger
        )
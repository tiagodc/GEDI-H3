#! python
DEBUG=True

def getCmdArgs():
    p = argparse.ArgumentParser(description = "Download GEDI data from NASA's SOC")    
   
    p.add_argument("-s", "--spatial", dest="spatial", required=False, type=str, default=None, help="path to vector (.shp, .gpkg, .kml etc.) file with region of interest")
    p.add_argument("-b", "--box", dest="box", required=False, type=int, default=None, nargs=4, help="region of interest extent (in degrees) to intersect data (xmin ymin xmax ymax)")
    p.add_argument("-d0", "--date-start", dest="date_start", required=False, type=str, default=None, help="start search date in YYYY-MM-DD format")
    p.add_argument("-d1", "--date-end", dest="date_end", required=False, type=str, default=None, help="end search date in YYYY-MM-DD format")    
    
    p.add_argument("-hr", "--h3-resolution", dest="h3_resolution", required=False, type=int, default=12, help="H3 level for data indexing [0-15]")    
    p.add_argument("-hp", "--h3-partition", dest="h3_partition", required=False, type=int, default=3, help="H3 level for file partitioning [0-15]")

    p.add_argument("-l1b", "--l1b", dest="l1b", nargs='+', type=str, default=None, required=False, help="GEDI L1B variables to download")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to download")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None, required=False, help="GEDI L2B variables to download")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None, required=False, help="GEDI L4A variables to download")
    p.add_argument("-l4c", "--l4c", dest="l4c", nargs='+', type=str, default=None, required=False, help="GEDI L4C variables to download")

    p.add_argument("-v", "--version", dest="version", required=False, type=int, default=None, help="GEDI data version to download [default = latest version]")
    p.add_argument("-S", "--skip-download", dest="skip_download", action='store_true', help="skip downloading and build from local SOC database")
    p.add_argument("-o", "--outdir", dest="outdir", required=False, type=str, default=None, help="output directory for downloaded files (bypass GH3 default path)")
    p.add_argument("-r", "--resume", dest="resume", action='store_true', help="validate downloaded files and redownload missing or corrupted files")
        
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
        args.box = [-51,0,-50,1]
        # args.date_start = '2020-01-01'
        # args.date_end = '2020-07-01'
        # args.l1b = ['minimal']
        args.l2a = ['minimal']
        # args.l2b = ['minimal']
        args.l4a = ['minimal']
        args.l4c = ['minimal']
        args.n_cpus = 24
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
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_SOC_DIR
    from gedih3.utils import parse_gedi_args, parse_dask_args
    from gedih3.gh3builder import build_h3db
    from gedih3.logger import H3BuildLogger
    from dask.distributed import Client    
    
    if args.outdir is None:
        args.outdir = GH3_DEFAULT_H3_DIR
    os.makedirs(args.outdir, exist_ok=True)
    
    product_vars = parse_gedi_args(args)    
    spatial = args.spatial if args.spatial is not None else args.box
    
    # temporal = None
    # if args.date_start or args.date_end:
    #     temporal = (args.date_start, args.date_end)
    
    h3_logger = H3BuildLogger(
        product_vars=product_vars,
        spatial=spatial,
        res=args.h3_resolution,
        part=args.h3_partition,
        version=args.version,
        dir=args.outdir,
    )
    
    if not h3_logger.product_vars and not h3_logger.updating:
        raise ValueError("No GEDI product selected for download - please select at least one of --l1b, --l2a, --l2b, --l4a, --l4c")
    if h3_logger.get_spatial() is None:
        warnings.warn("No spatial filter provided - downloading global data", UserWarning)

    if h3_logger.updating:
        print("Build log exists, checking for updates.")

        if add_spatial := (h3_logger.new_spatial is not None):
            print("Spatial filter updated.")
        if add_vars := (h3_logger.new_product_vars is not None):
            print("Product variables updated.")            

    print(f"Building GEDI H3 database at {args.outdir}")
    h3_logger.save_log('PARTITIONING')    

    dask_kwargs = parse_dask_args(args)
    with Client(**dask_kwargs) as client:
        print("Dask dashboard available at:", client.dashboard_link)
        try:
            h3_files = build_h3db(
                product_vars=h3_logger.get_product_vars(),
                spatial=h3_logger.get_spatial(),
                res=h3_logger.res,
                part=h3_logger.part,
                soc_source=GH3_DEFAULT_SOC_DIR,
                h3_dir=args.outdir,
                skip_granules=h3_logger.get_finished_granules(),
            )
            
            h3_logger.set_post_build_info()
            h3_logger.save_log('COMPLETED')
        
        except Exception as e:
            h3_logger.save_log('FAILED')
            raise e
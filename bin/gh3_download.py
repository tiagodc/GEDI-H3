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

    # p.add_argument("-l3", "--l3", dest="l3", action='store_true', help="Download GEDI L3")
    # p.add_argument("-l4cf", "--l4c-fusion", dest="l4c_fusion", action='store_true', help="Download GEDI L4C Fusion")
    # p.add_argument("-l4d", "--l4d", dest="l4d", action='store_true', help="Download GEDI L4D")
        
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
        # args.box = [-51,0,-50,1]
        # args.date_start = '2019-01-01'
        args.date_end = '2025-04-01'
        # args.l1b = ['minimal']
        # args.l2a = ['minimal']
        # args.l2b = ['minimal']
        # args.l4a = ['minimal'] 
        # args.l4c = ['*']
        args.n_cpus = 32
        args.port = 9997
        # args.dask_scheduler = 'tcp://localhost:8786'
        import sys
        sys.path.insert(0, os.path.abspath('./src/'))

    import warnings    
    from gedih3.config import GH3_DEFAULT_SOC_DIR
    from gedih3.utils import parse_gedi_args, parse_dask_args
    from gedih3.gh3builder import download_soc
    from gedih3.logger import SOCDownloadLogger
    from dask.distributed import Client

    if args.outdir is None:
        args.outdir = GH3_DEFAULT_SOC_DIR
    os.makedirs(args.outdir, exist_ok=True)
    
    product_vars = parse_gedi_args(args)        
    spatial = args.spatial if args.spatial is not None else args.box    
    temporal = None
    if args.date_start or args.date_end:
        temporal = (args.date_start, args.date_end)
    
    soc_logger = SOCDownloadLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        dir=args.outdir
    )

    if not soc_logger.product_vars and not soc_logger.updating:
        raise ValueError("No GEDI product selected for download - please select at least one of --l1b, --l2a, --l2b, --l4a, --l4c")
    if soc_logger.get_spatial() is None:
        warnings.warn("No spatial filter provided - downloading global data", UserWarning)
    if soc_logger.get_temporal() is None:
        warnings.warn("No temporal filter provided - downloading data from all available dates", UserWarning)

    if soc_logger.updating:
        print("Download log exists, resuming downloads.")

        if soc_logger.new_spatial is not None:
            print("Spatial filter updated.")
        if soc_logger.new_temporal is not None:
            print("Temporal filter updated.")
        if soc_logger.new_product_vars is not None:
            print("Product variables updated.")

    print(f"Downloading GEDI data to {args.outdir}")
    soc_logger.save_log('DOWNLOADING')

    dask_kwargs = parse_dask_args(args)
    with Client(**dask_kwargs) as client:
        try:            
            soc_files = download_soc(
                product_vars=soc_logger.get_product_vars(),
                spatial=soc_logger.get_spatial(),
                temporal=soc_logger.get_temporal(),
                direct_access=False,
                update=True,
                odir=args.outdir
            )
            
            soc_logger.save_log('COMPLETED')
        
        except Exception as e:
            soc_logger.save_log('FAILED')
            raise e
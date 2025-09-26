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

def gh3_build_main(product_vars, spatial=None, temporal=None, res=12, part=3, direct_access=False, dask_client=None, skip_download=False, resume=False, update=False, db_type='both'):
    product_vars = gedi_vars_expand(product_vars)

    if isinstance(spatial, str):
        spatial = read_vector_file(spatial)

    build_logger = H3BuildLogger(product_vars, res=res, part=part, spatial=spatial, temporal=temporal, resume=resume, update=update, db_type=db_type)
    build_logger.save_log()

    soc_source = None
    if skip_download:
        validation_report = validate_soc_files(build_logger.prod_vars, soc_dir=GH3_DEFAULT_SOC_DIR)
        if not validation_report["can_skip"]:
            build_logger.set_status('FAILED')
            build_logger.save_log()
            raise ValueError(validation_report.get('error_msg', "SOC files validation failed."))
    else:
        soc_files = download_soc(build_logger.prod_vars, spatial=build_logger.spatial, temporal=build_logger.temporal, direct_access=direct_access, resume=resume, update=update, dask_client=dask_client, build_logger=build_logger)
        if direct_access:
            soc_source = soc_files

    # Only build H3 database if needed
    h3_products = {}
    if build_logger.db_type in ('h3', 'both'):
        try:
            for k,val in build_logger.prod_vars.items():
                build_logger.set_current_level(k)
                build_logger.save_log()

                print(f"Building H3 database for GEDI {k.upper()}")
                h3_files = build_h3db_from_soc(gedi_prod_level=k, h3_vars=val, res=build_logger.res, part=build_logger.part, spatial=build_logger.spatial, build_logger=build_logger, soc_source=soc_source)
                h3_products[k] = h3_files
        except Exception as e:
            build_logger.set_status('FAILED')
            build_logger.save_log()
            raise e

        build_logger.set_current_level(None)

    # Set final completion status
    build_logger.set_status('COMPLETED')
    build_logger.save_log()
    return h3_products

if __name__ == "__main__":
    import argparse, os
    args = getCmdArgs()
    
    if DEBUG:
        args.box = [-50.5,0.5,-50,1]
        args.date_start = '2020-01-01'
        args.date_end = '2020-07-01'
        args.l1b = ['minimal']
        args.l2a = ['minimal']
        args.l2b = ['minimal']
        args.l4a = ['minimal'] 
        args.l4c = ['*']
        args.n_cpus = 10
        args.port = 9997
        # args.dask_scheduler = 'tcp://localhost:8786'
        import sys
        sys.path.insert(0, '/gpfs/data1/vclgp/decontot/repos/gedih3/src')

    if args.outdir is not None:
        os.environ['GH3_DEFAULT_DOWNLOAD_DIR'] = os.path.abspath(args.outdir)
        from gedih3.config import configure_environment
        configure_environment()
        print("Overriding GH3 default output directory - new path is", os.environ['GH3_DEFAULT_DOWNLOAD_DIR'])

    import warnings
    from gedih3.utils import parse_gedi_args, parse_dask_args
    from gedih3.gh3builder import download_soc
    from gedih3.logger import H3BuildLogger
    from gedih3.config import GH3_DEFAULT_DOWNLOAD_DIR
    from dask.distributed import Client
    
    prod_vars = parse_gedi_args(args)
    if len(prod_vars) == 0:
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
        prod_vars=prod_vars,
        spatial=spatial,
        temporal=temporal,
        resume=args.resume,
        update=args.update,
        db_type='soc'
    )

    dask_kwargs = parse_dask_args(args)
    with Client(**dask_kwargs) as client:
        print("Dask client available at", client.dashboard_link)

        soc_files = download_soc(
            product_vars=prod_vars,
            spatial=spatial,
            temporal=temporal,
            direct_access=False,
            resume=args.resume,
            update=args.update,
            dask_client=client,
            build_logger=build_logger
        )
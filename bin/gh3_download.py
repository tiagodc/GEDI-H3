#! python
import argparse, os

def getCmdArgs():
    p = argparse.ArgumentParser(description = "Filter and export spatially indexed GEDI shots from multiple products using the h3 (hexagons) or egi (pixels) system")
    
    p.add_argument("-o", "--output", dest="output", required=True, type=str, help="output directory or file path")
    p.add_argument("-r", "--region", dest="region", required=False, type=str, default=None, help="path to vector (.shp, .gpkg, .kml etc.) or raster (.tif) file with region of interest to extract shots from OR iso3 country code - if not provided, all land surface data will be queried")
    p.add_argument("-l2a", "--l2a", dest="l2a", nargs='+', type=str, default=None, required=False, help="GEDI L2A variables to export")
    p.add_argument("-l2b", "--l2b", dest="l2b", nargs='+', type=str, default=None, required=False, help="GEDI L2B variables to export")
    p.add_argument("-l4a", "--l4a", dest="l4a", nargs='+', type=str, default=None, required=False, help="GEDI L4A variables to export")
    p.add_argument("-a", "--anci", dest="anci", type=str, default=None, required=False, help="quoted dictionary of ancillary variables to export - e.g. \"{'glad_forest_loss':['loss','lossyear']}\"")
    p.add_argument("-rh", "--rh", dest="rh", nargs='+', type=int, default=None, required=False, help="RH metrics to extract from the selected algorithm setting for each shot [space separated list of percentiles or -1 for all rh metrics]")
    p.add_argument("-t0", "--time_start", dest="time_start", type=str, default=None, required=False, help="start date to filter shots [YYYY-MM-DD]")
    p.add_argument("-t1", "--time_end", dest="time_end", type=str, default=None, required=False, help="end date to filter shots [YYYY-MM-DD]")
    p.add_argument("-q", "--query", dest="query", required=False, type=str, default=None, help="single string with custom filters upon listed variables - use python pandas.DataFrame.query notation")    
    p.add_argument("-y", "--quality", dest="quality", required=False, action='store_true', help="apply latest quality filter recipe")
    p.add_argument("-e", "--egi", dest="egi", required=False, action='store_true', help="export shots with EGI spatial index (exact pixels) instead of H3 (approximate hexagons) - much slower!")
    p.add_argument("-m", "--merge", dest="merge", required=False, action='store_true', help="merge outputs and export to single file")
    p.add_argument("-t", "--time", dest="time", required=False, action='store_true', help="create `datetime` column from delta_time")
    p.add_argument("-g", "--geo", dest="geo", required=False, action='store_true', help="export file as georreferenced points")
    p.add_argument("-f", "--format", dest="format", required=False, type=str, default='parquet', help="output files format [default = parquet]")
    p.add_argument("-D", "--debug", dest="debug", required=False, action='store_true', help="allow dask to display error messages")

    n = max(1, os.cpu_count() // 2)
    p.add_argument("-n", "--cores", dest="cores", required=False, type=int, default=n, help=f"number of cpu cores to use [default = {n}]")
    p.add_argument("-s", "--threads", dest="threads", required=False, type=int, default=1, help="number of threads per cpu [default = 1]")
    p.add_argument("-A", "--ram", dest="ram", required=False, type=int, default=20, help="maximum RAM usage per cpu - in Giga Bytes [default = 20]")
    p.add_argument("-p", "--port", dest="port", required=False, type=int, default=10000, help="port where to open dask dashboard [default = 10000]")
    
    cmdargs = p.parse_args()
    return cmdargs


if __name__ == "__main__":
    args = getCmdArgs()
    # from src.gh3_download import gedi_h3_download
    
    gedi_h3_download(
        output=args.output,
        region=args.region,
        l2a_vars=args.l2a,
        l2b_vars=args.l2b,
        l4a_vars=args.l4a,
        anci_vars=args.anci,
        rh_metrics=args.rh,
        time_start=args.time_start,
        time_end=args.time_end,
        query=args.query,
        quality_filter=args.quality,
        egi_index=args.egi,
        merge_outputs=args.merge,
        datetime_col=args.time,
        georeference=args.geo,
        output_format=args.format,
        debug_mode=args.debug,
        n_cores=args.cores,
        n_threads=args.threads,
        max_ram_per_core=args.ram,
        dask_port=args.port
    )

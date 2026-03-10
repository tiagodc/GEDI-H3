#!/usr/bin/env python
"""
Display H3 hexagon or EGI (EASE Grid Index) resolution levels and their characteristics.

Shows average edge length, area, and number of cells for each H3 resolution,
or pixel size and description for each EGI level, helping users choose
appropriate resolution and partition levels.

Author: Tiago de Conto
Package: gedih3
"""

import argparse


def format_number(n):
    """Format large numbers with commas"""
    return f"{n:,}"


def format_area(area_km2):
    """Format area with appropriate units"""
    if area_km2 >= 1:
        return f"{area_km2:,.2f} km²"
    elif area_km2 >= 0.001:
        return f"{area_km2 * 1_000_000:,.0f} m²"
    else:
        return f"{area_km2 * 1_000_000:,.2f} m²"


def format_edge(edge_km):
    """Format edge length with appropriate units"""
    if edge_km >= 1:
        return f"{edge_km:,.2f} km"
    elif edge_km >= 0.001:
        return f"{edge_km * 1000:,.1f} m"
    else:
        return f"{edge_km * 1000:,.2f} m"


def get_h3_resolution_info(res: int) -> dict:
    """
    Get H3 resolution characteristics from h3 library.

    Parameters
    ----------
    res : int
        H3 resolution level (0-15)

    Returns
    -------
    dict
        Dictionary with edge_km, area_km2, and cells count
    """
    import h3
    edge_m = h3.average_hexagon_edge_length(res, unit='m')
    area_m2 = h3.average_hexagon_area(res, unit='m^2')
    num_cells = h3.get_num_cells(res)

    return {
        'edge_km': edge_m / 1000,
        'area_km2': area_m2 / 1_000_000,
        'cells': num_cells
    }


# EGI level descriptions for display
EGI_DESCRIPTIONS = {
    1: "~1m (finest)",
    2: "~5m",
    3: "~25m (GEDI footprint)",
    4: "~100m (NISAR compatible)",
    5: "~200m (BIOMASS compatible)",
    6: "~1km (GEDI L4B baseline)",
    7: "~2km (GEDI threshold)",
    8: "~10km (GEDI wall-to-wall)",
    9: "~20km",
    10: "~40km",
    11: "~80km",
    12: "~160km (coarsest, partition level)",
}


def get_egi_resolution_info(level: int) -> dict:
    """
    Get EGI resolution characteristics.

    Parameters
    ----------
    level : int
        EGI resolution level (1-12)

    Returns
    -------
    dict
        Dictionary with resolution_m and description
    """
    from gedih3.egi.config import RESOLUTIONS as EGI_RESOLUTIONS
    return {
        'resolution_m': EGI_RESOLUTIONS[level],
        'description': EGI_DESCRIPTIONS.get(level, "")
    }


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="Display H3 hexagon or EGI (EASE Grid Index) resolution levels and characteristics"
    )

    p.add_argument(
        "-egi", "--egi",
        dest="egi",
        action="store_true",
        help="show EGI (EASE Grid Index) levels instead of H3"
    )

    p.add_argument(
        "-r", "--resolution",
        dest="resolution",
        type=int,
        metavar="LEVEL",
        default=None,
        help="show details for specific resolution only (H3: 0-15, EGI: 1-12)"
    )

    p.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="output in JSON format"
    )

    p.add_argument(
        "--csv",
        dest="csv_output",
        action="store_true",
        help="output in CSV format"
    )

    return p.parse_args()


def display_h3_resolutions(args):
    """Display H3 resolution table"""
    # Build resolution data from h3 library
    if args.resolution is not None:
        if not 0 <= args.resolution <= 15:
            print(f"ERROR: H3 resolution must be between 0 and 15, got {args.resolution}")
            return
        resolutions = {args.resolution: get_h3_resolution_info(args.resolution)}
    else:
        resolutions = {res: get_h3_resolution_info(res) for res in range(16)}

    # JSON output
    if args.json_output:
        import json
        print(json.dumps(resolutions, indent=2))
        return

    # CSV output
    if args.csv_output:
        print("resolution,edge_km,area_km2,cells")
        for res, data in resolutions.items():
            print(f"{res},{data['edge_km']},{data['area_km2']},{data['cells']}")
        return

    # Default table output
    print()
    print("H3 Hexagon Resolution Levels")
    print("=" * 75)
    print()
    print(f"{'Res':>4}  {'Avg Edge Length':>18}  {'Avg Hex Area':>18}  {'Total Cells':>20}")
    print("-" * 75)

    for res, data in resolutions.items():
        edge = format_edge(data['edge_km'])
        area = format_area(data['area_km2'])
        cells = format_number(data['cells'])

        # Add markers for common use cases
        marker = ""
        if res == 3:
            marker = " <- typical partition level"
        elif res == 12:
            marker = " <- typical index level for GEDI footprint resolution"

        print(f"{res:>4}  {edge:>18}  {area:>18}  {cells:>20}{marker}")

    print("-" * 75)
    print()
    print("Recommendations:")
    print("  - Partition level (--h3p): 2-4 for continental scale datasets")
    print("  - Index level (--h3r): 12 for GEDI footprint resolution (~25m)")
    print("  - Index level must be >= partition level")
    print()


def display_egi_resolutions(args):
    """Display EGI resolution table"""
    # Build resolution data
    if args.resolution is not None:
        if not 1 <= args.resolution <= 12:
            print(f"ERROR: EGI level must be between 1 and 12, got {args.resolution}")
            return
        resolutions = {args.resolution: get_egi_resolution_info(args.resolution)}
    else:
        resolutions = {level: get_egi_resolution_info(level) for level in range(1, 13)}

    # JSON output
    if args.json_output:
        import json
        print(json.dumps(resolutions, indent=2))
        return

    # CSV output
    if args.csv_output:
        print("level,resolution_m,description")
        for level, data in resolutions.items():
            print(f"{level},{data['resolution_m']},{data['description']}")
        return

    # Default table output
    print()
    print("EGI (EASE Grid Index) Resolution Levels")
    print("=" * 75)
    print()
    print(f"{'Level':>6}  {'Pixel Size':>15}  {'Description':<45}")
    print("-" * 75)

    for level, data in resolutions.items():
        res_m = data['resolution_m']
        if res_m >= 1000:
            pixel_str = f"{res_m/1000:,.2f} km"
        else:
            pixel_str = f"{res_m:,.2f} m"

        desc = data['description']

        # Add markers for common use cases
        marker = ""
        if level == 6:
            marker = " <- GEDI L4B native"

        print(f"{level:>6}  {pixel_str:>15}  {desc:<45}{marker}")

    print("-" * 75)
    print()
    print("Recommendations:")
    print("  - Level 3 (~25m): GEDI footprint resolution")
    print("  - Level 6 (~1km): GEDI L4B baseline")
    print("  - Level 8 (~10km): GEDI wall-to-wall products")
    print("  - Higher levels (9-12) for coarse regional/global analysis")
    print()


def main():
    args = get_cmd_args()

    if args.egi:
        display_egi_resolutions(args)
    else:
        display_h3_resolutions(args)


if __name__ == '__main__':
    main()

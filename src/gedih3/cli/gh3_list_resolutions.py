#!/usr/bin/env python
"""
Display H3 hexagon resolution levels and their characteristics.

Shows average edge length, area, and number of cells for each H3 resolution,
helping users choose appropriate resolution and partition levels.

Author: Tiago de Conto
Package: gedih3
"""

import argparse
import h3


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
    edge_m = h3.average_hexagon_edge_length(res, unit='m')
    area_m2 = h3.average_hexagon_area(res, unit='m^2')
    num_cells = h3.get_num_cells(res)

    return {
        'edge_km': edge_m / 1000,
        'area_km2': area_m2 / 1_000_000,
        'cells': num_cells
    }


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="Display H3 hexagon resolution levels and characteristics"
    )

    p.add_argument(
        "-r", "--resolution",
        dest="resolution",
        type=int,
        choices=range(0, 16),
        metavar="0-15",
        default=None,
        help="show details for specific resolution only"
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


def main():
    args = get_cmd_args()

    # Build resolution data from h3 library
    if args.resolution is not None:
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
            marker = " <- typical index (~9m)"

        print(f"{res:>4}  {edge:>18}  {area:>18}  {cells:>20}{marker}")

    print("-" * 75)
    print()
    print("Recommendations:")
    print("  - Partition level (--h3p): 2-4 for regional, 0-1 for global datasets")
    print("  - Index level (--h3r): 11-12 for GEDI footprint resolution (~25m)")
    print("  - Index level must be >= partition level")
    print()


if __name__ == '__main__':
    main()

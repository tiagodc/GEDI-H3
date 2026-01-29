#!/usr/bin/env python
"""
List available GEDI variables from products or H3 database.

This tool helps users discover what variables are available for extraction
or building H3 databases.

Author: Tiago de Conto
Package: gedih3
"""

import argparse
import sys


def get_cmd_args():
    """Parse command line arguments"""
    p = argparse.ArgumentParser(
        description="List available GEDI variables from products or H3 database"
    )

    p.add_argument(
        "-p", "--product",
        dest="product",
        type=str,
        choices=['L1B', 'L2A', 'L2B', 'L4A', 'L4C', 'all'],
        default='all',
        help="GEDI product to list variables for [default = all]"
    )

    p.add_argument(
        "-d", "--database",
        dest="database",
        type=str,
        default=None,
        help="path to H3 database to list available columns from"
    )

    p.add_argument(
        "-g", "--grep",
        dest="grep",
        type=str,
        default=None,
        help="filter variables by pattern (case-insensitive)"
    )

    p.add_argument(
        "-t", "--type",
        dest="var_type",
        choices=['default', 'minimal', 'all'],
        default='default',
        help="variable list type: default, minimal, or all [default = default]"
    )

    p.add_argument(
        "--no-header",
        dest="no_header",
        action="store_true",
        help="suppress column headers in output"
    )

    return p.parse_args()


def list_product_variables(product: str, var_type: str = 'default', grep: str = None):
    """List variables for a GEDI product"""
    from gedih3.config import GEDI_PRODUCTS

    if product.upper() not in GEDI_PRODUCTS:
        print(f"Unknown product: {product}", file=sys.stderr)
        return []

    prod_info = GEDI_PRODUCTS[product.upper()]

    if var_type == 'minimal':
        variables = prod_info.get('min_vars', [])
    elif var_type == 'all':
        # Read from default vars file
        vars_file = prod_info.get('default_vars_file')
        if vars_file:
            try:
                with open(vars_file, 'r') as f:
                    variables = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            except FileNotFoundError:
                variables = prod_info.get('min_vars', [])
        else:
            variables = prod_info.get('min_vars', [])
    else:  # default
        vars_file = prod_info.get('default_vars_file')
        if vars_file:
            try:
                with open(vars_file, 'r') as f:
                    variables = [line.strip() for line in f if line.strip() and not line.startswith('#')]
            except FileNotFoundError:
                variables = prod_info.get('min_vars', [])
        else:
            variables = prod_info.get('min_vars', [])

    # Apply grep filter
    if grep:
        grep_lower = grep.lower()
        variables = [v for v in variables if grep_lower in v.lower()]

    return variables


def list_database_columns(database: str, grep: str = None):
    """List columns available in an H3 database"""
    import os
    from gedih3.gh3driver import gh3_read_meta

    if not os.path.exists(database):
        print(f"Database not found: {database}", file=sys.stderr)
        return []

    columns = gh3_read_meta('h3_columns', gh3_root_dir=database)

    if columns is None:
        print(f"Could not read metadata from database: {database}", file=sys.stderr)
        return []

    # Apply grep filter
    if grep:
        grep_lower = grep.lower()
        columns = [c for c in columns if grep_lower in c.lower()]

    return sorted(columns)


def main():
    args = get_cmd_args()

    # If database specified, list its columns
    if args.database:
        columns = list_database_columns(args.database, args.grep)

        if not args.no_header:
            print(f"Columns in database: {args.database}")
            print("-" * 60)

        for col in columns:
            print(col)

        if not args.no_header:
            print("-" * 60)
            print(f"Total: {len(columns)} columns")

        return

    # List product variables
    from gedih3.config import GEDI_PRODUCTS

    products = [args.product.upper()] if args.product != 'all' else list(GEDI_PRODUCTS.keys())

    for product in products:
        variables = list_product_variables(product, args.var_type, args.grep)

        if not args.no_header:
            prod_info = GEDI_PRODUCTS[product]
            print(f"\n{product}: {prod_info.get('description', '')}")
            print("-" * 60)

        for var in variables:
            if args.no_header:
                print(f"{product}\t{var}")
            else:
                print(f"  {var}")

        if not args.no_header:
            print(f"  [{len(variables)} variables]")

    if not args.no_header and args.product == 'all':
        total = sum(len(list_product_variables(p, args.var_type, args.grep)) for p in products)
        print(f"\nTotal: {total} variables across {len(products)} products")


if __name__ == '__main__':
    main()

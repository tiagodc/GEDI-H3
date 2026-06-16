#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
GEDI Dataset Update Tool

Add new columns to an existing simplified dataset (created by gh3_extract).
Two modes:

Mode 1: Add columns from the source H3 database via shot_number join.
Mode 2: Merge columns from another simplified dataset via shot_number join.

Both modes modify the target dataset in place.
"""

import os
import re
import argparse


def _query_filter_columns(query_filter, available_columns):
    """Extract column names referenced in a pandas query filter string.

    Returns only identifiers that match known database columns,
    excluding Python keywords and numeric literals.
    """
    if not query_filter:
        return []
    available = set(available_columns)
    # Match backtick-quoted identifiers (may contain '/' or other special chars)
    backtick_tokens = set(re.findall(r'`([^`]+)`', query_filter))
    # Match regular word tokens
    word_tokens = set(re.findall(r'\b([a-zA-Z_]\w*)\b', query_filter))
    all_tokens = backtick_tokens | word_tokens
    python_keywords = {'and', 'or', 'not', 'in', 'True', 'False', 'None',
                       'is', 'nan', 'NaN', 'inf', 'Inf'}
    return [t for t in all_tokens if t in available and t not in python_keywords]


def _make_plain_reader(fmt, columns=None):
    """Create a plain pandas reader (no geometry) for merge sources."""
    import pandas as pd
    if fmt == 'parquet':
        return lambda f: pd.read_parquet(f, columns=columns)
    elif fmt == 'feather':
        return lambda f: pd.read_feather(f, columns=columns)
    else:
        raise ValueError(f"Unsupported format for plain reader: {fmt}")


def get_cmd_args():
    """Parse command line arguments for dataset update"""
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args, add_storage_args

    p = argparse.ArgumentParser(
        description="Add new columns to an existing simplified dataset",
        formatter_class=argparse.RawTextHelpFormatter
    )

    # Target dataset (modified in place)
    p.add_argument("-d", "--dataset", dest="dataset", required=True, type=str,
                   help="path to target simplified dataset directory (modified in place)")

    # Mode 1: Add columns from H3 database
    p.add_argument("-D", "--database", dest="database", type=str, default=None,
                   help="source H3 database (default: from dataset metadata)")
    p.add_argument("-l", "--list", dest="list", nargs='+', type=str, default=None,
                   help="variables to add (space-separated, file path, or wildcards like 'rh_*')")
    add_product_args(p, include_detail_level=False)

    # Mode 2: Merge from another dataset
    p.add_argument("-m", "--merge", dest="merge", type=str, default=None,
                   help="path to another simplified dataset to merge columns from")

    # Dask, storage, and verbosity
    add_dask_args(p)
    add_storage_args(p)
    add_verbosity_args(p)

    return p.parse_args()


def _detect_shot_number_col(col_names):
    """Find the shot_number column name in a schema."""
    sn_cols = [c for c in col_names if c.startswith('shot_number')]
    if not sn_cols:
        return None
    return sn_cols[0]


def _join_new_columns(target_df, new_df, new_cols, sn_col):
    """Left join new columns onto target via shot_number, preserving all target rows."""
    idx_name = target_df.index.name

    target_reset = target_df.reset_index()
    merge_subset = new_df[[sn_col] + new_cols].drop_duplicates(subset=[sn_col])
    merged = target_reset.merge(merge_subset, on=sn_col, how='left')

    if idx_name:
        merged = merged.set_index(idx_name)
    return merged


def _update_from_database(args, dataset_path, dataset_meta, logger):
    """Mode 1: Add columns from H3 database via shot_number join."""
    import json
    import numpy as np
    import pandas as pd
    from dask.distributed import Client, progress

    import gedih3.gh3driver as gh3
    from gedih3.cliutils import (collect_columns, parse_dask_args, detect_dataset_format,
                                 list_dataset_files, read_dataset_schema,
                                 make_dataset_reader, h3_col_name)

    # Determine source H3 database
    from gedih3.utils import smart_exists
    db_path = args.database or dataset_meta.get('source_database')
    if not db_path or not smart_exists(db_path):
        raise FileNotFoundError(
            f"Source H3 database not found: {db_path}\n"
            "Specify the H3 database with -D/--database"
        )

    # If source_database points to a simplified dataset (not an H3 database),
    # walk back the metadata chain to find the original H3 database.
    from gedih3.config import BUILD_LOG_FILENAME, DATASET_META_FILENAME
    from gedih3.utils import smart_join
    build_log_path = smart_join(db_path, BUILD_LOG_FILENAME)
    if not smart_exists(build_log_path):
        chain_meta_path = smart_join(db_path, DATASET_META_FILENAME)
        if smart_exists(chain_meta_path):
            with open(chain_meta_path, 'r') as f:
                chain_meta = json.load(f)
            upstream = chain_meta.get('source_database')
            if upstream and smart_exists(smart_join(upstream, BUILD_LOG_FILENAME)):
                logger.info(f"  source_database is a simplified dataset, tracing back to: {upstream}")
                db_path = upstream
            else:
                raise FileNotFoundError(
                    f"source_database '{db_path}' is not an H3 database and its upstream "
                    f"H3 database could not be found.\n"
                    "Specify the H3 database explicitly with -D/--database"
                )
        else:
            raise FileNotFoundError(
                f"Not an H3 database (no build log): {db_path}\n"
                "Specify the H3 database with -D/--database"
            )

    logger.info(f"  Source database: {db_path}")

    # Surface non-INDEXED granules in the source DB. Class A failures from
    # gh3_build (orbits whose HDF5 lacked a requested variable) leave gaps
    # that propagate silently into every downstream simplified dataset and
    # every gh3_update'd version — once the operator knows, they can
    # either re-run gh3_build with a trimmed var list to recover those
    # granules, or proceed knowingly. Read the source build log once and
    # count by status; cheap (single JSON read) and lives entirely in the
    # data we already opened above for db_path validation.
    try:
        with open(build_log_path, 'r') as _blf:
            _bl = json.load(_blf)
        _granules = _bl.get('granules') or []
        if _granules:
            from collections import Counter
            _status_counts = Counter(g.get('status', 'UNKNOWN') for g in _granules)
            _non_indexed = sum(
                c for s, c in _status_counts.items() if s != 'INDEXED'
            )
            if _non_indexed:
                logger.warning(
                    f"Source database has {_non_indexed}/{len(_granules)} "
                    f"non-INDEXED granule(s) — those shots will be ABSENT "
                    f"from the joined columns. Breakdown: {dict(_status_counts)}. "
                    f"Run `gh3_build` on the source database to retry; see "
                    f"its end-of-build advisory for the recovery recipe."
                )
    except (OSError, json.JSONDecodeError) as _e:
        logger.debug(f"Could not enumerate source-db granule status: {_e}")

    # Resolve new columns from product args
    # Set safe defaults for attributes collect_columns may access
    for attr, default in [('region', None), ('time_start', None), ('time_end', None),
                          ('quality', False), ('geo', False), ('add_datetime', False)]:
        if not hasattr(args, attr):
            setattr(args, attr, default)

    args_db = argparse.Namespace(**vars(args))
    args_db.database = db_path
    available_columns = gh3.gh3_read_meta('h3_columns', gh3_root_dir=db_path)
    new_cols = collect_columns(args_db, available_columns=available_columns)

    # Read target schema to filter out already-present columns
    fmt = detect_dataset_format(dataset_path)
    data_files = list_dataset_files(dataset_path, fmt)
    target_cols, _ = read_dataset_schema(data_files[0], fmt)
    sn_col = _detect_shot_number_col(target_cols)
    if not sn_col:
        raise ValueError(
            "Target dataset does not contain shot_number. "
            "Re-extract with the updated gh3_extract to include shot_number."
        )

    # Filter to truly new columns
    new_cols = [c for c in new_cols if c not in target_cols and c != 'geometry' and c != 'datetime']
    if not new_cols:
        logger.warning("No new columns to add — all requested columns already exist in the dataset.")
        return

    # Validate new columns exist in H3 database
    missing = [c for c in new_cols if c not in available_columns]
    if missing:
        raise ValueError(f"Columns not found in H3 database: {', '.join(missing)}")

    logger.info(f"  New columns to add: {new_cols}")

    # Determine index type and partition column
    index_type = dataset_meta.get('index_type', 'h3')
    query_filter = dataset_meta.get('query_filter')

    # Identify extra columns needed by the query filter that aren't in new_cols
    filter_cols = _query_filter_columns(query_filter, available_columns)
    extra_filter_cols = [c for c in filter_cols if c not in new_cols and c != sn_col]

    dask_kwargs = parse_dask_args(args)

    with Client(**dask_kwargs) as client:
        logger.info(f"  Dask dashboard: {client.dashboard_link}")

        if index_type == 'h3':
            _update_h3_partitions(
                dataset_path, db_path, data_files, fmt, new_cols,
                sn_col, query_filter, extra_filter_cols, dataset_meta,
                logger, args=args
            )
        elif index_type == 'egi':
            _update_egi_partitions(
                dataset_path, db_path, data_files, fmt, new_cols,
                sn_col, query_filter, extra_filter_cols, dataset_meta,
                logger, args=args
            )
        else:
            raise ValueError(f"Unsupported index type: {index_type}")

    # Update metadata
    existing_cols = dataset_meta.get('columns', [])
    updated_cols = sorted(set(existing_cols + new_cols))
    gh3.gh3_write_dataset_meta(
        opath=dataset_path,
        index_type=dataset_meta.get('index_type', 'h3'),
        index_level=dataset_meta.get('index_level'),
        columns=updated_cols,
        source_database=db_path,
        query_filter=query_filter,
        tool='gh3_update',
        file_format=fmt,
        **{k: v for k, v in dataset_meta.items()
           if k in ('egi_index_level', 'egi_partition_level', 'h3_partition_level')}
    )
    logger.info("  Metadata updated.")


def _update_h3_partitions(dataset_path, db_path, data_files, fmt, new_cols,
                           sn_col, query_filter, extra_filter_cols, dataset_meta,
                           logger, args=None):
    """Update H3-partitioned dataset files from source H3 database."""
    import numpy as np
    import pandas as pd
    import geopandas as gpd

    import gedih3.gh3driver as gh3
    from gedih3.cliutils import make_dataset_reader, h3_col_name, progress_iter
    from gedih3.utils import smart_exists, smart_join

    h3_part = gh3.gh3_read_meta('h3_partition_level', gh3_root_dir=db_path)
    h3_part_col = h3_col_name(h3_part)

    load_cols = [sn_col] + new_cols + extra_filter_cols
    reader = make_dataset_reader(fmt)
    n_updated = 0

    with progress_iter(data_files, desc="Updating H3 partitions",
                       args=args, unit="part") as bar:
        for fpath in bar:
            part_id = os.path.splitext(os.path.basename(fpath))[0]
            h3_dir = smart_join(db_path, f"{h3_part_col}={part_id}")

            target_df = reader(fpath)

            if len(target_df) == 0:
                continue

            if smart_exists(h3_dir):
                source_df = gh3.gh3_load_hex(h3_dir, columns=load_cols)
                if query_filter:
                    from gedih3.cliutils import safe_query
                    source_df = safe_query(source_df, query_filter)
                    if extra_filter_cols:
                        source_df = source_df.drop(columns=extra_filter_cols, errors='ignore')
                target_df = _join_new_columns(target_df, source_df, new_cols, sn_col)
            else:
                for c in new_cols:
                    target_df[c] = np.nan

            gh3._write_dataframe(target_df, fpath, fmt)
            n_updated += 1

    logger.info(f"  Updated {n_updated}/{len(data_files)} partition files.")


def _update_egi_partitions(dataset_path, db_path, data_files, fmt, new_cols,
                            sn_col, query_filter, extra_filter_cols, dataset_meta,
                            logger, args=None):
    """Update EGI-partitioned dataset files from source H3 database."""
    import numpy as np
    import pandas as pd
    import geopandas as gpd

    import gedih3.gh3driver as gh3
    from gedih3.cliutils import make_dataset_reader, progress_iter
    from gedih3.egi.config import egi_col_name
    from gedih3.utils import smart_exists, smart_join

    # Get EGI partition level from metadata
    egi_partition_level = dataset_meta.get('egi_partition_level', 12)
    egi_part_col = egi_col_name(egi_partition_level)

    # Prepare EGI↔H3 intersection at the dataset's partition level — the
    # egi_to_h3 keys must be the same level as the hashes encoded in the
    # partition filenames.
    egi_tiles, egi_to_h3, h3_part_col, _ = gh3._prepare_egi_loading(
        None, db_path, partition_level=egi_partition_level)

    load_cols = [sn_col] + new_cols + extra_filter_cols
    reader = make_dataset_reader(fmt)
    n_updated = 0

    with progress_iter(data_files, desc="Updating EGI partitions",
                       args=args, unit="part") as bar:
        for fpath in bar:
            part_id = os.path.splitext(os.path.basename(fpath))[0]
            # Filenames carry the EGI hash as a decimal string, but egi_to_h3
            # is keyed by the numeric hash (egi_tiles.index values).
            try:
                h3_list = egi_to_h3.get(np.uint64(part_id), [])
            except ValueError:
                logger.warning(f"  Skipping non-EGI partition filename: {fpath}")
                h3_list = []

            target_df = reader(fpath)

            if len(target_df) == 0:
                continue

            source_dfs = []
            for h3_id in h3_list:
                h3_dir = smart_join(db_path, f"{h3_part_col}={h3_id}")
                if smart_exists(h3_dir):
                    df = gh3.gh3_load_hex(h3_dir, columns=load_cols)
                    if len(df) > 0:
                        source_dfs.append(df)

            if source_dfs:
                source_df = pd.concat(source_dfs, ignore_index=True)
                if query_filter:
                    from gedih3.cliutils import safe_query
                    source_df = safe_query(source_df, query_filter)
                    if extra_filter_cols:
                        source_df = source_df.drop(columns=extra_filter_cols, errors='ignore')
                target_df = _join_new_columns(target_df, source_df, new_cols, sn_col)
            else:
                for c in new_cols:
                    target_df[c] = np.nan

            gh3._write_dataframe(target_df, fpath, fmt)
            n_updated += 1

    logger.info(f"  Updated {n_updated}/{len(data_files)} partition files.")


def _update_from_merge(args, dataset_path, dataset_meta, logger):
    """Mode 2: Merge columns from another simplified dataset via shot_number join."""
    import json
    import numpy as np
    import pandas as pd
    from dask.distributed import Client, progress

    import gedih3.gh3driver as gh3
    from gedih3.cliutils import (parse_dask_args, detect_dataset_format,
                                 list_dataset_files, read_dataset_schema,
                                 make_dataset_reader, is_internal_column)

    from gedih3.config import DATASET_META_FILENAME

    merge_path = args.merge

    # Validate merge dataset
    from gedih3.utils import smart_exists, smart_join
    merge_meta_path = smart_join(merge_path, DATASET_META_FILENAME)
    if not smart_exists(merge_meta_path):
        raise FileNotFoundError(f"Not a simplified dataset (no {DATASET_META_FILENAME}): {merge_path}")

    with open(merge_meta_path, 'r') as f:
        merge_meta = json.load(f)

    # Validate same index type
    target_index_type = dataset_meta.get('index_type')
    merge_index_type = merge_meta.get('index_type')
    if target_index_type != merge_index_type:
        raise ValueError(
            f"Index type mismatch: target is '{target_index_type}', "
            f"merge source is '{merge_index_type}'"
        )

    # Read schemas
    target_fmt = detect_dataset_format(dataset_path)
    merge_fmt = detect_dataset_format(merge_path)

    target_files = list_dataset_files(dataset_path, target_fmt)
    merge_files = list_dataset_files(merge_path, merge_fmt)

    target_cols, _ = read_dataset_schema(target_files[0], target_fmt)
    merge_cols, _ = read_dataset_schema(merge_files[0], merge_fmt)

    # Detect shot_number in both
    target_sn = _detect_shot_number_col(target_cols)
    merge_sn = _detect_shot_number_col(merge_cols)
    if not target_sn:
        raise ValueError(
            "Target dataset does not contain shot_number. "
            "Re-extract with the updated gh3_extract to include shot_number."
        )
    if not merge_sn:
        raise ValueError("Merge dataset does not contain shot_number.")

    # Use same shot_number column name (they should match, but handle suffix differences)
    sn_col = target_sn

    # Identify new columns (non-duplicate, non-internal)
    new_cols = [c for c in merge_cols
                if c not in target_cols
                and c != 'geometry'
                and not is_internal_column(c)]

    if not new_cols:
        logger.warning("No new columns to merge — all columns already exist in the target dataset.")
        return

    logger.info(f"  New columns to merge: {new_cols}")

    # Build merge file map: {partition_id: file_path}
    merge_map = {}
    for f in merge_files:
        part_id = os.path.splitext(os.path.basename(f))[0]
        merge_map[part_id] = f

    # Process each target file
    target_reader = make_dataset_reader(target_fmt)
    merge_reader_cols = [merge_sn] + new_cols
    # Use pandas (not geopandas) reader — geometry is not needed for the join
    # and gpd.read_parquet fails when the requested columns don't include geometry
    merge_reader = _make_plain_reader(merge_fmt, columns=merge_reader_cols)

    n_updated = 0
    n_no_match = 0

    # Check if partition names match between target and merge datasets
    matched_count = sum(1 for f in target_files
                        if os.path.splitext(os.path.basename(f))[0] in merge_map)

    from gedih3.cliutils import progress_iter

    if matched_count == 0 and merge_map:
        # Partition names don't match — fall back to global shot_number join
        logger.info("  Partition names don't match — joining by shot_number across all files")
        merge_all = pd.concat([merge_reader(f) for f in merge_files], ignore_index=True)
        merge_all = merge_all.drop_duplicates(subset=[merge_sn])
        if merge_sn != sn_col and merge_sn in merge_all.columns:
            merge_all = merge_all.rename(columns={merge_sn: sn_col})
        with progress_iter(target_files, desc="Merging columns",
                           args=args, unit="part") as bar:
            for fpath in bar:
                target_df = target_reader(fpath)
                if len(target_df) == 0:
                    continue
                target_df = _join_new_columns(target_df, merge_all, new_cols, sn_col)
                gh3._write_dataframe(target_df, fpath, target_fmt)
                n_updated += 1
    else:
        with progress_iter(target_files, desc="Merging partitions",
                           args=args, unit="part") as bar:
            for fpath in bar:
                part_id = os.path.splitext(os.path.basename(fpath))[0]
                target_df = target_reader(fpath)

                if len(target_df) == 0:
                    continue

                merge_file = merge_map.get(part_id)

                if merge_file and smart_exists(merge_file):
                    merge_df = merge_reader(merge_file)
                    # Rename shot_number if different between datasets
                    if merge_sn != sn_col and merge_sn in merge_df.columns:
                        merge_df = merge_df.rename(columns={merge_sn: sn_col})
                    target_df = _join_new_columns(target_df, merge_df, new_cols, sn_col)
                    n_updated += 1
                else:
                    for c in new_cols:
                        target_df[c] = np.nan
                    n_no_match += 1

                gh3._write_dataframe(target_df, fpath, target_fmt)

    logger.info(f"  Updated {n_updated}/{len(target_files)} files ({n_no_match} with no matching merge partition).")

    # Update metadata
    existing_cols = dataset_meta.get('columns', [])
    updated_cols = sorted(set(existing_cols + new_cols))
    gh3.gh3_write_dataset_meta(
        opath=dataset_path,
        index_type=dataset_meta.get('index_type', 'h3'),
        index_level=dataset_meta.get('index_level'),
        columns=updated_cols,
        source_database=dataset_meta.get('source_database'),
        query_filter=dataset_meta.get('query_filter'),
        tool='gh3_update',
        file_format=target_fmt,
        **{k: v for k, v in dataset_meta.items()
           if k in ('egi_index_level', 'egi_partition_level', 'h3_partition_level')}
    )
    logger.info("  Metadata updated.")


def main():
    args = get_cmd_args()

    from gedih3.cliutils import cli_exception_handler

    with cli_exception_handler(args):
        import json

        from gedih3.cliutils import setup_logging, print_banner, print_success, setup_storage, resolve_path_args

        logger = setup_logging(args, __name__)
        setup_storage(args, logger=logger)
        print_banner("GEDI Dataset Update Tool", logger=logger)

        resolve_path_args(args, ['dataset', 'database', 'merge'], logger=logger)

        from gedih3.config import DATASET_META_FILENAME

        dataset_path = args.dataset

        # Validate target dataset
        from gedih3.utils import smart_exists, smart_join
        meta_path = smart_join(dataset_path, DATASET_META_FILENAME)
        if not smart_exists(meta_path):
            raise FileNotFoundError(
                f"Not a simplified dataset (no {DATASET_META_FILENAME}): {dataset_path}\n"
                "gh3_update requires a dataset created by gh3_extract."
            )

        with open(meta_path, 'r') as f:
            dataset_meta = json.load(f)

        logger.info(f"  Target dataset: {dataset_path}")
        logger.info(f"  Index type: {dataset_meta.get('index_type')}")

        # Determine mode
        has_product_args = any(
            getattr(args, k.lower(), None) is not None
            for k in ('L1B', 'L2A', 'L2B', 'L4A', 'L4C')
        )
        has_list_arg = args.list is not None
        has_db_mode = has_product_args or has_list_arg
        has_merge_mode = args.merge is not None

        if has_db_mode and has_merge_mode:
            raise ValueError(
                "Cannot use both database mode (-l/-l2a/etc.) and merge mode (-m) simultaneously."
            )

        if not has_db_mode and not has_merge_mode:
            raise ValueError(
                "Specify columns to add from H3 database (-l/-l2a/-l4a/etc.) "
                "or a dataset to merge (-m)."
            )

        if has_db_mode:
            logger.info("Mode: Add columns from H3 database")
            _update_from_database(args, dataset_path, dataset_meta, logger)
        else:
            logger.info("Mode: Merge columns from another dataset")
            _update_from_merge(args, dataset_path, dataset_meta, logger)

        print_success("Dataset updated successfully", logger=logger)


if __name__ == '__main__':
    main()

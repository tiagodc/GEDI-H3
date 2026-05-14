#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

import argparse


def explicit_vars_missing_in_sample(product_vars, default_products, sample_dict):
    """Return ``{product: [bad_names]}`` for explicit-list products whose
    requested variables are absent from the sample HDF5.

    Pure function over its inputs — extracted from the gh3_build CLI so the
    pre-flight typo check can be unit-tested without invoking ``main()``.

    Parameters
    ----------
    product_vars : dict
        Mapping ``{product: list-or-None}``. ``None`` means "everything"
        (``*``/``all``) and is skipped. ``default``-sourced lists are
        identified by ``default_products`` and skipped (they are validated
        against the static manifest instead).
    default_products : set
        Products the user requested as ``default`` (skipped — manifest is
        the contract).
    sample_dict : dict
        One ``{product: hdf5_path}`` mapping from
        :func:`gedidriver.soc_file_tree`. Pass an empty dict to short-circuit.

    Returns
    -------
    dict
        Empty when every explicit-list variable was found (or no explicit
        lists exist). Otherwise ``{product: [missing_names]}``. Wildcards
        that match nothing in the sample HDF5 are surfaced as a single
        ``GediValidationError`` string entry under their product.
    """
    from gedih3.gedidriver import gedi_vars_from_h5, expand_var_wildcards
    from gedih3.exceptions import GediValidationError

    to_introspect = {
        p: v for p, v in (product_vars or {}).items()
        if v is not None and p not in (default_products or set())
    }
    if not to_introspect or not sample_dict:
        return {}

    missing = {}
    for prod, requested in to_introspect.items():
        if prod not in sample_dict:
            continue  # downstream gate handles product-presence
        try:
            available = gedi_vars_from_h5(sample_dict[prod])
        except Exception:
            # Caller surfaces this as a warning; treat as soft skip so a
            # broken sample file doesn't block an otherwise-valid request.
            continue
        try:
            expanded_names = expand_var_wildcards(requested, available)
        except GediValidationError as e:
            missing[prod] = [str(e)]
            continue
        avail_set = set(available)
        bad = [v for v in expanded_names if v not in avail_set]
        if bad:
            missing[prod] = bad
    return missing


def manifest_check_scope(h3_logger, product_vars):
    """Return the subset of ``product_vars`` that should be validated
    against the shipped static manifest, per the regime rule:

    - **Regime A** (fresh build): every product the user passed that was
      requested via ``default``/``def``.
    - **Regime C** (resume + explicit ``default`` re-request): only
      products in ``h3_logger.default_products`` that also appear in
      ``h3_logger.new_product_vars`` (i.e. produced a delta).
    - **Regimes B & D** (resume with granules-only or explicit-list
      expansion): empty — the build log is authoritative.

    An empty return signals the caller to skip validation entirely.

    Pure function over logger state so the regime gate can be unit-tested
    without the full CLI pipeline.
    """
    if not h3_logger.updating:
        scope = h3_logger.default_products & set(product_vars or {})
    else:
        delta = set(h3_logger.new_product_vars or {})
        scope = h3_logger.default_products & delta
    return {p: product_vars[p] for p in scope if p in (product_vars or {})}

def get_cmd_args():
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_product_args

    p = argparse.ArgumentParser(description="Build H3-indexed GEDI database from SOC files")

    # Spatial/temporal filtering
    p.add_argument("-r", "--region", dest="region", type=str, default=None,
                   help="vector file, bbox 'W,S,E,N', or ISO3 country code")
    p.add_argument("-t0", "--time-start", dest="time_start", type=str, default=None,
                   help="start date [YYYY-MM-DD]")
    p.add_argument("-t1", "--time-end", dest="time_end", type=str, default=None,
                   help="end date [YYYY-MM-DD]")

    # H3 configuration
    p.add_argument("-h3r", "--h3-resolution", dest="h3_resolution", type=int, default=12,
                   help="H3 index level [0-15, default=12]")
    p.add_argument("-h3p", "--h3-partition", dest="h3_partition", type=int, default=3,
                   help="H3 partition level [0-15, default=3]")

    # GEDI product variables
    add_product_args(p)

    # I/O paths
    p.add_argument("-o", "--output", dest="output", type=str, default=None,
                   help="output directory for H3 database (default: GH3_DEFAULT_H3_DIR)")
    p.add_argument("-i", '--indir', dest="indir", type=str, default=None,
                   help="directory with GEDI SOC files (default: GH3_DEFAULT_SOC_DIR)")
    p.add_argument("-dl", "--download", dest="download", action='store_true',
                   help="download missing GEDI data before building (embeds gh3_download as pre-step)")
    p.add_argument("-t", '--tmpdir', dest="tmpdir", type=str, default=None,
                   help="temporary directory for intermediate files")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="download from NASA S3 to temp directory (no persistent local download)")
    p.add_argument("--gedi-version", dest="version", type=int, default=None,
                   help="GEDI data version [default=latest available]")
    p.add_argument("--exclude", dest="exclude", action='append', default=None,
                   metavar='PATTERN',
                   help="exclude files whose basename matches the given fnmatch pattern. "
                        "Repeat the flag for multiple patterns. "
                        "Example: --exclude '*_SGS.h5' --exclude '*_BETA.h5'")

    # Dask and verbosity
    add_dask_args(p, profile='build')
    add_verbosity_args(p)

    return p.parse_args()


def _has_new_local_granules(soc_source, h3_logger):
    """Check if SOC directory has HDF5 files not tracked in the build log."""
    import os
    import glob as globmod
    from gedih3.gedidriver import GEDIFile

    if not soc_source or not os.path.isdir(soc_source):
        return False

    tracked = set()
    if hasattr(h3_logger, 'granule_info') and h3_logger.granule_info:
        for g in h3_logger.granule_info:
            tracked.add((g['orbit'], g['granule'], g['track']))

    for f in globmod.glob(os.path.join(soc_source, '**', 'GEDI*.h5'), recursive=True):
        try:
            gf = GEDIFile(f)
            if (gf.orbit, gf.orbit_granule, gf.track) not in tracked:
                return True
        except Exception:
            continue

    return False


def _detect_merge_resume_signal(h3_logger, parquet_dir):
    """Return a human-readable signal name if the resume should skip extract
    and go straight to merge, else return None.

    L1 (canonical): the on-disk log says ``status == 'MERGING'``. This is set
    by ``build_h3db`` immediately before the merge phase, so seeing it on
    resume proves the previous run finished extract.

    L2 (fallback for older builds): ``<parquet_dir>/_merge_progress.txt``
    exists with ≥1 non-empty line. A non-empty merge-progress file proves
    that merge already started, which in turn proves extract finished.

    The two signals are independent — either is sufficient.
    """
    import os
    if getattr(h3_logger, 'previous_status', None) == 'MERGING':
        return 'log status MERGING'
    progress_file = os.path.join(parquet_dir, '_merge_progress.txt')
    if os.path.isfile(progress_file):
        try:
            with open(progress_file, 'r') as fh:
                if any(line.strip() for line in fh):
                    return 'merge progress file present'
        except OSError:
            pass
    return None


def main():
    args = get_cmd_args()

    import os
    import sys
    import glob
    import warnings
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_SOC_DIR
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region, setup_logging, print_banner, print_success
    from gedih3.utils import get_system_resources
    from gedih3.gh3builder import build_h3db, download_soc, soc_file_tree, _reconcile_granules_from_disk, _merge_and_finalize
    from gedih3.gedidriver import GEDIFile, validate_soc_files, gedi_vars_expand
    from gedih3.logger import H3BuildLogger, SOCDownloadLogger
    from dask.distributed import Client

    # Setup logging and print banner
    logger = setup_logging(args, __name__)
    print_banner("GEDI H3 Database Builder Tool", logger=logger)

    if args.output is None:
        args.output = GH3_DEFAULT_H3_DIR
    os.makedirs(args.output, exist_ok=True)

    if args.tmpdir is None:
        args.tmpdir = os.path.join(args.output, '.tmp')
    os.makedirs(args.tmpdir, exist_ok=True)

    # Log detected resources and Dask configuration
    cpus, ram, storage = get_system_resources(disk_path=args.output)
    logger.info(f"System: {cpus} CPUs, {ram:.1f} GB RAM, {storage:.1f} GB free disk at {args.output}")
    logger.info(f"Dask config: {args.cores} workers, {args.threads} threads/worker, {args.memory} GB/worker")
    if storage < 10:
        logger.warning(f"Low disk space ({storage:.1f} GB free) — build may fail writing parquet output")

    # Determine source mode: --s3 (temp download+build) or local SOC directory
    if args.s3:
        soc_source = None  # S3 ETL to temp dir
        if args.indir:
            logger.warning("Both -i and --s3 specified. Using --s3 (S3 mode).")
        if args.download:
            logger.warning("--download is ignored when using --s3 (S3 mode downloads automatically).")
    elif args.indir:
        soc_source = args.indir
    else:
        soc_source = GH3_DEFAULT_SOC_DIR

    product_vars = parse_gedi_args(args)
    spatial = parse_region(args.region) if args.region is not None else None
    temporal = None
    if args.time_start or args.time_end:
        temporal = (args.time_start, args.time_end)

    h3_logger = H3BuildLogger(
        product_vars=product_vars,
        spatial=spatial,
        temporal=temporal,
        res=args.h3_resolution,
        part=args.h3_partition,
        version=args.version,
        dir=args.output,
        source_mode='s3' if args.s3 else 'local',
    )

    if not h3_logger.product_vars and not h3_logger.updating:
        raise ValueError(
            "No GEDI product selected - please select at least one of "
            "--l2a, --l2b, --l4a, --l4c (or use --detail-level for all four), "
            "and/or --l1b for waveform data"
        )
    if h3_logger.get_spatial() is None:
        logger.warning("No spatial filter provided - processing global data")

    if h3_logger.updating:
        logger.info("Build log exists, checking for updates")
        if h3_logger.new_spatial is not None:
            logger.info("Spatial filter updated")
        if h3_logger.new_temporal is not None:
            logger.info("Temporal filter updated")
        if h3_logger.new_product_vars is not None:
            logger.info("Product variables updated")

    # Detect variable-only update: only new products, no spatial/temporal changes
    is_variable_only_update = (
        h3_logger.updating
        and h3_logger.new_product_vars is not None
        and h3_logger.new_spatial is None
        and h3_logger.new_temporal is None
    )

    # Detect mixed update: spatial/temporal expansion AND new variables
    # Must be split into two phases to prevent data loss (P0-B)
    is_mixed_update = (
        h3_logger.updating
        and h3_logger.new_product_vars is not None
        and (h3_logger.new_spatial is not None or h3_logger.new_temporal is not None)
    )

    if is_mixed_update:
        logger.info("Mixed update detected (spatial/temporal + new variables)")
        logger.info("  Phase 1: expand spatial/temporal range with existing products")
        logger.info("  Phase 2: add new variable columns to all partitions")

    # Check for pending variable update from crash recovery
    pending_var_update = h3_logger.log_data.get('_pending_variable_update')

    # Early exit: database is already up-to-date with requested parameters
    # Only when a valid build log exists with partition data to confirm completeness
    if (h3_logger.is_up_to_date()
            and not pending_var_update
            and hasattr(h3_logger, 'h3_partition_ids')
            and h3_logger.h3_partition_ids):
        # S3/download modes: skip early exit — let the pipeline query CMR
        # or run download_soc() to discover new NASA granules
        if not args.s3 and not args.download:
            # Local mode: check SOC directory for new HDF5 files
            if not _has_new_local_granules(soc_source, h3_logger):
                if h3_logger.previous_status != 'COMPLETED':
                    h3_logger.set_post_build_info()
                    h3_logger.save_log('COMPLETED')
                logger.info("Database is already up-to-date with requested parameters")
                print_success("Database is up-to-date, no changes needed", logger=logger)
                return
            else:
                logger.info("New granules detected in SOC directory — updating database")

    if soc_source is None:
        source_label = "NASA S3 (temp download)"
    elif args.download:
        source_label = f"download+build: {soc_source}"
    else:
        source_label = f"local: {soc_source}"
    logger.info(f"Building GEDI H3 database at {args.output} (source: {source_label})")

    dask_kwargs = parse_dask_args(args)

    # Common kwargs shared across all build_h3db() calls
    _build_kwargs = dict(
        spatial=h3_logger.get_spatial(),
        temporal=h3_logger.get_temporal(),
        res=h3_logger.res,
        part=h3_logger.part,
        version=h3_logger.gedi_version,
        h3_dir=h3_logger._PARENT_DIR,
        status_callback=h3_logger.save_log,
        tmp_dir=args.tmpdir,
        exclude=args.exclude,
    )

    try:
        with Client(**dask_kwargs) as client:
            warnings.filterwarnings("ignore", message=r"Sending large graph of size.*", category=UserWarning, module="distributed.client")
            def _suppress_pandas_perf_warnings():
                import warnings
                import pandas as pd
                warnings.filterwarnings("ignore", message=r"DataFrame is highly fragmented.*", category=pd.errors.PerformanceWarning)

            client.run(_suppress_pandas_perf_warnings)

            logger.info(f"Dask dashboard available at: {client.dashboard_link}")

            # Validate/download SOC data when using local directory mode
            if soc_source is not None:
                os.makedirs(soc_source, exist_ok=True)
                # Parallel year/doy walk on the dask cluster — the serial
                # recursive glob the prior implementation used was the
                # dominant cost of `gh3_build` resume on multi-million-
                # file SOC trees. ``walk_soc_parallel`` pushes the doy-
                # level scandir to workers and applies the exclude filter
                # locally so we never ship excluded paths back to the
                # driver.
                from gedih3.parallel import walk_soc_parallel
                existing_h5 = walk_soc_parallel(
                    soc_source, pattern='GEDI*.h5', exclude=args.exclude,
                )
                if args.exclude:
                    logger.info(
                        f"Listed {len(existing_h5)} HDF5 files in {soc_source} "
                        f"(exclude filters: {args.exclude})"
                    )

                def _validate_existing_h5(product_vars, soc_dir):
                    """Validate requested products/variables exist in HDF5 files. Exits on mismatch.

                    Two stages:

                    1. ``default``-requested products are validated against
                       the shipped static manifest via :func:`validate_soc_files`.
                       The manifest is the contract for canonical NASA release
                       files.
                    2. Products with an *explicit* variable list (user-supplied
                       names, a file, or wildcards — anything that is not
                       ``default`` / ``*`` / ``minimal``) are sanity-checked
                       against the schema of one sample HDF5 file per product.
                       Catches typos and unknown names before a multi-hour
                       build hits a runtime ``KeyError``.

                    A single-file introspection cannot catch NASA-side schema
                    variance across orbit clusters (a variable present in
                    99% of granules but absent in a handful); those surface
                    as Stage1 warnings at build time and are recorded as
                    non-INDEXED in the build log for retry.
                    """
                    import copy

                    # ── Stage 1: default-products vs shipped manifest ──
                    scoped = manifest_check_scope(h3_logger, product_vars)
                    if scoped:
                        expanded = copy.deepcopy(scoped)
                        gedi_vars_expand(expanded, version=h3_logger.gedi_version)
                        try:
                            validation = validate_soc_files(
                                expanded, soc_dir, version=h3_logger.gedi_version,
                                exclude=args.exclude,
                            )
                        except Exception as val_err:
                            logger.warning(f"Could not validate HDF5 files (corrupt file?): {val_err}")
                            validation = None

                        if validation is not None:
                            if isinstance(validation, tuple):
                                can_skip = False
                                validation = validation[1] if len(validation) > 1 else {}
                            else:
                                can_skip = validation.get("can_skip", True)

                            if not can_skip:
                                msg_parts = ["Requested variables not found in existing HDF5 files:\n"]
                                if validation.get("missing_products"):
                                    msg_parts.append(f"  Missing products: {', '.join(validation['missing_products'])}")
                                if validation.get("missing_variables"):
                                    for prod, mvars in validation["missing_variables"].items():
                                        msg_parts.append(f"  Missing variables in {prod}: {', '.join(mvars)}")
                                if validation.get("error"):
                                    msg_parts.append(f"  {validation['error']}")
                                msg_parts.append("")
                                msg_parts.append("To fix:")
                                msg_parts.append("  1. Check available variables:  gh3_read_schema /path/to/file.h5")
                                msg_parts.append("  2. Adjust your -l2a/-l4a/... flags to match available data")
                                msg_parts.append("  3. Run gh3_download to fetch the required products")
                                msg_parts.append("  4. Or add --download to auto-fetch before building")
                                msg_parts.append("  5. Or use --s3 to build directly from NASA S3 (no persistent download)")
                                logger.error("\n".join(msg_parts))
                                sys.exit(2)

                    # ── Stage 2: explicit-list products vs sample HDF5 schema ──
                    has_explicit = any(
                        v is not None and p not in h3_logger.default_products
                        for p, v in (product_vars or {}).items()
                    )
                    if not has_explicit:
                        return

                    try:
                        sample = soc_file_tree(
                            soc_dir, to_list=True,
                            glob_kwargs=(
                                {'version': h3_logger.gedi_version}
                                if h3_logger.gedi_version is not None else None
                            ),
                            exclude=args.exclude,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Pre-flight var check: could not sample SOC tree "
                            f"({type(e).__name__}: {e})"
                        )
                        return
                    if not sample:
                        return  # nothing to introspect against

                    sample_dict = sample[0]
                    missing = explicit_vars_missing_in_sample(
                        product_vars, h3_logger.default_products, sample_dict,
                    )

                    if missing:
                        msg = [
                            "Pre-flight variable check failed: requested names "
                            "are not present in the sample HDF5 files.",
                            "Fix typos or unknown names and rerun. Nothing was built.",
                            "",
                        ]
                        for prod, bad in missing.items():
                            preview = bad[:10]
                            tail = '' if len(bad) <= 10 else f' (+{len(bad) - 10} more)'
                            msg.append(f"  {prod}: {preview}{tail}")
                        msg.extend([
                            "",
                            "To inspect available variables in a SOC file:",
                            "  gh3_read_schema /path/to/file.h5",
                            f"  gh3_read_schema {sample_dict.get(next(iter(missing)), '<file.h5>')}",
                        ])
                        logger.error("\n".join(msg))
                        sys.exit(2)

                if args.download:
                    # --download enabled: embed gh3_download as pre-step
                    soc_logger = SOCDownloadLogger(
                        product_vars=h3_logger.get_product_vars(),
                        spatial=h3_logger.get_spatial(),
                        temporal=h3_logger.get_temporal(),
                        version=h3_logger.gedi_version,
                        dir=soc_source,
                    )

                    needs_download = True

                    if soc_logger.updating and soc_logger.log_data.get('status') == 'COMPLETED':
                        # Completed download log exists — check if more data is needed
                        if (is_variable_only_update or is_mixed_update) and h3_logger.new_product_vars:
                            # Regime C: only consult the static manifest for products
                            # the user explicitly re-requested as `default`. For
                            # explicit-list var deltas, skip the manifest check
                            # and fall through to download — download_soc() is
                            # idempotent (resume=True skips files already on disk).
                            default_delta = h3_logger.default_products & set(h3_logger.new_product_vars)
                            if default_delta:
                                import copy
                                expanded_new = copy.deepcopy({p: h3_logger.new_product_vars[p] for p in default_delta})
                                gedi_vars_expand(expanded_new, version=h3_logger.gedi_version)
                                try:
                                    validation = validate_soc_files(
                                        expanded_new, soc_source,
                                        version=h3_logger.gedi_version,
                                        exclude=args.exclude,
                                    )
                                    can_skip = validation.get('can_skip', True) if isinstance(validation, dict) else False
                                except Exception:
                                    can_skip = False
                                if can_skip:
                                    needs_download = False
                                    logger.info(f"New products already available in {soc_source}")
                                else:
                                    logger.info("New products not found in existing HDF5 files — downloading them")
                            else:
                                # Regime D — explicit-list var delta; trust the user
                                # and let download_soc() resume idempotently.
                                needs_download = True
                                logger.info("New variables requested — downloading missing granules (idempotent resume)")
                        elif not h3_logger.new_spatial and not h3_logger.new_temporal and not h3_logger.new_product_vars:
                            # Same parameters — still run download to discover new NASA granules.
                            # download_soc() is idempotent: existing files are skipped via resume=True.
                            needs_download = True
                            logger.info(f"Checking for new GEDI data ({len(soc_logger.granule_info)} granules already downloaded)")
                        else:
                            # Regime B — spatial/temporal expansion without var change.
                            # No manifest consultation; the log is the contract and
                            # download_soc() will fetch granules for the new range.
                            needs_download = True  # Spatial/temporal expansion needs new granules

                    if needs_download:
                        logger.info(f"Downloading GEDI data to {soc_source}")
                        soc_logger.save_log('DOWNLOADING')

                        def _download_tracker(gran_info, status):
                            """Called from main thread (as_completed loop). Thread safe."""
                            if status == 'PENDING':
                                soc_logger.register_pending_granules([gran_info])
                            else:
                                soc_logger.update_granule_status(gran_info, status)
                            soc_logger.save_log('DOWNLOADING')

                        download_soc(
                            product_vars=soc_logger.get_product_vars(),
                            spatial=soc_logger.get_spatial(),
                            temporal=soc_logger.get_temporal(),
                            direct_access=False,
                            update=True,
                            version=h3_logger.gedi_version,
                            odir=soc_source,
                            on_granule_complete=_download_tracker,
                            ensure_l2a=not is_variable_only_update,
                        )
                        soc_logger.set_post_download_info()
                        soc_logger.save_log('COMPLETED')
                        logger.info("Download complete")
                else:
                    # --download NOT enabled: build from existing data only
                    if not existing_h5:
                        logger.error(
                            f"No HDF5 files found in {soc_source}.\n"
                            "Options:\n"
                            "  1. Run gh3_download first to fetch GEDI data\n"
                            "  2. Add --download to auto-fetch before building\n"
                            "  3. Use --s3 to build directly from NASA S3 (no persistent download)"
                        )
                        sys.exit(2)

                    logger.info(f"Building from {len(existing_h5)} existing HDF5 files in {soc_source}")
                    logger.info("Note: using only existing data (add --download to fetch missing data)")
                    logger.info("Validating product variables in existing HDF5 files")
                    _validate_existing_h5(h3_logger.get_product_vars(), soc_source)

                    # Opportunistic SOC manifest refresh: we have the
                    # file list in memory from the parallel walk above,
                    # so persisting the sentinel is free. Closes the
                    # "tree populated externally (NASA delivery, manual
                    # rsync) and gh3_download was never run" gap that
                    # left every consumer paying the recursive walk.
                    from gedih3.gedidriver import write_soc_manifest
                    n_written = write_soc_manifest(soc_source, files=existing_h5)
                    if n_written:
                        logger.info(f"SOC manifest refreshed ({n_written} files)")

            # ── Resume shortcut: skip reconcile + extract on merge-resume ──
            # If a previous run finished extract and was killed during merge,
            # the on-disk log status is 'MERGING' and we have nothing to
            # re-extract. Calling _merge_and_finalize directly (which is
            # itself resume-aware via _merge_progress.txt + atomic .merge.tmp
            # rename) is sufficient.
            #
            # Detection (see _detect_merge_resume_signal):
            #   L1 (canonical): h3_logger.previous_status == 'MERGING'.
            #   L2 (fallback for builds started before this code shipped):
            #     <tmp>/partitions/_merge_progress.txt exists with ≥1 entry.
            #
            # Contract: between crash and resume, the SOC tree is treated as
            # frozen. Adding new HDF5s mid-cycle is unsupported on this path —
            # finish the current build first, then run a separate gh3_build
            # to incorporate new granules. (The shortcut also leaves PENDING
            # entries in granule_info that the next non-shortcut run cleans
            # up; gh3_doctor can flag/fix this if needed.)
            _parquet_dir = os.path.join(args.tmpdir, 'partitions')
            _merge_signal = _detect_merge_resume_signal(h3_logger, _parquet_dir)
            if _merge_signal:
                logger.info(
                    f"Resume shortcut: {_merge_signal} — skipping register/"
                    f"reconcile/extract, running merge only"
                )
                h3_logger.save_log('MERGING')
                try:
                    h3_files = _merge_and_finalize(_parquet_dir, args.output)
                except Exception as _e:
                    h3_logger.save_log('FAILED')
                    logger.error(f"Merge-resume failed: {_e}")
                    raise
                # Fold per-failed-merge granule flip-backs into the build
                # log so the next resume re-extracts the affected granules
                # instead of treating their (corrupt) parquet as canonical.
                # Idempotent + truncates the sidecar after fold.
                from gedih3.gh3builder import apply_merge_failures_to_logger
                _flipped = apply_merge_failures_to_logger(h3_logger, _parquet_dir)
                if _flipped:
                    logger.warning(
                        f"Flipped {_flipped} granule(s) INDEXED → MERGE_FAILED "
                        f"after corrupt-fragment merge failures. They will be "
                        f"re-extracted on the next resume."
                    )
                h3_logger.set_post_build_info()
                h3_logger.log_data.pop('_pending_variable_update', None)
                h3_logger.save_log('COMPLETED')
                _n = len(h3_files) if h3_files else 0
                print_success(
                    f"{_n} files exported to {args.output} (merge-only resume)",
                    logger=logger,
                )
                return

            # Save log only after validation/download passes — prevents
            # writing unverified products (e.g. L4C) when data is missing
            h3_logger.save_log('PARTITIONING')

            try:
                # Register granules being submitted for build as PENDING
                # Only for local download mode (-i); S3 mode has no local SOC directory
                if soc_source is not None and isinstance(soc_source, str) and os.path.isdir(soc_source):
                    logger.info("Listing SOC files for granule registration")
                    _soc_for_build = soc_file_tree(soc_source, to_list=True, exclude=args.exclude)
                    # GEDI filename: GEDInn_L_DATE_O{orbit}_{granule}_T{track}_PPDS_PGE_GEN_V{ver}.h5
                    # Parse orbit/granule/track from the basename directly — no
                    # GEDIFile() so we skip the unused os.path.getsize call per
                    # granule (matters on network filesystems).
                    _build_granules = []
                    for _soc in _soc_for_build:
                        fl = os.path.basename(list(_soc.values())[0]).split('_')
                        _build_granules.append({
                            'orbit': int(fl[3][1:]),
                            'granule': int(fl[4]),
                            'track': int(fl[5][1:]),
                        })
                    h3_logger.register_pending_granules(_build_granules)

                    # Resume reconciliation: scan h3 db AND tmp/partitions for
                    # granules already represented on disk and flip them to
                    # INDEXED before stage 1 starts. Without this, a kill
                    # during stage 2 leaves all granules PENDING and stage 1
                    # re-extracts everything on the next rerun.
                    _reconcile_granules_from_disk(
                        args.output, h3_logger,
                        tmp_dir=os.path.join(args.tmpdir, 'partitions'),
                    )

                    h3_logger.save_log('PROCESSING')

                h3_files = None

                # ── Stage 1: Spatial/temporal build ──────────────────────
                # Runs for: fresh build, resume, spatial/temporal expansion,
                # or mixed update Phase 1.
                # Skipped for: variable-only update, pending variable resume.
                if not pending_var_update and not is_variable_only_update:
                    if is_mixed_update:
                        stage1_products = {
                            k: val.get('variables')
                            for k, val in h3_logger.log_data.get('products', {}).items()
                        }
                        stage1_skip = None  # Re-examine all granules for new spatial area
                        logger.info("Mixed update Phase 1: expanding with existing products")
                    else:
                        stage1_products = h3_logger.get_product_vars()
                        stage1_skip = h3_logger.get_finished_granules()

                    h3_files = build_h3db(
                        product_vars=stage1_products,
                        soc_source=soc_source,
                        skip_granules=stage1_skip,
                        variable_only_update=False,
                        **_build_kwargs,
                    )

                # ── Stage 2: Variable update ─────────────────────────────
                # Runs for: variable-only update, mixed update Phase 2,
                # or pending variable resume from crash.
                if pending_var_update or is_variable_only_update or is_mixed_update:
                    if pending_var_update:
                        stage2_products = pending_var_update['product_vars']
                        logger.info("Resuming pending variable update")
                    else:
                        stage2_products = dict(h3_logger.new_product_vars)
                        if is_mixed_update:
                            h3_logger.set_post_build_info()
                            logger.info("Mixed update Phase 2: adding new variables")
                        h3_logger.log_data['_pending_variable_update'] = {
                            'product_vars': stage2_products,
                        }
                        h3_logger.save_log('PROCESSING')

                    h3_files_s2 = build_h3db(
                        product_vars=stage2_products,
                        soc_source=soc_source,
                        skip_granules=None,
                        variable_only_update=True,
                        **_build_kwargs,
                    )
                    h3_files = (h3_files or []) + (h3_files_s2 or [])

                # ── Finalize ─────────────────────────────────────────────
                # Fold per-failed-merge granule flip-backs into the build
                # log so the next resume re-extracts the affected granules
                # instead of treating their (corrupt) parquet as canonical.
                # Idempotent + truncates the sidecar after fold. Skipped
                # silently when no recoverable merge failures occurred.
                from gedih3.gh3builder import (
                    apply_merge_failures_to_logger, _read_granule_failures,
                )
                _parquet_dir_for_fold = os.path.join(args.tmpdir, 'partitions')
                _flipped = apply_merge_failures_to_logger(h3_logger, _parquet_dir_for_fold)
                if _flipped:
                    logger.warning(
                        f"Flipped {_flipped} granule(s) INDEXED → MERGE_FAILED "
                        f"after corrupt-fragment merge failures. They will be "
                        f"re-extracted on the next resume."
                    )
                # Advisory: surface Stage 1 failure classes with actionable
                # recovery recipes. The granule failure sidecar persisted by
                # _append_granule_failure carries structured cause records,
                # which we group here so the operator sees a single summary
                # with the exact gh3_build command to fix each class.
                _gran_failures = _read_granule_failures(_parquet_dir_for_fold)
                if _gran_failures:
                    from collections import Counter
                    _by_class = Counter(
                        (f.get('kind'), f.get('product'), f.get('var'))
                        for f in _gran_failures
                    )
                    logger.warning(
                        f"{len(_gran_failures)} (granule × beam) task(s) failed "
                        f"Stage 1. Breakdown:"
                    )
                    for (kind, product, var), count in _by_class.most_common():
                        if kind == 'missing_var':
                            logger.warning(
                                f"  {count}x missing_var: {product or '<product>'} "
                                f"variable '{var or '<var>'}' absent from HDF5. "
                                f"Recovery: re-run gh3_build with an explicit "
                                f"-l{(product or 'XX').lower()[1:]} list that omits "
                                f"'{var}' (and any other listed vars on this line)."
                            )
                        else:
                            logger.warning(f"  {count}x {kind} ({product or 'N/A'})")
                h3_logger.set_post_build_info()
                # Clear pending variable update BEFORE saving — ensures the flag
                # is not persisted to disk after successful completion.
                # Crash safety: if crash between pop and save, disk still has
                # PROCESSING status with the flag → next run resumes correctly.
                h3_logger.log_data.pop('_pending_variable_update', None)
                h3_logger.save_log('COMPLETED')

                n_files = len(h3_files) if h3_files else 0
                print_success(f"{n_files} files exported to {args.output}", logger=logger)

                # Regime C: Stage 2 ran from a `default` re-expansion. Any vars
                # not present in the source HDF5 files were silently written
                # as NaN columns. gh3_doctor's backfill diagnosis flags these.
                if (is_variable_only_update or is_mixed_update) and h3_logger.new_product_vars and (
                    h3_logger.default_products & set(h3_logger.new_product_vars)
                ):
                    logger.info(
                        "New variables added via `default`; run "
                        "`gh3_doctor --check backfill` if you suspect any are "
                        "not present in source HDF5 files."
                    )

                if soc_source is not None and args.download:
                    logger.info(f"Note: Downloaded HDF5 files in {soc_source} are no longer needed and can be deleted to free disk space")

            except Exception as e:
                h3_logger.save_log('FAILED')
                logger.error(f"Build failed: {e}")
                raise e

    except KeyboardInterrupt:
        logger.warning("\nBuild interrupted by user")
        h3_logger.set_post_build_info()
        h3_logger.save_log('INTERRUPTED')
        sys.exit(130)

    except Exception as e:
        from gedih3.exceptions import (
            H3ValidationError,
            GediFileError,
            GediDatabaseError,
            GediError
        )

        if isinstance(e, H3ValidationError):
            logger.error(f"H3 parameter error: {e}")
            sys.exit(2)
        elif isinstance(e, GediFileError):
            logger.error(f"File error: {e}")
            sys.exit(3)
        elif isinstance(e, GediDatabaseError):
            logger.error(f"Database error: {e}")
            sys.exit(4)
        elif isinstance(e, GediError):
            logger.error(f"GEDI error: {e}")
            sys.exit(1)
        else:
            logger.error(f"Unexpected error: {type(e).__name__}: {e}")
            if args.verbose >= 2:
                import traceback
                traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    main()
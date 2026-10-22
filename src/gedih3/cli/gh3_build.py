#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

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
        (``*``/``all``) and is skipped. Preset-sourced lists (``default``,
        ``minimal``, ``all``/``*``) are identified by ``default_products``
        and skipped — their names come from an internal manifest/table, not
        the user, so a mismatch here is not a typo to catch.
    default_products : set
        Products to exempt from this check. Despite the name, callers pass
        the full preset set (e.g. ``H3BuildLogger.preset_products``), not
        just literal ``default`` requests — see that attribute's docstring.
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


def preflight_var_specs(h3_logger, product_vars):
    """Split requested variables between gh3_build's two pre-flight checks.

    Returns ``(manifest_specs, sample_specs)``:

    - ``manifest_specs``: the ``default`` preset of each
      :func:`manifest_check_scope` product, to validate against the shipped
      static manifest: exactly what the manifest vouches for. The requested
      list can hold other names too, given next to the preset
      (``H3BuildLogger.preset_extra_vars``) or, on an update, recorded by the
      database earlier. Those are not the manifest's and would always read
      as missing.
    - ``sample_specs``: what :func:`explicit_vars_missing_in_sample` checks
      against one sample HDF5 per product: every explicit list, plus the
      names given next to a preset. Pass it with an empty exemption set.

    Pure over logger state, like :func:`manifest_check_scope`.
    """
    from gedih3.gedidriver import gedi_vars_expand

    extras = getattr(h3_logger, 'preset_extra_vars', None) or {}
    manifest_specs = {p: ['default'] for p in manifest_check_scope(h3_logger, product_vars)}
    if manifest_specs:
        gedi_vars_expand(manifest_specs, version=h3_logger.gedi_version)
    sample_specs = {
        p: v for p, v in (product_vars or {}).items()
        if v is not None and p not in h3_logger.preset_products
    }
    sample_specs.update({p: list(names) for p, names in extras.items() if p in (product_vars or {})})
    return manifest_specs, sample_specs

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

    # H3 configuration. Default is None (not 12 / 3) so the logger can
    # distinguish "user-passed" from "fell-back-to-default" — the latter
    # is silently filled in for new builds (defaults: 12 / 3), the former
    # is strictly validated against the existing log on resume to refuse
    # silent layout drift. See H3BuildLogger.__init__.
    p.add_argument("-h3r", "--h3-resolution", dest="h3_resolution", type=int, default=None,
                   help="H3 index level [0-15, default=12 on fresh build; ignored on resume - must match existing log]")
    p.add_argument("-h3p", "--h3-partition", dest="h3_partition", type=int, default=None,
                   help="H3 partition level [0-15, default=3 on fresh build; ignored on resume - must match existing log]")

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
    p.add_argument("--no-bbox-index", dest="bbox_index", action='store_false',
                   default=True,
                   help="skip (re)building the _bbox_index.parquet sidecar after a "
                        "successful build (footer-only scan; spatial queries use it "
                        "to skip files a region/EGI tile cannot touch)")
    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="download from NASA S3 to temp directory (no persistent local download)")
    p.add_argument("--gedi-version", dest="version", type=int, default=None,
                   help="GEDI data release for every product (e.g. 3). A database is single-version: "
                        "on resume the build log's version is authoritative (passing a different one "
                        "is an error). Fresh local builds default to the release already in the SOC "
                        "directory (download log / filenames), else the package default (v3).")
    p.add_argument("--exclude", dest="exclude", action='append', default=None,
                   metavar='PATTERN',
                   help="exclude files whose basename matches the given fnmatch pattern. "
                        "Repeat the flag for multiple patterns. Remembered in the build log: later "
                        "runs, variable adds and gh3_doctor exclude the same files. "
                        "Example: --exclude '*_SGS.h5' --exclude '*_BETA.h5'")
    p.add_argument("--allow-missing-products", dest="allow_missing_products",
                   action=argparse.BooleanOptionalAction, default=None,
                   help="admit granules that have L2A but lack later products (L2B, L4A, L4C...) "
                        "not published yet. Their rows store those columns as null; a later "
                        "gh3_build fills them automatically once the product files appear "
                        "(or run: gh3_doctor --fix backfill). Remembered in the build log; "
                        "--no-allow-missing-products turns it off. Needs a local SOC directory.")
    p.add_argument("--replace-granules", dest="replace_granules", type=str, default=None, metavar='SPEC',
                   help="re-index granules that NASA re-released within the database's GEDI version: "
                        "their rows are replaced by the new files' rows. SPEC is 'auto' (every indexed "
                        "granule with a newer release in the SOC directory, as gh3_doctor --check "
                        "superseded_granules lists them), granule keys (O12345_01_T00123, comma-separated) "
                        "or a file of them, one per line. Runs on its own, on a completed database with a "
                        "local SOC directory; an interrupted run is resumed by the next gh3_build.")

    # Dask and verbosity
    add_dask_args(p, profile='build')
    add_verbosity_args(p)

    return p.parse_args()


def _local_soc_files(soc_source, h3_logger, exclude=None):
    """HDF5 files of the database's GEDI release under a local SOC directory.

    A serial glob, for the early-exit path, which has no dask Client. Files
    matching an ``exclude`` pattern (``gh3_build --exclude``) are left out.
    """
    import os
    import fnmatch
    import glob as globmod
    from gedih3.gedidriver import gedi_file_glob

    if not soc_source or not os.path.isdir(soc_source):
        return []
    pattern = gedi_file_glob(version=h3_logger.gedi_version) if h3_logger.gedi_version else 'GEDI*.h5'
    return [f for f in globmod.glob(os.path.join(soc_source, '**', pattern), recursive=True)
            if not (exclude and any(fnmatch.fnmatch(os.path.basename(f), pat) for pat in exclude))]


def _warn_superseded_granules(h3_logger, soc_entries, logger):
    """Warn about indexed granules whose source is not the listed release (#47).

    ``soc_entries``: ``{product: path}`` dicts, see
    :meth:`H3BuildLogger.superseded_releases` (log-only, against the recorded
    ``releases``). Reports, never re-indexes, and never fails a build: an
    error is logged at debug level.
    """
    from gedih3.exceptions import GediError
    try:
        found = h3_logger.superseded_releases(soc_entries)
    except (GediError, ValueError, OSError) as e:
        logger.debug(f"Superseded-release check skipped: {type(e).__name__}: {e}")
        return None

    def _example(side):
        key, prods = next(iter(sorted(found[side].items())))
        product, (recorded, listed) = next(iter(sorted(prods.items())))
        return f"O{key[0]:05d}_{key[1]:02d}_T{key[2]:05d} {product} {recorded} -> {listed}"

    if found['newer']:
        logger.warning(
            f"{len(found['newer'])} granule(s) were indexed from an older release than the SOC tree now holds "
            f"(e.g. {_example('newer')}). Their rows are not re-indexed, while variables added and products "
            f"filled later read the newer file. Re-index them with gh3_build --replace-granules auto, or move "
            f"the newer files out of the SOC tree. List them with gh3_doctor --check superseded_granules."
        )
    if found['older']:
        logger.warning(
            f"{len(found['older'])} granule(s) were indexed from a release the SOC tree no longer holds: only an "
            f"older one is listed (e.g. {_example('older')}). The indexed rows are kept and never replaced by an "
            f"older release, but variables added and products filled later would read it. Restore the file the "
            f"granule was read from."
        )
    return found


def _adopt_row_releases(h3_logger, h3_dir, logger):
    """Read, once, the releases of granules indexed before gedih3 recorded them (#67).

    A database built before 0.20.10 records no release per granule, so a
    re-release of one could not be found. Its rows name their source files
    (``root_file_<prod>``): ``releases_from_rows`` reads those columns on the
    workers and the log adopts them (in memory; the caller saves it). Needs a
    registered dask Client. Never fails the run: on an error the granules
    stay unknown, as before. Returns whether the log changed (it is marked
    so the scan runs once, even when the rows gave nothing).
    """
    from gedih3.exceptions import GediError
    from gedih3.gh3builder import releases_from_rows, _granule_key_str
    if not h3_logger.needs_row_releases():
        return False
    columns = set(h3_logger.log_data.get('h3_columns') or [])
    products = [p for p in h3_logger.product_vars or {} if f"root_file_{p.lower()}" in columns]
    logger.info("Reading the source releases of granules indexed before gedih3 recorded them, from their rows' "
                "root_file columns (once per database)")
    try:
        releases, mixed = releases_from_rows(h3_dir, products)
    except (GediError, OSError) as e:
        logger.warning(f"Could not read the source releases from the rows ({type(e).__name__}: {e}); granules "
                       f"indexed before gedih3 recorded releases are not checked for re-releases")
        return False
    n = h3_logger.adopt_row_releases(releases)
    logger.info(f"Recorded {n} source release(s) read from the rows")
    if mixed:
        key, prods = next(iter(sorted(mixed.items())))
        logger.warning(
            f"{len(mixed)} granule(s) have rows from more than one release of a product (e.g. {_granule_key_str(key)} "
            f"{', '.join(f'{p}: {t}' for p, t in sorted(prods.items()))}), as a replacement left half done would: "
            f"their release stays unknown. Re-index them by name with gh3_build --replace-granules <key>")
    return True


def _soc_files_by_product(files):
    """``{product: path}`` per GEDI file, for :func:`_warn_superseded_granules`."""
    import os
    import re
    out = []
    for f in files:
        m = re.match(r'GEDI0(\d)_([A-Z])_', os.path.basename(f))
        if m:
            out.append({f"L{m.group(1)}{m.group(2)}": f})
    return out


def _parse_replace_granules(spec):
    """Granule keys named by ``--replace-granules``; ``None`` for ``auto``.

    Accepts ``O12345_01_T00123`` keys or GEDI file names, comma- or
    whitespace-separated, inline or in a file. An empty spec names none.
    """
    import os
    import re
    from gedih3.exceptions import GediValidationError
    spec = spec.strip()
    if spec.lower() == 'auto':
        return None
    if re.search(r'_O\d+_\d{2}_T\d+_', os.path.basename(spec)) or re.fullmatch(r'O\d+_\d{2}_T\d+', spec):
        text = spec                                  # a GEDI file name or path, or a key: not a keys file
    elif os.path.isfile(spec):
        try:
            with open(spec) as f:
                text = f.read()
        except UnicodeDecodeError:
            raise GediValidationError(f"--replace-granules: {spec} is not a text file of granule keys")
    elif os.sep in spec or spec.endswith(('.txt', '.csv', '.lst')):
        raise GediValidationError(f"--replace-granules: file not found: {spec}")
    else:
        text = spec
    keys = set()
    for token in re.split(r'[\s,]+', text.strip()):
        if not token:
            continue
        m = re.fullmatch(r'O(\d+)_(\d{2})_T(\d+)', token) or re.search(r'_O(\d+)_(\d{2})_T(\d+)_', token)
        if m is None:
            raise GediValidationError(f"--replace-granules: {token!r} is not a granule key (O12345_01_T00123) "
                                      f"or a GEDI file name")
        keys.add((int(m.group(1)), int(m.group(2)), int(m.group(3))))
    return keys


def _replace_granules(args, h3_logger, soc_source, logger, tmp_dir):
    """Run, or resume, a granule replacement, then exit (#47).

    Errors map to exit codes as in :func:`main`: 2 for a refused request, 4
    for a failed merge, 1 for any other GEDI error, 130 on an interrupt (the
    next gh3_build resumes it). See :func:`_run_granule_replacement`.
    """
    import sys
    from gedih3.exceptions import GediError, GediValidationError, GediDatabaseError
    try:
        _run_granule_replacement(args, h3_logger, soc_source, logger, tmp_dir)
    except KeyboardInterrupt:
        logger.warning("\nRe-indexing interrupted: the next gh3_build resumes it")
        sys.exit(130)
    except GediValidationError as e:
        logger.error(str(e))
        sys.exit(2)
    except GediDatabaseError as e:
        logger.error(str(e))
        sys.exit(4)
    except GediError as e:
        logger.error(str(e))
        sys.exit(1)


def _exclude_changed(h3_logger):
    """Whether this run's ``--exclude`` adds patterns the build log does not keep yet."""
    return h3_logger.exclude != list(h3_logger.log_data.get('exclude') or [])


def _run_granule_replacement(args, h3_logger, soc_source, logger, tmp_dir):
    """Re-index granules from their new release, or resume that (#47).

    Targets come from ``--replace-granules`` (``auto``: the granules with a
    newer release listed, :meth:`H3BuildLogger.superseded_releases`), or from
    the journal of an unfinished replacement under ``tmp_dir`` (the build
    log's ``_pending_replacement``, whatever this run's ``--tmpdir``). A
    target needs a listed file for every product of the database, and is
    never replaced by an older release than the one it was read from. The
    build log takes the new releases once the rows are in; the scratch
    directory goes last.
    """
    import os
    import shutil
    from gedih3.config import BUILD_LOG_FILENAME
    from gedih3.cliutils import dask_client, parse_dask_args, print_success
    from gedih3.exceptions import GediValidationError
    from gedih3.gedidriver import soc_file_tree, gedi_release_rank, gedi_release_tag
    from gedih3.gh3builder import (_replace_granules_build, read_replacement_journal, replacement_abandon_hint,
                                   replacement_journal, check_replacement_sources,
                                   _granule_id_from_l2a_path, _granule_key_str, _REPLACE_DIRNAME, _REPLACE_MERGING)

    if not os.path.isfile(os.path.join(args.output, BUILD_LOG_FILENAME)):
        raise GediValidationError(f"No database at {args.output}: --replace-granules re-indexes granules of an "
                                  f"existing, completed database")
    rtmp = os.path.join(tmp_dir, _REPLACE_DIRNAME)
    journal = read_replacement_journal(tmp_dir)
    if args.s3 or args.download or not (isinstance(soc_source, str) and os.path.isdir(soc_source)):
        raise GediValidationError("Re-indexing granules needs a local SOC directory (-i). For an S3-built "
                                  "database, download the new files with gh3_download first.")
    if (args.allow_missing_products is not None
            or any(getattr(h3_logger, a, None) is not None for a in ('new_spatial', 'new_temporal', 'new_product_vars'))):
        raise GediValidationError(
            "An unfinished re-indexing must finish first: run gh3_build without region, date, variable or "
            "--allow-missing-products options" if journal is not None else
            "Re-index granules in a run of its own: leave out region, date, variable and "
            "--allow-missing-products changes, and apply them in another gh3_build")
    if journal is None and h3_logger.previous_status != 'COMPLETED':
        raise GediValidationError(f"The database has an unfinished build (status {h3_logger.previous_status}): "
                                  f"run gh3_build to finish it before re-indexing granules")
    requested = _parse_replace_granules(args.replace_granules) if args.replace_granules is not None else None
    if journal is not None and requested is not None and requested != set(journal):
        raise GediValidationError(f"A re-indexing of {len(journal)} other granule(s) is unfinished: run gh3_build "
                                  f"to finish it first, or {replacement_abandon_hint(tmp_dir)}")

    index = {(g['orbit'], g['granule'], g['track']): g for g in h3_logger.granule_info or []}
    targets, skipped = {}, []
    if journal is not None:
        def _recorded(key, files):
            return (index.get(key) or {}).get('releases') == {p: gedi_release_tag(n) for p, n in files.items()}
        if (h3_logger.previous_status == 'COMPLETED' and os.path.exists(os.path.join(rtmp, _REPLACE_MERGING))
                and all(_recorded(k, f) for k, f in journal.items())):
            # Finished and logged; only the cleanup was cut short.
            if _exclude_changed(h3_logger):
                h3_logger.save_log('COMPLETED')
            shutil.rmtree(rtmp, ignore_errors=True)
            logger.info("The re-indexing started earlier had finished; removed its scratch files")
            return
        logger.info(f"Resuming the re-indexing of {len(journal)} granule(s) started earlier")
        for key, files in journal.items():
            # The journal's exact files, by the SOC tree layout (<year>/<doy>/):
            # a release listed since then does not stand in for them.
            targets[key] = {}
            for product, name in files.items():
                stamp = name.split('_')[2]
                path = os.path.join(soc_source, stamp[:4], stamp[4:7], name)
                if not os.path.isfile(path):             # a tree laid out otherwise: look it up
                    import glob
                    path = next(iter(glob.glob(os.path.join(soc_source, '**', name), recursive=True)), path)
                if not os.path.isfile(path):
                    raise GediValidationError(f"The unfinished re-indexing reads {name}, which is no longer in the "
                                              f"SOC directory: restore it and re-run, or "
                                              f"{replacement_abandon_hint(tmp_dir)}")
                targets[key][product] = path

    product_vars = h3_logger.get_product_vars()
    products = set(product_vars)
    with dask_client(**parse_dask_args(args)):
        if journal is None and _adopt_row_releases(h3_logger, args.output, logger):
            h3_logger.save_log('COMPLETED')                  # the run needs a completed database
        if journal is None:
            listing = soc_file_tree(soc_source, to_list=True, require_all=False, exclude=args.exclude,
                                    glob_kwargs={'version': h3_logger.gedi_version})
            by_key = {}
            for entry in listing:
                key = _granule_id_from_l2a_path(next(iter(entry.values())))  # any product's name holds the key
                if key is not None:
                    by_key[key] = entry
            if requested is None:
                requested = set(h3_logger.superseded_releases(listing)['newer'])
            for key in sorted(requested):
                g, entry = index.get(key), by_key.get(key) or {}
                recorded = (g or {}).get('releases') or {}
                if g is None or g.get('status') not in ('INDEXED', 'NO_DATA'):
                    skipped.append((key, 'not indexed in this database'))
                elif products - set(entry):
                    skipped.append((key, f"no file listed for {', '.join(sorted(products - set(entry)))}"))
                elif any(p in recorded and gedi_release_rank(gedi_release_tag(entry[p])) < gedi_release_rank(recorded[p])
                         for p in products):
                    skipped.append((key, 'only an older release than the indexed one is listed'))
                else:
                    targets[key] = {p: entry[p] for p in products}
            for key, why in skipped[:5]:
                logger.warning(f"Not re-indexing {_granule_key_str(key)}: {why}")
            if len(skipped) > 5:
                logger.warning(f"... and {len(skipped) - 5} more granule(s) not re-indexed")
            if not targets:
                if _exclude_changed(h3_logger):
                    h3_logger.save_log('COMPLETED')     # remember --exclude, as an up-to-date run does
                logger.info("No granule to re-index")
                return

        check_replacement_sources(args.output, targets, tmp_dir=tmp_dir)
        logger.info(f"Re-indexing {len(targets)} granule(s) from their new source files")
        h3_logger.log_data['_pending_replacement'] = tmp_dir
        _status_before = h3_logger.log_data.get('status') or 'COMPLETED'
        h3_logger.save_log('PROCESSING')
        try:
            result = _replace_granules_build(
                args.output, targets, product_vars, h3_logger.res, h3_logger.part,
                spatial=h3_logger.get_spatial(), version=h3_logger.gedi_version, tmp_dir=tmp_dir)
        except GediValidationError:
            # Refused before anything was journaled or written (e.g. a target
            # whose rows record no source, #68): the log is as it was.
            if not os.path.exists(replacement_journal(tmp_dir)):
                h3_logger.log_data.pop('_pending_replacement', None)
                h3_logger.save_log(_status_before)
            raise

        # The rows are in: the granules take their new releases (the one place
        # a recorded release is replaced), and their product statuses are rebuilt.
        for g in h3_logger.granule_info:
            key = (g['orbit'], g['granule'], g['track'])
            if key in result['replaced']:
                g['releases'] = {p: tag for p, f in targets[key].items() if (tag := gedi_release_tag(f))}
                g.pop('products', None)
                g.pop('fill_attempts', None)
                g['status'] = 'NO_DATA' if key in result['empty'] else 'INDEXED'
        h3_logger.set_post_build_info()
        h3_logger.update_action = 'granule_replacement'
        h3_logger.log_data.pop('_pending_replacement', None)
        h3_logger.save_log('COMPLETED')
        shutil.rmtree(rtmp, ignore_errors=True)

        from gedih3.gh3driver import refresh_bbox_index_after_build
        refresh_bbox_index_after_build(args.output, enabled=args.bbox_index, logger=logger)
    if os.path.exists(os.path.join(args.output, 'gedi.ducklake')):
        logger.warning(f"Re-indexed rows changed files the DuckLake catalog caches; rebuild it: "
                       f"gh3_build_ducklake -d {args.output}")
    print_success(
        f"{len(result['replaced'])} granule(s) re-indexed from their new release"
        + (f" ({len(result['empty'])} with no row left in the region, now NO_DATA)" if result['empty'] else "")
        + ". Datasets extracted and rasters made from this database earlier are out of date.",
        logger=logger)


def _has_new_local_granules(soc_source, h3_logger, exclude=None, files=None):
    """Check if SOC directory has HDF5 files not tracked in the build log,
    or a product file for an indexed granule still waiting for it.

    Only files of the database's GEDI release count: a tree that holds
    another release side by side must not make a finished database look
    stale (those granules can never be built into it). The granule key and
    product come from the file name — no stat, no HDF5 open. Files matching an
    ``exclude`` pattern (``gh3_build --exclude``) never count. ``files``: a
    listing already made by :func:`_local_soc_files`, to reuse.
    """
    import os
    import re
    from gedih3.gh3builder import _granule_id_from_l2a_path

    if not soc_source or not os.path.isdir(soc_source):
        return False

    tracked = set()
    if hasattr(h3_logger, 'granule_info') and h3_logger.granule_info:
        for g in h3_logger.granule_info:
            tracked.add((g['orbit'], g['granule'], g['track']))
    awaiting = h3_logger.pending_product_fills() if hasattr(h3_logger, 'pending_product_fills') else {}

    if files is None:
        files = _local_soc_files(soc_source, h3_logger, exclude=exclude)
    for f in files:
        key = _granule_id_from_l2a_path(f)
        if key is None:
            continue
        if key not in tracked:
            return True
        if key in awaiting:
            m = re.match(r'GEDI0(\d)_([A-Z])_', os.path.basename(f))
            if m and f"L{m.group(1)}{m.group(2)}" in awaiting[key]:
                return True

    return False


def _count_merge_failures(parquet_dir):
    """Number of partition merges still failed, from the persisted sentinels.

    ``_merge_and_finalize`` writes one ``_merge_failures/`` sentinel per failed
    merge and the next merge's pre-clean consumes them, so any sentinel left
    at the end of a run is an unresolved failure. O(N_failures), no tree walk.
    """
    from gedih3.gh3builder import _scan_merge_failure_sentinels
    return len(_scan_merge_failure_sentinels(parquet_dir))


def _exit_merge_incomplete(n_failed, parquet_dir, h3_dir, logger):
    """Report a build whose merge phase did not finish and exit non-zero.

    The build log is left at ``MERGING`` so the next ``gh3_build`` resumes
    straight into the merge; the unmerged fragments stay in ``parquet_dir``.
    Exit code 4 is the CLI's database-error code.
    """
    logger.error(
        f"{n_failed} partition merge(s) failed: their new data is kept in {parquet_dir} and is NOT in "
        f"the database yet. Fix the cause shown in the 'Merge failed' lines above, then re-run the "
        f"same gh3_build command to retry only the merge. Details: "
        f"gh3_doctor -i {h3_dir} -t {parquet_dir} --check tmp_partitions_health"
    )
    import sys
    sys.exit(4)


def _detect_merge_resume_signal(h3_logger, parquet_dir, *, has_pending_new_work=False):
    """Return a human-readable signal name if the resume should skip extract
    and go straight to merge, else return None.

    L1 (canonical): the on-disk log says ``status == 'MERGING'``. This is set
    by ``build_h3db`` immediately before the merge phase, so seeing it on
    resume proves the previous run finished extract.

    L2 (fallback for older builds): ``<parquet_dir>/_merge_progress.txt``
    exists with ≥1 non-empty line. A non-empty merge-progress file proves
    that merge already started, which in turn proves extract finished.

    The two signals are independent — either is sufficient.

    **Veto: MERGE_FAILED granules.** If the build log contains any granule
    in ``MERGE_FAILED`` status, the shortcut is suppressed even when L1/L2
    fire. Those granules' partitions failed merge on a prior run with a
    corrupt-fragment error — they were flipped to ``MERGE_FAILED`` by
    ``apply_merge_failures_to_logger`` so that Stage 1 would re-extract
    them on the next resume. Taking the merge-only shortcut would skip the
    extract pass and re-attempt the same merge against the same bad
    fragments, looping forever. The full path is what closes the loop:
    ``build_h3db``'s pre-clean reopens the lost (granule × beam) tasks,
    Stage 1 re-writes those fragments from source HDF5 and the merge takes
    them, then ``_release_merge_failed`` hands the granules back to
    ``set_post_build_info`` (``MERGE_FAILED → PENDING → INDEXED``).

    **Veto: pending new product/variable work.** ``has_pending_new_work``
    is true when the caller has detected a variable-only update, a mixed
    update's Phase 2, or a crash-recovered pending variable update --
    i.e. there are products/variables requested that have never been
    through Stage 1/2 for this database. Neither L1 nor L2 proves anything
    about that new work: ``_merge_progress.txt`` and the ``MERGING`` status
    are written by the plain Stage-1 build-and-merge cycle, not by
    ``_build_add_variables``'s separate ``_var_merge_progress.txt``
    tracking. A leftover from an earlier, already-completed build in the
    same tmp directory would otherwise be misread as "this request's merge
    already started" -- silently skipping extract entirely for the new
    product while `_merge_and_finalize` finds nothing new to do and still
    reports success. The build log then records the product as added even
    though not one of its columns was ever written.
    """
    import os
    if has_pending_new_work:
        return None
    granule_info = getattr(h3_logger, 'granule_info', None) or []
    if any(g.get('status') == 'MERGE_FAILED' for g in granule_info):
        return None
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
    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_SOC_DIR, BUILD_LOG_FILENAME
    from gedih3.cliutils import parse_gedi_args, parse_dask_args, parse_region, setup_logging, print_banner, print_success, format_dask_cluster_info
    from gedih3.utils import get_system_resources
    from gedih3.gh3builder import build_h3db, download_soc, soc_file_tree, _reconcile_granules_from_disk, _merge_and_finalize
    from gedih3.gedidriver import GEDIFile, validate_soc_files, gedi_vars_expand
    from gedih3.logger import H3BuildLogger, SOCDownloadLogger, resolve_soc_version
    from gedih3.cliutils import dask_client

    # Setup logging and print banner
    logger = setup_logging(args, __name__)
    print_banner("GEDI H3 Database Builder Tool", logger=logger)

    if args.output is None:
        args.output = GH3_DEFAULT_H3_DIR
    args.output = os.path.abspath(args.output)
    os.makedirs(args.output, exist_ok=True)

    if args.tmpdir is None:
        args.tmpdir = os.path.join(args.output, '.tmp')
    args.tmpdir = os.path.abspath(args.tmpdir)
    os.makedirs(args.tmpdir, exist_ok=True)

    if args.indir:
        args.indir = os.path.abspath(args.indir)

    # Log detected resources. ``Driver host`` makes it explicit that the
    # CPUs/RAM are local to this process — an external ``--dask-scheduler``
    # cluster may live elsewhere. The cluster's actual workers/threads/RAM
    # are reported separately by the ``Dask config`` line below, after the
    # Client connects and we can query ``client.scheduler_info()``.
    cpus, ram, storage = get_system_resources(disk_path=args.output)
    logger.info(f"Driver host: {cpus} CPUs, {ram:.1f} GB RAM | Disk: {storage:.1f} GB free at {args.output}")
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

    # Fresh build from a local SOC tree with no --gedi-version: pin to the
    # release already on disk (download log, else the first file's _V00N
    # field) instead of the package default, so `default`/`minimal`
    # expansion and the per-version SOC glob match the files that exist.
    # On resume the build log is authoritative (H3BuildLogger peeks it).
    if (args.version is None and soc_source is not None
            and not os.path.isfile(os.path.join(args.output, BUILD_LOG_FILENAME))):
        detected = resolve_soc_version(soc_source)
        if detected is not None:
            args.version = detected
            logger.info(f"GEDI version {detected} detected in {soc_source}; building that release")

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

    if not h3_logger.product_vars and not h3_logger.updating and args.replace_granules is None:
        raise ValueError(
            "No GEDI product selected - please select at least one of "
            "--l2a, --l2b, --l4a, --l4c (or use --detail-level for all four), "
            "and/or --l1b for waveform data"
        )
    # Phased updates: the explicit flag wins, else the database's persisted choice.
    if args.allow_missing_products is not None:
        h3_logger.allow_missing_products = args.allow_missing_products
    elif h3_logger.allow_missing_products:
        logger.info("Phased updates are on for this database (build log): granules with L2A are indexed "
                    "before their later products; --no-allow-missing-products turns it off")
    # Source exclusions: the build log keeps every pattern ever passed, and this
    # run applies them all. Leaving --exclude out never re-admits a file the
    # database was built without (an _SGS variant would otherwise outrank it).
    _stored_exclude = list(h3_logger.exclude)
    h3_logger.exclude = list(dict.fromkeys(_stored_exclude + (args.exclude or [])))
    if set(_stored_exclude) - set(args.exclude or []):
        logger.info(f"Source files matching {', '.join(_stored_exclude)} are excluded for this database "
                    f"(build log); remove a pattern from the log's 'exclude' to admit them again")
    if _added_exclude := [p for p in h3_logger.exclude if p not in _stored_exclude]:
        logger.info(f"Source files matching {', '.join(_added_exclude)} are excluded: once this run saves the "
                    f"build log, every later run excludes them too")
    args.exclude = h3_logger.exclude or None
    # This run's choice; the logger attribute is what the log keeps.
    allow_missing = h3_logger.allow_missing_products
    if allow_missing and args.s3:
        if args.allow_missing_products:
            logger.error("--allow-missing-products needs a local SOC directory (-i, or --download); "
                         "S3 ETL mode builds complete granules only")
            sys.exit(2)
        allow_missing = False
        logger.info("S3 ETL mode builds complete granules only; phased updates skipped for this run")
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

    # Re-indexing re-released granules (#47): a run of its own, opted in with
    # --replace-granules and resumed by any later run until it finishes.
    # The journal's tmp dir comes from the log, so a run with another --tmpdir
    # still finds it. Marked but never journaled: nothing was touched.
    from gedih3.gh3builder import replacement_journal
    _replace_tmp = h3_logger.log_data.get('_pending_replacement') or args.tmpdir
    if not os.path.exists(replacement_journal(_replace_tmp)):
        h3_logger.log_data.pop('_pending_replacement', None)
    if args.replace_granules is not None or os.path.exists(replacement_journal(_replace_tmp)):
        return _replace_granules(args, h3_logger, soc_source, logger, _replace_tmp)

    # Early exit: database is already up-to-date with requested parameters
    # Only when a valid build log exists with partition data to confirm completeness
    # A previous run whose merges failed leaves status MERGING with its
    # fragments in tmp/partitions; declaring that database up to date would
    # strand them, so it always goes on to the merge-resume path below.
    if (h3_logger.is_up_to_date()
            and not pending_var_update
            and h3_logger.previous_status != 'MERGING'
            and hasattr(h3_logger, 'h3_partition_ids')
            and h3_logger.h3_partition_ids):
        # S3/download modes: skip early exit — let the pipeline query CMR
        # or run download_soc() to discover new NASA granules
        if not args.s3 and not args.download:
            # Local mode: check SOC directory for new HDF5 files
            _local_files = _local_soc_files(soc_source, h3_logger, exclude=args.exclude)
            if not _has_new_local_granules(soc_source, h3_logger, files=_local_files):
                _flag_changed = ((args.allow_missing_products is not None
                                  and args.allow_missing_products != bool(h3_logger.log_data.get('allow_missing_products')))
                                 or _exclude_changed(h3_logger))
                if h3_logger.previous_status != 'COMPLETED':
                    h3_logger.set_post_build_info()
                    h3_logger.save_log('COMPLETED')
                elif _flag_changed:
                    h3_logger.save_log('COMPLETED')  # remember --[no-]allow-missing-products, --exclude
                logger.info("Database is already up-to-date with requested parameters")
                if _local_files and h3_logger.needs_row_releases():
                    with dask_client(**parse_dask_args(args)):
                        if _adopt_row_releases(h3_logger, args.output, logger):
                            h3_logger.save_log('COMPLETED')
                if _local_files:
                    _warn_superseded_granules(h3_logger, _soc_files_by_product(_local_files), logger)
                # Up-to-date DB: create the index only if it is missing
                # (e.g. first run after upgrading gedih3) — the data is
                # unchanged, so an existing index remains valid. No client
                # exists yet on this early-exit path, so honor -N with a
                # short-lived one instead of a silent thread fallback.
                from gedih3.config import BBOX_INDEX_FILENAME
                from gedih3.gh3driver import refresh_bbox_index_after_build
                if args.bbox_index and not os.path.exists(
                        os.path.join(args.output, BBOX_INDEX_FILENAME)):
                    logger.info("Creating missing bbox index (footer-only scan)")
                    with dask_client(**parse_dask_args(args)):
                        refresh_bbox_index_after_build(args.output, logger=logger)
                print_success("Database is up-to-date, no changes needed", logger=logger)
                return
            else:
                logger.info("New granules or awaited product files detected in SOC directory — updating database")

    _awaiting = h3_logger.pending_product_fills()
    if _awaiting:
        _n_prod = sum(len(v) for v in _awaiting.values())
        logger.info(f"{len(_awaiting)} indexed granule(s) await {_n_prod} product(s) published after them; "
                    f"they are filled when the product files appear")

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

    # Set right after each final save_log('COMPLETED'): a Ctrl-C during the
    # post-completion bbox-index build must not downgrade a finished build.
    build_completed = False

    try:
        with dask_client(**dask_kwargs) as client:
            warnings.filterwarnings("ignore", message=r"Sending large graph of size.*", category=UserWarning, module="distributed.client")
            def _suppress_pandas_perf_warnings():
                import warnings
                import pandas as pd
                warnings.filterwarnings("ignore", message=r"DataFrame is highly fragmented.*", category=pd.errors.PerformanceWarning)

            client.run(_suppress_pandas_perf_warnings)

            logger.info(f"Dask config: {format_dask_cluster_info(client)}")
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
                    # ── Stage 1: default-products vs shipped manifest ──
                    expanded, sample_specs = preflight_var_specs(h3_logger, product_vars)
                    if expanded:
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
                    # A preset's own names come from an internal table/manifest,
                    # not something the user typed, so they are not checked here
                    # (H3BuildLogger.preset_products); names typed next to a
                    # preset are (preflight_var_specs).
                    if not sample_specs:
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
                    missing = explicit_vars_missing_in_sample(sample_specs, set(), sample_dict)

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
                        elif (h3_logger.new_spatial is None and h3_logger.new_temporal is None
                              and not h3_logger.new_product_vars):
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
            #
            # has_pending_new_work vetoes both signals whenever this
            # invocation widens the region or dates: the shortcut would save
            # the union as COMPLETED without reading the added area (#42).
            # It also vetoes them for a variable-only update, a mixed update's
            # Phase 2, or a crash-recovered pending variable update: neither
            # signal proves anything about product/variable work that has never
            # been through Stage 1/2, and a stale _merge_progress.txt left
            # over from an earlier, already-completed build in the same tmp
            # directory would otherwise be misread as "this request's merge
            # already started" — silently no-opping the whole product add
            # while still reporting success.
            _parquet_dir = os.path.join(args.tmpdir, 'partitions')
            # A run killed between a merge failure and its post-merge fold
            # leaves the sidecar behind. Fold it before choosing the path: the
            # MERGE_FAILED veto must see it, or the shortcut would merge those
            # partitions without their lost fragments. The next save_log
            # (MERGING or PARTITIONING, just below) persists it.
            from gedih3.gh3builder import apply_merge_failures_to_logger, _release_merge_failed
            _flagged = apply_merge_failures_to_logger(h3_logger, _parquet_dir)
            if _flagged:
                logger.warning(f"Flagged {_flagged} granule(s) MERGE_FAILED from an earlier failed merge")
            _merge_signal = _detect_merge_resume_signal(
                h3_logger, _parquet_dir,
                has_pending_new_work=bool(
                    is_variable_only_update or is_mixed_update or pending_var_update
                    or h3_logger.requested_expansion
                ),
            )
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
                _flipped = apply_merge_failures_to_logger(h3_logger, _parquet_dir)
                if _flipped:
                    logger.warning(
                        f"Flagged {_flipped} granule(s) MERGE_FAILED after "
                        f"corrupt-fragment merge failures. They will be "
                        f"re-extracted on the next resume."
                    )
                h3_logger.set_post_build_info()
                h3_logger.log_data.pop('_pending_variable_update', None)
                h3_logger.update_ran = True     # a MERGING save retries merges only
                _n_merge_failed = _count_merge_failures(_parquet_dir)
                h3_logger.save_log('MERGING' if _n_merge_failed else 'COMPLETED')
                build_completed = not _n_merge_failed
                # AFTER the final log save — an index older than the log is
                # treated as stale by the loaders.
                from gedih3.gh3driver import refresh_bbox_index_after_build
                refresh_bbox_index_after_build(args.output, enabled=args.bbox_index,
                                               logger=logger)
                if _n_merge_failed:
                    _exit_merge_incomplete(_n_merge_failed, _parquet_dir, args.output, logger)
                _pending_fills = h3_logger.pending_product_fills()
                if _pending_fills:
                    logger.info(f"{len(_pending_fills)} granule(s) await products; re-run gh3_build to fill "
                                f"them once their files are present")
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
                _stage1_listed = None
                _soc_for_build = None
                if soc_source is not None and isinstance(soc_source, str) and os.path.isdir(soc_source):
                    logger.info("Listing SOC files for granule registration")
                    _soc_for_build = soc_file_tree(
                        soc_source, to_list=True, exclude=args.exclude,
                        glob_kwargs={'version': h3_logger.gedi_version},
                        require_all=not allow_missing,
                    )
                    if allow_missing:
                        # A granule must have L2A and what is published no later
                        # (L1B) — the products build_h3db requires in this mode.
                        from gedih3.gh3builder import _PRODUCTS_BEFORE_L2B
                        _req = _PRODUCTS_BEFORE_L2B & set(h3_logger.get_product_vars() or {})
                        _soc_for_build = [s for s in _soc_for_build if _req.issubset(s)]
                    # GEDI filename: GEDInn_L_DATE_O{orbit}_{granule}_T{track}_PPDS_PGE_GEN_V{ver}.h5
                    # Parse orbit/granule/track from the basename directly — no
                    # GEDIFile() so we skip the unused os.path.getsize call per
                    # granule (matters on network filesystems).
                    # Each granule's source releases for the products built,
                    # so a later re-release can be detected (#47).
                    from gedih3.gedidriver import gedi_release_tag
                    _build_products = set(h3_logger.get_product_vars() or {})
                    _build_granules = []
                    for _soc in _soc_for_build:
                        fl = os.path.basename(list(_soc.values())[0]).split('_')
                        _build_granules.append({
                            'orbit': int(fl[3][1:]),
                            'granule': int(fl[4]),
                            'track': int(fl[5][1:]),
                            'releases': {p: t for p, f in _soc.items()
                                         if p in _build_products and (t := gedi_release_tag(f))},
                        })
                    h3_logger.register_pending_granules(_build_granules)
                    _adopt_row_releases(h3_logger, args.output, logger)   # saved with the build's next save
                    _warn_superseded_granules(h3_logger, _soc_for_build, logger)
                    _stage1_listed = {(g['orbit'], g['granule'], g['track']) for g in _build_granules}

                    # Resume reconciliation: scan h3 db AND tmp/partitions for
                    # granules already represented on disk and flip them to
                    # INDEXED before stage 1 starts. Without this, a kill
                    # during stage 2 leaves all granules PENDING and stage 1
                    # re-extracts everything on the next rerun.
                    # Sentinels are trusted only if recorded under the scope Stage 1
                    # is about to run with (build_h3db fingerprints the expanded
                    # variables, which is the form a resumed log holds).
                    from gedih3.gh3builder import _stage1_scope_fingerprint
                    if is_mixed_update:
                        _s1_vars = {k: val.get('variables')
                                    for k, val in h3_logger.log_data.get('products', {}).items()}
                    else:
                        _s1_vars = h3_logger.get_product_vars()
                    _reconcile_granules_from_disk(
                        args.output, h3_logger,
                        tmp_dir=os.path.join(args.tmpdir, 'partitions'),
                        expected_scope=_stage1_scope_fingerprint(
                            _build_kwargs['spatial'], h3_logger.res, h3_logger.part, _s1_vars),
                    )

                    h3_logger.save_log('PROCESSING')

                h3_files = None
                # Filled by Stage 1 only when it drained every task (see
                # build_h3db); empty = no proof, so no granule is marked NO_DATA.
                _stage1_outcome = {}

                # ── Stage 1: Spatial/temporal build ──────────────────────
                # Runs for: fresh build, resume, spatial/temporal expansion,
                # or mixed update Phase 1.
                # Skipped for: variable-only update, pending variable resume.
                if not pending_var_update and not is_variable_only_update:
                    # Sentinels with no scope record, left by an older gedih3
                    # after a build that COMPLETED: not an in-flight resume, so
                    # nothing proves which scope they were empty under.
                    from gedih3.gh3builder import _discard_unscoped_sentinels
                    if h3_logger.previous_status == 'COMPLETED' and _discard_unscoped_sentinels(
                            os.path.join(args.tmpdir, 'partitions')):
                        logger.info("Discarded completion sentinels left by an older gedih3's completed build")
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
                    # None except on a spatial-only expansion (#42): see stage1_only_granules.
                    stage1_only = None if is_mixed_update else h3_logger.stage1_only_granules()

                    def _record_granule_products(processed):
                        # Persisted before Stage 1 writes anything: a granule
                        # indexed without a product is MISSING_SOURCE for it
                        # until the backfill fills its rows (even across a
                        # crash). Only ever added here: a re-extraction does
                        # not overwrite rows already in the database, so only
                        # the backfill (Stage 3) clears the mark.
                        from gedih3.logger import PRODUCT_STATUS_MISSING_SOURCE
                        h3_logger.set_product_statuses({
                            k: {p: PRODUCT_STATUS_MISSING_SOURCE for p in miss}
                            for k, miss in processed.items() if miss
                        })
                        _partial = [m for m in processed.values() if m]
                        if _partial:
                            from collections import Counter
                            _by_prod = Counter(p for m in _partial for p in m)
                            logger.info(
                                f"Indexing {len(_partial)} granule(s) before some products are published: "
                                + ", ".join(f"{p} missing for {n}" for p, n in sorted(_by_prod.items()))
                                + " (recorded as MISSING_SOURCE, filled once the files appear)"
                            )
                        h3_logger.save_log('PROCESSING')

                    def _capture_stage1_outcome(incomplete, empty):
                        _stage1_outcome['incomplete'] = incomplete
                        _stage1_outcome['empty'] = empty

                    h3_files = build_h3db(
                        product_vars=stage1_products,
                        soc_source=soc_source,
                        skip_granules=stage1_skip,
                        only_granules=stage1_only,
                        variable_only_update=False,
                        allow_missing_products=allow_missing,
                        granule_products_callback=_record_granule_products,
                        stage1_outcome_callback=_capture_stage1_outcome,
                        **_build_kwargs,
                    )
                    # Stage 1 never skips a MERGE_FAILED granule and its pre-clean
                    # reopened the lost tasks, so every listed one was re-extracted
                    # and merged: PENDING lets set_post_build_info index it, and
                    # the fold below re-flags any whose merge failed again.
                    # A spatial-only expansion admits only finished granules, so it
                    # re-extracted no MERGE_FAILED one: each keeps its flag for the
                    # next build (released, it would be marked INDEXED from the
                    # partition metadata with its lost fragments never redone).
                    if stage1_only is None:
                        _release_merge_failed(h3_logger, listed=_stage1_listed)

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

                    def _record_var_releases(releases):
                        # Products the variable add gives source columns (#68).
                        if h3_logger.record_releases(releases):
                            h3_logger.save_log('PROCESSING')
                    h3_files_s2 = build_h3db(
                        product_vars=stage2_products,
                        soc_source=soc_source,
                        skip_granules=None,
                        variable_only_update=True,
                        releases_callback=_record_var_releases,
                        **_build_kwargs,
                    )
                    h3_files = (h3_files or []) + (h3_files_s2 or [])

                # ── Stage 3: product backfill ────────────────────────────
                # Runs whenever indexed granules still await products
                # published after them (MISSING_SOURCE, recorded by a phased
                # run) — a log-only check, so a database with nothing pending
                # pays nothing. Fills only the null cells of the files that
                # hold those granules; see _build_fill_products. Deferred
                # while partition merges await a retry. Best effort: an error
                # here is logged and the granules stay pending; it never fails
                # a build whose own merge succeeded.
                _awaiting = h3_logger.pending_product_fills()
                _n_backfilled = 0
                if _awaiting and isinstance(soc_source, str) and os.path.isdir(soc_source):
                    if _count_merge_failures(os.path.join(args.tmpdir, 'partitions')):
                        logger.warning(f"Product backfill of {len(_awaiting)} granule(s) deferred: partition "
                                       f"merges are pending retry; it runs once a gh3_build completes them")
                    else:
                        try:
                            from gedih3.gh3builder import _build_fill_products
                            h3_logger.save_log('PROCESSING')

                            def _record_fill_releases(releases):
                                if h3_logger.record_releases(releases):
                                    h3_logger.save_log('PROCESSING')
                            _fill = _build_fill_products(
                                args.output, _awaiting,
                                # Reuse the registration listing when it kept partial granules.
                                soc_source=(_soc_for_build if allow_missing and _soc_for_build is not None
                                            else soc_source),
                                version=h3_logger.gedi_version,
                                tmp_dir=args.tmpdir,
                                exclude=args.exclude,
                                releases_callback=_record_fill_releases,
                            )
                            _given_up = h3_logger.record_fill_results(_fill)
                            _n_backfilled = len(_fill['updated_files'])
                            if _fill['failed']:
                                logger.warning(f"Product backfill: {len(_fill['failed'])} granule(s) failed; "
                                               f"retried on the next runs (up to 3 attempts)")
                            if _given_up:
                                logger.warning(
                                    f"Product backfill: gave up on {sum(map(len, _given_up.values()))} granule "
                                    f"product(s) (repeated failures, or no database file lists the granule); "
                                    f"marked FAILED, no longer retried automatically. Inspect with "
                                    f"gh3_doctor -i {args.output} --check backfill; retry with --fix backfill")
                            if _fill['updated_files'] and os.path.exists(os.path.join(args.output, 'gedi.ducklake')):
                                logger.warning(f"Filled columns changed file statistics the DuckLake catalog "
                                               f"caches; rebuild it: gh3_build_ducklake -d {args.output}")
                        except Exception as _e:
                            logger.error(f"Product backfill failed ({type(_e).__name__}: {_e}); the granules "
                                         f"stay MISSING_SOURCE and are retried on the next run")
                elif _awaiting:
                    logger.info(f"{len(_awaiting)} granule(s) await products; the backfill needs a local SOC "
                                f"directory (or: gh3_doctor --fix backfill --s3)")

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
                        f"Flagged {_flipped} granule(s) MERGE_FAILED after "
                        f"corrupt-fragment merge failures. They will be "
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
                _n_merge_failed = _count_merge_failures(_parquet_dir_for_fold)
                # With any Stage 1 outcome mark_no_data runs: it reopens failed NO_DATA
                # granules always, but marks only under the full scope (a spatial-only /
                # temporal-only expansion reads just the added area, stage1_full_scope),
                # so the metadata verification is needed only then.
                _no_data_proof = (bool(_stage1_outcome) and _stage1_listed is not None
                                  and not _n_merge_failed)
                h3_logger.set_post_build_info(
                    verify_observed=_no_data_proof and h3_logger.stage1_full_scope())
                if _no_data_proof:
                    # Granules read in full that hold no rows: skipped by later
                    # builds instead of re-read forever. Needs Stage 1's proof
                    # AND the metadata's full listing; see mark_no_data.
                    _nd = h3_logger.mark_no_data(
                        _stage1_listed, _stage1_outcome['incomplete'], _stage1_outcome['empty'])
                    if _nd['marked']:
                        logger.info(
                            f"{_nd['marked']} granule(s) have no data in the ROI; recorded as NO_DATA "
                            f"and skipped by later builds")
                    if _nd['reopened']:
                        logger.info(f"{_nd['reopened']} NO_DATA granule(s) failed this run; retried next build")
                # Clear pending variable update BEFORE saving — ensures the flag
                # is not persisted to disk after successful completion.
                # Crash safety: if crash between pop and save, disk still has
                # PROCESSING status with the flag → next run resumes correctly.
                h3_logger.log_data.pop('_pending_variable_update', None)
                # Every stage ran under the update: a MERGING save (merges to
                # retry) must not keep it pending, or the retry re-reads it all.
                h3_logger.update_ran = True
                h3_logger.save_log('MERGING' if _n_merge_failed else 'COMPLETED')
                build_completed = not _n_merge_failed

                # AFTER the final log save — an index older than the log is
                # treated as stale by the loaders.
                from gedih3.gh3driver import refresh_bbox_index_after_build
                refresh_bbox_index_after_build(args.output, enabled=args.bbox_index,
                                               logger=logger)
                if _n_merge_failed:
                    _exit_merge_incomplete(_n_merge_failed, _parquet_dir_for_fold, args.output, logger)

                n_files = len(h3_files) if h3_files else 0
                _backfilled = f", {_n_backfilled} files backfilled" if _n_backfilled else ""
                print_success(f"{n_files} files exported to {args.output}{_backfilled}", logger=logger)

                # Regime C: Stage 2 ran from a `default` re-expansion. Any vars
                # not present in the source HDF5 files were silently written
                # as NaN columns. gh3_doctor's backfill diagnosis flags these.
                if (is_variable_only_update or is_mixed_update) and h3_logger.new_product_vars and (
                    h3_logger.default_products & set(h3_logger.new_product_vars)
                ):
                    logger.info(
                        "New variables added via a `default` request; run "
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
        # A Ctrl-C during the post-completion bbox-index build must not
        # downgrade a genuinely finished build's log to INTERRUPTED — the
        # index is derived and rebuildable, the COMPLETED status is not.
        if not build_completed:
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
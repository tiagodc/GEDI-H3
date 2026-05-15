#! python

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at otc@umd.edu

"""gh3_doctor — audit and (optionally) heal a gedih3 database."""

import argparse


def _build_epilog():
    """Render the help epilog with the live diagnosis registry + alias groups.

    Importing the diagnoses package auto-registers every check, so `--check`
    options are derived from runtime state and stay in sync as new diagnoses
    are added (no hardcoded lists to drift).
    """
    import gedih3.doctor.diagnoses  # noqa: F401  (auto-register)
    from gedih3.doctor.runner import get_diagnoses, ALIAS_GROUPS

    diagnoses = get_diagnoses()
    diag_lines = []
    for name in sorted(diagnoses):
        d = diagnoses[name]
        fixable = ' (fixable)' if d.fix is not None else ''
        diag_lines.append(f"  {name:<16}{fixable:<11} {d.description}")

    alias_lines = []
    for group, members in ALIAS_GROUPS.items():
        if members is None:
            members_str = '<all registered diagnoses>'
        else:
            members_str = ', '.join(members)
        alias_lines.append(f"  {group:<6} → {members_str}")

    return f"""\
Available diagnoses (use with --check / --fix):
{chr(10).join(diag_lines)}

Alias groups:
{chr(10).join(alias_lines)}

Exit codes:
  0    no findings (or --fix resolved every finding)
  1    findings remain after the run
  2    a check or fix raised an unhandled exception, OR bad CLI args

Examples:
  gh3_doctor -i /db                        # read-only audit, default 'db' alias
  gh3_doctor -i /db --check all            # include soc_health too
  gh3_doctor -i /db --check backfill,parquet_health
  gh3_doctor -i /db --fix                  # apply safe remedies for all checked
  gh3_doctor -i /db --fix backfill --s3    # backfill via S3 ETL temp
  gh3_doctor -i /db --online               # decorate with NASA upstream check
  gh3_doctor -i /db --report report.json   # machine-readable JSON output

Common knobs:
  --orphan-age-hours  protects in-progress builds (default 24h: anything younger is ignored)
  --soc-dir           required for backfill / soc_health if not at default location
  -s tcp://...        attach to an existing dask cluster (for parallel diagnoses)
"""


def get_cmd_args():
    from gedih3.cliutils import add_dask_args, add_verbosity_args, add_storage_args

    p = argparse.ArgumentParser(
        description="Audit and (optionally) heal a gedih3 database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=_build_epilog(),
    )

    p.add_argument("-i", "--indir", dest="indir", type=str, default=None,
                   help="root directory of the gedih3 database (default: GH3_DEFAULT_H3_DIR)")
    p.add_argument("--soc-dir", dest="soc_dir", type=str, default=None,
                   help="root directory of SOC HDF5 files (default: GH3_DEFAULT_SOC_DIR)")
    p.add_argument("-t", "--tmpdir", dest="tmpdir", type=str, default=None,
                   help="temp directory for orphan scans and S3 ETL")

    p.add_argument("--check", dest="check", type=str, default=None,
                   help="comma-separated diagnosis names or aliases (db, soc, all). "
                        "Default: 'db' alias. See epilog for the full list.")
    p.add_argument("--fix", dest="fix", type=str, nargs='?', const='__ALL__', default=None,
                   help="apply safe remedies (only some diagnoses are fixable — see epilog). "
                        "Optional comma-separated names; bare flag = all checked.")

    p.add_argument("-s3", "--s3", dest="s3", action='store_true',
                   help="for backfill, fetch missing source files via NASA S3 ETL temp directory")
    p.add_argument("--online", dest="online", action='store_true',
                   help="query NASA CMR for granule availability and emit recommendations")

    p.add_argument("--report", dest="report", type=str, default=None,
                   help="write machine-readable JSON report to this path")
    p.add_argument("--orphan-age-hours", dest="orphan_age_hours", type=float, default=24.0,
                   help="minimum age (hours) before orphan files/dirs are eligible for cleanup [default=24]")
    p.add_argument("--gedi-version", dest="version", type=int, default=None,
                   help="GEDI data version (only needed when --online uses CMR)")

    add_dask_args(p)
    add_verbosity_args(p)
    add_storage_args(p)

    return p.parse_args()


def _resolve_request(arg_value, default_aliases):
    """Split a comma-separated arg into a list of names. None → default_aliases."""
    if arg_value is None:
        return list(default_aliases)
    return [n.strip() for n in arg_value.split(',') if n.strip()]


def main():
    args = get_cmd_args()

    import os
    import sys
    import json

    from gedih3.config import GH3_DEFAULT_H3_DIR, GH3_DEFAULT_SOC_DIR
    from gedih3.cliutils import (
        setup_logging, print_banner, print_success, cli_exception_handler, parse_dask_args, setup_storage,
    )
    from gedih3.logger import H3BuildLogger
    from gedih3.doctor import DoctorContext, run_diagnoses, get_diagnoses
    from gedih3.doctor.runner import resolve_names
    # Importing the diagnoses package auto-registers all known diagnoses.
    import gedih3.doctor.diagnoses  # noqa: F401
    from gedih3.doctor.inspect import discover_partition_dirs

    logger = setup_logging(args, __name__)
    print_banner("GEDI H3 Database Doctor", logger=logger)

    if args.indir is None:
        args.indir = GH3_DEFAULT_H3_DIR
    if args.soc_dir is None:
        args.soc_dir = GH3_DEFAULT_SOC_DIR if os.path.isdir(GH3_DEFAULT_SOC_DIR) else None
    # No default for tmpdir: the build's tmp directory layout varies
    # (sometimes under the DB root, sometimes a sibling, sometimes
    # outside the DB tree entirely). Scanning a guessed path that
    # doesn't exist produces no findings; scanning an *unrelated* path
    # is worse (false positives). Diagnoses that need a tmp tree
    # (``tmp_partitions_health``, the tmp_dir leg of ``orphans``) skip
    # silently when ``args.tmpdir`` is None.

    # Resolve all input paths to absolute. Per-partition workers serialize
    # over pickle to remote dask workers, whose CWD differs from the
    # driver's — a relative path passed in via ``-i database/`` would
    # fail every ``os.scandir`` on the worker side (silently treated as
    # an OSError by the doctor helpers) and produce 10k false
    # ``empty_partition`` + ``missing_partition_meta`` findings.
    args.indir = os.path.abspath(args.indir)
    if args.soc_dir is not None:
        args.soc_dir = os.path.abspath(args.soc_dir)
    if args.tmpdir is not None:
        args.tmpdir = os.path.abspath(args.tmpdir)

    if not os.path.isdir(args.indir):
        logger.error(f"Database directory not found: {args.indir}")
        sys.exit(2)

    setup_storage(args, logger=logger)

    # Always create a dask Client so every diagnosis dispatches through
    # the same code path. ``parse_dask_args`` returns the address of a
    # remote scheduler when ``--scheduler-address`` is given, otherwise
    # the kwargs (n_workers / threads / memory) for a transient
    # LocalCluster. The doctor's ``parallel_map`` primitive assumes a
    # Client is registered — keeping CLI startup uniform with every
    # other gedih3 tool (gh3_extract, gh3_aggregate, gh3_build, …)
    # eliminates the dual-path "serial fallback" that used to ship
    # whenever a user invoked the doctor without ``-s``.
    from dask.distributed import Client
    dask_kwargs = parse_dask_args(args)
    if dask_kwargs.get('address'):
        logger.info(f"Connecting to dask cluster: {dask_kwargs['address']}")
    else:
        logger.info(
            f"Spinning up local dask cluster ({args.cores} workers, "
            f"{args.threads} threads/worker)"
        )

    with Client(**dask_kwargs) as _client, cli_exception_handler(args, logger=logger):
        try:
            logger.info(f"Dask dashboard available at: {_client.dashboard_link}")
        except Exception:
            pass

        # Load the build log (lazy upgrade applied automatically). If absent,
        # most diagnoses still work — they fall back to filesystem inspection.
        try:
            h3_logger = H3BuildLogger(product_vars=None, dir=args.indir, version=args.version)
        except Exception as e:
            logger.warning(f"Could not load build log: {e}. Diagnoses will skip log-only checks.")
            h3_logger = None

        partition_dirs = discover_partition_dirs(args.indir)
        logger.info(f"Database: {args.indir} ({len(partition_dirs)} partitions)")
        if args.soc_dir:
            logger.info(f"SOC directory: {args.soc_dir}")

        ctx = DoctorContext(
            h3_dir=args.indir,
            soc_dir=args.soc_dir,
            tmp_dir=args.tmpdir,
            h3_logger=h3_logger,
            partition_dirs=partition_dirs,
            args=args,
        )

        # Optionally enrich with upstream availability info before diagnoses run.
        if args.online:
            try:
                from gedih3.doctor.upstream import gather_upstream
                ctx.upstream = gather_upstream(ctx, logger=logger)
            except Exception as e:
                logger.warning(f"Upstream check failed (continuing without): {e}")

        # Determine which diagnoses to check
        registry = get_diagnoses()
        if not registry:
            logger.error("No diagnoses registered. This is a packaging bug.")
            sys.exit(2)

        check_names = _resolve_request(args.check, default_aliases=['db'])

        try:
            check_resolved = resolve_names(check_names)
        except ValueError as e:
            logger.error(str(e))
            sys.exit(2)

        logger.info(f"Running diagnoses: {', '.join(check_resolved)}")

        # Run as 'check' (read-only) first to know what's wrong.
        check_reports = run_diagnoses(ctx, check_names, mode='check')

        # If --fix is requested, run only the requested fix subset against
        # the same context (re-runs the check then applies remedy).
        fix_reports = None
        if args.fix is not None:
            if args.fix == '__ALL__':
                fix_names = check_resolved
            else:
                fix_names = _resolve_request(args.fix, default_aliases=check_resolved)
            try:
                resolve_names(fix_names)
            except ValueError as e:
                logger.error(str(e))
                sys.exit(2)
            logger.info(f"Applying fixes: {', '.join(fix_names)}")
            fix_reports = run_diagnoses(ctx, fix_names, mode='fix')
            # Persist any logger mutations made by fix routines
            if h3_logger is not None and any(getattr(r, 'applied', False) for r in fix_reports):
                try:
                    h3_logger.save_log(h3_logger.previous_status or 'COMPLETED')
                except Exception as e:
                    logger.warning(f"Failed to persist build log updates: {e}")

        reports_to_emit = fix_reports if fix_reports is not None else check_reports
        _print_summary(reports_to_emit, logger)

        if ctx.upstream is not None:
            _print_upstream(ctx.upstream, logger)

        if args.report:
            from gedih3.utils import AtomicFileWriter
            payload = {
                'reports': [r.to_dict() for r in reports_to_emit],
                'upstream': ctx.upstream.to_dict() if ctx.upstream is not None else None,
            }
            # Atomic write so an interrupted run leaves no half-written
            # report file at the user's target path.
            with AtomicFileWriter(args.report) as tmp:
                with open(tmp, 'w') as f:
                    json.dump(payload, f, indent=2, default=str)
            logger.info(f"Wrote report: {args.report}")

        worst = _worst_severity(reports_to_emit)
        if not any(r.has_findings for r in reports_to_emit):
            print_success("Database is clean", logger=logger)
            sys.exit(0)

        # Exit code: 0 if --fix was used and resolved everything, 1 if findings remain
        if fix_reports is not None and all(r.is_clean for r in fix_reports):
            print_success("All issues resolved", logger=logger)
            sys.exit(0)

        sys.exit(2 if worst == 'error' else 1)


def _worst_severity(reports):
    order = {'info': 0, 'warn': 1, 'error': 2}
    if not reports:
        return 'info'
    return max((r.severity.value if hasattr(r.severity, 'value') else r.severity for r in reports),
               key=lambda s: order.get(s, 0))


def _print_summary(reports, logger):
    logger.info("")
    logger.info("=" * 70)
    logger.info(" Doctor Summary".center(70))
    logger.info("=" * 70)
    for r in reports:
        sev = r.severity.value if hasattr(r.severity, 'value') else str(r.severity)
        marker = {'info': '·', 'warn': '!', 'error': 'X'}.get(sev, '?')
        status = 'fixed' if r.applied else 'check'
        logger.info(f" [{marker}] {r.name:<18} {status:<5} {len(r.findings):>4} findings  {r.summary}")
        for rec in r.recommendations:
            logger.info(f"        → {rec}")
    logger.info("=" * 70)


def _print_upstream(upstream, logger):
    logger.info("")
    logger.info("=" * 70)
    logger.info(" Upstream NASA Availability".center(70))
    logger.info("=" * 70)
    for cls, items in sorted(upstream.classifications.items()):
        logger.info(f"  {cls:<18}: {len(items)} (granule × product)")
    if upstream.recommendations:
        logger.info("")
        logger.info(" Recommended commands ".center(70, '-'))
        for line in upstream.recommendations:
            logger.info(f"  {line}")
    logger.info("=" * 70)


if __name__ == '__main__':
    main()

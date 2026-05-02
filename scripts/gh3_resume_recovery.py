#! /usr/bin/env python
"""One-shot recovery for an interrupted gh3_build.

Reconciles ``gedih3_build_log.json`` against on-disk state so the next
``gh3_build`` rerun resumes correctly:

  1. Refuses to run if a ``gh3_build`` process is alive.
  2. Scans ``<indir>/database/`` and ``<indir>/tmp/partitions/`` to identify
     granules already extracted on disk; flips their ``granule_info`` status
     to ``INDEXED`` (so stage 1 skips them on the next run).
  3. Cleans stale ``*.merge.tmp`` files in ``database/`` (left over from
     in-flight merges at a prior crash).
  4. Reconciles ``tmp/partitions/_merge_progress.txt`` against actual
     finalized parquet paths in ``database/``: drops entries whose target
     parquet is missing; appends entries for finalized parquets whose tmp
     source dir is gone (proof of successful merge).
  5. Persists the reconciled log via ``H3BuildLogger.save_log('INTERRUPTED')``.

Idempotent. Run with ``--dry-run`` first to inspect.

Usage:
  python scripts/gh3_resume_recovery.py --indir /gpfs/.../iss_gedi/h3_v3
  python scripts/gh3_resume_recovery.py --indir <path> --dry-run
"""
import argparse
import glob
import os
import subprocess
import sys
from collections import Counter


def _reconcile_with_pool(h3_dir, h3_logger, tmp_dir, n_workers):
    """Reconcile granule status using a multiprocessing.Pool — faster startup
    than Dask LocalCluster on shared filesystems and avoids the dashboard /
    scheduler overhead.
    """
    from gedih3.gh3builder import (
        _granule_ids_in_fragment, _granule_id_from_l2a_path,
    )
    from gedih3.config import PARTITION_META_FILENAME
    from gedih3.utils import json_read

    if not hasattr(h3_logger, 'granule_info') or not h3_logger.granule_info:
        return 0

    indexed_ids = set()

    print(f'      scanning {h3_dir} for finalized partition metadata...')
    meta_files = glob.glob(os.path.join(h3_dir, 'h3_*', f'*{PARTITION_META_FILENAME}'))
    meta_files += glob.glob(os.path.join(h3_dir, 'h3_*', '*', f'*{PARTITION_META_FILENAME}'))
    print(f'      found {len(meta_files)} metadata files; reading...')
    for mf in meta_files:
        try:
            for g in (json_read(mf) or {}).get('granules', []):
                indexed_ids.add((g['orbit'], g['granule'], g['track']))
        except Exception:
            continue
    print(f'      → {len(indexed_ids)} granules in finalized partitions')

    if tmp_dir and os.path.isdir(tmp_dir):
        print(f'      listing tmp fragments under {tmp_dir} (streaming via os.scandir)...')
        import time as _time
        t0 = _time.time()

        # Streaming walk via os.scandir is dramatically faster than glob on GPFS
        # because it (a) skips full glob expansion, (b) uses a single readdir
        # syscall per directory, and (c) avoids building all paths in memory
        # before any work begins. Print incremental progress so the user knows
        # it's alive while traversing 7M+ files.
        frag_files = []
        h3_dir_count = 0
        for h3_entry in os.scandir(tmp_dir):
            if not h3_entry.is_dir() or not h3_entry.name.startswith('h3_'):
                continue
            h3_dir_count += 1
            try:
                for year_entry in os.scandir(h3_entry.path):
                    if not year_entry.is_dir() or not year_entry.name.startswith('year='):
                        continue
                    try:
                        for f_entry in os.scandir(year_entry.path):
                            if f_entry.is_file() and f_entry.name.endswith('.parquet'):
                                frag_files.append(f_entry.path)
                    except OSError:
                        continue
            except OSError:
                continue
            if h3_dir_count % 500 == 0:
                print(f'      ... scanned {h3_dir_count} h3 dirs, {len(frag_files)} fragments so far '
                      f'({_time.time() - t0:.0f}s)')

        t1 = _time.time()
        print(f'      → found {len(frag_files)} tmp fragments across {h3_dir_count} h3 dirs in {t1 - t0:.1f}s')

        # Dedupe by basename: with by_beam=True + partition_on=[h3,year], the
        # same `part.<i>.parquet` index represents the SAME (granule, beam)
        # source partition replicated across many leaf dirs. Reading one
        # instance per unique basename is enough to enumerate granules — this
        # cuts the work by ~30x for typical builds (19.5M files → 587K).
        unique_files = {}
        for path in frag_files:
            bn = os.path.basename(path)
            if bn not in unique_files:
                unique_files[bn] = path
        sample_files = list(unique_files.values())
        print(f'      → {len(sample_files)} unique partition indices '
              f'(reading 1 instance per index = {len(sample_files) / len(frag_files):.1%} of total)')

        if sample_files:
            try:
                from tqdm import tqdm as tqdm_bar
            except ImportError:
                tqdm_bar = lambda it, **kw: it

            if n_workers > 0 and len(sample_files) > 64:
                print(f'      processing fragments with multiprocessing pool ({n_workers} workers)...')
                from multiprocessing import Pool
                with Pool(n_workers) as pool:
                    for s in tqdm_bar(
                        pool.imap_unordered(_granule_ids_in_fragment, sample_files, chunksize=64),
                        total=len(sample_files),
                        desc='Reconcile fragments',
                        unit='file',
                        smoothing=0.05,
                    ):
                        indexed_ids.update(s)
            else:
                print(f'      processing fragments sequentially...')
                for f in tqdm_bar(sample_files, desc='Reconcile fragments', unit='file'):
                    indexed_ids.update(_granule_ids_in_fragment(f))

    if not indexed_ids:
        return 0

    n_flipped = 0
    for g in h3_logger.granule_info:
        key = (g['orbit'], g['granule'], g['track'])
        if key in indexed_ids and g.get('status') != 'INDEXED':
            g['status'] = 'INDEXED'
            n_flipped += 1
    return n_flipped


def _find_gh3_build_pids():
    try:
        out = subprocess.check_output(['pgrep', '-af', 'gh3_build'], text=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return []
    return [line for line in out.strip().splitlines() if 'resume_recovery' not in line]


def _resolve_paths(indir):
    """Return (h3_dir, tmp_partitions_dir) given the user's --indir."""
    candidates = [
        (os.path.join(indir, 'database'), os.path.join(indir, 'tmp', 'partitions')),
        (indir, os.path.join(indir, 'tmp', 'partitions')),
        (indir, os.path.join(os.path.dirname(indir.rstrip('/')), 'tmp', 'partitions')),
    ]
    for h3d, tmpd in candidates:
        if os.path.isdir(h3d) and os.path.exists(os.path.join(h3d, 'gedih3_build_log.json')):
            return h3d, tmpd
    return candidates[0]


def main():
    # Force unbuffered stdout for live progress when piped or tee'd.
    sys.stdout.reconfigure(line_buffering=True)
    ap = argparse.ArgumentParser(description=__doc__.split('\n', 1)[0])
    ap.add_argument('-i', '--indir', required=True,
                    help='gh3 build root (contains database/ and tmp/partitions/)')
    ap.add_argument('--dry-run', action='store_true',
                    help='report only; do not modify any file')
    ap.add_argument('-N', '--workers', type=int, default=32,
                    help='dask local-cluster workers for the fragment scan '
                         '(default: 32). Set 0 to skip parallelization.')
    args = ap.parse_args()

    pids = _find_gh3_build_pids()
    if pids:
        print('REFUSING: a gh3_build process appears to be running:', file=sys.stderr)
        for p in pids:
            print(f'  {p}', file=sys.stderr)
        print('Stop it before running recovery.', file=sys.stderr)
        sys.exit(2)

    h3_dir, tmp_dir = _resolve_paths(args.indir)
    print(f'h3_dir : {h3_dir}')
    print(f'tmp_dir: {tmp_dir}')
    if not os.path.isdir(h3_dir):
        print(f'ERROR: h3_dir does not exist', file=sys.stderr)
        sys.exit(2)

    from gedih3.logger import H3BuildLogger
    from gedih3.gh3builder import _reconcile_granules_from_disk

    h3_logger = H3BuildLogger(product_vars=None, dir=h3_dir)
    if not h3_logger.updating:
        print('ERROR: no build log at h3_dir/gedih3_build_log.json', file=sys.stderr)
        sys.exit(2)

    n_granules = len(getattr(h3_logger, 'granule_info', []) or [])
    status_counts_before = Counter(g.get('status') for g in (h3_logger.granule_info or []))
    print(f'\nBuild log: {n_granules} granules, status before: {dict(status_counts_before)}')
    print(f'Build log status: {h3_logger.previous_status!r}')

    print('\n[1/4] Reconciling granule status from on-disk fragments...')
    n_flipped = _reconcile_with_pool(h3_dir, h3_logger, tmp_dir, args.workers)
    status_counts_after = Counter(g.get('status') for g in (h3_logger.granule_info or []))
    print(f'      flipped {n_flipped} granules to INDEXED')
    print(f'      status after: {dict(status_counts_after)}')

    print('\n[2/4] Cleaning stale .merge.tmp files in database/...')
    stale = glob.glob(os.path.join(h3_dir, 'h3_*', '*', '*.merge.tmp'))
    print(f'      found {len(stale)} stale .merge.tmp files')
    if not args.dry_run:
        for s in stale:
            try:
                os.unlink(s)
            except OSError as e:
                print(f'      WARN: could not delete {s}: {e}')
        print(f'      deleted {len(stale)} files')
    else:
        print('      (dry-run: not deleted)')

    print('\n[3/4] Reconciling _merge_progress.txt against database/...')
    progress_file = os.path.join(tmp_dir, '_merge_progress.txt')
    progress_entries = set()
    if os.path.exists(progress_file):
        with open(progress_file) as f:
            progress_entries = {line.strip() for line in f if line.strip()}
    print(f'      existing entries: {len(progress_entries)}')

    final_parquets = glob.glob(os.path.join(h3_dir, 'h3_*', '*', '*.parquet'))
    print(f'      finalized partitions on disk: {len(final_parquets)}')

    new_entries = set()
    for pq in final_parquets:
        year_dir = os.path.dirname(pq)
        h3_part_dir = os.path.dirname(year_dir)
        h3_part = os.path.basename(h3_part_dir)
        year = os.path.basename(year_dir)
        if h3_part.startswith('h3_') and '=' not in h3_part:
            cell_id = h3_part.split('_', 2)[-1] if h3_part.count('_') == 2 else h3_part
            for hp_dir in glob.glob(os.path.join(tmp_dir, f'h3_*={cell_id}')):
                tmp_year_dir = os.path.join(hp_dir, f'year={year}')
                new_entries.add(tmp_year_dir.rstrip('/'))
        guess = os.path.join(tmp_dir, f'h3_*={h3_part.split("_")[-1]}', f'year={year}')
        for d in glob.glob(guess):
            new_entries.add(d.rstrip('/'))

    valid_progress = set()
    dropped = 0
    if progress_entries and os.path.isdir(tmp_dir):
        existing_tmp_dirs = {d.rstrip('/') for d in glob.glob(os.path.join(tmp_dir, '*/*/'))}
        for entry in progress_entries:
            if entry in existing_tmp_dirs:
                valid_progress.add(entry)
            else:
                dropped += 1

    appended = new_entries - progress_entries - valid_progress
    final_entries = progress_entries | new_entries
    print(f'      kept: {len(progress_entries) - dropped}  '
          f'dropped (stale): {dropped}  '
          f'appended (orphan finalized): {len(appended)}')

    if not args.dry_run and (appended or dropped):
        os.makedirs(tmp_dir, exist_ok=True)
        with open(progress_file, 'w') as f:
            for line in sorted(final_entries):
                f.write(line + '\n')
        print(f'      wrote {len(final_entries)} entries to {progress_file}')

    print('\n[4/4] Persisting build log...')
    if args.dry_run:
        print('      (dry-run: not saved)')
    else:
        target_status = h3_logger.previous_status if h3_logger.previous_status in (
            'PARTITIONING', 'MERGING', 'PROCESSING', 'INTERRUPTED'
        ) else 'INTERRUPTED'
        h3_logger.save_log(target_status)
        print(f"      saved with status={target_status!r}")

    indexed_after = status_counts_after.get('INDEXED', 0)
    pending_after = status_counts_after.get('PENDING', 0)
    print('\n=== summary ===')
    print(f'granules INDEXED  : {indexed_after} / {n_granules}')
    print(f'granules PENDING  : {pending_after} / {n_granules}')
    print(f'tmp partitions    : {len(glob.glob(os.path.join(tmp_dir, "*/*/")))} '
          f'remaining for stage 2')
    print(f'stale .merge.tmp  : {len(stale)} ' + ('cleaned' if not args.dry_run else 'would be cleaned'))
    print(f'_merge_progress   : {len(final_entries)} entries '
          + ('written' if not args.dry_run and (appended or dropped) else 'unchanged'))
    if args.dry_run:
        print('\n(dry-run: nothing was written; rerun without --dry-run to apply)')


if __name__ == '__main__':
    main()

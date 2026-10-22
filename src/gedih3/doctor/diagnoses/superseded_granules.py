# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""superseded_granules diagnosis — indexed granules whose source was re-released.

Detects (read-only, no fix):
  - ``newer_release``: the SOC tree holds a later release of a granule
    product (PPDS, PGE, generation or suffix, ``gedi_release_rank``) than
    the one its rows were read from. NASA re-releases granules within one
    GEDI version to fix faulty data; ``gh3_build --replace-granules``
    re-indexes them (#47).
  - ``older_release``: only an older release than the one read is listed.
    It is never applied, but variables added and products filled later read
    the listed file.
  - ``mixed_releases``: a granule's rows name more than one release of a
    product (a replacement left half done), as read from its rows and kept
    in the build log (``mixed_releases``) until it is re-indexed.

Compares the build log's recorded ``releases`` (gh3_build >= 0.20.10) with
the file a listing picks now, by granule key (orbit, granule, track) within
the database's GEDI version, leaving out the files the build excludes (the
log's ``exclude``, gh3_build --exclude). For granules indexed before releases
were recorded, the releases are read from the rows' ``root_file_<prod>``
columns first (``releases_from_rows``, #67), in memory only: ``gh3_build``
records them on its next run. A granule with neither is counted, not reported.

Performance: one parallel SOC walk (``soc_file_tree``) and a log-only
comparison; on a database built before 0.20.10, also one worker task per
(cell, year) file reading only its source columns. No HDF5 is opened.
"""

from __future__ import annotations

import os

from ..report import Report, DoctorContext, Severity
from ..runner import register


def superseded_granules_check(ctx: DoctorContext) -> Report:
    if not ctx.soc_dir:
        return Report(name='superseded_granules', severity=Severity.INFO,
                      summary='no SOC directory configured; skipping')
    if not os.path.isdir(ctx.soc_dir):
        return Report(name='superseded_granules', severity=Severity.INFO,
                      summary=f"{ctx.soc_dir} is not a local directory; detection covers local SOC trees only")
    log = ctx.h3_logger
    version = getattr(log, 'gedi_version', None)
    if not version:
        return Report(name='superseded_granules', severity=Severity.INFO,
                      summary='the build log records no GEDI version; skipping')
    n_from_rows, failed, n_unread = None, [], 0
    keys = log.row_release_targets() if hasattr(log, 'row_release_targets') else set()
    if keys:
        # Read-only: the releases are adopted into a copy, never into the
        # shared logger a --fix would save; gh3_build records them.
        import copy
        from ...gh3builder import releases_from_rows
        columns = set(log.log_data.get('h3_columns') or [])
        from ...exceptions import GediError
        try:
            releases, mixed, unread, failed = releases_from_rows(
                ctx.h3_dir, [p for p in log.product_vars or {} if f"root_file_{p.lower()}" in columns], keys,
                version=version)
        except (GediError, OSError) as e:
            failed = [f"{type(e).__name__}: {e}"]
        else:
            log = copy.copy(log)
            log.granule_info = copy.deepcopy(log.granule_info)
            n_from_rows = log.adopt_row_releases(releases, keys - unread, mixed=mixed)
            n_unread = len(unread)
    mixed = log.mixed_release_granules() if hasattr(log, 'mixed_release_granules') else {}
    granules = getattr(log, 'granule_info', None) or []
    checked = [g for g in granules if g.get('status') in ('INDEXED', 'NO_DATA')]
    n_unknown = sum(1 for g in checked if not g.get('releases') and not g.get('mixed_releases'))
    if len(checked) == n_unknown and not failed:
        return Report(name='superseded_granules', severity=Severity.INFO,
                      summary=f"no recorded source releases to check ({n_unknown} granule(s) indexed before "
                              f"gedih3 recorded them)")

    from ...gedidriver import soc_file_tree
    entries = soc_file_tree(ctx.soc_dir, to_list=True, require_all=False, glob_kwargs={'version': version},
                            exclude=getattr(log, 'exclude', None) or None)
    found = log.superseded_releases(entries)

    findings = [{'kind': 'mixed_releases', 'granule': {'orbit': k[0], 'granule': k[1], 'track': k[2]},
                 'products': {p: {'rows': tags} for p, tags in sorted(prods.items())}}
                for k, prods in sorted(mixed.items())]
    for side, kind in (('newer', 'newer_release'), ('older', 'older_release')):
        for key, prods in sorted(found[side].items()):
            findings.append({
                'kind': kind,
                'granule': {'orbit': key[0], 'granule': key[1], 'track': key[2]},
                'products': {p: {'indexed': r, 'listed': t} for p, (r, t) in sorted(prods.items())},
            })

    n_newer, n_older = len(found['newer']), len(found['older'])
    summary = f"{n_newer} granule(s) with a newer release in the SOC tree; {n_older} with only an older one"
    if mixed:
        summary += f"; {len(mixed)} with rows from more than one release"
    if n_from_rows is not None:
        summary += (f" (releases of granules indexed before gedih3 recorded them read from the rows: "
                    f"{n_from_rows} tag(s); gh3_build records them on its next run)")
    if failed:
        summary += (f"; {len(failed)} database file(s) unreadable (e.g. {failed[0] or 'a batch of files'}), "
                    f"{n_unread} granule(s) they may hold not checked")
    if n_unknown:
        summary += f"; {n_unknown} granule(s) without a recorded release not checked"
    recommendations = []
    if mixed:
        recommendations.append(
            "Rows of these granules come from more than one release of a product, as a replacement left half "
            "done would: re-index them from the newest with gh3_build -i <soc> -o <db> --replace-granules auto "
            "(or by key), restoring that file in the SOC tree first if it is gone")
    if n_newer:
        recommendations.append(
            "Rows of these granules come from the older release: re-index them with gh3_build -i <soc> -o <db> "
            "--replace-granules auto. Until then, variables added and products filled later read the newer "
            "files. Files matching the build's --exclude patterns (kept in its log) are left out")
    if failed:
        recommendations.append("Check the unreadable files with gh3_doctor --check parquet_health")
    if n_older:
        recommendations.append(
            "Restore the release these granules were indexed from before adding variables or filling products")
    return Report(name='superseded_granules', severity=Severity.WARN if findings or failed else Severity.INFO,
                  findings=findings, summary=summary, recommendations=recommendations)


register(
    'superseded_granules',
    'Indexed granules whose source file has a newer (or only an older) release in the SOC tree',
    scope='global',
)(superseded_granules_check)

# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""superseded_granules diagnosis — indexed granules whose source was re-released.

Detects (read-only, no fix):
  - ``newer_release``: the SOC tree holds a later release of a granule
    product (PPDS, PGE, generation or suffix, ``gedi_release_rank``) than
    the one its rows were read from. NASA re-releases granules within one
    GEDI version to fix faulty data; gh3_build does not re-index them (#47).
  - ``older_release``: only an older release than the one read is listed.
    It is never applied, but variables added and products filled later read
    the listed file.

Compares the build log's recorded ``releases`` (gh3_build >= 0.20.10) with
the file a listing picks now. A granule indexed earlier has no recorded
release and cannot be checked; it is counted, not reported.

Performance: one parallel SOC walk (``soc_file_tree``) and a log-only
comparison; no HDF5 or parquet is opened.
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
    granules = getattr(log, 'granule_info', None) or []
    checked = [g for g in granules if g.get('status') in ('INDEXED', 'NO_DATA')]
    n_unknown = sum(1 for g in checked if not g.get('releases'))
    if len(checked) == n_unknown:
        return Report(name='superseded_granules', severity=Severity.INFO,
                      summary=f"no recorded source releases to check ({n_unknown} granule(s) indexed before "
                              f"gedih3 recorded them)")

    from ...gedidriver import soc_file_tree
    version = getattr(log, 'gedi_version', None)
    entries = soc_file_tree(ctx.soc_dir, to_list=True, require_all=False,
                            glob_kwargs={'version': version} if version else None)
    found = log.superseded_releases(entries)

    findings = []
    for side, kind in (('newer', 'newer_release'), ('older', 'older_release')):
        for key, prods in sorted(found[side].items()):
            findings.append({
                'kind': kind,
                'granule': {'orbit': key[0], 'granule': key[1], 'track': key[2]},
                'products': {p: {'indexed': r, 'listed': t} for p, (r, t) in sorted(prods.items())},
            })

    n_newer, n_older = len(found['newer']), len(found['older'])
    summary = f"{n_newer} granule(s) with a newer release in the SOC tree; {n_older} with only an older one"
    if n_unknown:
        summary += f"; {n_unknown} granule(s) without a recorded release not checked"
    recommendations = []
    if n_newer:
        recommendations.append(
            "Rows of these granules come from the older release and gh3_build does not re-index them yet. "
            "Until it does, move the newer files out of the SOC tree, so variables added and products filled "
            "later read the release the rows came from (adding variables does not apply --exclude, and this "
            "check does not either)")
    if n_older:
        recommendations.append(
            "Restore the release these granules were indexed from before adding variables or filling products")
    return Report(name='superseded_granules', severity=Severity.WARN if findings else Severity.INFO,
                  findings=findings, summary=summary, recommendations=recommendations)


register(
    'superseded_granules',
    'Indexed granules whose source file has a newer (or only an older) release in the SOC tree',
    scope='global',
)(superseded_granules_check)

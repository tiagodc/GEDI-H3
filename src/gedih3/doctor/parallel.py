# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Doctor-internal scandir helpers + back-compat re-export of
:func:`gedih3.parallel.parallel_map`.

The general parallelism primitive has been promoted to
:mod:`gedih3.parallel` (it is used by the build, download, and
manifest-writing paths too, not only by the doctor diagnoses). This
module now keeps only the doctor-internal O(1) scandir helpers used by
``doctor/diagnoses/orphans.py`` and re-exports ``parallel_map`` so any
external caller importing from here keeps working.
"""

from __future__ import annotations

import os
from typing import List

# Re-export for backward compatibility — see module docstring.
from ..parallel import parallel_map  # noqa: F401


def partition_is_empty(partition_dir: str) -> bool:
    """O(1) emptiness check via :func:`os.scandir` — no recursive glob.

    Mirrors the build pipeline's emptiness check at
    ``gh3builder.py:1356-1362``. A partition is considered empty when
    it contains no parquet file at any depth (year subdir or root).
    """
    try:
        for entry in os.scandir(partition_dir):
            if entry.name.endswith('.parquet'):
                return False
            if entry.is_dir(follow_symlinks=False):
                try:
                    for sub in os.scandir(entry.path):
                        if sub.name.endswith('.parquet'):
                            return False
                except OSError:
                    continue
    except OSError:
        return True
    return True


def list_year_dirs(partition_dir: str) -> List[str]:
    """List immediate subdirectories of a partition directory.

    Single ``os.scandir`` call instead of ``glob.glob('*/')``; same
    primitive the build pipeline uses to enumerate per-h3 work units.
    """
    out = []
    try:
        for entry in os.scandir(partition_dir):
            if entry.is_dir(follow_symlinks=False):
                out.append(entry.path + os.sep)
    except OSError:
        return []
    return sorted(out)


def year_dir_is_empty(year_dir: str) -> bool:
    """O(1) parquet presence check inside one year=*/ directory."""
    try:
        for entry in os.scandir(year_dir):
            if entry.name.endswith('.parquet'):
                return False
    except OSError:
        return True
    return True

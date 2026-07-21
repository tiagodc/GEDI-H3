# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Report and DoctorContext dataclasses shared by all diagnoses."""

from dataclasses import dataclass, field, asdict
from enum import Enum
from typing import Any, Dict, List, Optional


class Severity(str, Enum):
    INFO = 'info'
    WARN = 'warn'
    ERROR = 'error'


@dataclass
class Report:
    """Uniform output from a diagnosis check or fix.

    Attributes
    ----------
    name : str
        Diagnosis identifier (e.g. 'backfill', 'orphans').
    severity : Severity
        Worst severity across all findings.
    findings : list of dict
        Each finding describes one issue with enough context to remediate.
    applied : bool
        True when produced by a ``fix()``; False for a ``check()``.
    summary : str
        One-line human-readable summary.
    recommendations : list of str
        Suggested next-step CLI commands (printed verbatim, never executed).
    """
    name: str
    severity: Severity = Severity.INFO
    findings: List[Dict[str, Any]] = field(default_factory=list)
    applied: bool = False
    summary: str = ''
    recommendations: List[str] = field(default_factory=list)

    @property
    def has_findings(self) -> bool:
        return len(self.findings) > 0

    @property
    def is_clean(self) -> bool:
        return not self.has_findings or (self.applied and self.severity == Severity.INFO)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d['severity'] = self.severity.value
        return d


@dataclass
class DoctorContext:
    """Pre-loaded state passed to every diagnosis.

    The runner constructs this once per invocation and shares it across all
    diagnoses to avoid redundant filesystem walks and log re-reads.

    Attributes
    ----------
    h3_dir : str
        Root directory of the gedih3 database.
    soc_dir : str or None
        Root directory of downloaded SOC HDF5 files. None when running the
        doctor against a database whose source files are not local.
    tmp_dir : str or None
        Configured temp directory (for orphan scans and S3 ETL).
    h3_logger : H3BuildLogger
        Loaded build log (lazy-upgraded on construction).
    soc_logger : SOCDownloadLogger or None
        Loaded download log when ``soc_dir`` is set.
    partition_dirs : list of str
        Discovered ``h3_*/`` directories under ``h3_dir``.
    args : object
        Parsed argparse namespace (CLI args). Diagnoses read flags such as
        ``--orphan-age-hours``, ``--s3``, ``--online`` from here.
    upstream : dict or None
        Populated by :mod:`gedih3.doctor.upstream` when ``--online`` is set;
        keyed by product code.
    """
    h3_dir: str
    soc_dir: Optional[str] = None
    tmp_dir: Optional[str] = None
    h3_logger: Optional[Any] = None
    soc_logger: Optional[Any] = None
    partition_dirs: List[str] = field(default_factory=list)
    args: Optional[Any] = None
    upstream: Optional[Dict[str, Any]] = None

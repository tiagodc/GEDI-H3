"""gh3_doctor — audit and (optionally) heal a gedih3 database.

Public API:

- :class:`DoctorContext`   — pre-loaded shared state passed to every diagnosis.
- :class:`Report`          — uniform diagnosis output.
- :func:`get_diagnoses`    — registry lookup.
- :func:`run_diagnoses`    — orchestrator (Dask futures for partition-scope work).

Diagnoses register themselves on import via :func:`register`. Importing
``gedih3.doctor.diagnoses`` is sufficient to populate the registry.
"""

from .report import Report, DoctorContext, Severity
from .runner import register, get_diagnoses, run_diagnoses

__all__ = [
    'Report',
    'DoctorContext',
    'Severity',
    'register',
    'get_diagnoses',
    'run_diagnoses',
]

"""Diagnosis registry + orchestrator for gh3_doctor.

Each diagnosis is a callable with ``check(ctx) -> Report`` and optional
``fix(ctx, report) -> Report``. Diagnoses register themselves at import time
via :func:`register`; the package's :mod:`diagnoses` subpackage auto-imports
all known diagnosis modules.
"""

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .report import Report, DoctorContext, Severity


@dataclass
class Diagnosis:
    name: str
    description: str
    scope: str                          # 'global' or 'partition'
    check: Callable[[DoctorContext], Report]
    fix: Optional[Callable[[DoctorContext, Report], Report]] = None
    aliases: List[str] = None


_REGISTRY: Dict[str, Diagnosis] = {}

# Group aliases users can pass to --check/--fix. Resolved at dispatch time
# against whatever diagnoses have registered.
ALIAS_GROUPS = {
    'db': ['backfill', 'orphans', 'log_state', 'metadata', 'parquet_health'],
    'soc': ['soc_health'],
    'all': None,                         # special: all registered diagnoses
}


def register(name: str, description: str, scope: str = 'global',
             fix: Optional[Callable] = None):
    """Decorator to register a check function.

    Usage::

        @register('orphans', 'Leftover temp + empty partitions', scope='global', fix=orphans_fix)
        def orphans_check(ctx): ...
    """
    if scope not in ('global', 'partition'):
        raise ValueError(f"scope must be 'global' or 'partition', got {scope!r}")

    def decorator(check_fn):
        _REGISTRY[name] = Diagnosis(
            name=name,
            description=description,
            scope=scope,
            check=check_fn,
            fix=fix,
        )
        return check_fn

    return decorator


def get_diagnoses() -> Dict[str, Diagnosis]:
    """Return a copy of the diagnosis registry."""
    return dict(_REGISTRY)


def resolve_names(names: List[str]) -> List[str]:
    """Expand alias groups (``db``, ``soc``, ``all``) into concrete names."""
    if not names:
        return list(_REGISTRY.keys())

    out = []
    seen = set()
    for n in names:
        if n in ALIAS_GROUPS:
            members = ALIAS_GROUPS[n] if ALIAS_GROUPS[n] is not None else list(_REGISTRY.keys())
            for m in members:
                if m in _REGISTRY and m not in seen:
                    out.append(m)
                    seen.add(m)
        elif n in _REGISTRY:
            if n not in seen:
                out.append(n)
                seen.add(n)
        else:
            raise ValueError(
                f"Unknown diagnosis '{n}'. Available: {sorted(_REGISTRY)} "
                f"or aliases: {sorted(ALIAS_GROUPS)}"
            )
    return out


def run_diagnoses(ctx: DoctorContext, names: List[str], mode: str = 'check') -> List[Report]:
    """Run the named diagnoses (or aliases) against ``ctx``.

    Parameters
    ----------
    ctx : DoctorContext
        Shared state.
    names : list of str
        Diagnosis names or alias groups. Empty list = all registered.
    mode : str
        'check' (read-only) or 'fix' (apply remedies where supported).

    Returns
    -------
    list of Report
    """
    if mode not in ('check', 'fix'):
        raise ValueError(f"mode must be 'check' or 'fix', got {mode!r}")

    resolved = resolve_names(names)
    reports = []

    for name in resolved:
        diag = _REGISTRY[name]
        try:
            report = diag.check(ctx)
        except Exception as e:
            report = Report(
                name=name,
                severity=Severity.ERROR,
                summary=f"check failed: {type(e).__name__}: {e}",
                findings=[{'error': str(e), 'type': type(e).__name__}],
            )
            reports.append(report)
            continue

        if mode == 'fix' and report.has_findings and diag.fix is not None:
            try:
                report = diag.fix(ctx, report)
            except Exception as e:
                # Preserve the check report but flag the fix failure.
                report.severity = Severity.ERROR
                report.summary = f"{report.summary} | fix failed: {type(e).__name__}: {e}"
                report.findings.append({'fix_error': str(e), 'type': type(e).__name__})

        reports.append(report)

    return reports

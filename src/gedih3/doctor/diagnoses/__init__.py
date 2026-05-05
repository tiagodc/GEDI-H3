"""Diagnoses package — importing this module auto-registers all diagnoses.

Each diagnosis lives in its own submodule and registers itself via
:func:`gedih3.doctor.runner.register` at import time.
"""

# Order matters only for deterministic dispatch; runner.resolve_names preserves it.
from . import metadata          # noqa: F401
from . import orphans           # noqa: F401
from . import log_state         # noqa: F401
from . import parquet_health    # noqa: F401
from . import geoparquet_bbox   # noqa: F401
from . import backfill          # noqa: F401
from . import soc_health        # noqa: F401

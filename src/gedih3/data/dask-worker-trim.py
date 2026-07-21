# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""Dask worker preload: per-task gc + glibc malloc_trim cleanup.

Use as ``dask worker --preload /path/to/dask_worker_trim.py ...``.

Bounds unmanaged-memory growth on long-running tasks (multi-day GEDI builds)
by calling ``gc.collect()``, ``pyarrow.default_memory_pool().release_unused()``,
and ``libc.malloc_trim(0)`` after every task finishes. Combined with
``ARROW_DEFAULT_MEMORY_POOL=system`` and ``MALLOC_TRIM_THRESHOLD_=0`` in the
worker environment, this ensures Arrow buffers (parquet read/write) and pandas
allocations both flow through glibc and get returned to the OS promptly.

DO NOT USE with ``LD_PRELOAD=libjemalloc.so`` — calling glibc's ``malloc_trim``
while jemalloc is the active allocator can segfault (Dask docs warn explicitly).
For the jemalloc-preload strategy, omit this preload entirely and rely on
``MALLOC_CONF=background_thread:true,dirty_decay_ms:0,muzzy_decay_ms:0``.

Verify the preload was applied:
    client = Client("tcp://localhost:8786")
    client.run(lambda: __import__('os').environ.get("MALLOC_TRIM_THRESHOLD_"))
    client.run(lambda: __import__('pyarrow').default_memory_pool().backend_name)

References:
- https://distributed.dask.org/en/stable/worker-memory.html
- https://distributed.dask.org/en/stable/plugins.html
"""
import ctypes
import gc
import logging

from distributed.diagnostics.plugin import WorkerPlugin

_log = logging.getLogger(__name__)


class TrimPlugin(WorkerPlugin):
    """Run gc + Arrow pool release + malloc_trim after each task transition.

    Hooks ``transition`` and acts on terminal task states (memory, released,
    erred). Cheap per task (< few ms) and prevents slow RSS accumulation
    across the lifetime of a long-running worker.
    """

    name = "gh3-trim"

    def setup(self, worker):
        try:
            self._libc = ctypes.CDLL("libc.so.6")
        except OSError:
            self._libc = None
            _log.warning("TrimPlugin: libc.so.6 not loadable; malloc_trim disabled")
        try:
            import pyarrow as pa  # noqa: F401
            self._pa_pool = pa.default_memory_pool()
            backend = self._pa_pool.backend_name
            _log.info(f"TrimPlugin: pyarrow memory pool backend = {backend!r}")
            if backend != "system":
                _log.warning(
                    "TrimPlugin: pyarrow backend is %r — malloc_trim won't reclaim "
                    "Arrow buffers. Set ARROW_DEFAULT_MEMORY_POOL=system in the "
                    "worker environment for full effect.", backend,
                )
        except Exception:
            self._pa_pool = None

    def transition(self, key, start, finish, **kwargs):
        if finish not in ("memory", "released", "erred"):
            return
        gc.collect()
        if self._pa_pool is not None:
            try:
                self._pa_pool.release_unused()
            except Exception:
                pass
        if self._libc is not None:
            try:
                self._libc.malloc_trim(0)
            except Exception:
                pass


async def dask_setup(worker):
    """Entry point invoked by ``dask worker --preload <this-file>``.

    Async because ``Worker.plugin_add`` is a coroutine in modern Dask
    (≥2024.1). A sync ``dask_setup`` would leave the coroutine unawaited
    and the plugin would never register (silently — only a RuntimeWarning
    in the worker log).
    """
    await worker.plugin_add(TrimPlugin())
    _log.info("TrimPlugin registered on worker %s", getattr(worker, "address", "?"))

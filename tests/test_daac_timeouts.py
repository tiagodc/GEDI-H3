"""Tests for earthaccess HTTP timeout injection.

Verifies that `_install_request_timeouts` patches `SessionWithHeaderRedirection`
so that:

1. Calls without explicit `timeout=` get the injected default tuple.
2. Calls with explicit `timeout=` (e.g. earthaccess's 1s token probe) are
   preserved.
3. The patch is idempotent — repeat installs with the same values do not
   re-wrap; differing values re-install.
4. `is_retryable_error` recognizes `requests.exceptions.Timeout` so the
   download retry loop in `_download_with_retry` actually fires when a
   stalled CloudFront edge socket finally raises.
5. End-to-end against a stalling localhost server, a real read times out
   inside the injected window instead of blocking forever — the actual
   regression that motivated this patch.
"""
import os
import socket
import threading
import time

import pytest
import requests


@pytest.fixture
def reset_timeout_patch():
    """Strip any timeout patch from SessionWithHeaderRedirection both before
    and after each test, so cross-test ordering can't poison the class state.

    The full pytest suite imports gh3builder/daac in several places, and any
    indirect ``_install_request_timeouts()`` call leaves the class patched
    globally. Without an explicit pre-test wipe, ``_install_request_timeouts``
    short-circuits as a no-op (idempotent on matching values) and our fake
    ``request`` swap in the assertion tests never gets wrapped.
    """
    from earthaccess.auth import SessionWithHeaderRedirection as S

    def wipe():
        # Restore inheritance by stripping any override from the class —
        # do NOT trust ``_gh3_original_request`` here, because in tests
        # that also use monkeypatch, the saved original may be a stale
        # fake (monkeypatch tears down first, then this fixture runs).
        # ``type.__dict__`` is a mappingproxy (read-only), so we must use
        # ``delattr`` and gate it on ``in S.__dict__`` to skip inherited
        # attributes.
        for attr in ('request', '_gh3_original_request', '_gh3_timeouts_installed'):
            if attr in S.__dict__:
                delattr(S, attr)

    wipe()
    yield
    wipe()


def _capture_request_kwargs():
    """Return (capture_list, fake_request_callable).

    The fake replaces SessionWithHeaderRedirection.request before timeout
    installation, so we observe what kwargs the wrapper forwards.
    """
    captures = []

    def fake(self, method, url, **kwargs):
        captures.append(dict(method=method, url=url, **kwargs))
        # Mimic a successful response object minimally — only what tests check.
        return object()

    return captures, fake


def test_default_timeout_injected(reset_timeout_patch, monkeypatch):
    """Without explicit timeout, the wrapper injects (60, 300)."""
    from earthaccess.auth import SessionWithHeaderRedirection as S
    from gedih3.daac import _install_request_timeouts

    captures, fake = _capture_request_kwargs()
    monkeypatch.setattr(S, 'request', fake, raising=False)

    _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)

    sess = S('urs.earthdata.nasa.gov')
    sess.get('https://example.invalid/granule.h5', stream=True)

    assert len(captures) == 1
    assert captures[0]['timeout'] == (60.0, 300.0)


def test_explicit_timeout_preserved(reset_timeout_patch, monkeypatch):
    """earthaccess's 1s token probe must not be clobbered."""
    from earthaccess.auth import SessionWithHeaderRedirection as S
    from gedih3.daac import _install_request_timeouts

    captures, fake = _capture_request_kwargs()
    monkeypatch.setattr(S, 'request', fake, raising=False)

    _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)

    sess = S('urs.earthdata.nasa.gov')
    sess.get('https://urs.earthdata.nasa.gov/api/users/tokens', timeout=1)

    assert captures[0]['timeout'] == 1


def test_explicit_tuple_timeout_preserved(reset_timeout_patch, monkeypatch):
    """A caller passing an explicit (c, r) tuple must override the default."""
    from earthaccess.auth import SessionWithHeaderRedirection as S
    from gedih3.daac import _install_request_timeouts

    captures, fake = _capture_request_kwargs()
    monkeypatch.setattr(S, 'request', fake, raising=False)

    _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)

    sess = S('urs.earthdata.nasa.gov')
    sess.get('https://example.invalid/foo', timeout=(5, 10))

    assert captures[0]['timeout'] == (5, 10)


def test_install_is_idempotent(reset_timeout_patch):
    """Re-installing with the same values must not re-wrap."""
    from earthaccess.auth import SessionWithHeaderRedirection as S
    from gedih3.daac import _install_request_timeouts

    v1 = _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)
    wrapped_once = S.request
    v2 = _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)
    wrapped_twice = S.request

    assert v1 == v2 == (60.0, 300.0)
    assert wrapped_once is wrapped_twice  # no re-wrap


def test_install_reinstalls_on_value_change(reset_timeout_patch):
    """Different values overwrite the previous wrapper."""
    from earthaccess.auth import SessionWithHeaderRedirection as S
    from gedih3.daac import _install_request_timeouts

    _install_request_timeouts(connect_timeout=60.0, read_timeout=300.0)
    new_vals = _install_request_timeouts(connect_timeout=30.0, read_timeout=120.0)
    assert new_vals == (30.0, 120.0)
    assert S._gh3_timeouts_installed == (30.0, 120.0)


def test_env_vars_respected(reset_timeout_patch, monkeypatch):
    monkeypatch.setenv('GH3_DOWNLOAD_CONNECT_TIMEOUT', '15')
    monkeypatch.setenv('GH3_DOWNLOAD_READ_TIMEOUT', '90')

    from gedih3.daac import _install_request_timeouts

    vals = _install_request_timeouts()
    assert vals == (15.0, 90.0)


def test_requests_timeout_is_retryable():
    """is_retryable_error must catch requests.exceptions.Timeout directly,
    so the _download_with_retry loop fires when the injected timeout pops."""
    from gedih3.exceptions import is_retryable_error

    assert is_retryable_error(requests.exceptions.ReadTimeout("Read timed out"))
    assert is_retryable_error(requests.exceptions.ConnectTimeout("Connect timed out"))
    # requests.ConnectionError != builtin ConnectionError — must still match.
    assert is_retryable_error(
        requests.exceptions.ConnectionError("Connection aborted")
    )


# ---------------------------------------------------------------------------
# End-to-end: real socket-level read-timeout against a local stalling server.
# This is the regression test for the CloudFront CLOSE-WAIT zombie scenario
# the patch is meant to fix.
# ---------------------------------------------------------------------------


class _StallingTCPServer(threading.Thread):
    """Accept one connection, send an HTTP header + partial body, then sleep.

    Mimics the dead-edge symptom: bytes flow briefly, then the socket goes
    silent without sending FIN. Without an injected read timeout, the
    client's recv() blocks forever; with one, ReadTimeout fires.
    """
    def __init__(self):
        super().__init__(daemon=True)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(('127.0.0.1', 0))
        self.sock.listen(1)
        self.port = self.sock.getsockname()[1]
        self._stop = threading.Event()

    def run(self):
        try:
            conn, _ = self.sock.accept()
            # Read the request line + headers far enough to unblock client send.
            conn.recv(4096)
            # Send a valid HTTP response header claiming a large body, then
            # stream a single byte and stall. Client read() will block.
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Length: 1000000\r\n"
                b"Content-Type: application/octet-stream\r\n"
                b"\r\n"
                b"X"
            )
            # Hold connection open without sending more bytes.
            while not self._stop.wait(0.5):
                pass
            conn.close()
        finally:
            self.sock.close()

    def stop(self):
        self._stop.set()


def test_end_to_end_read_timeout_fires(reset_timeout_patch):
    """A stalled stream raises a retryable error inside the injected window.

    Note: `requests` re-wraps urllib3's `ReadTimeoutError` as
    `requests.exceptions.ConnectionError` when it fires inside `iter_content`
    (see requests.models.iter_content). Either flavor is fine — both are
    recognized by `is_retryable_error` and trigger `_download_with_retry`.
    """
    from gedih3.exceptions import is_retryable_error
    from earthaccess.auth import SessionWithHeaderRedirection
    from gedih3.daac import _install_request_timeouts

    # Short read timeout so the test is fast. Connect can stay generous.
    _install_request_timeouts(connect_timeout=5.0, read_timeout=2.0)

    server = _StallingTCPServer()
    server.start()
    try:
        sess = SessionWithHeaderRedirection('urs.earthdata.nasa.gov')
        start = time.monotonic()
        raised = None
        try:
            with sess.get(f'http://127.0.0.1:{server.port}/x', stream=True) as r:
                # Force a body read — this is where _download_file blocks.
                for _chunk in r.iter_content(chunk_size=1024 * 1024):
                    pass
        except Exception as e:
            raised = e
        elapsed = time.monotonic() - start
    finally:
        server.stop()
        server.join(timeout=5)

    # An exception must have been raised (no infinite block).
    assert raised is not None, "stalled stream did not raise — patch is not active"
    # The error class must be retryable, so `_download_with_retry` actually fires.
    assert is_retryable_error(raised), (
        f"injected timeout fired but is_retryable_error rejected it: "
        f"{type(raised).__name__}: {raised}"
    )
    # And it must have fired inside the injected window (2s read + slack).
    assert elapsed < 10.0, f"timeout did not fire in expected window: {elapsed:.1f}s"


def test_end_to_end_no_timeout_blocks(reset_timeout_patch):
    """Sanity check: without the patch, the same stall blocks past the
    short test budget. Confirms the bug exists and the patch is the fix."""
    from earthaccess.auth import SessionWithHeaderRedirection

    # Ensure no patch is installed (fixture restores at the end).
    assert not hasattr(SessionWithHeaderRedirection, '_gh3_timeouts_installed')

    server = _StallingTCPServer()
    server.start()
    try:
        sess = SessionWithHeaderRedirection('urs.earthdata.nasa.gov')

        result = {'raised': False, 'elapsed': None}

        def do_request():
            start = time.monotonic()
            try:
                with sess.get(
                    f'http://127.0.0.1:{server.port}/x', stream=True
                ) as r:
                    for _chunk in r.iter_content(chunk_size=1024 * 1024):
                        pass
                result['raised'] = False
            except Exception:
                result['raised'] = True
            result['elapsed'] = time.monotonic() - start

        t = threading.Thread(target=do_request, daemon=True)
        t.start()
        t.join(timeout=3.0)

        # Without the patch, the request thread is still blocked — the
        # stalling server holds it open with no timeout to free it.
        assert t.is_alive(), (
            "Expected request to still be blocked without timeout patch; "
            f"got elapsed={result['elapsed']}, raised={result['raised']}"
        )
    finally:
        server.stop()
        server.join(timeout=5)
        # Don't wait for the leaked request thread — server.stop() unblocks it.

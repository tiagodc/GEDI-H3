"""Remote storage configuration and existence-check behaviour.

Covers the two failure modes seen against a self-hosted, unauthenticated
S3 endpoint (``rclone serve s3`` / MinIO with auth off):

1. ``--s3-endpoint`` alone used to drop the anonymous-access default, so
   s3fs walked the botocore credential chain, raised ``NoCredentialsError``
   inside its own ``exists()``, and the caller saw a bare ``False``
   ("Database directory not found").
2. The database-root existence check degraded to a bucket LIST, which some
   servers answer only after walking the entire tree.
"""

import os

import pytest

from gedih3 import utils
from gedih3.config import (BUILD_LOG_FILENAME, DATASET_META_FILENAME,
                           MANIFEST_FILENAME)


@pytest.fixture(autouse=True)
def _clean_storage_options():
    """Isolate the module-level storage config from other tests."""
    saved = dict(utils._storage_options)
    utils._storage_options.clear()
    yield
    utils._storage_options.clear()
    utils._storage_options.update(saved)


# ---------------------------------------------------------------- options

def test_s3_defaults_to_anonymous_when_unconfigured():
    assert utils.get_storage_options('s3') == {'anon': True}


def test_endpoint_only_keeps_anonymous_default(monkeypatch):
    """The regression: an endpoint with no key/secret must stay anonymous
    when the botocore chain has nothing to offer either."""
    monkeypatch.setattr(utils, '_ambient_aws_credentials', lambda: False)
    utils.configure_storage('s3', endpoint_url='http://localhost:8855/')
    opts = utils.get_storage_options('s3')
    assert opts['anon'] is True
    assert opts['client_kwargs']['endpoint_url'] == 'http://localhost:8855/'


def test_endpoint_with_ambient_credentials_leaves_the_chain_alone(monkeypatch):
    """--s3-endpoint must not disable working env-var / profile / IAM auth."""
    monkeypatch.setattr(utils, '_ambient_aws_credentials', lambda: True)
    utils.configure_storage('s3', endpoint_url='http://minio:9000')
    assert 'anon' not in utils.get_storage_options('s3')


def test_unconfigured_default_is_anonymous_even_with_ambient_credentials(monkeypatch):
    """The long-standing public-bucket default is untouched."""
    monkeypatch.setattr(utils, '_ambient_aws_credentials', lambda: True)
    assert utils.get_storage_options('s3') == {'anon': True}


def test_requester_pays_suppresses_the_anonymous_default(monkeypatch):
    """requester_pays is meaningless anonymously — never emit both."""
    monkeypatch.setattr(utils, '_ambient_aws_credentials', lambda: False)
    utils.configure_storage('s3', requester_pays=True)
    assert 'anon' not in utils.get_storage_options('s3')


def test_credentials_suppress_the_anonymous_default():
    utils.configure_storage('s3', endpoint_url='http://localhost:8855/',
                            key='AK', secret='SK')
    opts = utils.get_storage_options('s3')
    assert 'anon' not in opts
    assert opts['key'] == 'AK' and opts['secret'] == 'SK'


def test_explicit_anon_false_is_respected():
    utils.configure_storage('s3', anon=False)
    assert utils.get_storage_options('s3')['anon'] is False


def test_non_s3_protocols_get_no_anon_default():
    assert utils.get_storage_options('http') == {}
    utils.configure_storage('http', headers={'Authorization': 'Bearer t'})
    assert utils.get_storage_options('http') == {'headers': {'Authorization': 'Bearer t'}}


# ------------------------------------------------------- existence checks

class _StubFS:
    """Minimal fsspec-like stub that records what was asked of it."""

    def __init__(self, present=(), list_raises=None):
        self.present = set(present)
        self.list_raises = list_raises
        self.exists_calls = []
        self.info_calls = []

    def exists(self, path):
        self.exists_calls.append(path)
        if self.list_raises is not None and path.rstrip('/') not in self.present:
            # Mimic s3fs: every error inside exists() is swallowed to False
            return False
        return path.rstrip('/') in self.present

    def info(self, path):
        self.info_calls.append(path)
        if self.list_raises is not None:
            raise self.list_raises
        if path.rstrip('/') not in self.present:
            raise FileNotFoundError(path)
        return {'name': path}


def _patch_fs(monkeypatch, fs):
    monkeypatch.setattr(utils, '_get_filesystem', lambda *a, **k: fs)
    return fs


@pytest.mark.parametrize('sidecar', [DATASET_META_FILENAME, BUILD_LOG_FILENAME,
                                     MANIFEST_FILENAME])
def test_database_exists_resolves_from_sidecar_without_listing(monkeypatch, sidecar):
    root = 's3://gedi_l2a_v3'
    fs = _patch_fs(monkeypatch, _StubFS(present=[f'{root}/{sidecar}']))
    assert utils.smart_database_exists(root) is True
    # The bucket root itself is never probed — no LIST is issued.
    assert all(c.rstrip('/') != root for c in fs.exists_calls)


def test_database_exists_falls_back_for_plain_parquet_dir(monkeypatch):
    root = 's3://bucket/plain_dir'
    fs = _patch_fs(monkeypatch, _StubFS(present=[root]))
    assert utils.smart_database_exists(root) is True
    assert root in [c.rstrip('/') for c in fs.exists_calls]


def test_database_exists_shortcuts_single_files(monkeypatch):
    path = 's3://bucket/one.parquet'
    fs = _patch_fs(monkeypatch, _StubFS(present=[path]))
    assert utils.smart_database_exists(path) is True
    assert fs.exists_calls == [path]  # no sidecar probing for a file


def test_local_paths_bypass_remote_machinery(tmp_path):
    assert utils.smart_database_exists(str(tmp_path)) is True
    assert utils.smart_database_exists(str(tmp_path / 'nope')) is False


# ------------------------------------------------- endpoint-in-URL parsing

def _ns(**kw):
    from argparse import Namespace
    base = dict(s3_endpoint=None, s3_key=None, s3_secret=None, s3_profile=None,
                s3_anon=False, remote_user=None, remote_pass=None,
                remote_token=None, ssh_key=None)
    base.update(kw)
    return Namespace(**base)


def test_endpoint_parsed_from_host_style_s3_url():
    from gedih3.cliutils import endpoint_from_s3_urls
    args = _ns(database='s3://localhost:8855/gedi_l2a_v3')
    assert endpoint_from_s3_urls(args) == 'http://localhost:8855'
    assert args.database == 's3://gedi_l2a_v3'


def test_endpoint_port_443_implies_https():
    from gedih3.cliutils import endpoint_from_s3_urls
    args = _ns(database='s3://minio.example.org:443/bucket/sub/dir')
    assert endpoint_from_s3_urls(args) == 'https://minio.example.org:443'
    assert args.database == 's3://bucket/sub/dir'


def test_plain_bucket_url_is_left_alone():
    from gedih3.cliutils import endpoint_from_s3_urls
    args = _ns(database='s3://my.bucket.name/key')
    assert endpoint_from_s3_urls(args) is None
    assert args.database == 's3://my.bucket.name/key'


def test_conflicting_endpoints_are_rejected():
    from gedih3.cliutils import endpoint_from_s3_urls
    from gedih3.exceptions import GediValidationError
    args = _ns(database='s3://host-a:9000/bucket', output='s3://host-b:9000/bucket')
    with pytest.raises(GediValidationError):
        endpoint_from_s3_urls(args)


def test_explicit_flag_wins_but_path_is_still_normalized(monkeypatch):
    from gedih3.cliutils import setup_storage
    monkeypatch.setattr(utils, '_ambient_aws_credentials', lambda: False)
    args = _ns(database='s3://localhost:8855/bucket', s3_endpoint='http://other:9000')
    setup_storage(args)
    assert args.database == 's3://bucket'
    opts = utils.get_storage_options('s3')
    assert opts['client_kwargs']['endpoint_url'] == 'http://other:9000'
    assert opts['anon'] is True


def test_s3_endpoint_flag_itself_is_never_rewritten():
    """--s3-endpoint s3://minio:9000 used to be mangled into the literal
    's3://', which then WON over the correctly derived endpoint."""
    from gedih3.cliutils import setup_storage
    args = _ns(database='s3://minio:9000/b/db', s3_endpoint='s3://minio:9000')
    setup_storage(args)
    assert args.s3_endpoint == 's3://minio:9000'  # untouched
    assert args.database == 's3://b/db'
    opts = utils.get_storage_options('s3')
    assert opts['client_kwargs']['endpoint_url'] == 'http://minio:9000'


def test_host_url_without_bucket_is_rejected():
    from gedih3.cliutils import endpoint_from_s3_urls
    from gedih3.exceptions import GediValidationError
    args = _ns(database='s3://host:9000')
    with pytest.raises(GediValidationError, match='no bucket'):
        endpoint_from_s3_urls(args)


def test_s3_profile_flag_reaches_storage_options():
    from gedih3.cliutils import setup_storage
    args = _ns(s3_profile='prod')
    setup_storage(args)
    opts = utils.get_storage_options('s3')
    assert opts['profile'] == 'prod'
    assert 'anon' not in opts  # a profile IS the credential intent


def test_isfile_is_extension_based_for_remote_paths(monkeypatch):
    """No probe at all — remote file-vs-dir must not cost a LIST."""
    fs = _patch_fs(monkeypatch, _StubFS(present=[]))
    assert utils.smart_isfile('s3://bucket/tile.parquet') is True
    assert utils.smart_isfile('http://host/db/part.feather') is True
    assert utils.smart_isfile('s3://bucket/dataset') is False
    assert utils.smart_isfile('s3://bucket/dataset/') is False
    # A trailing slash is an explicit directory signal — it must win even
    # over a data extension (gh3_extract -o s3://b/out.parquet writes a dir).
    assert utils.smart_isfile('s3://bucket/out.parquet/') is False
    assert not fs.exists_calls and not fs.info_calls


def test_isfile_local_paths_hit_the_filesystem(tmp_path):
    f = tmp_path / 'a.parquet'
    f.write_bytes(b'')
    assert utils.smart_isfile(str(f)) is True
    assert utils.smart_isfile(str(tmp_path)) is False


# ------------------------------------------------------ columnar read path

def test_open_columnar_disables_readahead_for_remote(monkeypatch):
    """fsspec's block cache re-fetches around every range pyarrow asks for."""
    seen = {}

    class _FS:
        def open(self, path, mode, **kw):
            seen.update(path=path, mode=mode, **kw)
            return 'handle'

    monkeypatch.setattr(utils, '_get_filesystem', lambda *a, **k: _FS())
    assert utils.smart_open_columnar('s3://bucket/part.parquet') == 'handle'
    assert seen['cache_type'] == 'none'
    assert seen['mode'] == 'rb'


def test_open_columnar_leaves_local_paths_alone(tmp_path):
    f = tmp_path / 'a.parquet'
    f.write_bytes(b'x')
    with utils.smart_open_columnar(str(f)) as fh:
        assert fh.read() == b'x'


def test_read_parquet_coalesced_round_trips(tmp_path):
    """Non-geo goes through read_table; the frame must be unchanged."""
    import pandas as pd

    df = pd.DataFrame({'a': [1, 2, 3], 'b': [1.5, 2.5, 3.5]})
    p = tmp_path / 'x.parquet'
    df.to_parquet(p)

    out = utils.read_parquet_coalesced(str(p), columns=['a'], geo=False)
    assert list(out.columns) == ['a']
    assert out['a'].tolist() == [1, 2, 3]
    assert out['a'].dtype == pd.read_parquet(p, columns=['a'])['a'].dtype


def test_read_parquet_coalesced_preserves_named_index(tmp_path):
    """A column-projected read must keep the index recorded in the pandas
    metadata — dropping it desyncs the Dask meta from the partitions and
    breaks h3 reindex/aggregation on remote (and only remote) loads."""
    import pandas as pd

    df = pd.DataFrame({'h3_12': ['8c0e4', '8c0e5', '8c0e6'],
                       'a': [1, 2, 3], 'b': [1.5, 2.5, 3.5]}).set_index('h3_12')
    p = tmp_path / 'idx.parquet'
    df.to_parquet(p)

    out = utils.read_parquet_coalesced(str(p), columns=['a'], geo=False)
    expected = pd.read_parquet(p, columns=['a'])
    assert out.index.name == 'h3_12'
    pd.testing.assert_frame_equal(out, expected)


def test_read_parquet_coalesced_passes_pre_buffer(monkeypatch):
    captured = {}

    def fake_read_parquet(source, **kw):
        captured.update(kw)
        return 'gdf'

    import geopandas as gpd
    monkeypatch.setattr(gpd, 'read_parquet', fake_read_parquet)
    assert utils.read_parquet_coalesced('s3://b/x.parquet', columns=['g'], geo=True) == 'gdf'
    assert captured['pre_buffer'] is True
    assert captured['columns'] == ['g']


def test_json_read_cached_reads_once_for_remote(monkeypatch):
    """The build log is 21 MB on a continental DB — read it once per process."""
    calls = []

    def fake_json_read(path, mode='r'):
        calls.append(path)
        return {'h3_partition_level': 3}

    monkeypatch.setattr(utils, 'json_read', fake_json_read)
    url = 's3://bucket/gedih3_build_log.json'
    utils._json_cache.pop(url, None)
    assert utils.json_read_cached(url)['h3_partition_level'] == 3
    assert utils.json_read_cached(url)['h3_partition_level'] == 3
    assert calls == [url]


def test_json_read_cached_revalidates_local_writes(tmp_path):
    import json
    p = tmp_path / 'gedih3_build_log.json'
    p.write_text(json.dumps({'v': 1}))
    assert utils.json_read_cached(str(p)) == {'v': 1}
    # os.replace, as AtomicFileWriter does — new inode, possibly same mtime tick
    other = tmp_path / 'tmp.json'
    other.write_text(json.dumps({'v': 2}))
    os.replace(other, p)
    assert utils.json_read_cached(str(p)) == {'v': 2}


@pytest.fixture
def utils_errors():
    """Collect ERROR records from the gedih3.utils logger.

    ``caplog`` cannot see them: ``logging_config.setup_logging`` sets
    ``propagate = False`` on the package logger, so records never reach
    the root handler pytest installs.
    """
    import logging

    class _Capture(logging.Handler):
        def __init__(self):
            super().__init__(level=logging.ERROR)
            self.messages = []

        def emit(self, record):
            self.messages.append(record.getMessage())

    handler = _Capture()
    log = logging.getLogger('gedih3.utils')
    log.addHandler(handler)
    try:
        yield handler.messages
    finally:
        log.removeHandler(handler)


def test_swallowed_remote_error_is_logged(monkeypatch, utils_errors):
    """A credential/endpoint failure must not masquerade as 'not found'.

    The loud (ERROR) probe lives in smart_database_exists — the one place
    the caller is about to hard-fail. smart_exists itself logs at DEBUG,
    because it also probes optional sidecars on successful loads (and S3
    answers a missing-key HEAD with 403 when ListBucket is denied).
    """
    boom = PermissionError('Unable to locate credentials')
    _patch_fs(monkeypatch, _StubFS(present=[], list_raises=boom))
    assert utils.smart_database_exists('s3://gedi_l2a_v3') is False
    assert any('Unable to locate credentials' in m for m in utils_errors)


def test_optional_sidecar_probe_does_not_log_errors(monkeypatch, utils_errors):
    """smart_exists on a 403-answering server stays quiet at ERROR level."""
    boom = PermissionError('AccessDenied')
    _patch_fs(monkeypatch, _StubFS(present=[], list_raises=boom))
    assert utils.smart_exists('s3://bucket/db/gedih3_build_log.json') is False
    assert not utils_errors


def test_genuine_miss_logs_nothing(monkeypatch, utils_errors):
    fs = _patch_fs(monkeypatch, _StubFS(present=[]))
    assert utils.smart_exists('s3://bucket/missing') is False
    assert not utils_errors
    assert fs.info_calls  # the probe ran, it just found a plain miss


# ------------------------------------------ host-URL parsing in the Python API

def test_split_s3_host_url_forms():
    from gedih3.exceptions import GediValidationError

    assert utils.split_s3_host_url('s3://minio:9000/b/db') == \
        ('http://minio:9000', 's3://b/db')
    assert utils.split_s3_host_url('s3://minio.example.org:443/b') == \
        ('https://minio.example.org:443', 's3://b')
    assert utils.split_s3_host_url('s3://my.bucket/key') == \
        (None, 's3://my.bucket/key')
    assert utils.split_s3_host_url('/local/path') == (None, '/local/path')
    assert utils.split_s3_host_url(None) == (None, None)
    with pytest.raises(GediValidationError, match='no bucket'):
        utils.split_s3_host_url('s3://host:9000')


def test_resolve_s3_source_sets_endpoint_once():
    norm = utils.resolve_s3_source('s3://minio:9000/b/db')
    assert norm == 's3://b/db'
    assert utils.get_storage_options('s3')['client_kwargs']['endpoint_url'] == \
        'http://minio:9000'
    # an explicitly configured endpoint wins; the path is still normalized
    utils.configure_storage('s3', endpoint_url='http://other:7000')
    norm2 = utils.resolve_s3_source('s3://minio:9000/b/db')
    assert norm2 == 's3://b/db'
    assert utils.get_storage_options('s3')['client_kwargs']['endpoint_url'] == \
        'http://other:7000'
    # non-host forms untouched
    assert utils.resolve_s3_source('s3://bucket/key') == 's3://bucket/key'
    assert utils.resolve_s3_source('/local/db') == '/local/db'


def test_python_loaders_normalize_host_urls(monkeypatch):
    """gh3_load / egi_load route through _detect_source; the host URL must
    arrive at the source detector as a plain bucket path with the endpoint
    configured — the same behavior the CLI gets from setup_storage."""
    import gedih3.cliutils as cliutils
    import gedih3.gh3driver as gh3drv

    captured = {}

    def fake_info(path):
        captured['path'] = path
        return {'source_type': 'h3_database', 'index_type': 'h3'}

    monkeypatch.setattr(cliutils, 'get_dataset_index_info', fake_info)
    path, info = gh3drv._detect_source('s3://minio:9000/bucket/db')
    assert path == 's3://bucket/db'
    assert captured['path'] == 's3://bucket/db'
    assert utils.get_storage_options('s3')['client_kwargs']['endpoint_url'] == \
        'http://minio:9000'

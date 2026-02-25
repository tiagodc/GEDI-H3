"""
Tests for gedih3.exceptions -- exception hierarchy and retry utilities.

Verifies that all 26 exception types are importable, follow the correct
inheritance hierarchy, carry custom attributes, and that retry utilities
work as expected.
"""

import socket
import urllib.error

import pytest

from gedih3.exceptions import (
    # Base
    GediError,
    # Network
    GediNetworkError,
    GediDownloadError,
    GediAuthenticationError,
    GediS3AccessError,
    # Validation
    GediValidationError,
    H3ValidationError,
    EGIValidationError,
    GediProductError,
    GediVariableError,
    # File
    GediFileError,
    GediHDF5Error,
    GediParquetError,
    GediCorruptedFileError,
    GediTransactionError,
    # Database
    GediDatabaseError,
    GediDatabaseNotFoundError,
    GediDatabaseCorruptedError,
    GediMergeError,
    # Spatial/Temporal
    GediSpatialError,
    GediTemporalError,
    # Processing
    GediProcessingError,
    GediAggregationError,
    GediRasterizationError,
    GediImageSamplingError,
    GediSpatialJoinError,
    # Utilities
    is_retryable_error,
    RETRY_DEFAULTS,
)


# =============================================================================
# Test: Exception Hierarchy
# =============================================================================

class TestExceptionHierarchy:

    def test_all_inherit_from_gedi_error(self):
        all_exceptions = [
            GediNetworkError, GediDownloadError, GediAuthenticationError,
            GediS3AccessError, GediValidationError, H3ValidationError,
            EGIValidationError, GediProductError, GediVariableError,
            GediFileError, GediHDF5Error, GediParquetError,
            GediCorruptedFileError, GediTransactionError, GediDatabaseError,
            GediDatabaseNotFoundError, GediDatabaseCorruptedError,
            GediMergeError, GediSpatialError, GediTemporalError,
            GediProcessingError, GediAggregationError,
            GediRasterizationError, GediImageSamplingError,
            GediSpatialJoinError,
        ]
        for exc_cls in all_exceptions:
            assert issubclass(exc_cls, GediError), f"{exc_cls.__name__} should be GediError"

    def test_all_inherit_from_exception(self):
        assert issubclass(GediError, Exception)

    def test_network_hierarchy(self):
        assert issubclass(GediDownloadError, GediNetworkError)
        assert issubclass(GediAuthenticationError, GediNetworkError)
        assert issubclass(GediS3AccessError, GediNetworkError)
        assert issubclass(GediNetworkError, GediError)

    def test_validation_hierarchy(self):
        assert issubclass(H3ValidationError, GediValidationError)
        assert issubclass(EGIValidationError, GediValidationError)
        assert issubclass(GediProductError, GediValidationError)
        assert issubclass(GediVariableError, GediValidationError)
        assert issubclass(GediValidationError, GediError)

    def test_file_hierarchy(self):
        assert issubclass(GediHDF5Error, GediFileError)
        assert issubclass(GediParquetError, GediFileError)
        assert issubclass(GediCorruptedFileError, GediFileError)
        assert issubclass(GediTransactionError, GediFileError)
        assert issubclass(GediFileError, GediError)

    def test_database_hierarchy(self):
        assert issubclass(GediDatabaseNotFoundError, GediDatabaseError)
        assert issubclass(GediDatabaseCorruptedError, GediDatabaseError)
        assert issubclass(GediMergeError, GediDatabaseError)
        assert issubclass(GediDatabaseError, GediError)

    def test_processing_hierarchy(self):
        assert issubclass(GediAggregationError, GediProcessingError)
        assert issubclass(GediRasterizationError, GediProcessingError)
        assert issubclass(GediImageSamplingError, GediProcessingError)
        assert issubclass(GediSpatialJoinError, GediProcessingError)
        assert issubclass(GediProcessingError, GediError)

    def test_spatial_temporal_direct(self):
        assert issubclass(GediSpatialError, GediError)
        assert issubclass(GediTemporalError, GediError)

    def test_isinstance_chain(self):
        """A GediDownloadError should be catchable as GediNetworkError and GediError."""
        err = GediDownloadError("test", granule_id="G123", attempts=3)
        assert isinstance(err, GediDownloadError)
        assert isinstance(err, GediNetworkError)
        assert isinstance(err, GediError)
        assert isinstance(err, Exception)


# =============================================================================
# Test: Custom Attributes
# =============================================================================

class TestCustomAttributes:

    def test_download_error_attributes(self):
        err = GediDownloadError("failed", granule_id="G123", attempts=3)
        assert err.granule_id == "G123"
        assert err.attempts == 3
        assert str(err) == "failed"

    def test_download_error_defaults(self):
        err = GediDownloadError("failed")
        assert err.granule_id is None
        assert err.attempts is None

    def test_h3_validation_error_attributes(self):
        err = H3ValidationError("bad res", param_name="resolution", value=20)
        assert err.param_name == "resolution"
        assert err.value == 20

    def test_egi_validation_error_attributes(self):
        err = EGIValidationError("bad level", param_name="level", value=0)
        assert err.param_name == "level"
        assert err.value == 0

    def test_hdf5_error_attributes(self):
        err = GediHDF5Error("corrupt file", file_path="/path/to/file.h5")
        assert err.file_path == "/path/to/file.h5"

    def test_parquet_error_attributes(self):
        err = GediParquetError("read failed", file_path="/path/to/file.parquet")
        assert err.file_path == "/path/to/file.parquet"

    def test_corrupted_file_error_attributes(self):
        err = GediCorruptedFileError("corrupted", file_path="/path/to/file")
        assert err.file_path == "/path/to/file"

    def test_transaction_error_attributes(self):
        err = GediTransactionError("failed", source_path="/a", dest_path="/b")
        assert err.source_path == "/a"
        assert err.dest_path == "/b"


# =============================================================================
# Test: is_retryable_error
# =============================================================================

class TestIsRetryableError:

    def test_connection_error(self):
        assert is_retryable_error(ConnectionError("reset")) is True

    def test_timeout_error(self):
        assert is_retryable_error(TimeoutError("timed out")) is True

    def test_socket_timeout(self):
        assert is_retryable_error(socket.timeout("timeout")) is True

    def test_socket_gaierror(self):
        assert is_retryable_error(socket.gaierror("name resolution")) is True

    def test_url_error(self):
        assert is_retryable_error(urllib.error.URLError("connection refused")) is True

    def test_500_in_message(self):
        assert is_retryable_error(Exception("HTTP Error 500")) is True

    def test_503_in_message(self):
        assert is_retryable_error(Exception("503 Service Unavailable")) is True

    def test_timeout_in_message(self):
        assert is_retryable_error(Exception("request timed out")) is True

    def test_non_retryable(self):
        assert is_retryable_error(ValueError("bad value")) is False

    def test_non_retryable_404(self):
        assert is_retryable_error(Exception("404 Not Found")) is False


# =============================================================================
# Test: RETRY_DEFAULTS
# =============================================================================

class TestRetryDefaults:

    def test_keys_present(self):
        assert 'max_attempts' in RETRY_DEFAULTS
        assert 'initial_wait' in RETRY_DEFAULTS
        assert 'max_wait' in RETRY_DEFAULTS
        assert 'exponential_base' in RETRY_DEFAULTS
        assert 'jitter' in RETRY_DEFAULTS

    def test_values_reasonable(self):
        assert RETRY_DEFAULTS['max_attempts'] >= 1
        assert RETRY_DEFAULTS['initial_wait'] > 0
        assert RETRY_DEFAULTS['max_wait'] >= RETRY_DEFAULTS['initial_wait']
        assert RETRY_DEFAULTS['exponential_base'] >= 1.0
        assert isinstance(RETRY_DEFAULTS['jitter'], bool)

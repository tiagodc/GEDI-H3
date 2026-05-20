#! python
"""
gedih3 Exception Hierarchy

Custom exceptions for structured error handling throughout the package.
All exceptions inherit from GediError for easy catching of package-specific errors.

Usage:
    from gedih3.exceptions import GediDownloadError, GediValidationError

    try:
        download_granule(granule, odir)
    except GediDownloadError as e:
        logger.error(f"Download failed: {e}")
"""


class GediError(Exception):
    """Base exception for all gedih3 errors."""
    pass


# =============================================================================
# Network/Download Errors
# =============================================================================

class GediNetworkError(GediError):
    """Base exception for network-related errors."""
    pass


class GediDownloadError(GediNetworkError):
    """Error during file download from NASA DAAC."""

    def __init__(self, message: str, granule_id: str = None, attempts: int = None):
        self.granule_id = granule_id
        self.attempts = attempts
        super().__init__(message)


class GediAuthenticationError(GediNetworkError):
    """Error during NASA Earthdata authentication."""
    pass


class GediS3AccessError(GediNetworkError):
    """Error accessing GEDI data via S3."""
    pass


# =============================================================================
# Validation Errors
# =============================================================================

class GediValidationError(GediError):
    """Base exception for validation errors."""
    pass


class H3ValidationError(GediValidationError):
    """Error in H3 parameter validation."""

    def __init__(self, message: str, param_name: str = None, value = None):
        self.param_name = param_name
        self.value = value
        super().__init__(message)


class EGIValidationError(GediValidationError):
    """Error in EGI parameter validation."""

    def __init__(self, message: str, param_name: str = None, value = None):
        self.param_name = param_name
        self.value = value
        super().__init__(message)


class GediProductError(GediValidationError):
    """Error with GEDI product specification."""
    pass


class GediVariableError(GediValidationError):
    """Error with GEDI variable specification."""
    pass


# =============================================================================
# File/IO Errors
# =============================================================================

class GediFileError(GediError):
    """Base exception for file-related errors."""
    pass


class GediHDF5Error(GediFileError):
    """Error reading/processing HDF5 file."""

    def __init__(self, message: str, file_path: str = None):
        self.file_path = file_path
        super().__init__(message)


class GediParquetError(GediFileError):
    """Error reading/writing Parquet file."""

    def __init__(self, message: str, file_path: str = None):
        self.file_path = file_path
        super().__init__(message)


class GediCorruptedFileError(GediFileError):
    """File is corrupted or unreadable."""

    def __init__(self, message: str, file_path: str = None):
        self.file_path = file_path
        super().__init__(message)


class GediTransactionError(GediFileError):
    """Error during atomic file operation."""

    def __init__(self, message: str, source_path: str = None, dest_path: str = None):
        self.source_path = source_path
        self.dest_path = dest_path
        super().__init__(message)


# =============================================================================
# Database Errors
# =============================================================================

class GediDatabaseError(GediError):
    """Base exception for H3 database errors."""
    pass


class GediDatabaseNotFoundError(GediDatabaseError):
    """H3 database directory not found."""
    pass


class GediDatabaseCorruptedError(GediDatabaseError):
    """H3 database is corrupted or inconsistent."""
    pass


class GediMergeError(GediDatabaseError):
    """Error merging H3 partitions or databases."""
    pass


# =============================================================================
# Spatial/Temporal Errors
# =============================================================================

class GediSpatialError(GediError):
    """Error with spatial operations or filters."""
    pass


class GediTemporalError(GediError):
    """Error with temporal operations or filters."""
    pass


# =============================================================================
# Processing Errors
# =============================================================================

class GediProcessingError(GediError):
    """Base exception for data processing errors."""
    pass


class GediAggregationError(GediProcessingError):
    """Error during data aggregation."""
    pass


class GediRasterizationError(GediProcessingError):
    """Error during rasterization."""
    pass


class GediImageSamplingError(GediProcessingError):
    """Error during raster image sampling at GEDI shot locations."""
    pass


class GediSpatialJoinError(GediProcessingError):
    """Error during spatial join of vector data to GEDI shot locations."""
    pass


# =============================================================================
# Retry Configuration
# =============================================================================

# Default retry settings for network operations
RETRY_DEFAULTS = {
    'max_attempts': 3,
    'initial_wait': 1.0,  # seconds
    'max_wait': 60.0,  # seconds
    'exponential_base': 2.0,
    'jitter': True,
}


def is_retryable_error(exception: Exception) -> bool:
    """
    Determine if an exception is retryable.

    Returns True for transient network errors that may succeed on retry.
    """
    import socket
    import urllib.error

    # `requests.exceptions.Timeout` does NOT inherit from builtin
    # ``TimeoutError``; it lives under ``requests.exceptions.RequestException``
    # → ``IOError``. List it explicitly so injected per-request timeouts
    # (see gedih3.daac._install_request_timeouts) reliably trigger the
    # download retry loop instead of falling back to string-pattern matching.
    try:
        from requests.exceptions import (
            Timeout as _RequestsTimeout,
            ConnectionError as _RequestsConnectionError,
        )
        _requests_retryable = (_RequestsTimeout, _RequestsConnectionError)
    except ImportError:
        _requests_retryable = ()

    # Network-level errors
    retryable_types = (
        ConnectionError,
        TimeoutError,
        socket.timeout,
        socket.gaierror,
        urllib.error.URLError,
    ) + _requests_retryable

    if isinstance(exception, retryable_types):
        return True

    # Check for HTTP status codes in exception message
    error_msg = str(exception).lower()
    retryable_patterns = [
        '500', '502', '503', '504',  # Server errors
        'timeout', 'timed out',
        'connection reset',
        'connection refused',
        'temporary failure',
        'service unavailable',
    ]

    return any(pattern in error_msg for pattern in retryable_patterns)

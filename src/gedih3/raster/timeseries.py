# Copyright (C) 2026, University of Maryland. All Rights Reserved.
# Authors: Tiago de Conto, Amelia Grace Holcomb
# For commercial licensing inquiries, contact UM Ventures at umdtechtransfer@umd.edu

"""
Time-Series Raster Generation Module

This module provides functions for generating time-series raster products
from GEDI data. It supports:
- Temporal filtering by date range
- Time-windowed aggregation (years, months, weeks, days)
- Batch raster generation for time intervals
"""
from typing import Callable, Generator, List, Optional, Tuple, Union
import datetime
import pandas as pd
import geopandas as gpd
import xarray as xr
from dateutil.relativedelta import relativedelta

from .config import TIME_UNITS, GEDI_START_DATE_STR


# GEDI mission start date
GEDI_START_DATE = pd.Timestamp(GEDI_START_DATE_STR)


def parse_datetime_column(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    time_col: Optional[str] = None
) -> Optional[str]:
    """
    Find and identify the datetime column in a DataFrame.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input data
    time_col : str, optional
        Explicit column name to use

    Returns
    -------
    str or None
        Name of the datetime column, or None if not found
    """
    if time_col is not None and time_col in df.columns:
        return time_col

    # Look for common datetime column names
    for col in df.columns:
        col_lower = str(col).lower()
        if 'datetime' in col_lower or col_lower == 'time':
            return col
        if 'delta_time' in col_lower:
            return col

    return None


def convert_delta_time_to_datetime(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    delta_time_col: str = 'delta_time',
    output_col: str = 'datetime'
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Convert GEDI delta_time (seconds since epoch) to datetime.

    GEDI delta_time is seconds since 2018-01-01 00:00:00 UTC.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input data with delta_time column
    delta_time_col : str
        Name of the delta_time column
    output_col : str
        Name for the output datetime column

    Returns
    -------
    DataFrame or GeoDataFrame
        Data with new datetime column added
    """
    if delta_time_col not in df.columns:
        raise ValueError(f"Column '{delta_time_col}' not found in DataFrame")

    # Convert delta_time (seconds since GEDI epoch) to datetime
    df = df.copy()
    df[output_col] = pd.to_datetime(
        df[delta_time_col] + GEDI_START_DATE.timestamp(),
        unit='s'
    )

    return df


def generate_time_windows(
    start_date: Union[str, datetime.datetime],
    end_date: Union[str, datetime.datetime],
    interval: int,
    units: str = 'years'
) -> Generator[Tuple[datetime.datetime, datetime.datetime, str], None, None]:
    """
    Generate time windows for temporal aggregation.

    Parameters
    ----------
    start_date : str or datetime
        Start date (YYYY-MM-DD format if string)
    end_date : str or datetime
        End date (YYYY-MM-DD format if string)
    interval : int
        Number of time units per window
    units : str
        Time unit: 'years', 'months', 'weeks', 'days'

    Yields
    ------
    tuple
        (window_start, window_end, suffix_string)

    Examples
    --------
    >>> # Generate yearly windows
    >>> for t0, t1, suffix in generate_time_windows('2020-01-01', '2023-01-01', 1, 'years'):
    ...     print(f"{suffix}: {t0} to {t1}")
    2020-01-01-to-2021-01-01: 2020-01-01 00:00:00 to 2021-01-01 00:00:00
    2021-01-01-to-2022-01-01: 2021-01-01 00:00:00 to 2022-01-01 00:00:00
    2022-01-01-to-2023-01-01: 2022-01-01 00:00:00 to 2023-01-01 00:00:00
    """
    if units not in TIME_UNITS:
        raise ValueError(f"Invalid time unit: {units}. Must be one of {TIME_UNITS}")

    # Parse dates
    if isinstance(start_date, str):
        start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d')
    if isinstance(end_date, str):
        end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d')

    # Warn about pre-GEDI dates
    gedi_start = datetime.datetime(2019, 4, 17)
    if start_date < gedi_start:
        import warnings
        warnings.warn(f"No GEDI data available before 2019-04-17. Start date is {start_date}")

    # Generate windows
    time_delta = relativedelta(**{units: interval})
    t0 = start_date

    while t0 < end_date:
        t1 = t0 + time_delta
        if t1 > end_date:
            t1 = end_date

        # Create suffix string
        suffix = f"{t0.strftime('%Y-%m-%d')}-to-{t1.strftime('%Y-%m-%d')}"

        yield t0, t1, suffix
        t0 = t1


def filter_by_time_range(
    df: Union[pd.DataFrame, gpd.GeoDataFrame],
    start_date: Optional[Union[str, datetime.datetime]] = None,
    end_date: Optional[Union[str, datetime.datetime]] = None,
    time_col: str = 'datetime'
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Filter DataFrame by time range.

    Parameters
    ----------
    df : DataFrame or GeoDataFrame
        Input data with datetime column
    start_date : str or datetime, optional
        Start date for filtering (inclusive)
    end_date : str or datetime, optional
        End date for filtering (exclusive)
    time_col : str
        Name of the datetime column

    Returns
    -------
    DataFrame or GeoDataFrame
        Filtered data
    """
    if time_col not in df.columns:
        raise ValueError(f"Datetime column '{time_col}' not found")

    result = df

    if start_date is not None:
        if isinstance(start_date, str):
            start_date = pd.Timestamp(start_date)
        result = result[result[time_col] >= start_date]

    if end_date is not None:
        if isinstance(end_date, str):
            end_date = pd.Timestamp(end_date)
        result = result[result[time_col] < end_date]

    return result


def build_temporal_query(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    time_col: str = 'datetime'
) -> Optional[str]:
    """
    Build a pandas query string for temporal filtering.

    Parameters
    ----------
    start_date : str, optional
        Start date (YYYY-MM-DD)
    end_date : str, optional
        End date (YYYY-MM-DD)
    time_col : str
        Name of the datetime column

    Returns
    -------
    str or None
        Query string for pandas.query(), or None if no filters
    """
    queries = []

    if start_date:
        queries.append(f"{time_col} >= '{start_date}'")
    if end_date:
        queries.append(f"{time_col} < '{end_date}'")

    if queries:
        return ' and '.join(queries)
    return None


class TimeSeriesRasterizer:
    """
    Class for generating time-series raster products.

    This class provides a convenient interface for generating multiple
    raster outputs for different time windows from a single dataset.

    Parameters
    ----------
    gdf : GeoDataFrame or dask GeoDataFrame
        Input H3 or EGI-indexed data
    time_col : str
        Name of the datetime column
    aggregation : str, list, dict, or callable
        Aggregation specification for spatial aggregation
    target_level : int
        Target spatial resolution level (H3 or EGI)
    use_egi : bool
        If True, use EGI; if False, use H3
    columns : list, optional
        Columns to include in output

    Examples
    --------
    >>> rasterizer = TimeSeriesRasterizer(
    ...     gdf=data,
    ...     time_col='datetime',
    ...     aggregation='mean',
    ...     target_level=6,
    ...     use_egi=True
    ... )
    >>>
    >>> # Generate yearly rasters
    >>> for raster, suffix in rasterizer.generate(
    ...     start_date='2020-01-01',
    ...     end_date='2023-01-01',
    ...     interval=1,
    ...     units='years'
    ... ):
    ...     raster.rio.to_raster(f"output_{suffix}.tif")
    """

    def __init__(
        self,
        gdf: Union[pd.DataFrame, gpd.GeoDataFrame],
        time_col: str = 'datetime',
        aggregation: Union[str, List, dict, Callable] = 'mean',
        target_level: int = 6,
        use_egi: bool = False,
        columns: Optional[List[str]] = None
    ):
        self.gdf = gdf
        self.time_col = time_col
        self.aggregation = aggregation
        self.target_level = target_level
        self.use_egi = use_egi
        self.columns = columns

        # Verify datetime column exists
        if time_col not in gdf.columns:
            # Try to find it
            found_col = parse_datetime_column(gdf)
            if found_col:
                self.time_col = found_col
            else:
                raise ValueError(f"Datetime column '{time_col}' not found in data")

    def generate(
        self,
        start_date: str,
        end_date: str,
        interval: int,
        units: str = 'years',
        to_raster: bool = True
    ) -> Generator[Tuple[Union[gpd.GeoDataFrame, xr.Dataset], str], None, None]:
        """
        Generate time-series outputs.

        Parameters
        ----------
        start_date : str
            Start date (YYYY-MM-DD)
        end_date : str
            End date (YYYY-MM-DD)
        interval : int
            Number of time units per window
        units : str
            Time unit: 'years', 'months', 'weeks', 'days'
        to_raster : bool
            If True, convert to raster; if False, return GeoDataFrame

        Yields
        ------
        tuple
            (output_data, time_suffix)
        """
        for t0, t1, suffix in generate_time_windows(start_date, end_date, interval, units):
            # Filter data for this time window
            window_data = filter_by_time_range(
                self.gdf,
                start_date=t0,
                end_date=t1,
                time_col=self.time_col
            )

            if len(window_data) == 0:
                continue

            # Aggregate spatially
            aggregated = self._aggregate(window_data)

            if to_raster:
                # Convert to raster
                raster = self._rasterize(aggregated)
                raster = raster.assign_attrs(
                    time_start=t0.isoformat(),
                    time_end=t1.isoformat()
                )
                yield raster, suffix
            else:
                yield aggregated, suffix

    def _aggregate(self, gdf: Union[pd.DataFrame, gpd.GeoDataFrame]) -> gpd.GeoDataFrame:
        """Perform spatial aggregation."""
        if self.use_egi:
            from .. import egi
            # Convert to EGI and aggregate
            egi_df = egi.egi_dataframe(gdf, level=self.target_level)
            return egi.egi_aggregate(egi_df, mapper=self.aggregation)
        else:
            # H3 aggregation
            from ..gh3driver import gh3_aggregate_func, gh3_add_geometry
            agg_df = gh3_aggregate_func(
                gdf,
                res=self.target_level,
                agg=self.aggregation,
                cols=self.columns
            )
            return gh3_add_geometry(agg_df)

    def _rasterize(self, gdf: gpd.GeoDataFrame) -> xr.Dataset:
        """Convert aggregated data to raster."""
        if self.use_egi:
            from .. import egi
            # A time-window aggregate over an arbitrary ROI legitimately
            # spans multiple 160-km outer tiles — split per tile and merge,
            # instead of relying on geodf_to_raster's single-tile fallback
            # (which rasterizes only the dominant tile and drops the rest).
            rasters = egi.rasterize_partition(gdf, columns=self.columns)
            if len(rasters) == 0:
                return xr.Dataset()
            if len(rasters) == 1:
                return rasters.iloc[0]
            return egi.merge_raster_partitions(rasters)
        else:
            from .h3_raster import h3_to_raster
            return h3_to_raster(gdf, columns=self.columns)

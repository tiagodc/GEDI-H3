# Python API

All functionality available through the CLI is also accessible from Python — with two important advantages:

1. **No intermediate files**: Chain operations in memory without saving and reloading datasets between steps.
2. **Custom aggregation functions**: Pass any Python callable to `gh3_aggregate` or `egi_aggregate`, enabling analyses that are impossible from the CLI alone.

---

## Core Workflow in Python

The following mirrors the 5-step CLI Quick Start entirely in Python:

```python
from gedih3.daac import gedi_download
import gedih3.gh3driver as gh3
from gedih3 import raster

# Step 1: Download GEDI data
gedi_download(
    product_vars={'L2A': ['minimal'], 'L4A': ['minimal']},
    spatial=[-51, 0, -50, 1],           # [W, S, E, N]
    temporal=('2020-01-01', '2022-12-31'),
    resume=True,
)

# Step 2: Build the H3 database
from gedih3.gh3builder import build_h3_database
build_h3_database(
    region=[-51, 0, -50, 1],
    product_vars={'L2A': ['minimal'], 'L4A': ['minimal']},
)

# Step 3: Load and filter data
ddf = gh3.gh3_load(
    source='~/gedi_data/h3/',
    columns=['agbd_l4a', 'rh_098_l2a'],
    region=[-51, 0, -50, 1],
    query='quality_flag_l2a == 1 and agbd_l4a > 0',
)

# Step 4: Aggregate to H3 level 6 (~36 km²)
agg_df = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')
gdf = agg_df.compute()  # collect results

# Step 5: Export as GeoTIFF
xras = raster.h3_to_raster(gdf)
raster.export_raster(xras, 'agbd_mean.tif', compress='LZW')
```

---

## Custom Aggregation Functions

This is the most powerful feature of the Python API. `gh3_aggregate` accepts any callable that takes a DataFrame (one H3 hexagon's worth of data) and returns a DataFrame with results. This enables analyses such as regression, statistical modeling, or custom metrics — computed independently per hexagon across all Dask partitions.

### Example: Linear Regression Per Hexagon

```python
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score, mean_squared_error
import gedih3.gh3driver as gh3

def fit_height_biomass(df):
    """Fit a linear regression of biomass ~ canopy height for each H3 hexagon."""
    mask = ~(df['agbd_l4a'].isna() | df['rh_098_l2a'].isna())
    X = df.loc[mask, 'rh_098_l2a'].values.reshape(-1, 1)
    y = df.loc[mask, 'agbd_l4a'].values

    if len(X) < 2:
        return pd.DataFrame({'r2': [np.nan], 'rmse': [np.nan], 'coef': [np.nan], 'n': [len(df)]})

    model = LinearRegression().fit(X, y)
    y_pred = model.predict(X)
    return pd.DataFrame({
        'r2':        [r2_score(y, y_pred)],
        'rmse':      [np.sqrt(mean_squared_error(y, y_pred))],
        'coef':      [model.coef_[0]],
        'intercept': [model.intercept_],
        'n':         [len(df)],
    })

ddf = gh3.gh3_load(source='~/gedi_data/h3/', columns=['agbd_l4a', 'rh_098_l2a'])

# Each hexagon gets its own regression — Dask processes all partitions in parallel
results = gh3.gh3_aggregate(ddf, target_res=6, agg=fit_height_biomass)
results_df = results.compute()
```

> **Suggested image**: A map visualization of the per-hexagon R² values from this regression, showing spatial patterns of biomass-height correlation strength across a forested region. Generated with matplotlib and geopandas from the `results_df` output.

The callable receives a pandas DataFrame with all shots in one Dask partition. The return value must be a DataFrame with one row per group — in this case, one row per H3 hexagon.

---

## Loading Datasets

### From the H3 Database

```python
import gedih3.gh3driver as gh3

# Load with spatial and temporal filters
ddf = gh3.gh3_load(
    source='/path/to/h3_database/',
    columns=['agbd_l4a', 'rh_098_l2a'],
    region='region.shp',              # shapefile, bbox "W,S,E,N", or ISO3 code
    query='quality_flag_l2a == 1',    # pandas-style filter string
    t0='2020-01-01',                  # temporal filter (optional)
    t1='2022-12-31',
)
```

### From a Simplified Dataset (gh3_extract / gh3_aggregate output)

```python
# Load as GeoDataFrame (in memory)
gdf = gh3.gh3_load_dataset('/path/to/extracted/')

# Load lazily as Dask DataFrame (for large datasets)
ddf = gh3.gh3_load_dataset_lazy('/path/to/extracted/')
```

---

## Aggregation

### Built-in Functions

```python
# Single function
agg = gh3.gh3_aggregate(ddf, target_res=6, agg='mean')

# Multiple functions at once
agg = gh3.gh3_aggregate(ddf, target_res=6, agg=['mean', 'std', 'count'])
```

Available built-in functions: `mean`, `sum`, `median`, `std`, `count`, `min`, `max`.

### Custom Callable

```python
# Any function: DataFrame → DataFrame (one row per group)
agg = gh3.gh3_aggregate(ddf, target_res=6, agg=my_custom_function)
```

---

## Rasterization

```python
from gedih3 import raster

# H3 hexagon GeoDataFrame → xarray Dataset
gdf = gh3.gh3_aggregate(ddf, target_res=6, agg='mean').compute()
xras = raster.h3_to_raster(gdf, columns=['agbd_l4a_mean'])

# Export to GeoTIFF
raster.export_raster(xras, 'agbd.tif', compress='LZW')

# Visualize in-memory
xras['agbd_l4a_mean'].plot()
```

### Time-Series Rasterization

```python
from gedih3.raster import TimeSeriesRasterizer

ts = TimeSeriesRasterizer(gdf, time_col='datetime', target_level=6)
for xras, suffix in ts.generate('2020-01-01', '2023-01-01', 1, 'years'):
    raster.export_raster(xras, f'agbd_{suffix}.tif')
```

---

## Ancillary Data Integration

### Sample a Raster at GEDI Shot Locations

```python
from gedih3.imgutils import from_image, parse_window_specs

# Sample DEM elevation at each GEDI shot location
ddf = from_image(
    image_path='/path/to/dem.tif',
    data_source='/path/to/h3_database/',
    region='region.shp',
    band_names=['elevation'],
)
```

### Join Polygon Attributes to GEDI Shots

```python
from gedih3.vecutils import join_polygons_to_points

# Use within map_partitions for Dask integration
result = ddf.map_partitions(
    join_polygons_to_points,
    vector_path='ecoregions.shp',
    join_columns=['ECO_NAME', 'BIOME_NAME'],
    prefix='eco_',
)
```

---

## Exporting Data

```python
# Export to flat Parquet (default)
gh3.gh3_export(ddf, output='path/to/output/', drop_internal=True)

# Export with geometry (GeoParquet)
gh3.gh3_export(ddf, output='path/to/output/', geometry=True)
```

The exported dataset is compatible with `gh3_rasterize`, `gh3_from_img`, `gh3_from_polygon`, and any tool that reads Parquet (pandas, R, QGIS, DuckDB).

---

## Dask Client Configuration

For large datasets, configure the Dask distributed client directly:

```python
from dask.distributed import Client

client = Client(n_workers=8, threads_per_worker=2, memory_limit='8GB')

# All gh3 operations automatically use this client
ddf = gh3.gh3_load(source='~/gedi_data/h3/', columns=['agbd_l4a'])
agg = gh3.gh3_aggregate(ddf, target_res=6, agg='mean').compute()

client.close()
```

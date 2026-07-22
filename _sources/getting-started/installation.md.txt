# Installation

## Prerequisites

- Python 3.12+
- conda (recommended) or pip
- NASA Earthdata account (for downloading GEDI data)

No system libraries are required by either install path. GDAL, GEOS, PROJ and
HDF5 all arrive prebuilt — vendored inside the wheels on pip, or as conda
packages.

## Using conda (recommended)

```bash
git clone https://github.com/tiagodc/GEDI-H3
cd GEDI-H3

conda env create -f environment.yml -n gedih3
conda activate gedih3

gh3_build --help
```

Recommended for HPC and shared clusters: conda-forge resolves the full native
stack as a coherent set, and the environment includes extras that pip does not
install — JupyterLab, the plotting stack, and the GDAL Python bindings.

## Using pip

```bash
pip install gedih3
```

This is self-contained. Every dependency with a native component ships binary
wheels that vendor their own libraries, so nothing needs to be installed
system-wide beforehand:

| Native library | Comes from | Notes |
|---|---|---|
| GDAL | `rasterio` | vendored as `rasterio.libs/libgdal-*.so` |
| GEOS | `shapely` | vendored as `shapely.libs/libgeos-*.so` |
| PROJ (+ proj-data) | `pyproj` | vendored, including the datum grids |
| HDF5 | `h5py` | vendored |
| OGR / vector drivers | `pyogrio` | geopandas' IO engine |

These vendored copies are privately renamed, so they neither require nor
conflict with any GDAL/GEOS/PROJ already installed on the machine.

### Platform support

Wheels cover the mainstream targets. Where no wheel matches, pip falls back to
building from source, which *does* require a full system toolchain (compilers,
`libgdal-dev`, `libgeos-dev`, `libproj-dev`, `libhdf5-dev`) — use conda there
instead.

| Platform | CPython 3.12 / 3.13 |
|---|---|
| Linux x86-64 | fully supported |
| Linux aarch64 | fully supported |
| macOS (Apple Silicon) | fully supported |
| macOS (Intel) | `numba` and `h3` no longer publish Intel-macOS wheels — use conda |
| Windows x64 | fully supported |
| Windows ARM64 | several dependencies have no wheels — use conda |

### Optional: GDAL Python bindings

gedih3 does not require the `osgeo` GDAL bindings. They are used for one thing
— building the VRT mosaic that accompanies tiled raster output — and
`gh3_rasterize`, `gh3_aggregate -R` and `gh3_from_img` all fall back to a
rasterio-only VRT writer when they are absent.

If you want the bindings anyway, note that **`pip install gedih3[gdal]` will
not work**. PyPI's GDAL package ships no wheels, only a source distribution,
and it refuses to build unless a system libgdal of the *exact same version* is
already installed — while pip, left to itself, always selects the newest
release. The version must be matched by hand:

```bash
# Debian / Ubuntu
sudo apt-get install -y libgdal-dev gdal-bin
pip install "GDAL==$(gdal-config --version)"
```

Or simply use the conda environment, where `gdal` is included.

## Runtime requirements

Two things are needed at run time rather than install time.

### NASA Earthdata credentials

GEDI data is hosted by the NASA DAACs (Distributed Active Archive Centers).
Authentication is required for downloads.

1. Create an account at [https://urs.earthdata.nasa.gov/](https://urs.earthdata.nasa.gov/)
2. Create `~/.netrc` with your credentials:

```
machine urs.earthdata.nasa.gov
    login YOUR_USERNAME
    password YOUR_PASSWORD
```

3. Verify authentication:

```bash
python -c "import earthaccess; earthaccess.login()"
```

### DuckDB extensions (`gh3_build_ducklake` only)

On first run, `gh3_build_ducklake` downloads the DuckDB `spatial` and
community `h3` extensions from the DuckDB extension repository. This needs
outbound network access once; the extensions are then cached locally.

On air-gapped systems, pre-populate an extension directory on a connected
machine and point DuckDB at it via the `extension_directory` parameter of
`gedih3.sqlutils.init_duckdb`.

## Verify Installation

```bash
gh3_build --help
gh3_list_resolutions
```

# Installation

## Prerequisites

- Python 3.12+
- NASA Earthdata account (for downloading GEDI data)

gedih3 is published on [PyPI](https://pypi.org/project/gedih3/) and
[conda-forge](https://anaconda.org/conda-forge/gedih3). No system libraries are
required by any install path: GDAL, GEOS, PROJ and HDF5 arrive prebuilt —
vendored inside the wheels on PyPI, or as shared conda packages resolved by the
solver on conda-forge.

## conda / mamba (recommended for HPC and shared clusters)

```bash
conda install -c conda-forge gedih3
# or
mamba install -c conda-forge gedih3
```

conda-forge resolves the full native stack as one coherent set of shared
libraries, which is why it is the recommended choice on HPC and shared systems.

### In an environment file

List `gedih3` and let the solver pull its own stack. Re-declaring its
dependencies is redundant, and a loose re-declaration can pin one below the
floor gedih3 asks for:

```yaml
name: my-project
channels:
  - conda-forge
dependencies:
  - python >=3.12
  - gedih3
```

Two things to avoid, both of which cost solver time rather than failing
outright:

- **Do not add `duckdb`.** On conda-forge that name is the DuckDB CLI, and it
  pins `python-duckdb` to its own exact version. gedih3 requires
  `python-duckdb >=1.4.0,<1.5` — the DuckLake catalog format is version-locked
  to the 1.4.x series — so an unpinned `duckdb` resolves to a newer release
  that contradicts it, and the solver backtracks through every version in
  between before settling. The `duckdb` module you `import` is provided by
  `python-duckdb`, which gedih3 already installs. If you also need the CLI
  binary, constrain it to match: `duckdb >=1.4.0,<1.5`.

- **Do not widen the Python floor below `>=3.12`.** A bound such as
  `python >=3.10` makes the solver expand build variants for every compiled
  package at Python versions gedih3 will ultimately reject.

## pip

```bash
pip install gedih3
```

## uv

```bash
uv pip install gedih3    # into the currently active environment
uv add gedih3            # add to a uv-managed project (pyproject.toml)
```

`pip` and `uv` install the same PyPI wheels. This is self-contained: every
dependency with a native component ships binary wheels that vendor their own
libraries, so nothing needs to be installed system-wide beforehand:

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

Or simply install gedih3 from conda-forge, where the `gdal` bindings are pulled
in as a dependency.

## From source (development)

To work on gedih3 itself, clone the repository and install in editable mode.
The bundled conda environment additionally provides JupyterLab, the plotting
stack, and the GDAL Python bindings:

```bash
git clone https://github.com/tiagodc/GEDI-H3
cd GEDI-H3

conda env create -f environment.yml -n gedih3
conda activate gedih3

# ...or into any virtualenv, no system libraries required:
# pip install -e ".[test]"

gh3_build --help
```

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

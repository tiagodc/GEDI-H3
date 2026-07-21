# Configuration

gedih3 works out of the box with no configuration. By default, all files are stored under `~/gedi_data/`:

| Data Type | Default Location |
|-----------|-----------------|
| Downloaded HDF5 files | `~/gedih3_db/soc/` |
| H3 database | `~/gedih3_db/h3/` |
| Temporary files | `~/gedih3_db/tmp/` |

If the defaults work for you, skip this page entirely.

---

## Customizing Storage Paths

To use different storage locations, set environment variables or create a `~/.gedih3.env` file.

### Option A: Environment Variables

```bash
export GH3_DEFAULT_DOWNLOAD_DIR=/data/gedi
export GH3_DEFAULT_H3_DIR=/data/gedi/h3_db
export GH3_DEFAULT_SOC_DIR=/data/gedi/soc
export GH3_DEFAULT_TMP_DIR=/data/gedi/tmp
```

### Option B: create a `~/.gedih3.env` File

```bash
GH3_DEFAULT_DOWNLOAD_DIR=/data/gedi
GH3_DEFAULT_H3_DIR=/data/gedi/h3_db
GH3_DEFAULT_SOC_DIR=/data/gedi/soc
GH3_DEFAULT_TMP_DIR=/data/gedi/tmp
```

:::{tip} You don't need to set all the above environment variables, only the ones you want to modify - only setting `GH3_DEFAULT_H3_DIR` is enough for most applications.
:::

### Configuration Priority

Settings are applied in this order (highest to lowest priority):

1. CLI arguments (e.g., `-d /path/to/database`)
2. Environment variables
3. `~/.gedih3.env` file
4. Package defaults (`~/gedi_data/`)

---

## Dask Configuration

gedih3 uses Dask for distributed processing. By default, it creates a local cluster sized to your machine. Most users don't need to change this.

### CLI Dask Flags

| Flag | Description |
|------|-------------|
| `-N` | Number of workers (default: half of all available cores) |
| `-T` | Threads per worker |
| `-M` | Memory per worker (e.g., `8GB`, default splits all memory equaly across workers) |
| `-P` | Dask dashboard port |
| `-s` | Connect to an existing Dask scheduler (e.g. tcp://localhost:8787) |

### Python: Dask Client

```python
from dask.distributed import Client

client = Client(n_workers=8, threads_per_worker=1, memory_limit='8GB')
# All gedih3 operations automatically use this client
```

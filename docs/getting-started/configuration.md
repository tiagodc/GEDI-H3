# Configuration

gedih3 reads configuration from three sources, in decreasing priority:

1. Command-line arguments (highest)
2. Environment variables (`GH3_DEFAULT_*`)
3. `~/.gedih3.env` file (lowest)

## Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `GH3_DEFAULT_DOWNLOAD_DIR` | Base directory for all data | `~/gedi_data` |
| `GH3_DEFAULT_TMP_DIR` | Temporary files | `<download_dir>/tmp` |
| `GH3_DEFAULT_SOC_DIR` | Downloaded GEDI HDF5 files | `<download_dir>/soc` |
| `GH3_DEFAULT_H3_DIR` | H3-indexed parquet database | `<download_dir>/h3` |

## `~/.gedih3.env` file

```bash
GH3_DEFAULT_DOWNLOAD_DIR=/data/gedi
GH3_DEFAULT_H3_DIR=/data/gedi/h3_db
```

## Dask Configuration

For large-scale processing, a custom Dask config file can be passed:

```bash
gh3_build --dask-config dask-config-aggressive-memory.yaml -r "W,S,E,N" ...
```

The aggressive memory config enables:
- Worker restart every 15 minutes (prevents memory accumulation)
- Lower memory thresholds (target: 10%, pause: 80%)
- Smaller 64 MiB chunk sizes

### Dask CLI flags

| Flag | Description |
|------|-------------|
| `-N` | Number of workers |
| `-T` | Threads per worker |
| `-M` | Memory per worker (e.g., `8GB`) |
| `-P` | Dask dashboard port |
| `-s` | Connect to existing Dask scheduler |
| `--dask-config` | Path to Dask YAML config file |

# gedih3 Docker Image

A self-contained Docker image for running all `gedih3` CLI tools without a conda installation.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or [Podman](https://podman.io/) — see [Rootless Usage](#rootless-usage-podman) below)
- NASA Earthdata account — required for downloading GEDI data ([register here](https://urs.earthdata.nasa.gov/))

---

## Build the Image

Run from the repository root:

```bash
docker build -f docker/Dockerfile -t gedih3:latest .
```

The build copies only `pyproject.toml` and `src/` into the image context (see `.dockerignore`). All Python dependencies are installed globally via `pip` — no conda overhead.

Expected image size: ~2.5–3 GB (dominated by scipy, numba, and geospatial wheel bundles).

---

## Quick Start

### List available resolutions

```bash
docker run --rm gedih3:latest gh3_list_resolutions
```

### Interactive shell

```bash
docker run -it --rm \
  -v /your/local/data:/data \
  -v ~/.netrc:/root/.netrc:ro \
  gedih3:latest
```

Inside the container, all `gh3_*` CLI tools are available on `PATH`.

---

## Volume Mounts

The container uses `/data` as its base data directory with the following layout:

| Path | Purpose | env var |
|------|---------|---------|
| `/data` | Base directory | `GH3_DEFAULT_DOWNLOAD_DIR` |
| `/data/soc` | Downloaded GEDI HDF5 files | `GH3_DEFAULT_SOC_DIR` |
| `/data/h3` | H3-indexed parquet database | `GH3_DEFAULT_H3_DIR` |
| `/data/tmp` | Temporary processing files | `GH3_DEFAULT_TMP_DIR` |

Mount your host data directory to `/data`:

```bash
-v /path/to/your/data:/data
```

Or override individual paths via environment variables:

```bash
-e GH3_DEFAULT_H3_DIR=/mnt/fast_disk/h3db
```

---

## NASA Earthdata Credentials

NASA GEDI downloads require authentication. Pass credentials via `~/.netrc`:

```bash
# Create ~/.netrc if it doesn't exist
cat >> ~/.netrc <<EOF
machine urs.earthdata.nasa.gov
    login YOUR_USERNAME
    password YOUR_PASSWORD
EOF
chmod 600 ~/.netrc
```

Mount it read-only into the container:

```bash
docker run --rm \
  -v /your/local/data:/data \
  -v ~/.netrc:/root/.netrc:ro \
  gedih3:latest \
  gh3_download -r="-10,10,10,20" -l4a default
```

---

## Full Workflow Example

```bash
DATA=/your/local/data
REGION="W,S,E,N"   # e.g. "-60,-10,-50,0"
CREDS="-v $HOME/.netrc:/root/.netrc:ro"
VOL="-v $DATA:/data"

# 1. Download GEDI L4A data
docker run --rm $VOL $CREDS gedih3:latest \
  gh3_download -r="$REGION" -l4a default

# 2. Build H3 database
docker run --rm $VOL gedih3:latest \
  gh3_build -r="$REGION" -l4a default -h3r 12 -h3p 3

# 3. Extract data
docker run --rm $VOL -v $DATA/output:/workspace gedih3:latest \
  gh3_extract -r="$REGION" -l4a agbd -o /workspace/
```

---

## Configuration via Environment File

Instead of passing `-e` flags, write a `.env` file and use `--env-file`:

```bash
# gedih3.env
GH3_DEFAULT_DOWNLOAD_DIR=/data
GH3_DEFAULT_SOC_DIR=/data/soc
GH3_DEFAULT_H3_DIR=/data/h3
GH3_DEFAULT_TMP_DIR=/data/tmp
```

```bash
docker run --rm \
  --env-file gedih3.env \
  -v /your/local/data:/data \
  gedih3:latest \
  gh3_build --help
```

---

## Rootless Usage (Podman)

[Podman](https://podman.io/) is a Docker-compatible container runtime that runs entirely without root privileges, making it suitable for HPC environments and systems without admin access.

**Install via conda (no admin required):**

```bash
conda install -c conda-forge podman
```

Podman uses the same command syntax as Docker — simply replace `docker` with `podman`:

```bash
podman build -f docker/Dockerfile -t gedih3:latest .
podman run --rm -v /your/data:/data gedih3:latest gh3_list_resolutions
```

> **Note:** Rootless Podman requires Linux kernel ≥ 3.8 with user namespace support enabled (`/proc/sys/kernel/unprivileged_userns_clone` = 1 on Debian-based systems). RHEL/Rocky/AlmaLinux 9 supports this by default.

---

## Dask Configuration

For large datasets, tune Dask memory limits by mounting a custom config:

```bash
docker run --rm \
  -v /your/data:/data \
  -v ./dask-config.yaml:/root/.config/dask/dask.yaml:ro \
  gedih3:latest \
  gh3_aggregate -egi 6 -a mean -o /data/output/
```

---

## Available CLI Tools

All `gedih3` commands are installed on `PATH`:

| Command | Purpose |
|---------|---------|
| `gh3_download` | Download GEDI HDF5 files from NASA DAAC |
| `gh3_build` | Build H3-indexed parquet database |
| `gh3_extract` | Extract data from H3 database |
| `gh3_aggregate` | Aggregate extracted data |
| `gh3_rasterize` | Convert to GeoTIFF |
| `gh3_list_resolutions` | List H3/EGI resolution levels |
| `gh3_read_schema` | Inspect dataset schema and variables |
| `gh3_update` | Update existing dataset |
| `gh3_from_img` | Sample raster values at shot locations |
| `gh3_from_polygon` | Join polygon attributes to shots |
| `gh3_build_ducklake` | Build DuckLake format database |

For usage details: `docker run --rm gedih3:latest gh3_<command> --help`

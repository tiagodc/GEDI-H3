# Docker

Docker provides a self-contained runtime for all `gedih3` CLI tools — no conda installation required. This is useful for:

- Machines where conda is not available or not permitted
- Reproducible, isolated execution environments
- CI/CD pipelines and automated workflows
- Sharing a consistent environment across teams

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or [Podman](https://podman.io/) — see below)
- A [NASA Earthdata account](https://urs.earthdata.nasa.gov/) for downloading GEDI data

## Build the Image

From the repository root:

```bash
docker build -f docker/Dockerfile -t gedih3:latest .
```

The image is based on `python:3.12-slim`. All Python dependencies are installed globally via `pip`. The geospatial libraries (GDAL, PROJ, HDF5) are bundled inside the Python wheels — no system-level packages are required.

## Data Volumes

The container exposes `/data` as its base directory:

```
/data/
├── soc/    ← downloaded GEDI HDF5 files
├── h3/     ← H3-indexed parquet database
└── tmp/    ← temporary processing files
```

Mount your host data directory:

```bash
docker run -it --rm \
  -v /path/to/your/data:/data \
  -v ~/.netrc:/root/.netrc:ro \
  gedih3:latest
```

## NASA Credentials

Add your Earthdata credentials to `~/.netrc` (see [Configuration](configuration.md)), then mount it read-only:

```bash
-v ~/.netrc:/root/.netrc:ro
```

## Running Commands

Any `gh3_*` tool can be run directly:

```bash
# List resolution levels
docker run --rm gedih3:latest gh3_list_resolutions

# Download GEDI data
docker run --rm \
  -v /your/data:/data \
  -v ~/.netrc:/root/.netrc:ro \
  gedih3:latest \
  gh3_download -r="-60,-10,-50,0" -l4a default

# Build H3 database
docker run --rm \
  -v /your/data:/data \
  gedih3:latest \
  gh3_build -r="-60,-10,-50,0" -l4a default -h3r 12 -h3p 3
```

## Rootless Usage (Podman)

[Podman](https://podman.io/) is a Docker-compatible runtime that runs without root. It can be installed via conda on Linux — no admin privileges required:

```bash
conda install -c conda-forge podman
```

Replace `docker` with `podman` in all commands above. The Dockerfile and image are fully compatible.

```{note}
Rootless Podman requires Linux kernel ≥ 3.8 with user namespace support. This is satisfied on most modern distributions including RHEL/Rocky/AlmaLinux 9.
```

## Further Reference

See [`docker/README.md`](https://github.com/tiagodc/GEDI-H3/blob/main/docker/README.md) in the repository for the full reference including workflow examples, environment file configuration, and Dask tuning.

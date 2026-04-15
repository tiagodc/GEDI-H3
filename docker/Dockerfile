FROM python:3.12-slim

# OpenMP runtime required by numba for parallel JIT compilation
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /gedih3

# Copy only package metadata and source — everything else is excluded by .dockerignore
COPY pyproject.toml ./
COPY src/ ./src/

# Install gedih3 and all dependencies globally via pip (no conda, no venv)
# rasterio/fiona/pyproj/h5py ship manylinux wheels that bundle GDAL/PROJ/HDF5 —
# no system GDAL libraries needed.
RUN pip install --no-cache-dir .

# Default data directory layout; mount your host data directory here
RUN mkdir -p /data/h3 /data/soc /data/tmp

# Point gedih3 at /data — override with -e or --env-file at runtime
ENV GH3_DEFAULT_DOWNLOAD_DIR=/data
ENV GH3_DEFAULT_TMP_DIR=/data/tmp
ENV GH3_DEFAULT_SOC_DIR=/data/soc
ENV GH3_DEFAULT_H3_DIR=/data/h3

# Convenient working directory for output files
WORKDIR /workspace

# Default to interactive shell; override with any gh3_ command
CMD ["bash"]

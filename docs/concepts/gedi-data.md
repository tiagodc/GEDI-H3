# GEDI Data

## What is GEDI?

The **Global Ecosystem Dynamics Investigation (GEDI)** is a NASA full-waveform LiDAR instrument mounted on the International Space Station (ISS). Launched in December 2018, GEDI fires laser pulses at the Earth's surface and records the returning energy waveform — capturing the vertical distribution of vegetation and terrain beneath the canopy.

GEDI covers the Earth between approximately 51.6°N and 51.6°S (the ISS orbital inclination), providing dense coverage of the world's tropical and temperate forests — the ecosystems where carbon monitoring matters most.

> **Suggested image**: A global map of GEDI orbit coverage density, showing the concentration of tracks over tropical and temperate forests. This is available from NASA/ORNL DAAC or can be generated with gedih3 data plotted on a world basemap.

---

## What Does GEDI Measure?

Each laser pulse illuminates a circular footprint approximately 25 m in diameter on the ground. The returned waveform is processed into a series of data products:

| Product | Description | Key Variables |
|---------|-------------|---------------|
| **L1B** | Geolocated raw waveforms | Full waveform data |
| **L2A** | Ground elevation and canopy height | RH percentiles (rh_000–rh_100), canopy top height |
| **L2B** | Canopy cover and vertical structure | Cover, PAI, PAVD vertical profiles |
| **L4A** | Footprint-level aboveground biomass (AGBD) | `agbd`, prediction intervals |
| **L4C** | Structural complexity | WSCI (Woody Structural Complexity Index) |

The most widely used products for forest science are **L2A** (canopy height) and **L4A** (aboveground biomass density), which together provide a direct view of forest structure and carbon storage at a global scale.

> **Suggested image**: A schematic of a LiDAR waveform showing the ground return and canopy return, with annotations linking waveform features to L2A metrics (RH percentiles, canopy height). This is a standard conceptual diagram widely used in GEDI publications.

---

## Why Is GEDI Data Hard to Work With?

Despite its scientific importance, raw GEDI data presents significant technical challenges:

**1. Orbit-organized, not spatially organized**
GEDI files are organized by acquisition time (year/day-of-year), not by geography. Answering "give me all shots over the Amazon" requires scanning thousands of files spanning multiple years.

**2. Complex HDF5 file format**
Each granule is a large HDF5 file (~1–3 GB) with a deeply nested structure: 8 beams per file, hundreds of variables per beam, and a non-intuitive hierarchy. Reading GEDI data correctly requires understanding this structure and using `h5py` or similar specialized libraries.

**3. Quality filtering is non-trivial**
Each product has its own quality flags (`quality_flag`, `l4_quality_flag`, `degrade_flag`, `sensitivity`), and best practices for data filtering involve combining multiple criteria. Getting this wrong leads to noisy or biased results.

**4. Scale**
The full GEDI dataset spans billions of footprints across thousands of HDF5 files. Even simple regional analyses can take hours without proper spatial indexing and distributed processing.

**5. Variable proliferation**
L2A alone provides over 300 variables per beam. Knowing which variables are relevant for a given analysis requires domain expertise.

> **Suggested image**: A schematic comparing the raw GEDI file structure (nested HDF5 with orbit files, beams, and variables) against the gedih3 output structure (flat GeoParquet partitioned by H3 cell). This illustrates the transformation gedih3 performs.

---

## How gedih3 Addresses These Challenges

gedih3 was built by remote sensing scientists with direct experience working with GEDI data at scale. Rather than exposing raw complexity, it provides:

**Expert-curated variable presets**
Instead of navigating hundreds of variables, use `minimal` (essential metrics only) or `default` (standard science-ready set) presets for each product. These presets were designed for common use cases and reflect community best practices.

```bash
# Build a database with the default science-ready variable set
gh3_build -r "-51,0,-50,1" -l2a default -l4a default
```

**Pre-configured quality filtering**
A single `-y` flag applies scientifically-validated quality filters, combining multiple quality criteria correctly across products.

```bash
# Extract only high-quality observations
gh3_extract -y -l agbd_l4a rh_098_l2a -o filtered/
```

**Spatial indexing from the ground up**
gedih3 converts GEDI's orbit-organized HDF5 files into a spatially-indexed GeoParquet database. Once built, regional queries that would take hours on raw HDF5 complete in seconds.

**Transparent, reproducible pipelines**
Every database build is logged with metadata (products, variables, region, resolution levels). `gh3_read_schema` lets you inspect what any database or output file contains.

---

## GEDI Mission Resources

- [GEDI homepage at UMD](https://gedi.umd.edu/)
- [ORNL DAAC GEDI data access](https://daac.ornl.gov/gedi/)
- [GEDI product documentation](https://gedi.umd.edu/dataproducts/download/)
- [earthaccess — NASA Earthdata Python library](https://earthaccess.readthedocs.io/en/stable/)

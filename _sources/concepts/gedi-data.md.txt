# GEDI Data

## What is GEDI?

The **Global Ecosystem Dynamics Investigation (GEDI)** is a NASA full-waveform LiDAR instrument mounted on the International Space Station (ISS). Launched in December 2018, GEDI fires laser pulses at the Earth's surface and records the returning energy waveform — capturing the vertical distribution of vegetation and terrain beneath the canopy.

GEDI covers the Earth between approximately 51.6°N and 51.6°S (the ISS orbital inclination), providing dense coverage of the world's tropical and temperate forests. See the [GEDI homepage](https://gedi.umd.edu/) for more information.

:::{figure} ../imgs/gedi_tracks.png
:alt: GEDI shot locations over a 1°×1° study area
GEDI shot locations 2019–2025 over a 1°×1° study area in the western Amazon basin, colored by acquisition year. The parallel diagonal lines reveal the ISS orbital structure.
:::

---

## What Does GEDI Measure?

Each laser pulse illuminates a circular footprint approximately 25 m in diameter on the ground. The returned waveform is processed into a series of data products:

| Product | Description | Key Variables |
|---------|-------------|---------------|
| **L1B** | Geolocated raw waveforms | Full waveform data |
| **L2A** | Ground elevation and canopy height | ground geolocation, relative height (RH) percentiles |
| **L2B** | Canopy cover and vertical structure | Cover, PAI, PAVD vertical profiles |
| **L4A** | Footprint-level aboveground biomass (AGBD) | aboveground biomass density |
| **L4C** | Structural complexity | WSCI (Waveform Structural Complexity Index) |

The most widely used products for forest science are **L2A** (canopy height) and **L4A** (aboveground biomass density), which provide a direct view of forest structure and carbon storage at a near-global scale.

:::{figure} ../imgs/lidar_waveform.png
:alt: Schematic full-waveform LiDAR profile and RH metrics.
:::

---

## Key Variables by Product

The tables below list the most often used variables from each GEDI product on applications related to remote sensing and forest monitoring. For every product the HDF5 variable name (as it appears in the raw `.h5` file) is shown alongside the column name produced by gedih3 (which appends a product suffix, e.g. `_l2a`). Quality flags are included in every product — filtering on them is **highly recommended** for science applications.

### L1B — Geolocated Waveforms

L1B contains the raw digitised waveform from each laser pulse. Most applications do not rely with L1B data directly — it is the input to L2A/L2B — but it is included here for completeness and for users building custom waveform-processing workflows.

:::{warning}
`rxwaveform` is a variable-length waveform. **gedih3** expands it into 1420 individual columns: `rxwaveform_0000_l1b` through `rxwaveform_1419_l1b`. For small study areas (a few thousand shots) this is manageable. For databases covering millions of shots it becomes impractical, with dramatic impacts on disk and memory usage. **Building L1B waveform data for large areas is strongly discouraged.** Use L1B only if your workflow specifically requires the raw waveform shape and/or you're working from an HPCC environment.
:::

| HDF5 variable | gedih3 column | Short description | Definition |
|---|---|---|---|
| `rxwaveform` | `rxwaveform_0000_l1b` … `rxwaveform_1419_l1b` (**1420 columns**) | Raw return waveform | Digitised energy profile of the full return signal; encodes the vertical distribution of surfaces from canopy top to ground |
| `noise_mean_corrected` | `noise_mean_corrected_l1b` | Corrected noise baseline | Mean noise level (ADC counts) for each laser shot, derived from the received waveform background after correction; used to subtract noise before waveform analysis |
| `rx_sample_start_index` | `rx_sample_start_index_l1b` | Waveform start index | 1-based index into the concatenated `rxwaveform` dataset pointing to the first sample of each shot's waveform; used together with `rx_sample_count` to locate each waveform |
| `rx_sample_count` | `rx_sample_start_index_l1b` | Waveform sample count | Number of sample intervals in each received waveform; defines the length of the waveform slice beginning at `rx_sample_start_index` |
| `geolocation/delta_time` | `geolocation/delta_time_l1b` | Acquisition timestamp | Seconds elapsed since the 2018-01-01 |

> **Data dictionary:** [GEDI L1B Data Dictionary (V2)](https://lpdaac.usgs.gov/documents/981/gedi_l1b_dictionary_P003_v2.html) · [LP DAAC product page](https://lpdaac.usgs.gov/products/gedi01_bv002/)

---

### L2A — Ground Elevation and Canopy Height Metrics

L2A is the workhorse product for canopy height studies. It decomposes the received waveform into Gaussian modes and reports the relative height (RH) at each 1-percentile energy interval measured from the ground return upward.

:::{note} 
`rh` is stored as a 101-element array per shot (`rh[0]`–`rh[100]`). gedih3 expands selected percentiles into individual named columns: `rh_000_l2a`, `rh_025_l2a`, `rh_050_l2a`, `rh_075_l2a`, `rh_098_l2a`, `rh_100_l2a`, etc.
:::

| HDF5 variable | gedih3 column | Short description | Definition |
|---|---|---|---|
| `rh[98]` | `rh_098_l2a` | **Canopy top height** (primary proxy) | Height (m above ground) below which 98% of waveform energy is returned. The most widely used GEDI metric for top-of-canopy height |
| `rh[100]` | `rh_100_l2a` | Absolute maximum canopy height | Height of the highest return; more sensitive to noise than rh_098 |
| `rh[75]` | `rh_075_l2a` | Mid-upper canopy height | Height at which 75% of energy is returned; useful for sub-dominant canopy layers |
| `rh[50]` | `rh_050_l2a` | Median canopy height | Height at which 50% of waveform energy is returned; less influenced by canopy top outliers |
| `rh[25]` | `rh_025_l2a` | Lower canopy / understorey height | Height below which 25% of energy is returned; sensitive to understorey presence |
| `elev_lowestmode` | `elev_lowestmode_l2a` | **Ground elevation** | WGS-84 ellipsoidal elevation of the centre of the lowest waveform mode (the ground return) |
| `lat_lowestmode` | `lat_lowestmode_l2a` | **Shot latitude** (primary geolocation) | Latitude of the ground return — the most precise geolocation point for each GEDI shot |
| `lon_lowestmode` | `lon_lowestmode_l2a` | **Shot longitude** (primary geolocation) | Longitude of the ground return |
| `quality_flag` | `quality_flag_l2a` | Shot usability filter | `1` = high quality ; `0` = low quality |
| `sensitivity` | `sensitivity_l2a` | Canopy penetration detectability | Maximum canopy cover that the algorithm could penetrate given the ambient noise conditions (0–1). Values below ~0.9 in dense forests indicate the ground return may be unreliable |

> **Data dictionary:** [GEDI L2A Data Dictionary (V2)](https://lpdaac.usgs.gov/documents/982/gedi_l2a_dictionary_P003_v2.html) · [LP DAAC product page](https://lpdaac.usgs.gov/products/gedi02_av002/)

---

### L2B — Canopy Cover and Vertical Structure Metrics

L2B uses the full waveform to estimate the vertical distribution of plant material within the canopy column. Key uses include characterising multi-layered canopy structure, estimating light availability at the forest floor, and habitat suitability modelling.

:::{note} `pai_z`, `pavd_z` and `cover_z` are 30-element arrays per shot, covering 5 m height bins from 0 to 150 m. gedih3 expands each into 30 individual columns (`pavd_z_000_l2b` … `pavd_z_029_l2b`, `cover_z_000_l2b` … `cover_z_029_l2b`). Building each adds 30 columns to the database — modest compared to waveforms, but worth keeping in mind for large databases.
:::

| HDF5 variable | gedih3 column | Short description | Definition |
|---|---|---|---|
| `cover` | `cover_l2b` | **Total canopy cover** | Fraction (0–1) of ground area covered by the vertical projection of any canopy element; equivalent to canopy closure viewed from nadir |
| `fhd_normal` | `fhd_normal_l2b` | **Foliage height diversity** | Shannon entropy of the vertical foliage distribution, normalised by total PAI. Higher values indicate more structurally complex, multi-layered canopies — a key indicator of habitat quality and biodiversity |
| `pai` | `pai_l2b` | Plant Area Index | Total one-sided plant area per unit ground area (m² m⁻²); includes leaves, branches, and stems. A proxy for leaf area index (LAI) in dense forests |
| `pavd_z` | `pavd_z_000_l2b` … `pavd_z_018_l2b` (**19 columns**) | Vertical foliage density profile | Plant Area Volume Density at each 5 m height bin (m² m⁻³); describes where foliage is concentrated in the vertical column — distinguishes emergent, canopy, sub-canopy, and understorey layers |
| `cover_z` | `cover_z_000_l2b` … `cover_z_018_l2b` (**19 columns**) | Cumulative canopy cover profile | Cumulative fraction of canopy cover from each height bin down to the ground; complements `pavd_z` for characterising canopy layering |
| `pgap_theta` | `pgap_theta_l2b` | Canopy gap fraction | Probability of a laser pulse passing through the canopy without interception at the mean beam zenith angle. Directly related to canopy transmittance and light penetration to the forest floor |
| `l2b_quality_flag` | `l2b_quality_flag_l2b` | Shot usability filter | `1` = valid L2B retrieval |

> **Data dictionary:** [GEDI L2B Data Dictionary (V2)](https://lpdaac.usgs.gov/documents/980/gedi_l2b_dictionary_P003_v2.html) · [LP DAAC product page](https://lpdaac.usgs.gov/products/gedi02_bv002/)

---

### L4A — Footprint-Level Aboveground Biomass Density

L4A predicts aboveground biomass density (AGBD) at each GEDI footprint by applying allometric models — calibrated against global forest inventory plots and airborne LiDAR surveys — to the L2A RH metrics. It is the primary GEDI product for forest carbon monitoring.

| HDF5 variable | gedih3 column | Short description | Definition |
|---|---|---|---|
| `agbd` | `agbd_l4a` | **Aboveground biomass density** | Predicted aboveground biomass density (Mg ha⁻¹) of woody vegetation. Derived from RH metrics via stratum-specific allometric models fitted to forest inventory data |
| `agbd_se` | `agbd_se_l4a` | Biomass prediction uncertainty | Standard error of the AGBD estimate (Mg ha⁻¹). Essential for uncertainty-aware carbon accounting; large SE values indicate low model confidence |
| `agbd_pi_lower` | `agbd_pi_lower_l4a` | Biomass lower prediction bound | Lower bound of the 95% prediction interval around `agbd` |
| `agbd_pi_upper` | `agbd_pi_upper_l4a` | Biomass upper prediction bound | Upper bound of the 95% prediction interval around `agbd` |
| `predict_stratum` | `predict_stratum_l4a` | Allometric model identifier | Character identifier of plant functional type (PFT) and continental region |
| `l4_quality_flag` | `l4_quality_flag_l4a` | Shot usability filter | `1` = valid AGBD estimate |

> **Data dictionary & user guide:** [ORNL DAAC L4A Guide](https://daac.ornl.gov/GEDI/guides/GEDI_L4A_AGB_Density_V2_1.html) · [L4A Data Dictionary PDF](https://data.ornldaac.earthdata.nasa.gov/public/gedi/GEDI_L4A_AGB_Density_V2_1/comp/GEDI_L4A_V2_Product_Data_Dictionary.pdf) · [ORNL DAAC product page (V2.1)](https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=2056)

---

### L4C — Footprint-Level Structural Complexity

L4C provides the Waveform Structural Complexity Index (WSCI) — a machine-learning-derived metric trained on matched airborne LiDAR point clouds that quantifies the three-dimensional complexity of forest structure beyond what a single height metric can capture. It is an integrative metric that combines both vertical and horizontal structural elements, useful for biodiversity and habitat quality studies.

| HDF5 variable | gedih3 column | Short description | Definition |
|---|---|---|---|
| `wsci` | `wsci_l4c` | **Waveform Structural Complexity Index** | Dimensionless index (≥ 0) measuring the overall 3-D entropy of the canopy. Derived from an XGBoost regression model trained on airborne LiDAR point clouds across plant functional types. Higher values indicate greater vertical and horizontal structural complexity |
| `wsci_z` | `wsci_z_l4c` | Vertical structural complexity | The vertical component of WSCI; captures complexity in the height distribution of canopy elements. Correlates strongly with `fhd_normal` from L2B |
| `wsci_xy` | `wsci_xy_l4c` | Horizontal structural complexity | The horizontal component of WSCI; captures spatial heterogeneity within the 25 m footprint — related to gap fraction heterogeneity and canopy patchiness |
| `wsci_pi_lower` | `wsci_pi_lower_l4c` | WSCI lower prediction bound | Lower bound of the 95% prediction interval around `wsci` |
| `wsci_pi_upper` | `wsci_pi_upper_l4c` | WSCI upper prediction bound | Upper bound of the 95% prediction interval around `wsci` |
| `wsci_quality_flag` | `wsci_quality_flag_l4c` | Shot usability filter | `1` = valid WSCI estimate |

> **Data dictionary & user guide:** [ORNL DAAC L4C Guide](https://daac.ornl.gov/GEDI/guides/GEDI_L4C_WSCI.html) · [L4C Data Dictionary PDF](https://data.ornldaac.earthdata.nasa.gov/public/gedi/GEDI_L4C_WSCI/comp/GEDI_L4C_WSCI_Data_Dictionary.pdf) · [ORNL DAAC product page (V2)](https://daac.ornl.gov/cgi-bin/dsviewer.pl?ds_id=2338)

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

::::{grid} 1 1 2 2
:gutter: 3

:::{grid-item-card} Raw GEDI (HDF5)
:shadow: md
:class-header: sd-bg-danger sd-bg-text-danger
:class-card: sd-border-danger comparison-card
^^^

**Files** — Hundreds of orbit files (~1–3 GB each) must be parsed even for a small study site

**Spatial scope** — Each file spans ~1/4 of an ISS orbital track; a large portion of data falls outside your region of interest

**Storage** — All variables included: hundreds of GB for a regional analysis

**Format** — Deeply nested HDF5 requiring `h5py` and domain knowledge of the beam/group structure

**Multi-product** — Each product (L2A, L4A, ...) is a separate file set; must be parsed and joined independently by shot

**Tools** — Limited to Python `h5py`, R `rhdf5`, or specialized GEDI tools
:::

:::{grid-item-card} gedih3 H3 Database (GeoParquet)
:shadow: md
:class-header: sd-bg-success sd-bg-text-success
:class-card: sd-border-success comparison-card
^^^

**Files** — ~4 spatial tiles for a 1°×1° region; queries read only tiles that intersect your area

**Spatial scope** — Spatially partitioned; each tile contains only shots within its H3 cell

**Storage** — Curated variable presets (`minimal`, `default`): 10–100× smaller on disk

**Format** — Flat GeoParquet DataFrames: one row per shot, one column per variable

**Multi-product** — Variables from all products (L2A, L4A, ...) are fused into a single cohesive dataset during build

**Tools** — *gedih3* API, Python (pandas, GeoPandas, Dask), R (sf, arrow, sfarrow), QGIS, DuckDB, and any Parquet-compatible tool
:::

::::

*`gh3_build` transforms GEDI's time-organized HDF5 hierarchy into a spatially partitioned H3 GeoParquet database. A 1°×1° region that spans hundreds of orbit files becomes a handful of queryable tiles.*

---

## How gedih3 Addresses These Challenges

gedih3 was built by remote sensing scientists with direct experience working with GEDI data at scale. Rather than exposing raw complexity, it provides:

**Curated variable presets**
Instead of navigating hundreds of variables, use `minimal` (essential metrics only) or `default` (standard science-ready set) presets for each product. These presets were designed for common use cases and reflect community best practices.

```bash
# Build a database with the default science-ready variable set
gh3_build -r "-51,0,-50,1" -l2a default -l4a default --download
```

→ See the [Variable Presets Reference](variable-presets.md) for the full list of variables in each preset, for all products and versions.

**Pre-configured quality filtering**
A single `-y` or `--quality` command line flag applies scientifically-validated quality filters, combining multiple quality criteria correctly across products.

```bash
# Extract only high-quality observations
gh3_extract -y -l agbd_l4a rh_098_l2a -o filtered/
```

**Spatial indexing from the ground up**
gedih3 converts GEDI's orbit-organized HDF5 files into a spatially-indexed GeoParquet database. Once built, regional queries that would take hours on raw HDF5 complete in seconds.

→ See [**Building a Database**](../user-guide/building-a-database.md) for a complete guide to the build process, variable selection, and subsetting strategies.

**Transparent, reproducible pipelines**
Every database build is logged with metadata (products, variables, region, resolution levels). `gh3_read_schema` lets you inspect what any database or output file contains.

---

## GEDI Mission Resources

- [GEDI homepage at UMD](https://gedi.umd.edu/)
- [NASA GEDI data access](https://daac.ornl.gov/gedi/)
- [GEDI products documentation](https://gedi.umd.edu/dataproducts/download/)
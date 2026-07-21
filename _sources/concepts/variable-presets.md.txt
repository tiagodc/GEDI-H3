# Variable Presets Reference

When you pass `-l2a default` or `-l2a minimal` on the command line, gedih3 expands that keyword into a concrete list of HDF5 variable names. This page documents exactly which variables each keyword selects, for every supported product and GEDI data version.

---

## Preset Levels

| Keyword | What it selects |
|---|---|
| `minimal` / `min` | Smallest usable set per product/version — primary science variables and quality flag. Geolocation, time, elevation, and degrade flag always come from L2A essentials. Hardcoded in `config.py`. |
| `default` / `def` | Expert-curated science-ready set, loaded from the bundled `.txt` file in `src/gedih3/data/`. Designed to cover common research workflows without requiring HDF5 expertise. |
| `all` / `*` / bare flag | Every variable present in the HDF5 file for that product (all beams). Use with caution — there are 100+ variables per beam for each product. |
| `/path/to/file.txt` | Plain-text file with one HDF5 variable name per line; `#`-prefixed lines are treated as comments. |
| explicit names | One or more exact HDF5 variable names inline, e.g. `-l2a rh quality_flag land_cover_data/pft_class -l4a agbd`. |
| wildcard pattern | Shell-style pattern using `*`, `?`, `[seq]`, `[!seq]` to match multiple variables at once (see below). |

---

## L2A Essentials — Always Present

Regardless of which products or presets you select, the build pipeline **always** includes
L2A essentials. These variables are guaranteed to be in every database:

::::{tab-set}
:::{tab-item} v2

```
shot_number
delta_time
quality_flag
degrade_flag
sensitivity
lat_lowestmode
lon_lowestmode
elev_lowestmode
```

:::
:::{tab-item} v3

```
shot_number
delta_time
l2a_quality_flag_rel3
degrade_flag
sensitivity
lat_lowestmode
lon_lowestmode
elev_lowestmode
```

:::
::::

Because L2A provides coordinates, time, elevation, and quality context for all shots,
downstream products (L2B, L4A, L4C) do **not** duplicate these variables in their
preset lists.

---

## Wildcard Patterns

All tools that accept variable names also support shell-style wildcard patterns.  This works across all stages — download, build, extract, and aggregate.

| Pattern | Matches |
|---|---|
| `rh_*` | All RH percentiles (`rh_000` through `rh_100`) |
| `geolocation/sensitivity_a*` | `sensitivity_a1`, `sensitivity_a2`, `sensitivity_a5`, ... |
| `geolocation/*_a[125]` | All algorithm a1/a2/a5 geolocation variables |
| `wsci_*` | All WSCI variables |
| `agbd_prediction/agbd_?e_a*` | `agbd_se_a1`, `agbd_se_a2`, `agbd_se_a5`, ... |

```bash
# Build with only algorithm-a1 geolocation variables
gh3_build -r "-51,0,-50,1" -l2a "geolocation/*_a1" -l4a default -s3

# Extract all RH percentiles
gh3_extract -d /path/to/database -l2a "rh_*" -o output/

# Aggregate all AGBD variables
gh3_aggregate -d /path/to/database -h3 6 -l4a "agbd*" -o output/

# Use --list with suffixed wildcard
gh3_extract -d /path/to/database -l "rh_*_l2a" -o output/
```

::::{important}
Quote wildcard patterns to prevent shell expansion: use `-l2a "rh_*"` or `-l2a 'rh_*'`, not `-l2a rh_*`.
::::

---

## CLI Usage

```bash
# Minimal set (fastest build, smallest files)
gh3_build -r "-51,0,-50,1" -l2a minimal -l4a minimal -s3

# Default science-ready set
gh3_build -r "-51,0,-50,1" -l2a default -l4a default

# Explicit variables
gh3_build -r "-51,0,-50,1" -l2a rh quality_flag lat_lowestmode lon_lowestmode -l2b cover -l4a agbd

# From a custom file (one variable per line, # comments allowed)
gh3_build -r "-51,0,-50,1" -l2a /path/to/my_l2a_vars.txt

# All variables (use with caution)
gh3_build -r "-51,0,-50,1" -l2a all
```

---

## L1B — Geolocated Waveforms

### minimal

::::{tab-set}
:::{tab-item} v2 — 8 variables
```
shot_number
stale_return_flag
noise_mean_corrected
rx_sample_start_index
rx_sample_count
rxwaveform
geolocation/elevation_bin0
geolocation/elevation_lastbin
```
:::
:::{tab-item} v3 — 9 variables
```
shot_number
stale_return_flag
rx_clipflag
noise_mean_corrected
rx_sample_start_index
rx_sample_count
rxwaveform
geolocation/elevation_bin0
geolocation/elevation_lastbin
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 29 variables

```
# Receive waveform (8)
shot_number
stale_return_flag
noise_mean_corrected
noise_stddev_corrected
rx_sample_start_index
rx_sample_count
rx_energy
rxwaveform

# Transmit pulse flag (1)
tx_pulseflag

# Geolocation (20) — position, elevation, errors, DEM, solar, beam angles
geolocation/degrade
geolocation/latitude_bin0
geolocation/longitude_bin0
geolocation/elevation_bin0
geolocation/latitude_lastbin
geolocation/longitude_lastbin
geolocation/elevation_lastbin
geolocation/latitude_bin0_error
geolocation/longitude_bin0_error
geolocation/elevation_bin0_error
geolocation/latitude_lastbin_error
geolocation/longitude_lastbin_error
geolocation/elevation_lastbin_error
geolocation/digital_elevation_model
geolocation/digital_elevation_model_srtm
geolocation/solar_elevation
geolocation/local_beam_elevation
geolocation/local_beam_elevation_error
geolocation/local_beam_azimuth
geolocation/local_beam_azimuth_error
```

:::
:::{tab-item} v3 — 33 variables

```
# Receive waveform (12) — new: rx_clip*, mean_64kadjusted
shot_number
stale_return_flag
noise_mean_corrected
noise_stddev_corrected
mean_64kadjusted
rx_clipbin0
rx_clipbin_count
rx_clipflag
rx_sample_start_index
rx_sample_count
rx_energy
rxwaveform

# Transmit pulse flag (1)
tx_pulseflag

# Geolocation (20) — position, elevation, errors, DEM, solar, beam angles
geolocation/degrade
geolocation/latitude_bin0
geolocation/longitude_bin0
geolocation/elevation_bin0
geolocation/latitude_lastbin
geolocation/longitude_lastbin
geolocation/elevation_lastbin
geolocation/latitude_bin0_error
geolocation/longitude_bin0_error
geolocation/elevation_bin0_error
geolocation/latitude_lastbin_error
geolocation/longitude_lastbin_error
geolocation/elevation_lastbin_error
geolocation/digital_elevation_model
geolocation/digital_elevation_model_srtm
geolocation/solar_elevation
geolocation/local_beam_elevation
geolocation/local_beam_elevation_error
geolocation/local_beam_azimuth
geolocation/local_beam_azimuth_error
```

:::
::::

---

## L2A — Elevation and Height Metrics

### minimal

::::{tab-set}
:::{tab-item} v2 — 9 variables
```
shot_number
delta_time
quality_flag
degrade_flag
sensitivity
lat_lowestmode
lon_lowestmode
elev_lowestmode
rh
```
:::
:::{tab-item} v3 — 9 variables
```
shot_number
delta_time
l2a_quality_flag_rel3
degrade_flag
sensitivity
lat_lowestmode
lon_lowestmode
elev_lowestmode
rh
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 88 variables

```
# Core (20)
beam
channel
degrade_flag
delta_time
digital_elevation_model
digital_elevation_model_srtm
elevation_bias_flag
selected_algorithm
selected_mode
num_detectedmodes
shot_number
solar_azimuth
solar_elevation
surface_flag
lat_lowestmode
lon_lowestmode
elev_lowestmode
quality_flag
rh
sensitivity

# Geolocation — stale flag (1)
geolocation/stale_return_flag

# Geolocation — algorithm a1 (13)
geolocation/elev_highestreturn_a1
geolocation/elev_lowestmode_a1
geolocation/elev_lowestreturn_a1
geolocation/lat_highestreturn_a1
geolocation/lat_lowestmode_a1
geolocation/lat_lowestreturn_a1
geolocation/lon_highestreturn_a1
geolocation/lon_lowestmode_a1
geolocation/lon_lowestreturn_a1
geolocation/num_detectedmodes_a1
geolocation/quality_flag_a1
geolocation/rh_a1
geolocation/sensitivity_a1

# Geolocation — algorithm a2 (13)
geolocation/elev_highestreturn_a2
geolocation/elev_lowestmode_a2
geolocation/elev_lowestreturn_a2
geolocation/lat_highestreturn_a2
geolocation/lat_lowestmode_a2
geolocation/lat_lowestreturn_a2
geolocation/lon_highestreturn_a2
geolocation/lon_lowestmode_a2
geolocation/lon_lowestreturn_a2
geolocation/num_detectedmodes_a2
geolocation/quality_flag_a2
geolocation/rh_a2
geolocation/sensitivity_a2

# Geolocation — algorithm a5 (13)
geolocation/elev_highestreturn_a5
geolocation/elev_lowestmode_a5
geolocation/elev_lowestreturn_a5
geolocation/lat_highestreturn_a5
geolocation/lat_lowestmode_a5
geolocation/lat_lowestreturn_a5
geolocation/lon_highestreturn_a5
geolocation/lon_lowestmode_a5
geolocation/lon_lowestreturn_a5
geolocation/num_detectedmodes_a5
geolocation/quality_flag_a5
geolocation/rh_a5
geolocation/sensitivity_a5

# rx_assess (7)
rx_assess/mean_64kadjusted
rx_assess/mean
rx_assess/sd_corrected
rx_assess/quality_flag
rx_assess/rx_assess_flag
rx_assess/rx_maxamp
rx_assess/rx_energy

# rx_processing — algorithm a1 (5)
rx_processing_a1/zcross
rx_processing_a1/toploc
rx_processing_a1/botloc
rx_processing_a1/rx_algrunflag
rx_processing_a1/selected_mode_flag

# rx_processing — algorithm a2 (5)
rx_processing_a2/zcross
rx_processing_a2/toploc
rx_processing_a2/botloc
rx_processing_a2/rx_algrunflag
rx_processing_a2/selected_mode_flag

# rx_processing — algorithm a5 (5)
rx_processing_a5/zcross
rx_processing_a5/toploc
rx_processing_a5/botloc
rx_processing_a5/rx_algrunflag
rx_processing_a5/selected_mode_flag

# land_cover_data (6)
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion
```

:::
:::{tab-item} v3 — 110 variables

```
# Core (21) — dual quality flags; num_detectedmodes added
beam
channel
degrade_flag
delta_time
digital_elevation_model
digital_elevation_model_srtm
elevation_bias_flag
selected_algorithm
selected_mode
num_detectedmodes
shot_number
solar_azimuth
solar_elevation
surface_flag
lat_lowestmode
lon_lowestmode
elev_lowestmode
l2a_quality_flag_rel2
l2a_quality_flag_rel3
rh
sensitivity

# Geolocation — stale flag (1)
geolocation/stale_return_flag

# Geolocation — algorithm a1 (13)
geolocation/elev_highestreturn_a1
geolocation/elev_lowestmode_a1
geolocation/elev_lowestreturn_a1
geolocation/lat_highestreturn_a1
geolocation/lat_lowestmode_a1
geolocation/lat_lowestreturn_a1
geolocation/lon_highestreturn_a1
geolocation/lon_lowestmode_a1
geolocation/lon_lowestreturn_a1
geolocation/num_detectedmodes_a1
geolocation/l2a_quality_flag_rel3_a1
geolocation/rh_a1
geolocation/sensitivity_a1

# Geolocation — algorithm a2 (13)
geolocation/elev_highestreturn_a2
geolocation/elev_lowestmode_a2
geolocation/elev_lowestreturn_a2
geolocation/lat_highestreturn_a2
geolocation/lat_lowestmode_a2
geolocation/lat_lowestreturn_a2
geolocation/lon_highestreturn_a2
geolocation/lon_lowestmode_a2
geolocation/lon_lowestreturn_a2
geolocation/num_detectedmodes_a2
geolocation/l2a_quality_flag_rel3_a2
geolocation/rh_a2
geolocation/sensitivity_a2

# Geolocation — algorithm a5 (13)
geolocation/elev_highestreturn_a5
geolocation/elev_lowestmode_a5
geolocation/elev_lowestreturn_a5
geolocation/lat_highestreturn_a5
geolocation/lat_lowestmode_a5
geolocation/lat_lowestreturn_a5
geolocation/lon_highestreturn_a5
geolocation/lon_lowestmode_a5
geolocation/lon_lowestreturn_a5
geolocation/num_detectedmodes_a5
geolocation/l2a_quality_flag_rel3_a5
geolocation/rh_a5
geolocation/sensitivity_a5

# Geolocation — algorithm a10 (13) — new in v3
geolocation/elev_highestreturn_a10
geolocation/elev_lowestmode_a10
geolocation/elev_lowestreturn_a10
geolocation/lat_highestreturn_a10
geolocation/lat_lowestmode_a10
geolocation/lat_lowestreturn_a10
geolocation/lon_highestreturn_a10
geolocation/lon_lowestmode_a10
geolocation/lon_lowestreturn_a10
geolocation/num_detectedmodes_a10
geolocation/l2a_quality_flag_rel3_a10
geolocation/rh_a10
geolocation/sensitivity_a10

# rx_assess (7)
rx_assess/mean_64kadjusted
rx_assess/mean
rx_assess/sd_corrected
rx_assess/quality_flag
rx_assess/rx_assess_flag
rx_assess/rx_maxamp
rx_assess/rx_energy

# rx_processing — algorithm a1 (5)
rx_processing_a1/zcross
rx_processing_a1/toploc
rx_processing_a1/botloc
rx_processing_a1/rx_algrunflag
rx_processing_a1/selected_mode_flag

# rx_processing — algorithm a2 (5)
rx_processing_a2/zcross
rx_processing_a2/toploc
rx_processing_a2/botloc
rx_processing_a2/rx_algrunflag
rx_processing_a2/selected_mode_flag

# rx_processing — algorithm a5 (5)
rx_processing_a5/zcross
rx_processing_a5/toploc
rx_processing_a5/botloc
rx_processing_a5/rx_algrunflag
rx_processing_a5/selected_mode_flag

# rx_processing — algorithm a10 (5) — new in v3
rx_processing_a10/zcross
rx_processing_a10/toploc
rx_processing_a10/botloc
rx_processing_a10/rx_algrunflag
rx_processing_a10/selected_mode_flag

# land_cover_data (9) — adds phenology_phase, phenology_year, worldcover_class
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion
land_cover_data/phenology_phase
land_cover_data/phenology_year
land_cover_data/worldcover_class
```

:::
::::

---

## L2B — Canopy Cover and Vertical Structure

### minimal

::::{tab-set}
:::{tab-item} v2 — 8 variables
```
shot_number
l2b_quality_flag
cover_z
fhd_normal
pai_z
pavd_z
cover
pai
```
:::
:::{tab-item} v3 — 9 variables
```
shot_number
l2b_quality_flag_rel3
cover_z
fhd_normal
pai_z
pavd_z
cover
pai
rch
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 52 variables

```
# Core (22) — includes total cover, pai, omega, pgap_theta
algorithmrun_flag
cover
cover_z
fhd_normal
l2b_quality_flag
omega
pai
pai_z
pavd_z
pgap_theta
pgap_theta_error
rg
rossg
rv
rh100
rx_range_highestreturn
selected_rg_algorithm
selected_l2a_algorithm
sensitivity
shot_number
stale_return_flag
surface_flag

# geolocation (6) — no L2A duplicates
geolocation/digital_elevation_model
geolocation/elev_highestreturn
geolocation/lat_highestreturn
geolocation/local_beam_elevation
geolocation/lon_highestreturn
geolocation/solar_elevation

# land_cover_data (6)
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/urban_proportion
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class

# rx_processing — algorithm a1 (6)
rx_processing/algorithmrun_flag_a1
rx_processing/pgap_theta_a1
rx_processing/rg_a1
rx_processing/rg_error_a1
rx_processing/rv_a1
rx_processing/rx_energy_a1

# rx_processing — algorithm a2 (6)
rx_processing/algorithmrun_flag_a2
rx_processing/pgap_theta_a2
rx_processing/rg_a2
rx_processing/rg_error_a2
rx_processing/rv_a2
rx_processing/rx_energy_a2

# rx_processing — algorithm a5 (6)
rx_processing/algorithmrun_flag_a5
rx_processing/pgap_theta_a5
rx_processing/rg_a5
rx_processing/rg_error_a5
rx_processing/rv_a5
rx_processing/rx_energy_a5
```

:::
:::{tab-item} v3 — 68 variables

```
# Core (27) — dual quality flags; new metrics rch, rhov_rhog, rv_rg_r; sensitivity retained
cover
cover_z
fhd_normal
l2_algrunflag
l2b_quality_flag_rel2
l2b_quality_flag_rel3
omega
pai
pai_z
pavd_z
pgap_theta
pgap_theta_error
rch
rg
rhov_rhog
rhov_rhog_se
rossg
rv
rv_rg_r
rh100
rx_range_highestreturn
selected_rg_algorithm
selected_l2a_algorithm
sensitivity
shot_number
stale_return_flag
surface_flag

# geolocation (4) — v3 removes DEM and highestreturn fields; adds solar_azimuth
geolocation/local_beam_azimuth
geolocation/local_beam_elevation
geolocation/solar_azimuth
geolocation/solar_elevation

# land_cover_data (9) — adds phenology_phase, phenology_year, worldcover_class
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/urban_proportion
land_cover_data/leaf_off_flag
land_cover_data/phenology_phase
land_cover_data/phenology_year
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/worldcover_class

# rx_processing — algorithms a1, a2, a5, a10 (7 each) — adds fhd_normal, rch per algo
rx_processing/fhd_normal_a1
rx_processing/l2_algrunflag_a1
rx_processing/rch_a1
rx_processing/rg_a1
rx_processing/rg_error_a1
rx_processing/rv_a1
rx_processing/rx_energy_a1
rx_processing/fhd_normal_a2
rx_processing/l2_algrunflag_a2
rx_processing/rch_a2
rx_processing/rg_a2
rx_processing/rg_error_a2
rx_processing/rv_a2
rx_processing/rx_energy_a2
rx_processing/fhd_normal_a5
rx_processing/l2_algrunflag_a5
rx_processing/rch_a5
rx_processing/rg_a5
rx_processing/rg_error_a5
rx_processing/rv_a5
rx_processing/rx_energy_a5
rx_processing/fhd_normal_a10
rx_processing/l2_algrunflag_a10
rx_processing/rch_a10
rx_processing/rg_a10
rx_processing/rg_error_a10
rx_processing/rv_a10
rx_processing/rx_energy_a10
```

v3 adds dual quality flags (`l2b_quality_flag_rel2/rel3`), new metrics (`rch`, `rhov_rhog`, `rv_rg_r`),
restructures geolocation, expands land cover, and adds `fhd_normal_aX`/`rch_aX` to rx_processing.

:::
::::

---

## L4A — Footprint-Level Aboveground Biomass

### minimal

::::{tab-set}
:::{tab-item} v2 — 4 variables
```
shot_number
agbd
agbd_se
l4_quality_flag
```
:::
:::{tab-item} v3 — 5 variables
```
shot_number
agbd
agbd_se
l4a_quality_flag_rel3
elev_highestreturn_outlier_flag
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 70 variables

```
# Core (16) — no L2A duplicates (coords/time/degrade from L2A)
agbd
agbd_se
agbd_t
agbd_t_se
algorithm_run_flag
l2_quality_flag
l4_quality_flag
predict_stratum
predictor_limit_flag
response_limit_flag
selected_algorithm
sensitivity
shot_number
solar_elevation
surface_flag
xvar

# land_cover_data (6)
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion

# geolocation — algorithms a1, a2, a5 (4 each)
geolocation/elev_lowestmode_a1
geolocation/lat_lowestmode_a1
geolocation/lon_lowestmode_a1
geolocation/sensitivity_a1
geolocation/elev_lowestmode_a2
geolocation/lat_lowestmode_a2
geolocation/lon_lowestmode_a2
geolocation/sensitivity_a2
geolocation/elev_lowestmode_a5
geolocation/lat_lowestmode_a5
geolocation/lon_lowestmode_a5
geolocation/sensitivity_a5

# agbd_prediction — algorithms a1, a2, a5 (12 each)
agbd_prediction/agbd_a1
agbd_prediction/agbd_se_a1
agbd_prediction/agbd_t_a1
agbd_prediction/agbd_t_se_a1
agbd_prediction/algorithm_run_flag_a1
agbd_prediction/l2_quality_flag_a1
agbd_prediction/l4_quality_flag_a1
agbd_prediction/predictor_limit_flag_a1
agbd_prediction/response_limit_flag_a1
agbd_prediction/selected_mode_a1
agbd_prediction/selected_mode_flag_a1
agbd_prediction/xvar_a1
agbd_prediction/agbd_a2
agbd_prediction/agbd_se_a2
agbd_prediction/agbd_t_a2
agbd_prediction/agbd_t_se_a2
agbd_prediction/algorithm_run_flag_a2
agbd_prediction/l2_quality_flag_a2
agbd_prediction/l4_quality_flag_a2
agbd_prediction/predictor_limit_flag_a2
agbd_prediction/response_limit_flag_a2
agbd_prediction/selected_mode_a2
agbd_prediction/selected_mode_flag_a2
agbd_prediction/xvar_a2
agbd_prediction/agbd_a5
agbd_prediction/agbd_se_a5
agbd_prediction/agbd_t_a5
agbd_prediction/agbd_t_se_a5
agbd_prediction/algorithm_run_flag_a5
agbd_prediction/l2_quality_flag_a5
agbd_prediction/l4_quality_flag_a5
agbd_prediction/predictor_limit_flag_a5
agbd_prediction/response_limit_flag_a5
agbd_prediction/selected_mode_a5
agbd_prediction/selected_mode_flag_a5
agbd_prediction/xvar_a5
```

:::
:::{tab-item} v3 — 90 variables

```
# Core (20) — no L2A duplicates; new v3 flags and prediction intervals; sensitivity retained
agbd
agbd_pi_lower
agbd_pi_upper
agbd_se
agbd_t
agbd_t_se
degrade_include_flag
elev_highestreturn_outlier_flag
l2_algrunflag
l4a_quality_flag_rel3
predict_stratum
predictor_limit_flag
response_limit_flag
selected_algorithm
selected_mode_flag
sensitivity
shot_number
solar_elevation
surface_flag
xvar

# land_cover_data (10) — new: pft_infilled_class, worldcover_class, phenology
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/phenology_phase
land_cover_data/phenology_year
land_cover_data/pft_class
land_cover_data/pft_infilled_class
land_cover_data/region_class
land_cover_data/urban_proportion
land_cover_data/worldcover_class

# geolocation — algorithms a1, a2, a5, a10 (4 each)
geolocation/elev_lowestmode_a1
geolocation/lat_lowestmode_a1
geolocation/lon_lowestmode_a1
geolocation/sensitivity_a1
geolocation/elev_lowestmode_a2
geolocation/lat_lowestmode_a2
geolocation/lon_lowestmode_a2
geolocation/sensitivity_a2
geolocation/elev_lowestmode_a5
geolocation/lat_lowestmode_a5
geolocation/lon_lowestmode_a5
geolocation/sensitivity_a5
geolocation/elev_lowestmode_a10
geolocation/lat_lowestmode_a10
geolocation/lon_lowestmode_a10
geolocation/sensitivity_a10

# agbd_prediction — algorithms a1, a2, a5, a10 (11 each)
# v3: adds prediction intervals; removes algorithm_run_flag_a*, l2/l4_quality_flag_a*,
#     selected_mode_a*; renames to l2_algrunflag_a*
agbd_prediction/agbd_a1
agbd_prediction/agbd_pi_lower_a1
agbd_prediction/agbd_pi_upper_a1
agbd_prediction/agbd_se_a1
agbd_prediction/agbd_t_a1
agbd_prediction/agbd_t_se_a1
agbd_prediction/l2_algrunflag_a1
agbd_prediction/predictor_limit_flag_a1
agbd_prediction/response_limit_flag_a1
agbd_prediction/selected_mode_flag_a1
agbd_prediction/xvar_a1
agbd_prediction/agbd_a2
agbd_prediction/agbd_pi_lower_a2
agbd_prediction/agbd_pi_upper_a2
agbd_prediction/agbd_se_a2
agbd_prediction/agbd_t_a2
agbd_prediction/agbd_t_se_a2
agbd_prediction/l2_algrunflag_a2
agbd_prediction/predictor_limit_flag_a2
agbd_prediction/response_limit_flag_a2
agbd_prediction/selected_mode_flag_a2
agbd_prediction/xvar_a2
agbd_prediction/agbd_a5
agbd_prediction/agbd_pi_lower_a5
agbd_prediction/agbd_pi_upper_a5
agbd_prediction/agbd_se_a5
agbd_prediction/agbd_t_a5
agbd_prediction/agbd_t_se_a5
agbd_prediction/l2_algrunflag_a5
agbd_prediction/predictor_limit_flag_a5
agbd_prediction/response_limit_flag_a5
agbd_prediction/selected_mode_flag_a5
agbd_prediction/xvar_a5
agbd_prediction/agbd_a10
agbd_prediction/agbd_pi_lower_a10
agbd_prediction/agbd_pi_upper_a10
agbd_prediction/agbd_se_a10
agbd_prediction/agbd_t_a10
agbd_prediction/agbd_t_se_a10
agbd_prediction/l2_algrunflag_a10
agbd_prediction/predictor_limit_flag_a10
agbd_prediction/response_limit_flag_a10
agbd_prediction/selected_mode_flag_a10
agbd_prediction/xvar_a10
```

v3 renames `algorithm_run_flag` → `l2_algrunflag`, adds prediction intervals (`agbd_pi_lower/upper`),
and expands land cover with phenology and PFT infilled class.

:::
::::

---

## L4C — Footprint-Level Structural Complexity

### minimal

::::{tab-set}
:::{tab-item} v2 — 8 variables
```
shot_number
wsci
wsci_xy
wsci_z
wsci_pi_lower
wsci_pi_upper
wsci_quality_flag
land_cover_data/worldcover_class
```
:::
:::{tab-item} v3 — 9 variables
```
shot_number
wsci
wsci_xy
wsci_z
wsci_pi_lower
wsci_pi_upper
l4c_quality_flag_rel3
land_cover_data/worldcover_class
elev_highestreturn_outlier_flag
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 69 variables

```
# Core (18)
shot_number
wsci
wsci_xy
wsci_z
wsci_pi_lower
wsci_pi_upper
wsci_xy_pi_lower
wsci_xy_pi_upper
wsci_z_pi_lower
wsci_z_pi_upper
wsci_quality_flag
l2_quality_flag
algorithm_run_flag
sensitivity
surface_flag
selected_algorithm
solar_elevation
fhd_normal

# Land cover (7)
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion
land_cover_data/worldcover_class

# wsci_prediction — algorithms a1, a2, a5, a10 (11 each)
wsci_prediction/algorithm_run_flag_a1
wsci_prediction/wsci_quality_flag_a1
wsci_prediction/wsci_a1
wsci_prediction/wsci_pi_lower_a1
wsci_prediction/wsci_pi_upper_a1
wsci_prediction/wsci_xy_a1
wsci_prediction/wsci_xy_pi_lower_a1
wsci_prediction/wsci_xy_pi_upper_a1
wsci_prediction/wsci_z_a1
wsci_prediction/wsci_z_pi_lower_a1
wsci_prediction/wsci_z_pi_upper_a1
wsci_prediction/algorithm_run_flag_a2
wsci_prediction/wsci_quality_flag_a2
wsci_prediction/wsci_a2
wsci_prediction/wsci_pi_lower_a2
wsci_prediction/wsci_pi_upper_a2
wsci_prediction/wsci_xy_a2
wsci_prediction/wsci_xy_pi_lower_a2
wsci_prediction/wsci_xy_pi_upper_a2
wsci_prediction/wsci_z_a2
wsci_prediction/wsci_z_pi_lower_a2
wsci_prediction/wsci_z_pi_upper_a2
wsci_prediction/algorithm_run_flag_a5
wsci_prediction/wsci_quality_flag_a5
wsci_prediction/wsci_a5
wsci_prediction/wsci_pi_lower_a5
wsci_prediction/wsci_pi_upper_a5
wsci_prediction/wsci_xy_a5
wsci_prediction/wsci_xy_pi_lower_a5
wsci_prediction/wsci_xy_pi_upper_a5
wsci_prediction/wsci_z_a5
wsci_prediction/wsci_z_pi_lower_a5
wsci_prediction/wsci_z_pi_upper_a5
wsci_prediction/algorithm_run_flag_a10
wsci_prediction/wsci_quality_flag_a10
wsci_prediction/wsci_a10
wsci_prediction/wsci_pi_lower_a10
wsci_prediction/wsci_pi_upper_a10
wsci_prediction/wsci_xy_a10
wsci_prediction/wsci_xy_pi_lower_a10
wsci_prediction/wsci_xy_pi_upper_a10
wsci_prediction/wsci_z_a10
wsci_prediction/wsci_z_pi_lower_a10
wsci_prediction/wsci_z_pi_upper_a10
```

:::
:::{tab-item} v3 — 73 variables

```
# Core (20) — dual quality flags; new v3-specific flags
shot_number
wsci
wsci_xy
wsci_z
wsci_pi_lower
wsci_pi_upper
wsci_xy_pi_lower
wsci_xy_pi_upper
wsci_z_pi_lower
wsci_z_pi_upper
l4c_quality_flag_rel2
l4c_quality_flag_rel3
l2_algrunflag
algorithm_run_flag
elev_highestreturn_outlier_flag
degrade_include_flag
surface_flag
selected_algorithm
solar_elevation
fhd_normal

# Land cover (9) — adds phenology_phase, phenology_year
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/phenology_phase
land_cover_data/phenology_year
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion
land_cover_data/worldcover_class

# wsci_prediction — algorithms a1, a2, a5, a10 (11 each) — wsci_quality_flag_aX → l4c_quality_flag_rel3_aX
wsci_prediction/algorithm_run_flag_a1
wsci_prediction/l4c_quality_flag_rel3_a1
wsci_prediction/wsci_a1
wsci_prediction/wsci_pi_lower_a1
wsci_prediction/wsci_pi_upper_a1
wsci_prediction/wsci_xy_a1
wsci_prediction/wsci_xy_pi_lower_a1
wsci_prediction/wsci_xy_pi_upper_a1
wsci_prediction/wsci_z_a1
wsci_prediction/wsci_z_pi_lower_a1
wsci_prediction/wsci_z_pi_upper_a1
wsci_prediction/algorithm_run_flag_a2
wsci_prediction/l4c_quality_flag_rel3_a2
wsci_prediction/wsci_a2
wsci_prediction/wsci_pi_lower_a2
wsci_prediction/wsci_pi_upper_a2
wsci_prediction/wsci_xy_a2
wsci_prediction/wsci_xy_pi_lower_a2
wsci_prediction/wsci_xy_pi_upper_a2
wsci_prediction/wsci_z_a2
wsci_prediction/wsci_z_pi_lower_a2
wsci_prediction/wsci_z_pi_upper_a2
wsci_prediction/algorithm_run_flag_a5
wsci_prediction/l4c_quality_flag_rel3_a5
wsci_prediction/wsci_a5
wsci_prediction/wsci_pi_lower_a5
wsci_prediction/wsci_pi_upper_a5
wsci_prediction/wsci_xy_a5
wsci_prediction/wsci_xy_pi_lower_a5
wsci_prediction/wsci_xy_pi_upper_a5
wsci_prediction/wsci_z_a5
wsci_prediction/wsci_z_pi_lower_a5
wsci_prediction/wsci_z_pi_upper_a5
wsci_prediction/algorithm_run_flag_a10
wsci_prediction/l4c_quality_flag_rel3_a10
wsci_prediction/wsci_a10
wsci_prediction/wsci_pi_lower_a10
wsci_prediction/wsci_pi_upper_a10
wsci_prediction/wsci_xy_a10
wsci_prediction/wsci_xy_pi_lower_a10
wsci_prediction/wsci_xy_pi_upper_a10
wsci_prediction/wsci_z_a10
wsci_prediction/wsci_z_pi_lower_a10
wsci_prediction/wsci_z_pi_upper_a10
```

v3 adds dual quality flags, drops `sensitivity` (from L2A v3 essentials), expands land cover with phenology,
and renames `wsci_quality_flag_aX` → `l4c_quality_flag_rel3_aX` in wsci_prediction.

:::
::::

---

## Quality Filtering

The `--quality` / `-y` flag applies recommended quality conditions for each product
present in your query. L2A conditions (including `degrade_flag == 0`) are **always**
applied since L2A data is present in every database.

| Product | Version | Conditions Applied |
|---|---|---|
| L1B | v2 | `stale_return_flag == 0` |
| L1B | v3 | `stale_return_flag == 0` AND `rx_clipflag == 0` |
| L2A (always) | v2 | `quality_flag == 1` AND `degrade_flag == 0` |
| L2A (always) | v3 | `l2a_quality_flag_rel3 == 1` AND `degrade_flag == 0` |
| L2B | v2 | `l2b_quality_flag == 1` |
| L2B | v3 | `l2b_quality_flag_rel3 == 1` |
| L4A | v2 | `l4_quality_flag == 1` |
| L4A | v3 | `l4a_quality_flag_rel3 == 1` AND `elev_highestreturn_outlier_flag == 0` |
| L4C | v2 | `wsci_quality_flag == 1` (includes degrade+sensitivity+surface internally) |
| L4C | v3 | `l4c_quality_flag_rel3 == 1` AND `elev_highestreturn_outlier_flag == 0` |

`degrade_flag == 0` is always applied through L2A regardless of which products are
selected. L4C v2's `wsci_quality_flag` already encapsulates degrade, sensitivity, and
surface checks internally.

---

## Source Files

The `default` preset for each product/version is loaded directly from a plain-text file shipped with the package. You can inspect or copy these as a starting point for custom variable lists:

| Product | Version | Source file |
|---|---|---|
| L1B | v2 | `src/gedih3/data/GEDI01_B_DATASETS_002.txt` |
| L1B | v3 | `src/gedih3/data/GEDI01_B_DATASETS_003.txt` |
| L2A | v2 | `src/gedih3/data/GEDI02_A_DATASETS_002.txt` |
| L2A | v3 | `src/gedih3/data/GEDI02_A_DATASETS_003.txt` |
| L2B | v2 | `src/gedih3/data/GEDI02_B_DATASETS_002.txt` |
| L2B | v3 | `src/gedih3/data/GEDI02_B_DATASETS_003.txt` |
| L4A | v2 | `src/gedih3/data/GEDI04_A_DATASETS_002.txt` |
| L4A | v3 | `src/gedih3/data/GEDI04_A_DATASETS_003.txt` |
| L4C | v2 | `src/gedih3/data/GEDI04_C_DATASETS_002.txt` |
| L4C | v3 | `src/gedih3/data/GEDI04_C_DATASETS_003.txt` |

The `minimal` preset is hardcoded in `src/gedih3/config.py` under `_GEDI_MIN_VARS`.

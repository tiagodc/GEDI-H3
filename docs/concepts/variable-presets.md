# Variable Presets Reference

When you pass `-l2a default` or `-l2a minimal` on the command line, gedih3 expands that keyword into a concrete list of HDF5 variable names. This page documents exactly which variables each keyword selects, for every supported product and GEDI data version.

---

## Preset Levels

| Keyword | What it selects |
|---|---|
| `minimal` / `min` | Smallest usable set per product/version — geolocation, timestamp, primary quality flag, and the one or two headline scientific variables. Hardcoded in `config.py`. |
| `default` / `def` | Expert-curated science-ready set, loaded from the bundled `.txt` file in `src/gedih3/data/`. Designed to cover common research workflows without requiring HDF5 expertise. |
| `all` / `*` / bare flag | Every variable present in the HDF5 file for that product (all beams). Use with caution — there are 100+ variables per beam for each product. |
| `/path/to/file.txt` | Plain-text file with one HDF5 variable name per line; `#`-prefixed lines are treated as comments. |
| explicit names | One or more exact HDF5 variable names inline, e.g. `-l2a rh agbd quality_flag`. |

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

Only version 2 is available. The `minimal` and `default` presets select identical variables.

### minimal = default (5 variables)

```
shot_number
noise_mean_corrected
rx_sample_start_index
rx_sample_count
rxwaveform
```

---

## L2A — Elevation and Height Metrics

### minimal

::::{tab-set}
:::{tab-item} v2 — 7 variables
```
shot_number
delta_time
quality_flag
lat_lowestmode
lon_lowestmode
elev_lowestmode
rh
```
:::
:::{tab-item} v3 — 7 variables
```
shot_number
delta_time
l2a_quality_flag_rel3       # renamed from quality_flag in v2
lat_lowestmode
lon_lowestmode
elev_lowestmode
rh
```
:::
::::

### default

::::{tab-set}
:::{tab-item} v2 — 86 variables

```
# Core (18)
beam
channel
degrade_flag
delta_time
digital_elevation_model
digital_elevation_model_srtm
elevation_bias_flag
selected_algorithm
selected_mode
shot_number
solar_azimuth
solar_elevation
surface_flag
lat_lowestmode
lon_lowestmode
elev_lowestmode
quality_flag
rh

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
:::{tab-item} v3 — 105 variables

```
# Core (19) — quality_flag renamed; sensitivity added at top level
beam
channel
degrade_flag
delta_time
digital_elevation_model
digital_elevation_model_srtm
elevation_bias_flag
selected_algorithm
selected_mode
shot_number
solar_azimuth
solar_elevation
surface_flag
lat_lowestmode
lon_lowestmode
elev_lowestmode
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

# land_cover_data (6)
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion
```

:::
::::

---

## L2B — Canopy Cover and Vertical Structure

### minimal

The minimal set is identical between v2 and v3 (5 variables):

```
shot_number
cover_z
fhd_normal
pai_z
pgap_theta
```

### default

::::{tab-set}
:::{tab-item} v2 — 49 variables

```
# Core (14)
algorithmrun_flag
fhd_normal
cover_z
pai_z
pavd_z
l2b_quality_flag
rh100
rx_range_highestreturn
selected_rg_algorithm
selected_l2a_algorithm
sensitivity
shot_number
stale_return_flag
surface_flag

# geolocation (11)
geolocation/degrade_flag
geolocation/delta_time
geolocation/digital_elevation_model
geolocation/elev_lowestmode
geolocation/elev_highestreturn
geolocation/lat_lowestmode
geolocation/lat_highestreturn
geolocation/local_beam_elevation
geolocation/lon_lowestmode
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
:::{tab-item} v3 — 55 variables

```
# Core (14) — quality flag renamed to l2b_quality_flag_rel3
algorithmrun_flag
fhd_normal
cover_z
pai_z
pavd_z
l2b_quality_flag_rel3
rh100
rx_range_highestreturn
selected_rg_algorithm
selected_l2a_algorithm
sensitivity
shot_number
stale_return_flag
surface_flag

# geolocation (11)
geolocation/degrade_flag
geolocation/delta_time
geolocation/digital_elevation_model
geolocation/elev_lowestmode
geolocation/elev_highestreturn
geolocation/lat_lowestmode
geolocation/lat_highestreturn
geolocation/local_beam_elevation
geolocation/lon_lowestmode
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

# rx_processing — algorithm a10 (6) — new in v3
rx_processing/algorithmrun_flag_a10
rx_processing/pgap_theta_a10
rx_processing/rg_a10
rx_processing/rg_error_a10
rx_processing/rv_a10
rx_processing/rx_energy_a10
```

:::
::::

---

## L4A — Footprint-Level Aboveground Biomass

Only version 2 is available.

### minimal (4 variables)

```
shot_number
agbd
sensitivity
l4_quality_flag
```

### default (75 variables)

```
# Core and land cover (27)
agbd
agbd_se
agbd_t
agbd_t_se
algorithm_run_flag
degrade_flag
delta_time
elev_lowestmode
l2_quality_flag
l4_quality_flag
lat_lowestmode
lon_lowestmode
predict_stratum
predictor_limit_flag
response_limit_flag
selected_algorithm
sensitivity
shot_number
solar_elevation
surface_flag
xvar
land_cover_data/landsat_treecover
land_cover_data/landsat_water_persistence
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/urban_proportion

# geolocation — algorithm a1 (4)
geolocation/elev_lowestmode_a1
geolocation/lat_lowestmode_a1
geolocation/lon_lowestmode_a1
geolocation/sensitivity_a1

# geolocation — algorithm a2 (4)
geolocation/elev_lowestmode_a2
geolocation/lat_lowestmode_a2
geolocation/lon_lowestmode_a2
geolocation/sensitivity_a2

# geolocation — algorithm a5 (4)
geolocation/elev_lowestmode_a5
geolocation/lat_lowestmode_a5
geolocation/lon_lowestmode_a5
geolocation/sensitivity_a5

# agbd_prediction — algorithm a1 (12)
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

# agbd_prediction — algorithm a2 (12)
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

# agbd_prediction — algorithm a5 (12)
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

---

## L4C — Footprint-Level Structural Complexity

Only version 2 is available.

### minimal (8 variables)

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

### default (15 variables)

```
shot_number
wsci
wsci_pi_lower
wsci_pi_upper
wsci_quality_flag
wsci_xy
wsci_xy_pi_lower
wsci_xy_pi_upper
wsci_z
wsci_z_pi_lower
wsci_z_pi_upper
land_cover_data/leaf_off_flag
land_cover_data/pft_class
land_cover_data/region_class
land_cover_data/worldcover_class
```

---

## Source Files

The `default` preset for each product/version is loaded directly from a plain-text file shipped with the package. You can inspect or copy these as a starting point for custom variable lists:

| Product | Version | Source file |
|---|---|---|
| L1B | v2 | `src/gedih3/data/GEDI01_B_DATASETS_002.txt` |
| L2A | v2 | `src/gedih3/data/GEDI02_A_DATASETS_002.txt` |
| L2A | v3 | `src/gedih3/data/GEDI02_A_DATASETS_003.txt` |
| L2B | v2 | `src/gedih3/data/GEDI02_B_DATASETS_002.txt` |
| L2B | v3 | `src/gedih3/data/GEDI02_B_DATASETS_003.txt` |
| L4A | v2 | `src/gedih3/data/GEDI04_A_DATASETS_002.txt` |
| L4C | v2 | `src/gedih3/data/GEDI04_C_DATASETS_002.txt` |

The `minimal` preset is hardcoded in `src/gedih3/config.py` under `_GEDI_MIN_VARS`.

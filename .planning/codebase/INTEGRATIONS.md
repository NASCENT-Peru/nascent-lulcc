# INTEGRATIONS — nascent-lulcc
_Last updated: 2026-04-30_

## Summary
nascent-lulcc integrates with a range of external climate, geospatial, and socioeconomic data sources—all accessed as static file downloads rather than live APIs. The only runtime external software dependency is Dinamica EGO 8 (proprietary spatial simulation engine). There is no database, no authentication service, and no cloud provider; compute infrastructure is an on-premises HPC cluster (ETH Euler).

---

## Compute Infrastructure

**HPC Cluster:**
- Platform: ETH Euler (SLURM)
- Code location: `/cluster/home/bblack/nascent-lulcc`
- Data/scratch location: `/cluster/scratch/bblack/nascent-lulcc`
- Job scripts: `scripts/submit_*.sh`, `scripts/master_pipeline.sh`
- Environment management: micromamba (`scripts/install_micromamba.sh`)

**Local Development:**
- Windows workstation; data on `E:/nascent-lulcc-agg`
- Config: `config/local_config.yaml`

---

## External Software — Dinamica EGO 8

- **What it is:** Proprietary spatial land-use allocation simulation engine
- **How it is called:** Via `r-processx` from R (`src/dinamica_utils.r`, `src/allocation.r`)
- **Required environment variables:**
  - `DINAMICA_EGO_8_HOME` — path to Dinamica EGO installation directory (used to locate shared libraries via `LD_LIBRARY_PATH`)
  - `DinamicaConsole` must be on `PATH`
- **Model files:** `dinamica/dinamica_model/allocation.ego-decoded`, `dinamica/dinamica_model/evoland.ego-decoded`, and submodels under `dinamica/dinamica_model/evoland_ego_Submodels/`
- **Integration point:** R spawns `DinamicaConsole` as a child process with the `.ego` model file path as argument; stdout/stderr are written to timestamped log files

---

## Climate Data — CHELSA-CMIP6

- **Source:** CHELSA climate data service (https://chelsa-climate.org/), accessed via the `chelsa_cmip6` Python library
- **Data type:** Bioclimatic variables (bio1–bio19) and growing degree days (GDD) as NetCDF files
- **Scripts:** `scripts/download_historic_climate_data.py`, `scripts/download_future_climate_data.py`
- **Coverage:**
  - Historical: GFDL-ESM4 model, 30-year windows centred on 2010, 2014, 2018, 2022
  - Future: 5 GCMs (GFDL-ESM4, IPSL-CM6A-LR, MPI-ESM1-2-LR, MRI-ESM2-0, UKESM1-0-LL), SSPs ssp126/ssp245/ssp370/ssp585, 4-year steps 2024–2060
- **Spatial extent:** Peru bounding box (xmin=-81.411, xmax=-68.665, ymin=-18.348, ymax=0.000)
- **Processing:** `scripts/process_climate_data.r` (multi-GCM ensemble averaging, GDAL VRT/warp, GeoTIFF LERC_ZSTD compression), `scripts/calculate_mean_rsds_1981_2010.r`, `scripts/calculate_et0.r`
- **Output format:** GeoTIFF (`.tif`) placed under `inputs/predictors/prepared/layers/climatic/`
- **Access method:** `use_esgf=False` — downloads via CHELSA HTTP (not ESGF node)

---

## Land Use / Land Cover Data

- **Source:** Rasterized LULC maps for Peru (years 2010, 2014, 2018, 2022)
- **Input format:** GeoTIFF rasters stored in `inputs/lulc/original/`
- **Original classification:** MapBiomas-style numeric codes (Forest=3, Dry Forest=4, Mangrove=5, Agriculture=18, Oil palm=35, Mining=30, etc.)
- **Aggregation schema:** `config/lulc_schema.json` — 7 aggregated classes (Forested Areas=101, Natural Grasslands and Shrublands=102, Low-Intensity Ag=103, High-Intensity Ag=104, Built-Up and Barren=105, Mining=106, Water body=107)
- **Processing:** `src/lulc_data_prep.r` — reclassification via `terra::classify()`, spatial aggregation to 100m reference grid (EPSG:2056)

---

## Administrative Boundaries (OCHA / IGN Peru)

- **Source:** OCHA Humanitarian Data Exchange
- **URL:** https://data.humdata.org/dataset/54fc7f4d-f4c0-4892-91f6-2fe7c1ecf363/
- **Data:** Peru administrative boundaries ADM0–ADM3 (country, regions, provinces, districts) as Shapefiles
- **Format:** `.shp` (Shapefile)
- **Config:** `config/ancillary_data.yaml`
- **Raw storage:** `inputs/ancillary_spatial_data/` (`ancillary_spatial_dir`)
- **Prepared outputs:** `ancillary_spatial_data/prepared/country.shp`, `regions.shp`, `provinces.shp`, `districts.shp`

---

## Terrain / Elevation Data (DEM)

- **Source:** Configured via `config/pred_data.yaml` (path: `elevation.raw_dir / elevation.raw_filename`)
- **Format:** Raster (likely GeoTIFF)
- **Processing:** `src/terrain_pred_prep.r` — aligned to reference grid, then `terra::terrain()` derives slope, aspect, TPI, TRI, roughness
- **Output:** `inputs/predictors/prepared/layers/terrain/*.tif`

---

## Soil Data

- **Source:** Configured via `config/pred_data.yaml` (entries with `grouping: soil`)
- **Format:** Raster
- **Processing:** `src/soil_pred_prep.r` — aligned to reference grid
- **Output:** `inputs/predictors/prepared/layers/soil/soil_*.tif`

---

## Hydrological Data (Vector)

- **Source:** Configured via `config/pred_data.yaml` (entries with `grouping: hydrological`)
- **Format:** Shapefile (`.shp`)
- **Processing:** Distance rasters computed on HPC (`scripts/run_dist_calc.r`, submitted via `scripts/submit_dist_calc.sh` using SLURM sbatch with 48 CPUs); `src/hydrological_pred_prep.r` registers prepared layers
- **Output:** `inputs/predictors/prepared/layers/hydrological/*.tif`

---

## Infrastructure Data (Roads, Airports)

- **Sources:** Road networks and airport shapefiles (configured via `config/pred_data.yaml`, `grouping: infrastructure`)
- **Format:** Shapefile (`.shp`)
- **Processing:** `src/infrastructure_pred_prep.r` — reprojection to reference CRS, merging (secondary roads), then distance calculation on HPC
- **Output:** `inputs/predictors/prepared/layers/infrastructure/*.tif`

---

## Socioeconomic Data (INEI)

- **Source:** Instituto Nacional de Estadística e Informática (INEI) Peru — CSV files with district-level indicators including Gross Added Value (`vab_total`)
- **Format:** CSV (`.csv`) stored in `inputs/predictors/raw/socio_economic/`
- **Processing:** `src/inei_pred_prep.r` — spatially joined to district polygons, rasterised to reference grid
- **Output:** Predictor rasters in `inputs/predictors/prepared/layers/`

---

## Population Data

- **Source:** Configured via `config/pred_data.yaml` (implied by `src/population_pred_prep.r`)
- **Processing:** `src/population_pred_prep.r` (file is currently empty / stub — 1 line)

---

## Scenario Demand Data

- **Source:** Internal tools (Excel/CSV)
  - `tools/simulation_lulc_areas_2060.csv` — target LULC areas per scenario
  - `tools/calibration_control.csv` — calibration run configuration
  - `tools/simulation_control.csv` — simulation run configuration
  - `tools/model_lookup.xlsx` — model specification lookup table
- **Scenarios:** BAU, NAT, CUL, SOC mapped to SSP245/SSP126 (`config/hpc_config.yaml`)
- **Transition rate demand:** `inputs/lulc/future_demand/lulc_demand_results.csv`

---

## Spatial Intervention Masks

- **Format:** Raster (`.tif`, `.grd`)
- **Configured in:** `config/SSP0_interventions.yml` through `config/SSP5_interventions.yml`
- **Mask types:** Static (single raster) and Dynamic (one raster per simulation timestep, e.g., expanding conservation areas)
- **Storage:** `inputs/spat_prob_perturb/` (HPC: `spat_prob_perturb_dir`)
- **Intervention categories:** Urban densification/migration (building zone rasters), agricultural abandonment (marginality rasters), conservation expansion/preservation (protected area rasters per SSP/year)

---

## File Formats Summary

| Format | Use |
|--------|-----|
| GeoTIFF (`.tif`) | All raster inputs/outputs (LULC, predictors, allocation results) |
| Shapefile (`.shp`) | Vector administrative boundaries and infrastructure |
| Parquet (`.parquet`) | Tabular predictor datasets for model training (`r-arrow`) |
| NetCDF (`.nc`) | Raw CHELSA climate downloads |
| YAML (`.yaml`, `.yml`) | Configuration, intervention definitions, predictor catalogue |
| JSON (`.json`) | LULC classification schema (`config/lulc_schema.json`) |
| CSV (`.csv`) | Transition rates, LULC demand, INEI socioeconomic data |
| Excel (`.xlsx`) | Model lookup and control tables (`tools/`) |
| `.ego-decoded` | Dinamica EGO model files |

---

## Authentication & Secrets

- No authentication services or API keys identified
- CHELSA data is accessed anonymously via HTTP
- HPC access uses standard SSH (credential management external to this repo)
- `.env` / `.env.template` files are referenced in `README.md` for path configuration but are not committed

---

## Monitoring & Logging

- No external error tracking or monitoring service
- SLURM captures stdout/stderr per job to `logs/` directory (e.g., `logs/lulc-allocation-%j.out`)
- R scripts emit structured log messages via `message()` and a custom `log_msg()` function
- Optional profiling: `ALLOCATION_PROFILE=TRUE` env var activates per-stage RSS memory and timing logs in `src/allocation.r`

---

## Gaps / Unknowns

- The full predictor catalogue in `config/pred_data.yaml` (931KB) was not fully read — specific source URLs and data providers for each predictor are unknown beyond what is implied by grouping names (terrain, soil, hydrological, infrastructure, climatic, socioeconomic)
- Population data source (`src/population_pred_prep.r`) is unspecified; the file is currently empty
- No CI/CD integration found; pipeline correctness is not automatically tested
- ESGF access (`use_esgf=False`) is disabled; if CHELSA HTTP becomes unavailable, no fallback is configured

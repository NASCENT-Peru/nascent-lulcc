climatic_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  LULC_years = c(2010, 2014, 2018, 2022)
) {
  message("starting infrastructure predictor prep")

  # Load predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # load reference grid
  ref_grid <- terra::rast(config[["ref_grid_path"]])

  # for efficiency all of the climatic predictor data for the calibration and simulation periods
  # is downloaded and prepared in a HPC environment using the following script:
  # scripts/run_climatic_data_prep.sbatch

  # This script uses the environment envs/clim_data_env.yml which must be created on your system
  # The script calls the following R and python scripts in sequence:
  # 1. inst/download_historic_climate_data.py to download the historic climate data from CHELSA
  # 2. inst/download_future_climate_data.py to download the future climate data from CHELSA
  # 2. inst/process_climate_data.r to process the data (aggregate across gcms, create .tifs, compress)
  # 3. inst/calculate_mean_rsds_1981_2010.r to prepare the srad monthly data for evapotranspiration calculation (srad data manually from Chelsa website)
  # 4. inst/calculate_et0.r to calculate evapotranspiration

  # the remainder of this function simply updates the predictor table with the layer info for the climatic predictors

  # list files in prepped climatic predictor directory
  climatic_pred_dir <- file.path(
    config[["prepped_lyr_path"]],
    "climatic"
  )
  climatic_files <- list.files(
    climatic_pred_dir,
    pattern = "\\.tif$",
    full.names = TRUE
  )

  # stack and mask all files simultaneously
  climatic_stack <- terra::rast(climatic_files)
  start_time <- Sys.time()
  climatic_stack <- terra::mask(climatic_stack, ref_grid)
  end_time <- Sys.time()

  # vector some basic info to help create metadata
  ssps <- c("ssp126", "ssp245", "ssp585")
  modelling_periods <- c("2010_2014", "2014_2018", "2018_2022")

  # clean names for climatic variables
  climatic_var_list <- list(
    "Annual Mean Temperature" = "bio1",
    "Mean Diurnal Range" = "bio2",
    "Isothermality" = "bio3",
    "Temperature Seasonality" = "bio4",
    "Max Temperature of Warmest Month" = "bio5",
    "Min Temperature of Coldest Month" = "bio6",
    "Temperature Annual Range" = "bio7",
    "Mean Temperature of Wettest Quarter" = "bio8",
    "Mean Temperature of Driest Quarter" = "bio9",
    "Mean Temperature of Warmest Quarter" = "bio10",
    "Mean Temperature of Coldest Quarter" = "bio11",
    "Annual Precipitation" = "bio12",
    "Precipitation of Wettest Month" = "bio13",
    "Precipitation of Driest Month" = "bio14",
    "Precipitation Seasonality" = "bio15",
    "Precipitation of Wettest Quarter" = "bio16",
    "Precipitation of Driest Quarter" = "bio17",
    "Precipitation of Warmest Quarter" = "bio18",
    "Precipitation of Coldest Quarter" = "bio19",
    "Growing Degree Days" = "gdd",
    "Average Temperature" = "tas",
    "Maximum Temperature" = "tasmax",
    "Minimum Temperature" = "tasmin",
    "Precipitation" = "pr",
    "Evapotranspiration" = "et0"
  )

  # loop over them and mask to the ref grid and update pred_table
  lapply(climatic_files, function(f) {
    message(paste("Processing", basename(f)))

    # extract predictor name from filename as everything before .tif
    pred_name <- tolower(gsub("\\.tif$", "", basename(f)))

    # split on '_' and keep first element(s) to identify variable
    base_name <- unlist(strsplit(pred_name, "_"))[1]

    # match on climatic_var_names to get clean name
    var <- which(climatic_var_list %in% base_name)
    clean_name <- names(climatic_var_list)[var]

    # match to ssp using vectorized grep
    scenario_variant <- ssps[sapply(ssps, function(x) grepl(x, pred_name))]
    if (length(scenario_variant) == 0) {
      scenario_variant <- NULL
    }

    outpath <- file.path(
      config[["prepped_lyr_path"]],
      "climatic",
      tolower(basename(f))
    )

    # extract four digit numerical period from filename
    period <- regmatches(pred_name, regexpr("\\d{4}", pred_name))

    # update YAML
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = pred_name,
      base_name = base_name,
      clean_name = clean_name,
      pred_category = "suitability",
      static_or_dynamic = "dynamic",
      metadata = "Karger, D.N., Schmatz, D., Detttling, D., Zimmermann, N.E. (2020) High resolution monthly precipitation and temperature timeseries for the period 2006-2100. Scientific Data. https://doi.org/10.1038/s41597-020-00587-y",
      scenario_variant = scenario_variant,
      period = period,
      path = outpath |>
        fs::path_rel(config[["data_basepath"]]),
      grouping = "climatic",
      description = paste0(
        "Climatic predictor variable '",
        clean_name,
        if (!is.null(scenario_variant)) {
          paste0("' for scenario '", scenario_variant)
        } else {
          "historic"
        },
        "'."
      ),
      method = "Aligned to reference grid.",
      date = "2025",
      author = "CHELSA",
      sources = "https://envicloud.wsl.ch/#/?bucket=https%3A%2F%2Fos.zhdk.cloud.switch.ch%2Fchelsav2%2F&prefix=GLOBAL%2Fclimatologies%2F",
      raw_dir = NULL,
      raw_filename = NULL
    )
  })
}

#' Preparation of Predictor Data
#'
#' Does some predictor data preparation, relying on the xlsx file at `pred_table_path`
#' and other assumptions specified in [get_config()].
#'
#' @param config A list object
#'
#' @export

calibration_predictor_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  ignore_excel = FALSE
) {
  # Set temp directory for terra to another drive
  # because the default is on C: which often has limited space
  terra_temp <- "E:/terra_temp"
  ensure_dir(terra_temp)

  terra::terraOptions(
    memfrac = 0.5, # limit in-memory cache usage
    tempdir = terra_temp, # directory for temporary files
    progress = 1,
    todisk = TRUE
  )

  # Load in the grid to use use for re-projecting the CRS and extent of predictor data
  Ref_grid <- terra::rast(config[["ref_grid_path"]])

  # vector years of LULC data
  LULC_years <-
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = FALSE,
      pattern = ".tif"
    ) |>
    gsub(pattern = ".*?([0-9]+).*", replacement = "\\1", x = _)

  # create a list of the data/modelling periods
  modelling_periods <- list()
  for (i in 1:(length(LULC_years) - 1)) {
    modelling_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
  }
  names(modelling_periods) <- sapply(
    modelling_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  # get values of each of the four years preceding the first entry in each of modelling_periods
  pre_modelling_years <- sapply(
    modelling_periods,
    function(x) as.character(as.integer(x[1]) - 4:1),
    simplify = FALSE
  )

  # create base directory for prepared predictor layers
  ensure_dir(config[["prepped_lyr_path"]])

  # prepare topographical predictors
  terrain_pred_prep(
    config = config
  )

  # prepare soil predictors
  soil_pred_prep(
    config = config,
    refresh_cache = TRUE,
    terra_temp = terra_temp
  )

  # Prepare lulc neighbourhood based predictors
  nhood_predictor_prep(
    config = config,
    refresh_cache = refresh_cache,
    redo_random_matrices = FALSE,
    terra_temp = terra_temp
  )

  # prepare socio-economic predictors
  socio_economic_pred_prep(
    config = config,
    refresh_cache = refresh_cache,
    terra_temp = terra_temp
  )

  # prepare infrastructure predictors
  infrastructure_pred_prep(
    config = config,
    refresh_cache = refresh_cache
  )

  # perform distance calculations to prepare intermediate layers for hydrological and infrastructure predictors
  source("src/dist_calc_functions.r")
  source("scripts/run_dist_calc.r")

  # hydrological predictors
  hydrological_pred_prep(
    config = config,
    refresh_cache = refresh_cache,
    terra_temp = terra_temp
  )

  # prepare climatic predictors
  climatic_pred_prep(
    config = config,
    refresh_cache = refresh_cache
  )

  message(
    " Preparation of  predictor layers complete"
  )
}

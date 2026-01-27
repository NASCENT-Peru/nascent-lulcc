soil_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  terra_temp = tempdir()
) {
  message("Preparing soil predictor data...")

  ensure_dir(config[["prepped_lyr_path"]])

  # load the pred_table yaml file
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # subset to soil predictor entries grouping is soil
  soil_pred_entries <- Filter(
    function(x) !is.null(x$grouping) && x$grouping == "soil",
    pred_table
  )

  # combine the paths to get the full paths to the raw soil predictor files
  soil_pred_paths <- sapply(
    soil_pred_entries,
    function(x) {
      file.path(
        config[["predictors_raw_dir"]],
        x$raw_dir,
        x$raw_filename
      )
    },
    USE.NAMES = TRUE
  )

  # loop over each file
  for (f in seq_along(soil_pred_entries)) {
    message(paste("Processing", names(soil_pred_paths)[f]))

    soil_dir <- file.path(config[["prepped_lyr_path"]], "soil")
    ensure_dir(soil_dir)

    # save the aligned raster to the prepped_lyr_path directory
    out_name <- basename(soil_pred_paths[f])
    out_path <- file.path(
      soil_dir,
      paste0("soil_", out_name)
    )

    # skip if already exists and not refreshing
    if (file.exists(out_path) & !refresh_cache) {
      message(paste("File", out_name, "already exists. Skipping..."))
      next
    } else {
      message(paste("Saving to", out_path))
    }

    start_time <- Sys.time()
    # align the raster to the reference grid
    align_to_ref(
      x = soil_pred_paths[f],
      ref = config[["ref_grid_path"]],
      filename = out_path,
      tempdir = terra_temp
    )
    end_time <- Sys.time()
    message(paste(
      "Processed",
      out_name,
      "in",
      round(difftime(end_time, start_time, units = "mins"), 2),
      "minutes"
    ))

    # extract metadata for YAML update
    pred_name <- names(soil_pred_paths)[f]
    clean_name <- gsub("_", " ", pred_name)

    rel_path <- out_path |>
      fs::path_rel(config[["data_basepath"]])

    entry <- soil_pred_entries[[pred_name]]

    # update YAML
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = pred_name,
      clean_name = clean_name,
      pred_category = "suitability",
      static_or_dynamic = "static",
      metadata = if (!is.null(entry$metadata)) entry$metadata else NULL,
      scenario_variant = if (!is.null(entry$scenario_variant)) {
        entry$scenario_variant
      } else {
        NULL
      },
      period = if (!is.null(entry$period)) entry$period else "all",
      path = rel_path,
      grouping = "soil",
      description = paste0(clean_name, " – derived from SoilGrids dataset."),
      ,
      method = if (!is.null(entry$method)) {
        entry$method
      } else {
        "Aligned to reference grid."
      },
      date = Sys.Date(),
      author = "Your Name",
      sources = "https://www.soilgrids.org",
      raw_dir = entry$raw_dir,
      raw_filename = basename(soil_pred_paths[f])
    )
  }

  message("Soil predictor data preparation complete.")
}

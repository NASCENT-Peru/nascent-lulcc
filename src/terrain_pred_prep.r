terrain_pred_prep <- function(config = get_config()) {
  ensure_dir(config[["prepped_lyr_path"]])

  # Load existing DEM raster and predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # Load clipped DEM raster
  DEM_raster <- terra::rast(file.path(
    config[["predictors_raw_dir"]],
    pred_table[["elevation"]][["raw_dir"]],
    pred_table[["elevation"]][["raw_filename"]]
  ))

  # Align DEM raster to reference grid
  DEM_raster_aligned <- align_raster_to_ref(
    DEM_raster,
    config[["ref_grid_path"]]
  )

  # Save aligned DEM raster
  elevation_out_path <- file.path(config[["prepped_lyr_path"]], "elevation.tif")
  write_raster(DEM_raster_aligned, filename = elevation_out_path)

  # Calculate terrain measures
  terrain_measures <- c("slope", "aspect", "TPI", "TRI", "roughness")
  terrain_paths <- file.path(
    config[["prepped_lyr_path"]],
    "terrain",
    paste0(terrain_measures, ".tif")
  )

  for (i in seq_along(terrain_measures)) {
    out_raster <- terra::terrain(
      DEM_raster_aligned,
      v = terrain_measures[i],
      unit = "degrees"
    )
    write_raster(out_raster, filename = terrain_paths[i])
  }

  # Include elevation in terrain measures for YAML update
  all_measures <- c("elevation", terrain_measures)
  all_paths <- file.path(
    config[["prepped_lyr_path"]],
    "terrain",
    paste0(all_measures, ".tif")
  )

  # Update YAML for each terrain measure
  for (i in seq_along(all_measures)) {
    measure <- all_measures[i]
    path <- all_paths[i]

    # make the path relative to the predictors_prepped_dir
    path <- path |>
      fs::path_rel(config[["data_basepath"]])

    clean_name <- gsub("_", " ", measure)

    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = measure,
      clean_name = clean_name,
      pred_category = "suitability",
      static_or_dynamic = "static",
      metadata = NULL,
      scenario_variant = NULL,
      period = "all",
      path = path,
      grouping = "terrain",
      description = "Derived from NASADEM data",
      method = paste0(
        "Calculated using the terra::terrain function from the aligned DEM raster. ",
        "Slope and aspect are in degrees; TPI, TRI, and roughness are unitless."
      ),
      date = Sys.Date(),
      author = "Your Name",
      sources = "https://www.earthdata.nasa.gov/data/catalog/lpcloud-nasadem-hgt-001",
      raw_dir = "terrain",
      raw_filename = "masked_dem.tif"
    )
  }
}

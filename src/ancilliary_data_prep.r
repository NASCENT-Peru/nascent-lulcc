#' ancillary_data_prep: Prepare ancillary spatial data layers
#' #' This function prepares ancillary spatial data layers required for the preparation of predictors
#' @param config A list containing configuration parameters. Default is obtained from get_config().
#' @param refresh_cache A logical indicating whether to refresh cached data. Default is FALSE.
#'
ancillary_data_prep <- function(
  config = get_config(),
  refresh_cache = TRUE,
  terra_temp = tempdir()
) {
  # ensure a prepared ancillary data directory exists
  ensure_dir(file.path(config[["ancillary_spatial_dir"]], "prepared"))

  # load the yaml file listing the ancillary data
  ancillary_yaml_path <- config[["ancillary_data_table"]]
  ancillary_table <- yaml::yaml.load_file(ancillary_yaml_path)

  # loop over each ancillary data entry
  for (entry_name in names(ancillary_table)) {
    entry <- ancillary_table[[entry_name]]

    message(paste("Processing ancillary data:", entry_name))

    raw_path <- file.path(
      config[["ancillary_spatial_dir"]],
      entry$raw_dir,
      entry$raw_filename
    )

    # create path depending on whether file is a shapefile or raster (.tif)
    if (grepl("\\.shp$", entry$raw_filename)) {
      # shapefile
      prepped_path <- file.path(
        config[["ancillary_spatial_dir"]],
        "prepared",
        paste0(entry_name, ".shp")
      )
    } else if (grepl("\\.tif$", entry$raw_filename)) {
      # raster file
      prepped_path <- file.path(
        config[["ancillary_spatial_dir"]],
        "prepared",
        paste0(entry_name, ".tif")
      )
    } else {
      stop(paste("Unsupported file format for", entry$raw_filename))
    }

    # add the prepped_path to the entry for future reference
    entry$path <- prepped_path |>
      fs::path_rel(config[["data_basepath"]])

    # skip if already exists and not refreshing
    if (file.exists(prepped_path) & !refresh_cache) {
      message(paste("File", prepped_path, "already exists. Skipping..."))
      next
    } else {
      message(paste("Saving to", prepped_path))
    }

    # if the file is raster then use the function to align to ref grid
    if (grepl("\\.tif$", entry$raw_filename)) {
      # align the raster data to the reference grid
      align_to_ref(
        x = raw_path,
        ref = config[["ref_grid_path"]],
        filename = prepped_path,
        tempdir = terra_temp
      )
    } else if (grepl("\\.shp$", entry$raw_filename)) {
      # if shapefile then make sure it has the same extent and crs as the ref grid
      ref_raster <- terra::rast(config[["ref_grid_path"]])
      raw_vect <- terra::vect(raw_path)
      # reproject if needed
      if (!terra::crs(raw_vect) == terra::crs(ref_raster)) {
        message("Reprojecting vector to match reference raster CRS.")
        raw_vect <- terra::project(raw_vect, terra::crs(ref_raster))
      }
      # crop to ref extent
      message("Cropping vector to match reference raster extent.")
      cropped_vect <- terra::crop(raw_vect, ref_raster)
      # save the cropped vect
      terra::writeVector(
        cropped_vect,
        prepped_path,
        filetype = "ESRI Shapefile",
        overwrite = TRUE
      )
    }

    # update the yaml entry with the prepped path
    ancillary_table[[entry_name]] <- entry
  }

  # replace the entries in the existing yaml file if they exist if not add them
  yaml::write_yaml(ancillary_table, ancillary_yaml_path)

  message("Ancilliary data preparation complete.")
}

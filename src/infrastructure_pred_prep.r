#' Prepare infrastructure predictors by reprojecting and saving intermediate files
#' to a specified directory ready for distance calculations in a HPC environment.
#' @description The infrastructure predictors are of similar complexity to the other vector
#' predictors e.g. hydrological features and hence this function simply re-projects them to match
#' the reference grid and saves them as intermediate files, so that they can be used for distance
#' calculations in a HPC environment.
#' @param config A list containing configuration parameters, including file paths.
#' Defaults to the result of `get_config()`.
#' @param refresh_cache A logical indicating whether to refresh cached data. Defaults to FALSE.
#' @return None. The function performs file operations and updates a YAML file.
infrastructure_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE
) {
  message("starting infrastructure predictor prep")

  # Load predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # load reference grid
  ref_grid <- terra::rast(config[["ref_grid_path"]])

  # Subset to entries where grouping is 'infrastructure'
  infra_pred_entries <- pred_table |>
    purrr::keep(~ .x$grouping == "infrastructure")

  # create file paths
  infra_vect_paths <- sapply(
    infra_pred_entries,
    function(x) {
      file.path(
        config[["predictors_raw_dir"]],
        x$raw_dir,
        x$raw_filename
      )
    },
    USE.NAMES = TRUE
  )
  # The dist_to_primary_roads and dist_to_airports predictors only use single shapefiles which
  #  need to be reprojected to the reference crs, whereas the dist_to_secondary_roads combines
  #  two shapefiles which both need to be reprojected and then merged before distance calculation.

  # seperate the secondary roads entry
  sec_road_paths <- infra_vect_paths[["dist_to_secondary_roads"]]

  # load the two shapefiles for secondary roads
  sec_roads_vects <- sapply(sec_road_paths, function(x) {
    # load the vector
    rd_vect <- terra::vect(x)

    # reproject to reference crs
    rd_vect_proj <- terra::project(
      rd_vect,
      ref_grid
    )

    # match extent to reference grid
    rd_vect_proj_matched <- terra::crop(
      rd_vect_proj,
      ref_grid
    )
    return(rd_vect_proj_matched)
  })

  # merge the two secondary road shapefiles
  sec_roads_combined <- terra::vect(sec_roads_vects)

  # load the other infrastructure predictors
  other_infra_preds <- infra_vect_paths[
    names(infra_vect_paths) != "dist_to_secondary_roads"
  ]

  other_infra_vects <- sapply(other_infra_preds, function(x) {
    # load the vector
    vect_data <- terra::vect(x)

    # reproject to reference crs
    vect_data_proj <- terra::project(
      vect_data,
      ref_grid
    )

    # match extent to reference grid
    vect_data_proj_matched <- terra::crop(
      vect_data_proj,
      ref_grid
    )

    # check for any invalid geometries
    invalid_geoms <- which(!terra::is.valid(vect_data_proj_matched))
    message(paste0(
      "Found ",
      length(invalid_geoms),
      " invalid geometries in ",
      x
    ))

    return(vect_data_proj_matched)
  })

  # combine into a named list
  infra_vect_list <- c(
    list(dist_to_secondary_roads = sec_roads_combined),
    other_infra_vects
  )

  # loop through each infrastructure predictor, save an intermediate file and update the pred_table
  lapply(seq_along(infra_vect_list), function(i) {
    pred_name <- names(infra_vect_list)[i]
    vect_data <- infra_vect_list[[i]]
    pred_entry <- infra_pred_entries[[pred_name]]

    # create output file path
    intermediate_path <- file.path(
      config[["predictors_intermediate_dir"]],
      # remove 'dist_to_' prefix for file naming
      paste0(gsub("dist_to_", "", pred_name), ".shp")
    )

    # save the dissolved shapefile
    terra::writeVector(
      vect_data,
      intermediate_path,
      filetype = "ESRI Shapefile",
      overwrite = TRUE
    )

    # update the predictor yaml
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = pred_name,
      clean_name = pred_entry$clean_name,
      pred_category = pred_entry$pred_category,
      static_or_dynamic = pred_entry$static_or_dynamic,
      period = pred_entry$period,
      intermediate_path = fs::path_rel(
        intermediate_path,
        config[["data_basepath"]]
      ),
      path = pred_entry$path,
      method = pred_entry$method,
      grouping = pred_entry$grouping,
      description = pred_entry$description,
      sources = pred_entry$sources,
      author = pred_entry$author,
      date = Sys.Date(),
      raw_dir = pred_entry$raw_dir,
      raw_filename = pred_entry$raw_filename
    )
  })
}

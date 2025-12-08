hydrological_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  terra_temp = tempdir()
) {
  message("Preparing hydrological predictor data...")

  # Load predictor YAML
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # Subset to entries where grouping is 'hydrological'
  hydro_pred_entries <- Filter(
    function(x) !is.null(x$grouping) && x$grouping == "hydrological",
    pred_table
  )

  # Construct full vector paths
  hydro_vect_paths <- sapply(
    hydro_pred_entries,
    function(x) {
      file.path(
        config[["predictors_raw_dir"]],
        x$raw_dir,
        x$raw_filename
      )
    },
    USE.NAMES = TRUE
  )

  # Given that these shapefiles have a large number of complex geometries
  # and that the extent and resolution of the reference grid is large,
  # calculating distance based rasters from them on a desktop computer is likely not feasible.

  # Instead the layers were calculated on a high-performance computing cluster using
  # a custom R script (R/dist_calc_hpc.r) that uses GDAL utilities.
  # This workflow can be reproduced by users with access to similar HPC resources.
  # First create the required environment using the script (scripts/setup_dist_calc_env.sh)
  # which uses envs/dist_calc_env.yml to create a conda environment with the required dependencies.
  # Then run the sbatch script (scripts/run_dist_calc.sbatch) to submit the job to the cluster.
  # Note that you will need to manually move the required shapefiles to the cluster and adjust paths in the scripts accordingly
  # and manually move the resulting rasters back to the appropriate directory in your local data structure.

  # This chunk provides a function to create the distance rasters locally using R and terra,
  # but it is commented out by default due to the computational demands described above.
  # the remaining code updates the predictor YAML with the paths to the pre-calculated rasters.

  # Loop over hydrological vector files
  for (i in seq_along(hydro_vect_paths)) {
    vect_path <- hydro_vect_paths[i]
    pred_name <- names(hydro_vect_paths)[i]
    message(paste("Processing", pred_name))

    out_name <- paste0(pred_name, ".tif")
    out_path <- file.path(
      config[["prepped_lyr_path"]],
      "hydrological",
      out_name
    )

    # # Skip if already exists and refresh_cache is FALSE
    # if (file.exists(out_path) & !refresh_cache) {
    #   message(paste("File", out_name, "already exists. Skipping..."))
    #   next
    # } else {
    #   message(paste("Saving to", out_path))
    # }

    # # apply distance_from_shapefile function
    # distance_from_shapefile(
    #   shapefile = vect_path,
    #   ref = config[["ref_grid_path"]],
    #   out_path = out_path,
    #   tempdir = terra_temp,
    #   datatype = "FLT8S",
    #   compress = c("DEFLATE", "ZSTD"),
    #   deflate_level = 9,
    #   predictor = 2,
    #   rasterize_field = 1,
    #   rasterize_background = NA,
    #   rasterize_all_touched = TRUE
    # )

    # message(paste(
    #   "Processed",
    #   out_name,
    #   "in",
    #   round(difftime(end_time, start_time, units = "mins"), 2),
    #   "minutes"
    # ))

    # Update YAML entry for this predictor
    entry <- hydro_pred_entries[[pred_name]]

    # remove the data_dir from the output path
    rel_path <- out_path |>
      fs::path_rel(config[["data_basepath"]])

    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = pred_name,
      clean_name = pred_name,
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
      grouping = "hydrological",
      description = if (!is.null(entry$description)) {
        entry$description
      } else {
        paste("Hydrological predictor:", pred_name)
      },
      method = if (!is.null(entry$method)) {
        entry$method
      } else {
        "Calculated as euclidean distance from hydrological feature."
      },
      date = Sys.Date(),
      author = "Your Name",
      sources = if (!is.null(entry$sources)) {
        entry$sources
      } else {
        NULL
      },
      raw_dir = if (!is.null(entry$raw_dir)) {
        entry$raw_dir
      } else {
        "hydrological"
      },
      raw_filename = if (!is.null(entry$raw_filename)) {
        entry$raw_filename
      } else {
        NULL
      }
    )
  }

  message("Hydrological predictor data preparation complete.")
}

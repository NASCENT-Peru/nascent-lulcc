#' Nhood_data_prep: Neighbourhood effect predictor layer preparation
#'
#' When devising neighborhood effect predictors there are two considerations:
#' 1.The size of the neighborhood (no. of cells: n)
#' 2.The decay rate from the central value outwards and to a lesser extent the choice of
#' method for interpolating the decay rate values.
#'
#' This script will follow the process for automatic rule detection procedure (ARD)
#' devised by Roodposhti et al. (2020) to test various permutations of these values; for
#' more details refer to:
#' http://www.spatialproblems.com/wp-content/uploads/2019/09/ARD.html
#'
#' @author Ben Black
#' @param config list of configuration parameters
#' @param redo_random_matrices logical indicating whether to re-generate the random matrices

nhood_predictor_prep <- function(
  config = get_config(),
  redo_random_matrices = FALSE,
  refresh_cache = FALSE,
  terra_temp = tempdir()
) {
  # A - Preparation ####
  # vector years of LULC data
  LULC_years <- gsub(
    ".*?([0-9]+).*",
    "\\1",
    list.files(
      config[["aggregated_lulc_dir"]],
      full.names = FALSE,
      pattern = ".tif"
    )
  )
  LULC_years <- sort(unique(as.numeric(LULC_years)))

  # create a list of the data/modelling periods
  LULC_change_periods <- c()
  for (i in 1:(length(LULC_years) - 1)) {
    LULC_change_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
  }
  names(LULC_change_periods) <- sapply(
    LULC_change_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  # character string for data period
  data_periods <- names(LULC_change_periods)

  # create folders required
  nhood_folder_names <- c(
    file.path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating"
    ),
    file.path(config[["preds_tools_dir"]], "neighbourhood_matrices"),
    file.path(config[["prepped_lyr_path"]], "neighbourhood")
  )

  purrr::walk(nhood_folder_names, ensure_dir)

  # B - Generate the desired number of focal windows for neighbourhood effect ####
  all_matrices_path <- file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_matrices",
    "all_matrices.rds"
  )
  if (redo_random_matrices || !fs::file_exists(all_matrices_path)) {
    # FIXME this looks like you could simply set the seed to be reproducible?
    # ONLY REPEAT THIS SECTION OF CODE IF YOU WISH TO REPLACE THE EXISTING RANDOM
    # MATRICES WHICH ARE CARRIED FORWARD IN THE TRANSITION MODELLING set the number of
    # neighbourhood windows to be tested and the maximum sizes of moving windows Specify
    # sizes of matrices to be used as focal windows (each value corresponds to row and
    # col size)
    matrix_sizes <- c(11, 9, 7, 5, 3) # (11x11; 9x9; 7x7; 5x5; 3x3)

    # Specify how many random decay rate matrices should be created for each size
    nw <- 5 # How many random matrices to create for each matrix size below

    # Create matrices
    All_matrices <- lapply(matrix_sizes, function(matrix_dim) {
      matrix_list_single_size <- random_pythagorean_matrix(
        n = nw,
        x = matrix_dim,
        interpolation = "smooth",
        search = "random"
      )
      names(matrix_list_single_size) <- c(paste0(
        "n",
        matrix_dim,
        "_",
        seq(1:nw)
      ))
      return(matrix_list_single_size)
    })

    # add top-level item names to list
    names(All_matrices) <- c(paste0("n", matrix_sizes, "_matrices"))

    # writing it to a file
    saveRDS(All_matrices, all_matrices_path)
  }

  ### =========================================================================
  # C- Applying the sets of random matrices to create focal window layers for each
  # active LULC type
  ### =========================================================================
  # Load back in the matrices
  All_matrices <- unlist(readRDS(all_matrices_path), recursive = FALSE)

  # adjust names
  names(All_matrices) <- sapply(names(All_matrices), function(x) {
    stringr::str_split(x, "[.]")[[1]][2]
  })

  # Load rasters of LULC data for historic periods (adjust list as necessary)
  # FIXME until here, LULC_years is a vector, from here on out it's a list
  LULC_years <- lapply(
    stringr::str_extract_all(
      stringr::str_replace_all(data_periods, "_", " "),
      "\\d+"
    ),
    function(x) x[[1]]
  )
  names(LULC_years) <- paste0("LULC_", LULC_years)

  LULC_rasters <- lapply(LULC_years, function(x) {
    LULC_pattern <- glob2rx(paste0("*", x, "*tif")) # generate regex
    terra::rast(
      list.files(
        config[["aggregated_lulc_dir"]],
        full.names = TRUE,
        pattern = LULC_pattern
      )
    )
  })

  # load the LULC scheme, disable simplification so nested lists remain lists
  scheme <- jsonlite::fromJSON(
    config[["LULC_aggregation_path"]],
    simplifyVector = FALSE
  )

  # get the values of class_name where nhood_class == FALSE
  Active_classes <- lapply(scheme, function(x) {
    if (isTRUE(x$nhood_class)) {
      return(x$class_name)
    } else {
      return(NA)
    }
  })
  Active_classes <- Active_classes[!is.na(Active_classes)]

  # name the active classes with their pixel values from the value field
  names(Active_classes) <- lapply(
    Active_classes,
    function(x) {
      scheme[[which(sapply(scheme, function(y) y$class_name == x))]]$value
    }
  )

  # create folder path to save neighbourhood rasters
  Nhood_folder_path <- file.path(
    config[["prepped_lyr_path"]],
    "neighbourhood"
  )
  ensure_dir(Nhood_folder_path)

  # mapply function over the LULC rasters and Data period names
  # saves rasters to file and return list of focal layer names
  # mapply(
  #   generate_nhood_rasters,
  #   lulc_raster = LULC_rasters,
  #   data_period = data_periods,
  #   MoreArgs = list(
  #     neighbourhood_matrices = All_matrices,
  #     active_lulc_classes = Active_classes,
  #     nhood_folder_path = Nhood_folder_path
  #   )
  # )

  # parrallel alternative
  # mapply(
  #   generate_nhood_rasters_parallel,
  #   lulc_raster = LULC_rasters,
  #   data_period = data_periods,
  #   MoreArgs = list(
  #     neighbourhood_matrices = All_matrices,
  #     active_lulc_classes = Active_classes,
  #     nhood_folder_path = Nhood_folder_path,
  #     ncores = parallel::detectCores() - 4,
  #     refresh = refresh_cache,
  #     temp_dir = terra_temp
  #   )
  # )

  # D - Manage file names/details for neighbourhood layers ####

  # get names of all neighbourhood layers from files
  new_nhood_names <- list.files(
    path = Nhood_folder_path,
    pattern = ".tif",
    full.names = TRUE
  )

  # split by period
  new_names_by_period <- lapply(data_periods, function(x) {
    grep(x, new_nhood_names, value = TRUE)
  })
  names(new_names_by_period) <- data_periods

  # function to do numerical re-ordering
  numerical.reorder <- function(period_names) {
    split_to_identifier <- sapply(strsplit(period_names, "nhood_n"), "[[", 2)
    split_nsize <- as.numeric(sapply(
      strsplit(split_to_identifier, "_"),
      "[[",
      1
    ))
    period_names[order(split_nsize, decreasing = TRUE)]
  }

  # use grepl over each period names to extract names by LULC class and then re-order
  new_names_period_LULC <- lapply(new_names_by_period, function(x) {
    LULC_class_names <- lapply(Active_classes, function(LULC_class_name) {
      grep(LULC_class_name, x, value = TRUE)
    })

    LULC_class_names_reordered <- lapply(LULC_class_names, numerical.reorder)

    return(Reduce(c, LULC_class_names_reordered))
  })

  # reduce nested list to a single vector of layer names
  layer_names <-
    Reduce(c, new_names_period_LULC) |>
    fs::path_rel(config[["data_basepath"]])

  # regex strings of period and active class names and matrix_IDs
  period_names_regex <- stringr::str_c(data_periods, collapse = "|")
  class_names_regex <- stringr::str_c(Active_classes, collapse = "|")
  matrix_id_regex <- stringr::str_c(names(All_matrices), collapse = "|")

  # create data.frame to store details of neighbourhood layers to be used when layers
  # are creating during simulations
  Focal_details <- setNames(
    data.frame(
      matrix(ncol = 4, nrow = length(layer_names))
    ),
    c("layer_name", "period", "active_lulc", "matrix_id")
  )
  Focal_details$path <- layer_names
  Focal_details$layer_name <- stringr::str_remove_all(
    stringr::str_remove_all(layer_names, paste0(Nhood_folder_path, "/")),
    ".tif"
  )
  Focal_details$period <- stringr::str_extract(
    Focal_details$layer_name,
    period_names_regex
  )
  Focal_details$active_lulc <- stringr::str_extract(
    Focal_details$layer_name,
    class_names_regex
  )
  Focal_details$matrix_id <- stringr::str_extract(
    Focal_details$layer_name,
    matrix_id_regex
  )

  # save dataframe to use as a look up table in dynamic focal layer creation.
  saveRDS(
    Focal_details,
    file.path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating",
      "focal_layer_lookup.rds"
    )
  )

  # E - Updating predictor table with layer names- updated xl. ####

  Focal_details <- readRDS(
    file.path(
      config[["preds_tools_dir"]],
      "neighbourhood_details_for_dynamic_updating",
      "focal_layer_lookup.rds"
    )
  )

  # Add additional columns to Focal details
  Focal_details$pred_name <- paste0(
    Focal_details$active_lulc,
    "_nhood_",
    Focal_details$matrix_id
  )
  Focal_details$pred_category <- "Neighbourhood"
  Focal_details$Predictor_category <- "Neighbourhood"
  Focal_details$clean_name <- mapply(
    function(LULC_class, MID) {
      width <- stringr::str_remove(stringr::str_split(MID, "_")[[1]][1], "n")
      version <- stringr::str_split(MID, "_")[[1]][2]
      paste0(
        LULC_class,
        " Neighbourhood effect matrix (size: ",
        width,
        "x",
        width,
        "cells; random central value and decay rate version:",
        version
      )
    },
    LULC_class = Focal_details$active_lulc,
    MID = Focal_details$matrix_id,
    SIMPLIFY = TRUE
  )
  Focal_details$static_or_dynamic <- "Dynamic"
  Focal_details$Prepared <- "Y"
  Focal_details$Original_resolution <- "100m"
  Focal_details$Temporal_coverage <- sapply(
    Focal_details$period,
    function(x) stringr::str_split(x, "_")[[1]][1]
  )

  Focal_details$Data_citation <- NA
  Focal_details$URL <- NA
  Focal_details$period <- Focal_details$period
  Focal_details$Raw_data_path <- NA
  Focal_details$scenario_variant <- NA

  # Loop over each row in Focal_details
  apply(Focal_details, 1, function(row) {
    # Construct clean_name following your previous logic
    width <- stringr::str_remove(
      stringr::str_split(row["matrix_id"], "_")[[1]][1],
      "n"
    )
    version <- stringr::str_split(row["matrix_id"], "_")[[1]][2]
    clean_name <- paste0(
      row["active_lulc"],
      " Neighbourhood effect matrix (size: ",
      width,
      "x",
      width,
      " cells; random central value and decay rate version: ",
      version,
      ")"
    )

    # Call the generic YAML update function
    update_predictor_yaml(
      yaml_file = config[["pred_table_path"]],
      pred_name = row["pred_name"],
      base_name = row["pred_name"],
      clean_name = clean_name,
      pred_category = "=neighbourhood",
      static_or_dynamic = "dynamic",
      metadata = NULL,
      scenario_variant = row["scenario_variant"],
      period = row["Temporal_coverage"], # from your previous column
      path = row["path"],
      grouping = "neighbourhood",
      description = clean_name, # you can customize description
      method = "Generated using random decay rate matrices applied to historical LULC data.",
      date = Sys.Date(),
      author = "Your Name",
      sources = NULL,
      raw_dir = NULL,
      raw_filename = NULL
    )
  })
  message("Updated predictor YAML with neighbourhood layers \n")

  message(" Preparation of Neighbourhood predictor layers complete \n")
}

#' function to set up a random pythagorean matrix generating the central values
#' of each matrix x0,0 and their corresponding decay rates
#' decay rates are positive, pseudorandom number from the uniform distribution
#' in the range 0>??>1. For a randomised search, ??  may be a pseudorandom number
#' from the uniform distribution, while for a grid search ??  may be any number
#' from a user-defined set
#' function to plot all generated random pythagorean matrices for checking,
#' with values of each cell labelled in the center
#' @param n the number of matrices to generate for each size
#' @param x the size of the matrix (must be an odd positive number)
#' @param interpolation the method of interpolation to use for calculating the values in the matrix ("smooth" or "linear")
#' @param search the method to use for generating the decay rates ("random" or "grid"). If "random", decay rates are generated randomly from a uniform distribution. If "grid", decay rates are generated from a predefined set of values.
#' @return a list of matrices of size x by x, where each matrix is generated using the specified method for decay rates and interpolation
random_pythagorean_matrix <- function(
  n,
  x,
  interpolation = "smooth",
  search = "random"
) {
  # current usage in the codebase, use this as simplifying assumption
  stopifnot(rlang::is_scalar_integerish(x))

  choices <- c("random", "grid")
  search <- choices[pmatch(search, choices, duplicates.ok = FALSE)]
  if (search == "random") {
    seed <- runif(n, 5, 50)
    drops <- mapply(runif, 1, 1, seed)
  } else {
    seed <- rep(seq.int(5, n, 5), each = 5)
    drops <- rep(c(3, 6, 12, 24, 48), times = n / 5)
  }
  Map(get_pythagorean_matrix, x, seed, drops, interpolation)
}

#' function that shapes the actual random pythagorean matrix using
#' smooth or linear interpolation
#' the function generates a matrix where the value of each cell is determined by its
#' distance from the central cell, with values decaying according to the specified decay
#' rate and interpolation method. The distance is calculated using the Pythagorean
#' theorem, resulting in a matrix that can be used for various applications such as spatial
#' analysis or image processing, where the influence of the central cell diminishes with
#' distance.
#' @param x the size of the matrix (must be an odd positive number)
#' @param mid the central value of the matrix
#' @param drop the decay rate that determines how quickly values decrease from the center
#' @param interpolation the method of interpolation to use for calculating the values in the matrix ("smooth" or "linear")
#' @return a matrix of size x by x where each cell's value is determined by its distance from the center and the specified decay rate and interpolation method
get_pythagorean_matrix <- function(x, mid, drop, interpolation = "smooth") {
  if (x %% 2 == 0 | x < 0) {
    stop("x must be an odd positive number")
  }

  choices <- c("smooth", "linear")
  interpolation <- choices[pmatch(
    interpolation,
    choices,
    duplicates.ok = FALSE
  )]

  dists <- outer(
    abs(1:x - ceiling(x / 2)),
    abs(1:x - ceiling(x / 2)),
    function(x, y) sqrt(x^2 + y^2)
  )
  if (interpolation == "smooth") {
    mat <- (1 / drop)^dists * mid
  } else {
    mat <- matrix(
      approx(x = 0:x, y = 0.1^(0:x) * mid, xout = dists)$y,
      ncol = x,
      nrow = x
    )
  }
  return(mat)
}

#' helper function to plot the generated pythagorean matrices for checking, with values of each cell labelled in the center
#' @param mat the matrix to be plotted
plot_pythagorean_matrix <- function(mat) {
  colors <- colorRampPalette(c(
    "deepskyblue4",
    "deepskyblue3",
    "darkslateblue",
    "deepskyblue1",
    "lightblue1",
    "gray88"
  ))(256)
  corrplot::corrplot(
    mat,
    is.corr = FALSE,
    method = "shade",
    col = colors,
    tl.pos = "n",
    number.cex = .7,
    addCoef.col = "black"
  )
  text(row(mat), col(mat), round(mat, 2), cex = 0.7)
}

#' function to plot the outcomes of applying different decay rates on every generated
#' central cell
#' @param mat the matrix to be plotted
#' @param plot logical indicating whether to create the plot (default is TRUE)
#' @param ... additional arguments to be passed to the plot function (e.g., main, xlim, ylim)
#' @return a data frame containing the x and y values used for plotting the decay curve, where x represents the distance from the center and y represents the corresponding value based on the decay rate
plot_pythagorean_matrix_decay <- function(mat, plot = TRUE, ...) {
  mid <- ceiling(ncol(mat) / 2)
  drop <- mat[mid, mid + 1] / mat[mid, mid]
  xs <- seq(0, mid, 0.1)
  if (isTRUE(all.equal(mat[mid + 1, mid + 1], drop^sqrt(2) * mat[mid, mid]))) {
    ys <- drop^(xs) * mat[mid, mid]
    if (plot) {
      plot(
        xs,
        ys,
        type = "l",
        las = 1,
        lwd = 2,
        xlab = "distance",
        ylab = "value",
        cex.axis = 0.7,
        mgp = c(2, 0.5, 0),
        tck = -0.01,
        ...
      )
    }
  } else {
    ys <- approx(0:mid, drop^(0:mid), xout = xs)$y * mat[mid, mid]
    if (plot) {
      plot(
        xs,
        ys,
        type = "l",
        las = 1,
        lwd = 2,
        xlab = "distance",
        ylab = "value",
        cex.axis = 0.7,
        mgp = c(2, 0.5, 0),
        tck = -0.01,
        ...
      )
    }
  }
  return(data.frame(x = xs, y = ys))
}

#' Generate Neighborhood Rasters for Land Use/Land Cover Classes
#'
#' Generate Neighborhood Rasters for Land Use/Land Cover Classes (Disk-Optimized)
#'
#' Efficiently generates and saves neighborhood rasters for each specified class
#' and neighborhood configuration, processing one class at a time.
#'
#' @param lulc_raster SpatRaster containing land use/land cover classes
#' @param neighbourhood_matrices Named list of neighborhood matrices
#' @param active_lulc_classes Named character vector of class labels (names = raster values)
#' @param data_period Character string identifying the data period
#' @param nhood_folder_path Directory to save output rasters
#' @return NULL (writes rasters to disk)
#' @export

generate_nhood_rasters <- function(
  lulc_raster,
  neighbourhood_matrices,
  active_lulc_classes,
  data_period,
  nhood_folder_path
) {
  message(
    "Generating neighborhood rasters for active LULC classes: ",
    paste(active_lulc_classes, collapse = ", "),
    " in period: ",
    data_period
  )

  # Loop over each active LULC class one at a time
  for (i in seq_along(active_lulc_classes)) {
    class_value <- as.numeric(names(active_lulc_classes)[i])
    class_name <- active_lulc_classes[i]

    message("\nProcessing class: ", class_name, " (value ", class_value, ")")

    # Subset raster to current class (binary mask)
    active_class_raster <- lulc_raster == class_value

    # Optional: write to temp file (ensures it stays on disk, not in memory)
    tmpfile <- file.path(
      nhood_folder_path,
      paste0("active_class_", class_name, ".tif")
    )
    terra::writeRaster(active_class_raster, tmpfile, overwrite = TRUE)
    active_class_raster <- terra::rast(tmpfile)

    # Loop over neighborhood matrices for this class
    for (matrix_name in names(neighbourhood_matrices)) {
      single_matrix <- neighbourhood_matrices[[matrix_name]]

      focal_file_name <- paste(
        class_name,
        "nhood",
        matrix_name,
        data_period,
        sep = "_"
      )
      focal_full_path <- file.path(
        nhood_folder_path,
        paste0(focal_file_name, ".tif")
      )

      message("  Applying neighborhood matrix: ", matrix_name)

      focal_layer <- terra::focal(
        x = active_class_raster,
        w = single_matrix,
        na.policy = "omit",
        fillvalue = 0,
        expand = TRUE,
        filename = focal_full_path,
        overwrite = TRUE,
        wopt = list(
          datatype = "INT2S",
          NAflag = -32768,
          gdal = c("COMPRESS=LZW", "ZLEVEL=9")
        )
      )

      message("    → Saved focal raster: ", focal_full_path)
      rm(focal_layer)
      gc() # free memory
    }

    # Clean up temporary raster for this class
    rm(active_class_raster)
    gc()
    if (file.exists(tmpfile)) file.remove(tmpfile)
  }

  message("\nAll neighborhood rasters generated successfully.")
}


#' Generate Neighborhood Rasters for LULC Classes (Optimized Parallel)
#'
#' Parallelizes focal operations *within* each class while subsetting only once per class.
#'
#' @param lulc_raster SpatRaster (terra)
#' @param neighbourhood_matrices Named list of matrices
#' @param active_lulc_classes Named character vector of classes (names = raster values)
#' @param data_period Character label (e.g., "2020")
#' @param nhood_folder_path Folder for outputs
#' @param ncores Number of CPU cores for parallelization
#' @return NULL
#' @export

generate_nhood_rasters_parallel <- function(
  lulc_raster,
  neighbourhood_matrices,
  active_lulc_classes,
  data_period,
  nhood_folder_path,
  ncores = parallel::detectCores() - 1,
  refresh = FALSE,
  temp_dir = tempdir()
) {
  library(terra)
  library(future)
  library(future.apply)

  message(
    "Starting optimized parallel neighborhood raster generation (",
    length(active_lulc_classes),
    " classes × ",
    length(neighbourhood_matrices),
    " matrices)..."
  )

  if (!dir.exists(nhood_folder_path)) {
    dir.create(nhood_folder_path, recursive = TRUE)
  }

  # Set terra options for efficient disk use
  terraOptions(
    threads = ncores,
    tempdir = temp_dir
  )

  # Loop over classes sequentially
  for (i in seq_along(active_lulc_classes)) {
    class_value <- as.numeric(names(active_lulc_classes)[i])
    class_name <- active_lulc_classes[i]

    message("\nProcessing class: ", class_name, " (value ", class_value, ")")

    # Create binary raster ONCE (1 for class, 0 otherwise)
    active_class_raster <- classify(
      lulc_raster,
      rcl = cbind(class_value, 1),
      others = 0,
      datatype = "INT1U"
    )

    # Write to temp file to avoid keeping in memory if raster is large
    tmpfile <- file.path(temp_dir, paste0("active_class_", class_name, ".tif"))
    writeRaster(active_class_raster, tmpfile, overwrite = TRUE)

    # Prepare matrices for parallel processing
    matrix_names <- names(neighbourhood_matrices)

    plan(multisession, workers = min(ncores, length(matrix_names)))

    # Parallel focal processing across matrices
    future_lapply(
      matrix_names,
      function(matrix_name) {
        single_matrix <- neighbourhood_matrices[[matrix_name]]

        # Construct output filename
        focal_file_name <- paste(
          class_name,
          "nhood",
          matrix_name,
          data_period,
          sep = "_"
        )
        focal_full_path <- file.path(
          nhood_folder_path,
          paste0(focal_file_name, ".tif")
        )

        # Skip if already exists and refresh_cache is FALSE
        if (file.exists(focal_full_path) & !isTRUE(refresh_cache)) {
          message(
            "  → Focal raster already exists, skipping: ",
            focal_full_path
          )
          return(focal_full_path)
        } else {
          message("  → Applying matrix: ", matrix_name)

          # Reload raster from temp file
          active_class_raster <- rast(tmpfile)

          # Use focalCpp for fast numeric kernel (sum)
          terra::focal(
            x = active_class_raster,
            w = single_matrix,
            fun = "sum",
            fillvalue = 0,
            expand = TRUE,
            filename = focal_full_path,
            overwrite = TRUE,
            wopt = list(
              datatype = "INT2U",
              gdal = c("COMPRESS=LZW", "ZLEVEL=9")
            )
          )

          message("    Saved: ", focal_full_path)
          return(focal_full_path)
        }
      },
      future.seed = TRUE
    )

    plan(sequential)

    rm(active_class_raster)
    gc()
  }

  message("\n✅ All neighborhood rasters generated successfully.")
}

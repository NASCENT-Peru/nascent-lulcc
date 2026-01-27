#' lulcc.neighbourhoodlayercreation
#'
#' TODO text
#'
#' @author Ben Black (lulcc function), Majid Shadman Roodposhti (matrix functions)
# A - Instantiate small functions for neighbourhood creation ####

# function to set up a random pythagorean matrix generating the central values
# of each matrix x0,0 and their corresponding decay rates
# decay rates are positive, pseudorandom number from the uniform distribution
# in the range 0>??>1. For a randomised search, ??  may be a pseudorandom number
# from the uniform distribution, while for a grid search ??  may be any number
# from a user-defined set
# function to plot all generated random pythagorean matrices for checking,
# with values of each cell labelled in the center
#' @export
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

# function that shapes the actual random pythagorean matrix using
# smooth or linear interpolation
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

#' @export
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

# function to plot the outcomes of applying different decay rates on every generated
# central cell
#' @export
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

lulcc_generatenhoodrasters <- function(
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

lulcc_generatenhoodrasters_parallel <- function(
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

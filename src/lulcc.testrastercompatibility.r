#' Test Raster Compatibility
#'
#' Confirm Raster compatibility prior to stacking
#'
#' @param rasterlist A list of Raster objects to be compared to the exemplar raster
#' @param exemplar_raster A Raster object to be compared to the list of rasters
#'
#' @author Ben Black
#' @export

lulcc.TestRasterCompatibility <- function(rasterlist, exemplar_raster) {
  # creating an empty list to store comparison results in
  comparison_result <- list()

  # for loop that compares all rasters in the list to the exemplar and return results of
  # any discrepancies to the empty list
  for (i in seq_along(rasterlist)) {
    comparison_result[[i]] <- testthat::capture_warnings(
      terra::compareGeom(
        exemplar_raster,
        rasterlist[[i]],
        stopOnError = FALSE,
        messages = TRUE
      )
    )
  }

  any_errs <- any(purrr::map_lgl(comparison_result, \(x) length(x) > 0))

  if (any_errs) {
    print(comparison_result)
    stop(
      "Differences in Raster characteristics means they are unsuitable for stacking, ",
      "refer to object Raster_comparison_results to locate problems"
    )
  }
  return(comparison_result)
}

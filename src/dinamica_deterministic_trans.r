#' dinamica_deterministic_trans
#'
#' Implement deterministic transitions following statistical transition potential during
#' during simulation step
#'
#' @param simulated_lulc_save_path Path (char) passed in from Dinamica
#' @param simulation_id integerish
#' @param simulated_lulc_year integerish
#' @param config list of config options
#'
#' @author Ben Black
#'
#' @export

dinamica_deterministic_trans <- function(
    simulated_lulc_save_path,
    simulation_id,
    simulated_lulc_year,
    config = get_config()) {
  params <- get_simulation_params(simulation_id = simulation_id)

  if (
    params[["is_simulation"]] &&
      grepl("Y", params[["deterministic_trans.string"]], ignore.case = TRUE)
  ) {
    # Load simulated LULC map for time step
    Current_lulc <- raster::raster(simulated_lulc_save_path)

    # convert raster to dataframe
    LULC_dat <- raster::as.data.frame(Current_lulc)

    # get name of first column
    Value_col <- colnames(LULC_dat)[1]

    # add ID column to dataset
    LULC_dat$ID <- seq.int(nrow(LULC_dat))

    # Get XY coordinates of cells
    xy_coordinates <- raster::coordinates(Current_lulc)

    # cbind XY coordinates to dataframe and seperate rows where all values = NA
    LULC_dat <- cbind(LULC_dat, xy_coordinates)

    # load scenario specific glacier index
    Glacier_index <- readRDS(
      file = list.files(
        fs::path(config[["glacial_change_path"]], "scenario_indices"),
        full.names = TRUE,
        pattern = params[["climate_scenario.string"]]
      )
    )[, c("ID_loc", simulated_lulc_year)]

    # seperate vector of cell IDs for glacier and non-glacer cells
    Non_glacier_IDs <- Glacier_index[Glacier_index[[paste(simulated_lulc_year)]] == 0, "ID_loc"]
    Glacier_IDs <- Glacier_index[Glacier_index[[paste(simulated_lulc_year)]] == 1, "ID_loc"]

    # replace the 1's and 0's with the correct LULC
    LULC_dat[LULC_dat$ID %in% Non_glacier_IDs, Value_col] <- 11
    LULC_dat[LULC_dat$ID %in% Glacier_IDs, Value_col] <- 19

    # convert back to raster
    Updated_raster <- raster::rasterFromXYZ(LULC_dat[, c("x", "y", Value_col)])

    # save updated LULC raster
    raster::writeRaster(
      Updated_raster,
      simulated_lulc_save_path,
      overwrite = TRUE,
      datatype = "INT1U"
    )
  }
}

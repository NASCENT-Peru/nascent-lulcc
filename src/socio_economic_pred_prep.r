socio_economic_pred_prep <- function(
  config = get_config(),
  refresh_cache = FALSE,
  terra_temp = tempdir()
) {
  ensure_dir(config[["predictors_intermediate_dir"]])

  # load the pred_table yaml file
  pred_yaml_file <- config[["pred_table_path"]]
  pred_table <- yaml::yaml.load_file(pred_yaml_file)

  # load the ancillary data table
  ancillary_yaml_path <- config[["ancillary_data_table"]]
  ancillary_table <- yaml::yaml.load_file(ancillary_yaml_path)

  # load the ref_grid
  ref_raster <- terra::rast(config[["ref_grid_path"]])

  # subset to soil predictor entries grouping is socioeconomic
  socio_economic_preds <- Filter(
    function(x) !is.null(x$grouping) && x$grouping == "socioeconomic",
    pred_table
  )

  # all variables require seperate logic for processing so no batch operations
  # gross added value ------------------------------------------------------

  message("Starting Gross Added Value processing...")
  gav_pred <- socio_economic_preds$gross_added_value

  # print the method for reference
  print(gav_pred$method)

  # load the shapefiles into a list
  gav_vects <- sapply(gav_pred$raw_filename, function(x) {
    terra::vect(file.path(config[["predictors_raw_dir"]], gav_pred$raw_dir, x))
  })

  # combine all the vects into one
  gav_combined <- terra::vect(gav_vects)

  # reproject to the same CRS as the ref raster
  gav_combined <- terra::project(gav_combined, terra::crs(ref_raster))

  # make sure extent matches ref raster
  gav_combined <- terra::crop(gav_combined, terra::ext(ref_raster))

  # load the district shapefile
  districts_vect <- terra::vect(file.path(
    config[["data_basepath"]],
    ancillary_table$districts$path
  ))

  # calculate the sum of vab_total by district
  districts_sum <- terra::zonal(
    gav_combined["vab_total"],
    z = districts_vect,
    fun = "sum",
    as.polygons = TRUE
  )

  # rasterize the vector to the ref grid
  gav_rast <- terra::rasterize(districts_sum, ref_raster, field = "vab_total")

  # mask to the ref raster
  gav_rast <- terra::mask(gav_rast, ref_raster)

  # save the raster
  gav_outpath <- file.path(
    config[["prepped_lyr_path"]],
    gav_pred$grouping,
    "gross_added_value.tif"
  )
  write_raster(gav_rast, filename = gav_outpath)

  # update the predictor yaml
  update_predictor_yaml(
    yaml_file = pred_yaml_file,
    pred_name = "gross_added_value",
    clean_name = "Gross Added Value",
    pred_category = gav_pred$pred_category,
    static_or_dynamic = gav_pred$static_or_dynamic,
    period = gav_pred$period,
    path = fs::path_rel(gav_outpath, config[["data_basepath"]]),
    method = gav_pred$method,
    grouping = gav_pred$grouping,
    description = gav_pred$description,
    sources = gav_pred$sources,
    author = gav_pred$author,
    date = Sys.Date(),
    raw_dir = gav_pred$raw_dir,
    raw_filename = gav_pred$raw_filename
  )

  message("Gross Added Value processing complete.")

  # population density -----------------------------------------------------

  message("Starting Population Growth Density processing...")

  pop_pred <- socio_economic_preds$pop_growth_density

  # print the method for reference
  print(pop_pred$method)

  # load the shapefile
  pop_vect <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    pop_pred$raw_dir,
    pop_pred$raw_filename
  ))

  # reproject to the same CRS as the ref raster
  pop_vect <- terra::project(pop_vect, terra::crs(ref_raster))

  # make sure extent matches ref raster
  pop_vect <- terra::crop(pop_vect, terra::ext(ref_raster))

  # get unique values of field 'descrip'
  unique_descrip <- unique(pop_vect$descrip)

  # add a new field 'pop_growth_cat' to hold the population growth categories
  pop_vect$pop_growth_cat <- NA

  # assign categories based on 'descrip' values
  for (i in seq_along(unique_descrip)) {
    descrip_value <- unique_descrip[i]
    pop_vect$pop_growth_cat[pop_vect$descrip == descrip_value] <- i
  }

  # rasterize the vector to the ref grid
  pop_rast <- terra::rasterize(
    pop_vect,
    ref_raster,
    field = "pop_growth_cat"
  )

  # mask to the ref raster
  pop_rast <- terra::mask(pop_rast, ref_raster)

  # output path
  pop_outpath <- file.path(
    config[["prepped_lyr_path"]],
    pop_pred$grouping,
    "pop_growth_density.tif"
  )

  # save the raster
  write_raster(pop_rast, filename = pop_outpath)

  # update description with category info
  pop_description <- "shapefile rasterized with numeric values from 1 to 8 representing population growth density categories: 
    1 = rango > -5.0
    2 = rango -5.0 to -2.5
    3 = rango -2.5 to 0.1
    4 = rango 0 to 0.5
    5 = rango 0.5 to 1.5
    6 = rango 1.5 to 3.0
    7 = rango 3.0 to 6.0
    8 = rango >6.0"

  # update the predictor yaml
  update_predictor_yaml(
    yaml_file = pred_yaml_file,
    pred_name = "pop_growth_density",
    clean_name = "Population Growth Density",
    pred_category = pop_pred$pred_category,
    static_or_dynamic = pop_pred$static_or_dynamic,
    period = pop_pred$period,
    path = fs::path_rel(pop_outpath, config[["data_basepath"]]),
    method = pop_pred$method,
    grouping = pop_pred$grouping,
    description = pop_description,
    sources = pop_pred$sources,
    author = pop_pred$author,
    date = Sys.Date(),
    raw_dir = pop_pred$raw_dir,
    raw_filename = pop_pred$raw_filename
  )

  message("Population Growth Density processing complete.")

  # market density ---------------------------------------------------------
  message("Starting Market Density processing...")

  market_pred <- socio_economic_preds$market_density
  print(market_pred$method)

  # load the shapefile
  market_vect <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    market_pred$raw_dir,
    market_pred$raw_filename
  ))

  # reproject to the same CRS as the ref raster
  market_vect <- terra::project(market_vect, terra::crs(ref_raster))
  # make sure extent matches ref raster
  market_vect <- terra::crop(market_vect, terra::ext(ref_raster))

  # get unique values of field 'nivel'
  unique_descrip <- unique(market_vect$rango)
  print(unique_descrip)

  # The field values are of increasing size, so they can be grouped accordingly
  market_groupings <- list(
    low_market_density_region = unique_descrip[1:3],
    high_market_density_region = unique_descrip[4:5]
  )

  # loop over the groupings, creating an intermediate grouped vector, saving, and adding to yaml
  lapply(names(market_groupings), function(group_name) {
    message(paste("Processing market group:", group_name))
    group_values <- market_groupings[[group_name]]
    group_vect <- market_vect[market_vect$rango %in% group_values, ]

    # define intermediate path
    intermediate_path <- file.path(
      config[["predictors_intermediate_dir"]],
      paste0(group_name, ".shp")
    )

    # final output path
    outpath <- file.path(
      config[["prepped_lyr_path"]],
      market_pred$grouping,
      paste0("dist_to_", group_name, ".tif")
    )

    # dissolve the polygons within the group
    message("Dissolving polygons and save")
    group_vect_agg <- terra::aggregate(
      group_vect
    )

    # save the dissolved shapefile
    terra::writeVector(
      group_vect_agg,
      intermediate_path,
      filetype = "ESRI Shapefile",
      overwrite = TRUE
    )

    # update the predictor yaml
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = paste0("dist_to_", group_name),
      clean_name = paste("Distance to ", gsub("_", " ", group_name)),
      pred_category = market_pred$pred_category,
      static_or_dynamic = market_pred$static_or_dynamic,
      period = market_pred$period,
      intermediate_path = fs::path_rel(
        intermediate_path,
        config[["data_basepath"]]
      ),
      path = fs::path_rel(outpath, config[["data_basepath"]]),
      method = paste(market_pred$method, "-", group_name),
      grouping = market_pred$grouping,
      description = paste(market_pred$description, "-", group_name),
      sources = market_pred$sources,
      author = market_pred$author,
      date = Sys.Date(),
      raw_dir = market_pred$raw_dir,
      raw_filename = market_pred$raw_filename
    )
  })

  message("Market Density processing complete.")

  # commercial density -----------------------------------------------------
  message("Starting Commercial Density processing...")

  commercial_pred <- socio_economic_preds$commercial_density
  print(commercial_pred$method)

  # load the shapefile
  commercial_vect <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    commercial_pred$raw_dir,
    commercial_pred$raw_filename
  ))

  # reproject to the same CRS as the ref raster
  commercial_vect <- terra::project(commercial_vect, terra::crs(ref_raster))
  # make sure extent matches ref raster
  commercial_vect <- terra::crop(commercial_vect, terra::ext(ref_raster))

  # get unique values of field 'rango'
  unique_descrip <- unique(commercial_vect$rango)

  # The field values are of increasing size, so they can be grouped accordingly
  commercial_groupings <- list(
    low_density_commercial_regions = unique_descrip[1:3],
    high_density_commercial_regions = unique_descrip[4:5]
  )

  # loop over the groupings, creating an intermediate grouped vector, saving, and adding to yaml
  lapply(names(commercial_groupings), function(group_name) {
    message(paste("Processing commercial group:", group_name))
    group_values <- commercial_groupings[[group_name]]
    group_vect <- commercial_vect[commercial_vect$rango %in% group_values, ]

    # define intermediate path
    intermediate_path <- file.path(
      config[["predictors_intermediate_dir"]],
      paste0(group_name, ".shp")
    )

    # final output path
    outpath <- file.path(
      config[["prepped_lyr_path"]],
      commercial_pred$grouping,
      paste0("dist_to_", group_name, ".tif")
    )

    # dissolve the polygons within the group
    message("Dissolving polygons and save")
    group_vect_agg <- terra::aggregate(
      group_vect
    )

    # save the dissolved shapefile
    terra::writeVector(
      group_vect_agg,
      intermediate_path,
      filetype = "ESRI Shapefile",
      overwrite = TRUE
    )

    # update the predictor yaml
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = paste0("dist_to_", group_name),
      clean_name = paste("Distance to ", gsub("_", " ", group_name)),
      pred_category = commercial_pred$pred_category,
      static_or_dynamic = commercial_pred$static_or_dynamic,
      period = commercial_pred$period,
      intermediate_path = fs::path_rel(
        intermediate_path,
        config[["data_basepath"]]
      ),
      path = fs::path_rel(outpath, config[["data_basepath"]]),
      method = paste(commercial_pred$method, "-", group_name),
      grouping = commercial_pred$grouping,
      description = paste(commercial_pred$description, "-", group_name),
      sources = commercial_pred$sources,
      author = commercial_pred$author,
      date = Sys.Date(),
      raw_dir = commercial_pred$raw_dir,
      raw_filename = commercial_pred$raw_filename
    )
  })

  message("Commercial Density processing complete.")

  # financial services density ---------------------------------------------

  message("Starting Financial Services Density processing...")

  financial_pred <- socio_economic_preds$financial_services_density
  print(financial_pred$method)

  # load the shapefile
  financial_vect <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    financial_pred$raw_dir,
    financial_pred$raw_filename
  ))

  # reproject to the same CRS as the ref raster
  financial_vect <- terra::project(financial_vect, terra::crs(ref_raster))
  # make sure extent matches ref raster
  financial_vect <- terra::crop(financial_vect, terra::ext(ref_raster))

  # final output path
  outpath <- file.path(
    config[["prepped_lyr_path"]],
    financial_pred$grouping,
    "dist_to_financial_services.tif"
  )

  # get unique values of field 'rango'
  unique_descrip <- unique(financial_vect$rango)

  # establish groups
  financial_groupings <- list(
    low_density_financial_services_region = unique_descrip[1:3],
    high_density_financial_services_region = unique_descrip[4:5]
  )

  lapply(names(financial_groupings), function(group_name) {
    message(paste("Processing financial services group:", group_name))
    group_values <- financial_groupings[[group_name]]
    group_vect <- financial_vect[financial_vect$rango %in% group_values, ]

    # intermediate path
    intermediate_path <- file.path(
      config[["predictors_intermediate_dir"]],
      paste0(group_name, ".shp")
    )

    # final output path
    outpath <- file.path(
      config[["prepped_lyr_path"]],
      financial_pred$grouping,
      paste0("dist_to_", group_name, ".tif")
    )

    # dissolve the polygons within the group
    message("Dissolving polygons and save")
    group_vect_agg <- terra::aggregate(
      group_vect
    )

    # save the dissolved shapefile
    terra::writeVector(
      group_vect_agg,
      intermediate_path,
      filetype = "ESRI Shapefile",
      overwrite = TRUE
    )

    # update the predictor yaml
    update_predictor_yaml(
      yaml_file = pred_yaml_file,
      pred_name = paste0("dist_to_", group_name),
      clean_name = paste("Distance to ", gsub("_", " ", group_name)),
      pred_category = financial_pred$pred_category,
      static_or_dynamic = financial_pred$static_or_dynamic,
      period = financial_pred$period,
      intermediate_path = fs::path_rel(
        intermediate_path,
        config[["data_basepath"]]
      ),
      path = fs::path_rel(outpath, config[["data_basepath"]]),
      method = paste(financial_pred$method, "-", group_name),
      grouping = financial_pred$grouping,
      description = paste(financial_pred$description, "-", group_name),
      sources = financial_pred$sources,
      author = financial_pred$author,
      date = Sys.Date(),
      raw_dir = financial_pred$raw_dir,
      raw_filename = financial_pred$raw_filename
    )
  })

  message("Financial Services Density processing complete.")

  # travel time to departmental captial ------------------------------------
  message("Starting Travel Time to Departmental Capital processing...")

  travel_time_pred <- socio_economic_preds$trav_time_to_dept_cap
  print(travel_time_pred$method)

  # load the shapefile
  travel_time_vect <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    travel_time_pred$raw_dir,
    travel_time_pred$raw_filename
  ))

  # reproject to the same CRS as the ref raster
  travel_time_vect <- terra::project(travel_time_vect, terra::crs(ref_raster))
  # make sure extent matches ref raster
  travel_time_vect <- terra::crop(travel_time_vect, terra::ext(ref_raster))

  # get unique values of field 'cat_tiempo'
  distance_cats <- unique(travel_time_vect$cat_tiempo)

  # remove NA category if present
  distance_cats <- distance_cats[!is.na(distance_cats)]

  # reverse order to sort low to high
  distance_cats <- rev.default(distance_cats)

  # create a new field 'trav_time' to hold the numeric travel time values
  travel_time_vect$trav_time <- NA
  # assign numeric travel time values based on categories
  for (i in seq_along(distance_cats)) {
    cat <- distance_cats[i]
    travel_time_vect$trav_time[travel_time_vect$cat_tiempo == cat] <- i
  }

  # final output path
  outpath <- file.path(
    config[["prepped_lyr_path"]],
    travel_time_pred$grouping,
    "trav_time_to_dept_cap.tif"
  )

  # rasterize the vector to the ref grid
  travel_time_rast <- terra::rasterize(
    travel_time_vect,
    ref_raster,
    field = "trav_time"
  )

  # mask to the ref raster
  travel_time_rast <- terra::mask(travel_time_rast, ref_raster)

  # save the raster
  write_raster(travel_time_rast, filename = outpath)

  # update method with category info
  travel_time_method <- "Shapefile converted to raster with the following values for categories (in minutes):
     1 = 0 - 15 min
    2 = 15 - 30 min
    3 = 30 - 60 min
    4 = 60 - 90 min
    5 = 90 - 120 min
    6 = 120 - 180 min
    7 = >180 min"

  # update the predictor yaml
  update_predictor_yaml(
    yaml_file = pred_yaml_file,
    pred_name = "trav_time_to_dept_cap",
    clean_name = "Travel Time to Departmental Capital",
    pred_category = travel_time_pred$pred_category,
    static_or_dynamic = travel_time_pred$static_or_dynamic,
    period = travel_time_pred$period,
    path = fs::path_rel(outpath, config[["data_basepath"]]),
    method = travel_time_method,
    grouping = travel_time_pred$grouping,
    description = travel_time_pred$description,
    sources = travel_time_pred$sources,
    author = travel_time_pred$author,
    date = Sys.Date(),
    raw_dir = travel_time_pred$raw_dir,
    raw_filename = travel_time_pred$raw_filename
  )

  message("Travel Time to Departmental Capital processing complete.")
}

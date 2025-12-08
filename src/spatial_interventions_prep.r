#' Spatial_prob_perturb
#'
#' Prepare spatial layers for perturbation of cellular transition probabilities in
#' simulation steps
#'
#' @param config list of config options
#'
#' @author Ben Black
#' @export

spatial_interventions_prep <- function(config = get_config()) {
  Ref_grid <- raster::raster(config[["ref_grid_path"]])
  Ref_crs <- raster::crs(Ref_grid)

  # load table of scenario specific spatial interventions
  Interventions <- readr::read_csv2(config[["spat_ints_path"]])

  # convert time_step and target_classes columns back to character vectors
  Interventions$time_step <- sapply(
    Interventions$time_step,
    function(x) {
      x <- stringr::str_remove_all(x, " ")
      rep <- unlist(strsplit(x, ","))
      rep
    },
    simplify = FALSE
  )

  Interventions$target_classes <- sapply(
    Interventions$target_classes,
    function(x) {
      x <- stringr::str_remove_all(x, " ")
      rep <- unlist(strsplit(x, ","))
      rep
    },
    simplify = FALSE
  )

  ### =========================================================================
  ### A- Prepare building zone data
  ### =========================================================================

  # from https://www.kgk-cgc.ch/geodaten/geodaten-bauzonen-schweiz
  bz_dir <- fs::path(config[["spat_prob_perturb_path"]], "building_zones")
  bauzonen_gpkg <- fs::path(
    bz_dir, "ch.are.bauzonen.gpkg"
  )
  if (!fs::file_exists(fs::path(bauzonen_gpkg))) {
    lulcc.downloadunzip(
      url = "https://www.kgk-cgc.ch/download_file/1018/239.zip",
      save_dir = bz_dir,
      filename = "geodaten-bauzonen-schweiz.zip"
    )
  }

  # load shapefile from geopackage
  shp_file <- terra::vect(bauzonen_gpkg)

  # re-project to research CRS
  shp_file <- terra::project(shp_file, Ref_crs)

  # swapped raster::rasterize with terra::rasterize because it's so much faster
  BZ_rast <- raster::raster(terra::rasterize(
    x = shp_file,
    y = terra::rast(Ref_grid),
    field = "CH_CODE_HN"
  ))

  # create a raster attribute table (RAT)
  BZ_rast <- raster::ratify(BZ_rast)
  BZ_rat <- raster::levels(BZ_rast)[[1]]

  # get the german zone names
  BZ_IDs <- unique(shp_file$CH_BEZ_D)

  # add name column to RAT
  # WARNING uncertain if unique() is always in the same order as the RAT ID col
  BZ_rat$Class_Names <- stringr::str_replace_all(BZ_IDs, " ", "_")

  # overwrite names in english
  BZ_rat$Class_Names <- c(
    "Residential zones",
    "Mixed zones",
    "Zones_for_public_uses",
    "Restricted_building_zones",
    "Work_zones",
    "Centre_zones",
    "Other_Building_Zones",
    "Tourism_and_Recreation_Zones",
    "Traffic_zones"
  )

  # add RAT to raster object
  # opposed to levels(), levels<-() dispatches correctly to raster::levels<-
  levels(BZ_rast) <- BZ_rat

  BZ_reclass_mat <- BZ_rat
  BZ_reclass_mat$Class_Names <- 1

  # convert raster to binary values (0 or 1 and NA)
  BZ_reclass <- raster::reclassify(BZ_rast, rcl = BZ_reclass_mat)

  # saving the raster in R's native .grd format which preserves the attribute table
  raster::writeRaster(
    BZ_rast,
    filename = fs::path(bz_dir, "bz_raster_all_classes.grd"),
    overwrite = TRUE
  )
  raster::writeRaster(
    BZ_reclass,
    filename = fs::path(bz_dir, "bz_raster.grd"),
    overwrite = TRUE
  )

  # load in land use raster to mask distance raster
  bz_mask <- terra::rast(fs::path(config[["historic_lulc_basepath"]], "lulc_2018_agg.grd"))

  # create a distance to building zones raster
  BZ_distance <-
    BZ_rast |>
    terra::rast() |>
    terra::mask(bz_mask, updatevalue = -999) |>
    terra::distance(exclude = -999)

  # save
  terra::writeRaster(
    BZ_distance,
    filename = fs::path(bz_dir, "bz_distance.tif"),
    overwrite = TRUE
  )

  ### =========================================================================
  ### B- Typology of municipalities
  ### =========================================================================

  Muni_shp <- terra::vect(
    fs::path(config[["ch_geoms_path"]], "swissboundaries3d_1_5_tlm_hoheitsgebiet.shp")
  )

  # filter out non-swiss municipalities
  Muni_shp <- Muni_shp[
    Muni_shp$ICC == "CH" & Muni_shp$OBJEKTART == "Gemeindegebiet",
  ]


  # Import data of typology of municipalities from FSO web service using condition:
  # 1. Municipality designations as of 01/05/2022 (to match mutations)

  # scrape content from html address
  Muni_type_content <- rvest::read_html(
    "https://www.agvchapp.bfs.admin.ch/de/typologies/results?SnapshotDate=01.05.2022&SelectedTypologies%5B0%5D=HR_GDETYP2012"
  )
  muni_typology <- as.data.frame(rvest::html_table(
    Muni_type_content,
    fill = TRUE
  )[[1]][-1, ]) # remove duplicate row names

  # remove columns 7 and 8 which specify the other typologies (3 and 9 categories)
  # and rename remaining col
  muni_typology <- muni_typology[, -c(7, 8)]
  colnames(muni_typology)[[7]] <- "muni_type_ID"
  muni_typology$`BFS-Gde Nummer` <- as.numeric(muni_typology$`BFS-Gde Nummer`)

  # create manual legend (data documentation only specifies categories in German/French)
  Muni_typ_legend <- tibble::tribble(
    ~ID, ~type,
    111, "City-center_large_agglomeration",
    112, "Urban_employment_municipality_large_agglomeration",
    113, "Residential_urban_municipality_large_agglomeration",
    121, "City-centre_medium_agglomeration",
    122, "Urban_employment_municipality_medium_agglomeration",
    123, "Residential_urban_municipality_medium_agglomeration",
    134, "Urban_tourist_municipality_of_a_small_agglomeration",
    136, "Industrial_urban_municipality_of_a_small_agglomeration",
    137, "Tertiary_urban_municipality_of_a_small_agglomeration",
    216, "High-density_industrial_peri-urban_municipality",
    217, "High-density_tertiary_peri-urban_municipality",
    226, "Mid-density_industrial_peri-urban_municipality",
    227, "Medium-density_tertiary_peri-urban_municipality",
    235, "Low-density_agricultural_peri-urban_municipality",
    236, "Low-density_industrial_peri-urban_municipality",
    237, "Low_density_tertiary_peri-urban_municipality",
    314, "Tourist_town_of_a_rural_center",
    316, "Industrial_municipality_of_a_rural_center",
    317, "Tertiary_municipality_of_a_rural_center",
    325, "Rural_agricultural_municipality_in_a_central_location",
    326, "Rural_industrial_municipality_in_a_central_location",
    327, "Tertiary_rural_municipality_in_a_central_location",
    334, "Peripheral_rural_tourist_municipality",
    335, "Peripheral_agricultural_rural_municipality",
    338, "Peripheral_mixed_rural_municipality"
  )

  # add municipality type to data
  muni_typology$muni_type <- sapply(
    as.numeric(muni_typology$muni_type_ID),
    function(x) {
      Muni_typ_legend[Muni_typ_legend$ID == x, "type"]
    }
  )

  # add Muni_type_ID to shapefile
  Muni_shp$muni_type_ID <- as.numeric(sapply(
    Muni_shp$BFS_NUMMER,
    function(Muni_num) {
      muni_typology[muni_typology$`BFS-Gde Nummer` == Muni_num, "muni_type_ID"]
    },
    simplify = TRUE
  ))

  # rasterize
  Muni_type_rast <- terra::rasterize(
    Muni_shp,
    terra::rast(Ref_grid),
    field = "muni_type_ID"
  )

  # link raster attribute table
  levels(Muni_type_rast) <- Muni_typ_legend

  # save
  save_dir <- fs::path(config[["spat_prob_perturb_path"]], "municipality_typology")
  ensure_dir(save_dir)
  terra::writeRaster(
    Muni_type_rast,
    filename = fs::path(save_dir, "Muni_type_raster.grd"),
    overwrite = TRUE
  )

  ### =========================================================================
  ### C- Mountain areas
  ### =========================================================================

  # Import data of municipalities in mountainous areas from FSO web service using condition:
  # 1. Municipality designations as of 01/05/2022 (to match mutations)

  # scrape content from html address
  Mount_content <- rvest::read_html(
    "https://www.agvchapp.bfs.admin.ch/de/typologies/results?SnapshotDate=01.05.2022&SelectedTypologies%5B0%5D=HR_MONT2019"
  )
  muni_mountains <- as.data.frame(rvest::html_table(
    Mount_content,
    fill = TRUE
  )[[1]][-1, ]) # remove duplicate row names

  # rename column of interest
  colnames(muni_mountains)[[7]] <- "mountainous"
  muni_mountains$`BFS-Gde Nummer` <- as.numeric(muni_mountains$`BFS-Gde Nummer`)

  # create attribute table: 0:non-mountain, 1:Mountainous
  Muni_mount_legend <- data.frame(
    ID = c(0, 1),
    type = c("Non-mountainous", "Mountainous")
  )

  # add Muni_type_ID to shapefile
  Muni_shp$mountainous <- as.numeric(sapply(
    Muni_shp$BFS_NUMMER,
    function(Muni_num) {
      muni_mountains[
        muni_typology$`BFS-Gde Nummer` == Muni_num,
        "mountainous"
      ]
    },
    simplify = TRUE
  ))

  # rasterize
  Muni_mount_rast <- terra::rasterize(
    Muni_shp,
    terra::rast(Ref_grid),
    field = "mountainous"
  )

  # link raster attribute table
  levels(Muni_mount_rast) <- Muni_mount_legend

  # save
  save_dir <- fs::path(config[["spat_prob_perturb_path"]], "mountainous_municipalities")
  ensure_dir(save_dir)
  terra::writeRaster(
    Muni_mount_rast,
    filename = fs::path(save_dir, "muni_mountainous_raster.grd"),
    overwrite = TRUE
  )

  ### =========================================================================
  ### D-  Agricultural areas
  ### =========================================================================

  # path to Biodiversity promotion areas .gpkg file
  # This file comes from
  # https://www.geodienste.ch/downloads/lwb_biodiversitaetsfoerderflaechen?data_format=gpkg
  # and requires permission from some of the cantons
  BPA_path <- fs::path(
    config[["spat_prob_perturb_path"]], "agriculture_bio_areas", "agri_bio_areas.gpkg"
  )

  BPA_layer <- terra::vector_layers(BPA_path)[[1]]
  BPAs <-
    terra::vect(BPA_path, layer = BPA_layer) |>
    terra::project(Ref_crs) |>
    terra::makeValid()
  BPAs$ID <- seq_len(nrow(BPAs))

  # rasterize using the most recent LULC layer as a mask
  bpa_mask <- terra::rast(fs::path(config[["historic_lulc_basepath"]], "lulc_2018_agg.grd"))
  BPA_raster <- terra::mask(bpa_mask, BPAs)

  # change non-NA values to 1
  BPA_raster <- terra::ifel(!is.na(BPA_raster), 1L, NA_integer_)

  terra::writeRaster(
    BPA_raster,
    fs::path(config[["spat_prob_perturb_path"]], "agriculture_bio_areas", "BPA_raster.tif")
  )

  ### =========================================================================
  ### E- Protected areas
  ### =========================================================================

  #-------------------------------------------------------------------------
  # E.1 Data preparation
  #-------------------------------------------------------------------------

  # create CRS object
  ProjCH <- "epsg:2056"

  # re-load ref_grid as terra::rast
  Ref_grid <- terra::rast(config[["ref_grid_path"]])

  # vector dir of raw data
  PA_raw_dir <- fs::path(config[["spat_prob_perturb_path"]], "protected_areas", "raw_data")

  # create dir for intermediate data layers produced
  PA_int_dir <- fs::path(config[["spat_prob_perturb_path"]], "protected_areas", "int_data")
  ensure_dir(PA_int_dir)

  New_PA_dir <- fs::path(config[["spat_prob_perturb_path"]], "protected_areas", "new_pas")
  ensure_dir(New_PA_dir)

  PA_final_dir <- fs::path(config[["spat_prob_perturb_path"]], "protected_areas", "future_pas")
  ensure_dir(PA_final_dir)

  # load PAs shapefile of SwissPAs layer compiled by Louis-Rey
  PA <- terra::vect(file.path(PA_raw_dir, "SwissPA.shp"))

  # Load cantonal PA layer provided by BAFU (cleaned by us)
  PA_cantons <- terra::vect(file.path(PA_raw_dir, "PA_cantons.shp"))

  # load the vector land cover data from Swiss TLM regio to identify settlement areas
  LC <- terra::vect(file.path(PA_raw_dir, "swissTLMRegio_LandCover.shp"))
  settlement <- subset(LC, LC$OBJVAL == "Siedl")
  terra::crs(settlement) <- ProjCH

  # load Swiss TLM region roads and railways layers to exclude
  road <- terra::vect(file.path(PA_raw_dir, "swissTLMRegio_Road.shp"))
  terra::crs(road) <- ProjCH
  road <- subset(road, road$CONSTRUCT == "Keine Kunstbaute")

  railway <- terra::vect(file.path(PA_raw_dir, "swissTLMRegio_Railway.shp"))
  railway <- subset(railway, railway$CONSTRUCT == "Keine Kunstbaute")
  terra::crs(railway) <- ProjCH

  # load BLN areas (for cultural landscapes)
  BLN <- terra::vect(
    file.path(
      PA_raw_dir,
      "N2017_Revision_landschaftnaturdenkmal_20170727_20221110.shp"
    )
  )
  terra::crs(BLN) <- ProjCH

  # load biodiversity prioritization map
  Biodiv_prio <- terra::rast(file.path(PA_raw_dir, "Bio_prio.tif"))
  terra::crs(Biodiv_prio) <- ProjCH

  # load NCP prioritization map
  NCP_prio <- terra::rast(file.path(PA_raw_dir, "NCP_prio.tif"))
  terra::crs(NCP_prio) <- ProjCH
  terra::ext(NCP_prio) <- terra::ext(Ref_grid)
  NCP_prio <- terra::resample(NCP_prio, Ref_grid, method = "bilinear")

  #-------------------------------------------------------------------------
  # E.2 Spatially identify PAs included in national targets
  #-------------------------------------------------------------------------

  # Subset the PAs data to only the types supposedly included in the calculation of
  # the national coverage estimates
  subset_rows <- PA$Res_Type %in%
    c(
      "Ramsar",
      "Swiss National Park",
      "Unesco_BiosphereReserve",
      "Unesco_CulturalSites",
      "Unesco_NaturalSites",
      "ProNatura reserves",
      "Emeraude"
    )
  PA_BAFU <- PA[subset_rows, ]
  PA_BAFU <- terra::project(PA_BAFU, ProjCH)
  terra::writeVector(
    PA_BAFU,
    file.path(PA_int_dir, "PA_BAFU.shp"),
    overwrite = TRUE
  )

  # merge PAs from BAFU with the cantonal PA provided by FOEN, name PA_BAFU is kept
  # in order not to change all the dependencies below
  PA_BAFU <- rbind(PA_BAFU, PA_cantons)

  # check for invalid polygons (i.e. holes)
  polys_invalid <- any(
    terra::is.valid(PA_BAFU, messages = FALSE, as.points = FALSE) == FALSE
  )

  # if invalid polygons then makeValid
  if (polys_invalid == TRUE) {
    PA_BAFU <- terra::makeValid(PA_BAFU)
  }

  # Rasterize the combined BAFU and cantonal PAs
  # Preferred approach with terra::mask() which results in same num of PA cells
  # as the original approach but keeps the ncells and extent consistent
  PA_BAFU_raster <- terra::mask(Biodiv_prio, PA_BAFU)

  # change non-NA values to 1
  PA_BAFU_raster <- terra::ifel(!is.na(PA_BAFU_raster), 1, 0)

  # combine PA raster with raster of Biodiversity promotion areas
  BPA_raster <- terra::rast(
    fs::path(config[["spat_prob_perturb_path"]], "agriculture_bio_areas", "BPA_raster.tif")
  )
  terra::crs(BPA_raster) <- ProjCH
  PA_total_rast <- PA_BAFU_raster + BPA_raster

  # addition results in 2's for overlap and 1 for non-overlapping BPAs
  # convert all values greater than 0 to 1 and the rest back to NA
  PA_total_rast <- terra::ifel(PA_total_rast == 0, NA, 1)

  terra::writeRaster(
    PA_total_rast,
    file.path(PA_int_dir, "PA_combined.tif"),
    overwrite = TRUE
  )

  #-------------------------------------------------------------------------
  # E.3 Calculate current PA areal coverage
  #-------------------------------------------------------------------------

  # The BAFU give a figure of 12% PA coverage but this is very instransparent and
  # should only be considered as an approximation.

  # Two components required each with possibilities to calculate differently:
  # 1. Area of Switzerland: raw area vs. raster area according to our grid/CRS
  # 2. Area of PAs: Areas from rasters or polygonal areas

  # 1. Area of Switzerland:
  area_ch_raw <- 41285 * 1000000

  # 2. Area of PAs:
  # if we use the area of the rasterized layer of PAs we will overestimate current
  # coverage because many BPAs for example only occupy portions of 100m cells
  # hence we should calculate areas from the Spatvectors

  # For the BPAs this is easy as there is an area attribute with values for all polygons
  # but for the PA shapefile there is several incomplete area columns so we will
  # need to estimate from the polygons however some are overlapping so first
  # we need to aggregate to non-overlapping areas only

  # 2.1 calculate discrepancy between area of BPAs from polygons vs. BPA raster cells
  # that do not overlap with other PAs
  # set 0's back to NA in BPA and PA_BAFU rasters
  BPA_raster <- terra::ifel(BPA_raster == 0, NA, 1)
  PA_BAFU_raster <- terra::ifel(PA_BAFU_raster == 0, NA, 1)

  # Identify BPA cells not in PAs (inverse masking)
  Non_PA_BPAs_rast <- terra::mask(BPA_raster, PA_BAFU_raster, inverse = TRUE)
  terra::writeRaster(
    Non_PA_BPAs_rast,
    file.path(PA_int_dir, "Non_PA_BPA_raster.tif"),
    overwrite = TRUE
  )

  # filter Spatvector of BPAs by first intersecting with the BAFU PAs then subsetting
  # the Spatvector by the intersecting polygons it would be faster to use
  # terra::crop but it is throwing an error: "TopologyException: Input geom 1 is invalid"
  # I have check geometry validity using terra::is.valid and apparently all are valid?
  # note: intersecting splits polygons meaning that there are rows with non-unique IDs
  intersecting_BPAs <- terra::intersect(BPAs, PA_BAFU)
  Non_PA_BPAs <- BPAs[which(!BPAs$ID %in% unique(intersecting_BPAs$ID)), ]
  terra::writeVector(
    Non_PA_BPAs,
    file.path(PA_int_dir, "Non_PA_BPA.shp"),
    overwrite = TRUE
  )

  # sum up areas of remaining BPA polygons
  # Calculate area for polygons missing flaeche_m2 using terra::expanse
  missing_area_idx <- is.na(Non_PA_BPAs$flaeche_m2)
  if (any(missing_area_idx)) {
    Non_PA_BPAs$flaeche_m2[missing_area_idx] <- terra::expanse(Non_PA_BPAs[missing_area_idx, ])
  }

  poly_area_BPA <- sum(Non_PA_BPAs$flaeche_m2)

  # 2.2 Out of interest, calculate discrepancy between area of PA polygons minus
  # the overlapping BPA areas and the raster cell total area

  # combine overlapping polygons
  PA_agg <- terra::aggregate(PA_BAFU, dissolve = TRUE)
  terra::writeVector(
    PA_agg,
    file.path(PA_int_dir, "PA_agg.shp"),
    overwrite = TRUE
  )

  # calc areas of remaining polygons
  PA_poly_area <- terra::expanse(PA_agg)

  # 3 calculate total PA coverage using the raw vs. raster areas of Switzerland
  # and the raster vs. polygonal areas of PAs

  # directly calculate area of protected cells and subtract the overestimation of BPAs
  area_prot_poly <- PA_poly_area + poly_area_BPA

  # under the raw CH area
  # FIXME this is now 16.9%, leading to findSumm not having a solution because the
  # demanded area is larger than the amount of cells available from the biodiversity
  # priority without protected areas.
  cover_poly_raw <- area_prot_poly / area_ch_raw # 17.8%
  cover_poly_raw <- 0.178

  #-------------------------------------------------------------------------
  # E.4 Calculate additional PA areal coverage required under scenarios
  #-------------------------------------------------------------------------

  # vector of percent of total area of Switzerland that should be protected by 2060
  # under each scenario
  perc_goals <- c(0.22, 0.25, 0.30)
  names(perc_goals) <- c("EI_SOC", "EI_CUL", "EI_NAT")

  # The raw CH area produces the estimated coverage that is closest to that
  # reported by the BAFU hence lets use that
  # additional % area of switzerland required to meet goal
  perc_todo <- perc_goals - cover_poly_raw

  # number of additional cells to protect in order to meet goal
  n_cells <- ceiling(perc_todo * area_ch_raw / prod(terra::res(Biodiv_prio)))

  #-------------------------------------------------------------------------
  # E.5 Identify locations for new PAs based on biodiversity prioritization map
  #-------------------------------------------------------------------------

  # mask Biodiv_prio map so that values inside PAs are 0
  Biodiv_prio_wo_pa <- terra::mask(Biodiv_prio, PA_total_rast, maskvalue = 1)

  # Exclude urban areas/roads and railways
  Biodiv_prio_wo_pa <- terra::mask(
    Biodiv_prio_wo_pa,
    settlement,
    inverse = TRUE
  )
  Biodiv_prio_wo_pa <- terra::mask(Biodiv_prio_wo_pa, road, inverse = TRUE)
  Biodiv_prio_wo_pa <- terra::mask(Biodiv_prio_wo_pa, railway, inverse = TRUE)

  # two approaches to trial for patch identification
  # 1. Simple approach: terra::patches, Detect patches (clumps) i.e. groups of
  # cells that are surrounded by cells that are NA. For this the prioritization
  # maps should be subsetted to only the n_cells with the highest priority values
  # see version at 5f7d43a79919c22ec24a38ba27e175d10156e241 for commented-out implementation

  # 2. landscapemetrics::get_patches which forms patches based on class values
  # which could be used to better identify patches of high priority
  # however we would need to discretize the continuous cell values into bins

  # reclassify raster to discrete
  Bio_prio_hist <- terra::hist(
    Biodiv_prio_wo_pa,
    maxcell = terra::ncell(Biodiv_prio_wo_pa)
  )

  # because we don't need to add that much PA cells it makes sense to create patches
  # using only the highest priority breaks that give sufficent cells counts to
  # cover the required amount

  # take the counts and iteratively sum from the last value until the max(n_cells) is exceeded
  Bio_running_sum <- cumsum(rev(Bio_prio_hist$counts))

  # identify the first cumsum that exceeds the required n_cells and add 1 to
  # include the lower bound of the break points
  Bio_min_ind <- min(which(
    Bio_running_sum > n_cells[names(n_cells) == "EI_NAT"]
  ))

  # subset only the high prioirty breaks that satisfy the desired n_cells
  Bio_cuts <- c(
    0,
    Bio_prio_hist$breaks[
      (length(Bio_prio_hist$breaks) - Bio_min_ind):length(Bio_prio_hist$breaks)
    ]
  )

  # reclassify using the break values
  Bio_prio_discrete <- terra::classify(Biodiv_prio_wo_pa, Bio_cuts)

  # create patches
  Bio_prio_patches <- landscapemetrics::get_patches(
    Bio_prio_discrete,
    directions = 8,
    return_raster = TRUE
  )

  # convert patch rasters to terra:rast and sum values excluding the first layer
  # (i.e. excluding the values of 0-min break)
  Bio_prio_patch_sum <- sum(terra::rast(
    lapply(
      Bio_prio_patches$layer_1[2:length(Bio_prio_patches$layer_1)],
      function(x) {
        terra::ifel(!is.na(x), 1, 0)
      }
    )
  ))

  # convert 0 to NA for patch identification
  Bio_prio_patch_sum[Bio_prio_patch_sum == 0] <- NA

  # Now we have patches based on high priority values now delineate them spatially
  # using terra::patches
  Bio_patches <- terra::patches(Bio_prio_patch_sum)
  terra::writeRaster(
    Bio_patches,
    file = file.path(PA_int_dir, "Bio_patches.tif"),
    overwrite = TRUE
  )
  Bio_patches <- terra::rast(file.path(PA_int_dir, "Bio_patches.tif"))

  #-------------------------------------------------------------------------
  # E.6 Zonal statistics for each biodiversity patches
  #-------------------------------------------------------------------------

  # small function calculating stats on patches
  Patch_stats <- function(Patch_raster, Val_raster) {
    # calculate the area of all patches
    area_df <- as.data.frame(terra::freq(Patch_raster))

    # subset cols and rename
    area_df <- area_df[c("value", "count")]
    colnames(area_df) <- c("patch_id", "num_cells")

    # stack with the prioritization map
    r_stack <- c(Patch_raster, Val_raster)

    # Use terra's zonal function to compute the median for each unique value in Patch_rast
    zonal_median <- terra::zonal(r_stack, Patch_raster, fun = function(x) {
      median(x, na.rm = TRUE)
    })
    zonal_median <- zonal_median[c(2, 3)]

    # Rename the columns of zonal_median
    colnames(zonal_median) <- c("patch_id", "median")

    # merge dfs with median of prio and area of each patch
    df_merge <- merge(area_df, zonal_median, by = "patch_id")

    df_merge
  }

  # calculate stats for each patch generation approach
  Bio_patch_stats <- Patch_stats(
    Patch_raster = Bio_patches,
    Val_raster = Biodiv_prio_wo_pa
  )

  # remove patches that are 2 cells or less
  Bio_patch_stats <- Bio_patch_stats[Bio_patch_stats$num_cells > 2, ]
  row.names(Bio_patch_stats) <- seq_len(nrow(Bio_patch_stats))

  # subset the patches raster and save along with the patch stats
  Bio_patches_subset <- terra::ifel(
    terra::match(Bio_patches, Bio_patch_stats[["patch_id"]]),
    Bio_patches,
    NaN
  )
  terra::writeRaster(
    Bio_patches_subset,
    file = file.path(PA_int_dir, "Bio_patches_subset.tif"),
    overwrite = TRUE
  )
  saveRDS(Bio_patch_stats, file = file.path(PA_int_dir, "Bio_patch_stats.rds"))

  # The LSM approach delivers a much high average median priority value per patch
  # whether or not single cells patches are excluded
  # Terra: 0.54
  # LSM: 0.80
  # Hence we should use the LSM patches

  #-------------------------------------------------------------------------
  # E.7 Identify locations for new PAs based on NCP prioritization map
  #-------------------------------------------------------------------------

  # mask NCP_prio map so that values inside PAs are 0
  NCP_prio_wo_pa <- terra::mask(NCP_prio, PA_total_rast, maskvalue = 1)

  # Exclude urban areas/roads and railways
  NCP_prio_wo_pa <- terra::mask(NCP_prio_wo_pa, settlement, inverse = TRUE)
  NCP_prio_wo_pa <- terra::mask(NCP_prio_wo_pa, road, inverse = TRUE)
  NCP_prio_wo_pa <- terra::mask(NCP_prio_wo_pa, railway, inverse = TRUE)

  # reclassify raster to discrete
  NCP_prio_hist <- terra::hist(
    NCP_prio_wo_pa,
    maxcell = terra::ncell(NCP_prio_wo_pa)
  )

  # because we don't need to add that much PA cells it makes sense to create patches
  # using only the highest priority breaks that give sufficent cells counts to
  # cover the required amount

  # take the counts and iteratively sum from the last value until the max(n_cells) is exceeded
  NCP_running_sum <- cumsum(rev(NCP_prio_hist$counts))

  # identify the first cumsum that exceeds the required n_cells and add 1 to
  # include the lower bound of the break points
  NCP_min_ind <- min(which(
    NCP_running_sum > (n_cells[names(n_cells) == "EI_SOC"]) * 1.5
  ))

  # subset only the high prioirty breaks that satisfy the desired n_cells
  NCP_cuts <- c(
    0,
    NCP_prio_hist$breaks[
      (length(NCP_prio_hist$breaks) - NCP_min_ind):length(NCP_prio_hist$breaks)
    ]
  )

  # reclassify using the break values
  NCP_prio_discrete <- terra::classify(NCP_prio_wo_pa, NCP_cuts)

  # create patches
  NCP_prio_patches <- landscapemetrics::get_patches(
    NCP_prio_discrete,
    directions = 8,
    return_raster = TRUE
  )

  # convert patch rasters to terra:rast and sum values excluding the first layer
  # (i.e. excluding the values of 0-min break)
  NCP_prio_patch_sum <- sum(terra::rast(lapply(
    NCP_prio_patches$layer_1[2:length(NCP_prio_patches$layer_1)],
    function(x) {
      terra::ifel(!is.na(x), 1, 0)
    }
  )))

  # convert 0 to NA for patch identification
  NCP_prio_patch_sum[NCP_prio_patch_sum == 0] <- NA

  # Now we have patches based on high priority values now delineate them spatially
  # using terra::patches
  NCP_patches <- terra::patches(NCP_prio_patch_sum)
  terra::writeRaster(
    NCP_patches,
    file = file.path(PA_int_dir, "NCP_patches.tif"),
    overwrite = TRUE
  )
  NCP_patches <- terra::rast(file.path(PA_int_dir, "NCP_patches.tif"))

  # calculate stats for NCP patches
  NCP_patch_stats <- Patch_stats(
    Patch_raster = NCP_patches,
    Val_raster = NCP_prio_wo_pa
  )

  # remove patches that are 2 cells or less
  NCP_patch_stats <- NCP_patch_stats[NCP_patch_stats$num_cells > 2, ]
  row.names(NCP_patch_stats) <- seq_len(nrow(NCP_patch_stats))

  # subset the patches raster and save
  NCP_patches_subset <- terra::ifel(
    terra::match(NCP_patches, NCP_patch_stats[["patch_id"]]),
    NCP_patches,
    NaN
  )
  terra::writeRaster(
    NCP_patches_subset,
    file = file.path(PA_int_dir, "NCP_patches_subset.tif"),
    overwrite = TRUE
  )
  saveRDS(NCP_patch_stats, file = file.path(PA_int_dir, "NCP_patch_stats.rds"))

  #-------------------------------------------------------------------------
  # E.8 Identify locations for new PAs based on cultural importance map
  #-------------------------------------------------------------------------

  # This is the exact same approach as with the biodiversity, except that the
  # proximity to BLN areas is considered mask Biodiv_prio map so that values
  # inside PAs are 0

  buffer <- terra::buffer(BLN, width = 2830)
  CULdiv_prio_wo_pa <- terra::mask(Biodiv_prio_wo_pa, buffer, inverse = TRUE)

  # reclassify raster to discrete
  CUL_prio_hist <- terra::hist(
    CULdiv_prio_wo_pa,
    maxcell = terra::ncell(Biodiv_prio_wo_pa)
  )

  # because we don't need to add that much PA cells it makes sense to create patches
  # using only the highest priority breaks that give sufficent cells counts to
  # cover the required amount

  # take the counts and iteratively sum from the last value until the max(n_cells) is exceeded
  CUL_running_sum <- cumsum(rev(CUL_prio_hist$counts))

  # identify the first cumsum that exceeds the required n_cells and add 1 to
  # include the lower bound of the break points
  CUL_min_ind <- min(which(
    CUL_running_sum > n_cells[names(n_cells) == "EI_NAT"]
  ))

  # subset only the high prioirty breaks that satisfy the desired n_cells
  CUL_cuts <- c(
    0,
    CUL_prio_hist$breaks[
      (length(CUL_prio_hist$breaks) - CUL_min_ind):length(CUL_prio_hist$breaks)
    ]
  )

  # reclassify using the break values
  CUL_prio_discrete <- terra::classify(CULdiv_prio_wo_pa, CUL_cuts)

  # create patches
  CUL_prio_patches <- landscapemetrics::get_patches(
    CUL_prio_discrete,
    directions = 8,
    return_raster = TRUE
  )

  # convert patch rasters to terra:rast and sum values excluding the first layer
  # (i.e. excluding the values of 0-min break)
  CUL_prio_patch_sum <- sum(
    terra::rast(lapply(
      CUL_prio_patches$layer_1[2:length(CUL_prio_patches$layer_1)],
      function(x) {
        terra::ifel(!is.na(x), 1, 0)
      }
    ))
  )

  # convert 0 to NA for patch identification
  CUL_prio_patch_sum[CUL_prio_patch_sum == 0] <- NA

  # Now we have patches based on high priority values now delineate them spatially
  # using terra::patches
  CUL_patches <- terra::patches(CUL_prio_patch_sum)
  terra::writeRaster(
    CUL_patches,
    file = file.path(PA_int_dir, "CUL_patches.tif"),
    overwrite = TRUE
  )
  CUL_patches <- terra::rast(file.path(PA_int_dir, "CUL_patches.tif"))

  # calculate patch stats
  CUL_patch_stats <- Patch_stats(
    Patch_raster = CUL_patches,
    Val_raster = CULdiv_prio_wo_pa
  )

  # remove patches that are 2 cells or less
  CUL_patch_stats <- CUL_patch_stats[CUL_patch_stats$num_cells > 2, ]
  row.names(CUL_patch_stats) <- seq_len(nrow(CUL_patch_stats))

  # subset the patches raster and save along with the patch stats
  CUL_patches_subset <- terra::ifel(
    terra::match(CUL_patches, CUL_patch_stats[["patch_id"]]),
    CUL_patches,
    NaN
  )
  terra::writeRaster(
    CUL_patches_subset,
    file = file.path(PA_int_dir, "CUL_patches_subset.tif"),
    overwrite = TRUE
  )
  saveRDS(CUL_patch_stats, file = file.path(PA_int_dir, "CUL_patch_stats.rds"))

  #-------------------------------------------------------------------------
  # E.9 Identify subsets of best patches to meet areal demand and seperate patches
  # according to scenario time steps
  #-------------------------------------------------------------------------

  # solver function from: https://stackoverflow.com/a/69622144
  # for identifying combination of patches whose sum total area
  # exceeds a specifcied amount
  findSumm <- function(xy,
                       sfind,
                       nmax = 10000,
                       tmax = 100000000000000000000000000) {
    # sort xy according to target variable
    xy <- xy[order(xy$num_cells, decreasing = TRUE), ]

    # seperate patch areas
    x <- xy[, "num_cells"]

    # stop if the sum of all patches does not exceed the desired area
    if (sum(x) < sfind) {
      stop("Impossible solution! sum(x)<sfind!")
    }

    # helper function to calculate difference from start time
    fTimeSec <- function() as.numeric(Sys.time() - l$tstart, units = "secs")

    # Create a vector the same length as the num of patches to start the loop,
    # first entry TRUE all the subsequent entries FALSE
    # updated iteratively in loop
    sel <- c(TRUE, rep(FALSE, length(x) - 1))

    # List of intermediate states of the vector sel
    lsel <- list()

    # List with a collection of parameters and results
    l <- list(
      patch_ids = list(),
      x = x,
      tstart = Sys.time(),
      chosen = list(),
      xfind = list(),
      time = c(),
      stop = FALSE,
      reason = ""
    )

    while (TRUE) {
      # Maximum Runtime Test
      if (fTimeSec() > tmax) {
        l$reason <- "Calculation time is greater than tmax.\n"
        l$stop <- TRUE
        break
      }

      # Record the solution and test the number of solutions
      if (sum(l$x[sel]) == sfind) {
        # Save solution
        l$chosen[[length(l$chosen) + 1]] <- sel
        l$xfind[[length(l$xfind) + 1]] <- l$x[sel]
        l$patch_ids[[length(l$patch_ids) + 1]] <- xy[sel, "patch_id"]
        l$time <- c(l$time, fTimeSec())

        # Test the number of solutions
        if (length(l$chosen) == nmax) {
          l$reason <- "Already found nmax solutions.\n"
          l$stop <- TRUE
          break
        }
      }

      idx <- which(sel)
      if (idx[length(idx)] == length(sel)) {
        if (length(lsel) == 0) {
          break
        }
        sel <- lsel[[length(lsel)]]
        idx <- which(sel)
        lsel[length(lsel)] <- NULL
        sel[idx[length(idx)]] <- FALSE
        sel[idx[length(idx)] + 1] <- TRUE
        next
      }

      if (sum(l$x[sel]) >= sfind) {
        sel[idx[length(idx)]] <- FALSE
        sel[idx[length(idx)] + 1] <- TRUE
        next
      } else {
        lsel[[length(lsel) + 1]] <- sel # Save the current state of sel vector
        sel[idx[length(idx)] + 1] <- TRUE
        next
      }
    }
    if (length(l$chosen) == 0 && !l$stop) {
      stop("No solutions!")
    }

    # vector summary of result
    l$reason <- paste(
      l$reason,
      "Found",
      length(l$chosen),
      "solutions in time",
      signif(fTimeSec(), 3),
      "seconds.\n"
    )

    # print summary of combinatorial step
    cat(l$reason)

    # return results object
    return(l)
  }

  # vector scenario names and layer keys to match on
  Scenario_keys <- c("NCP", "CUL", "Bio")
  names(Scenario_keys) <- names(n_cells)

  # subset interventions table
  PA_interventions <- Interventions[
    Interventions$intervention_id == "Protection",
  ]

  # loop function over cell targets for scenarios
  for (i in seq_along(n_cells)) {
    # Load the layer of patchs and patch stats appropriate for the scenario
    Scenario_patch_stats <- readRDS(file.path(
      PA_int_dir,
      paste0(Scenario_keys[i], "_patch_stats.rds")
    ))
    Scenario_patches <- terra::rast(file.path(
      PA_int_dir,
      paste0(Scenario_keys[i], "_patches_subset.tif")
    ))

    Solutions <- findSumm(
      xy = Scenario_patch_stats,
      sfind = n_cells[i]
    )
    saveRDS(
      Solutions,
      file.path(PA_int_dir, paste0("Solutions_10k_", names(n_cells)[i], ".rds"))
    )

    Solutions <- readRDS(file.path(
      PA_int_dir,
      paste0(
        "Solutions_10k_",
        names(n_cells)[i],
        ".rds"
      )
    ))

    # rank solutions according greatest value of the sum of median patch priority * patch size
    size <- 5000 * 1024^2
    options(future.globals.maxSize = size)
    Solution_scores <- data.table::rbindlist(future.apply::future_lapply(
      seq_along(Solutions$chosen),
      function(x) {
        # seperate vector of whether patches were chosen (by ID)
        idx <- Solutions$chosen[[x]]

        # calculate sum of median patch priority * patch size for solution
        return(list(
          "Solution_num" = x,
          "Patch_prio_sum" = sum(
            Scenario_patch_stats$median[idx] *
              Scenario_patch_stats$num_cells[idx]
          )
        ))
      }
    ))

    # sort results
    Ranked_solutions <- Solution_scores[
      order(Solution_scores$Patch_prio_sum, decreasing = TRUE),
    ]
    Best_solution_ID <- Ranked_solutions[[1, "Solution_num"]]

    # get best patches via id
    Patch_ids <- Solutions$patch_ids[[Best_solution_ID]]
    rm(Solutions, Solution_scores, Ranked_solutions)

    # subset patch stats to check total area
    Best_patch_stats <- Scenario_patch_stats[
      Scenario_patch_stats$patch_id %in% Patch_ids,
    ]

    # sort patches by median priority
    Best_patch_stats <- Best_patch_stats[
      order(Best_patch_stats$median, decreasing = TRUE),
    ]

    # seperate best patches in raster and save
    Best_patches <- terra::ifel(
      terra::match(Scenario_patches, Patch_ids),
      Scenario_patches,
      NaN
    )
    terra::writeRaster(
      Best_patches,
      file = file.path(New_PA_dir, paste0(names(n_cells)[i], "_all_future_PAs.tif")),
      overwrite = TRUE
    )

    # grab intervention time steps
    Scenario_time_steps <- unlist(PA_interventions[
      PA_interventions$scenario_id == names(n_cells)[i],
      "time_step"
    ])

    # becuase it is unlikely that new protection areas will be established before
    # 2025 and also because the scenario statements specify that PA targets be met by 2060
    # we will use the assumption that the addition of new patches takes places
    # between  2025-2055 however we still need to include the protective effects
    # of the existing PAs in the 2020 time point

    # seperate patches into groups according to number of time steps
    # (group sizes may be unqual in the case of non-integer division
    # of number of patches/time steps)

    PA_exp_steps <- Scenario_time_steps[-1] # exclude 2020 time step
    Split_ind <- rep(
      seq_along(PA_exp_steps),
      each = ceiling(nrow(Best_patch_stats) / length(PA_exp_steps))
    )[seq_len(nrow(Best_patch_stats))]
    Time_grouped_patches <- split(Best_patch_stats[, "patch_id"], Split_ind)
    names(Time_grouped_patches) <- PA_exp_steps

    # This has split patches into unique groups for each time step
    # but every subsequent time step needs to contain the patches from the
    # previous time step as well
    Time_cum_patches <- sapply(
      seq_along(Time_grouped_patches),
      function(x) {
        if (x == 1) {
          patches <- Time_grouped_patches[[x]]
        } else {
          patches <- append(
            Time_grouped_patches[[x]],
            Time_grouped_patches[[(x - 1)]]
          )
        }
        patches
      }
    )
    names(Time_cum_patches) <- PA_exp_steps

    # loop over time step patches saving as rasters
    for (step in Scenario_time_steps) {
      if (step == "2020") {
        Updated_PAs <- PA_total_rast
      } else {
        # identify patches for time step in raster (setting values to 1 otherwise NaN)
        Time_step_patches <- terra::ifel(
          terra::match(Best_patches, Time_cum_patches[[step]]),
          1,
          0
        )

        # save a layer of just the new patches without combining with the existing PAs
        terra::writeRaster(
          Time_step_patches,
          file = file.path(
            New_PA_dir,
            paste0(
              names(n_cells)[i],
              "_new_PAs_",
              step,
              ".tif"
            )
          ),
          overwrite = TRUE
        )

        # replace NA values in current PA layer for 0
        Current_PAs <- terra::ifel(is.na(PA_total_rast), 0, 1)

        # combine with existing PAs
        Updated_PAs <- Current_PAs + Time_step_patches

        # set 0's to NA
        Updated_PAs <- terra::ifel(Updated_PAs == 0, NA, 1)
      }

      # save
      terra::writeRaster(
        Updated_PAs,
        file = file.path(
          PA_final_dir,
          paste0(
            names(n_cells)[i],
            "_PAs_",
            step,
            ".tif"
          )
        ),
        overwrite = TRUE
      )
    } # close loop over scenario time steps
  } # close loop over scenario cell targets

  ### =========================================================================
  ### Intervention in allocation params
  ### =========================================================================

  # test to see if spatial zoning
  if (any(Interventions$intervention_type == "Param_adjust")) {
    # subset to interventions involving parameter adjustment
    Param_ints <- Interventions[
      Interventions$intervention_type == "Param_adjust",
    ]

    # load the LULC aggregation scheme
    LULC_agg <- openxlsx::read.xlsx(config[["LULC_aggregation_path"]])

    # swap the target classes for class numbers
    Param_ints$target_classes <- sapply(
      Param_ints$target_classes,
      function(x) {
        unique(LULC_agg[
          LULC_agg$Class_abbreviation %in% x,
          "Aggregated_ID"
        ])
      }
    )

    # loop over interventions adjust param tables
    sapply(
      seq_len(nrow(Param_ints)),
      function(i) {
        # get paths of param tables for relevant scenario and time points
        Param_table_paths <- list.files(
          file.path(
            config[["simulation_param_dir"]],
            Param_ints[i, "scenario_id"]
          ),
          pattern = paste(Param_ints[[i, "time_step"]], collapse = "|"),
          full.names = TRUE
        )

        # loop over paths adjusting tables
        sapply(Param_table_paths, function(tbl_path) {
          # load table
          param_table <- read.csv(tbl_path)

          # adjust column names
          colnames(param_table) <- c(
            "From*",
            "To*",
            " Mean_Patch_Size",
            "Patch_Size_Variance",
            "Patch_Isometry",
            "Perc_expander",
            "Perc_patcher"
          )

          # alter rows for target_classes
          param_table[
            param_table$`To*` %in% Param_ints[[i, "target_classes"]],
            "Perc_expander"
          ] <- 1
          param_table[
            param_table$`To*` %in% Param_ints[[i, "target_classes"]],
            "Perc_patcher"
          ] <- 0

          # save table
          readr::write_csv(param_table, file = tbl_path)
        }) # close loop over tables
      }
    ) # close loop over intervention rows
  } # close if statement
}

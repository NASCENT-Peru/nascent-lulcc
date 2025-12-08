#' Preparation of shapefiles from INEI variables
#'
#' Preparing shapefiles of INEI variables for use as predictors in land use
#' and land cover transition models
#' @author Ben Black
#'
#' @param config A list object
#'
#' @export
inei_pred_prep <- function(config = get_config()) {
  ensure_dir(config[["predictors_prepped_dir"]])

  # vector years of LULC data
  LULC_years <-
    list.files(config[["aggregated_lulc_dir"]], full.names = FALSE, pattern = ".tif") |>
    gsub(pattern = ".*?([0-9]+).*", replacement = "\\1", x = _)

  # create a list of the data/modelling periods
  modelling_periods <- list()
  for (i in 1:(length(LULC_years) - 1)) {
    modelling_periods[[i]] <- c(LULC_years[i], LULC_years[i + 1])
  }
  names(modelling_periods) <- sapply(
    modelling_periods,
    function(x) paste(x[1], x[2], sep = "_")
  )

  # get values of each of the four years preceding the first entry in each of modelling_periods
  pre_modelling_years <- sapply(
    modelling_periods,
    function(x) as.character(as.integer(x[1]) - 4:1), simplify = FALSE
  )

  # List of INEI csv files to process
  inei_files <- list.files(
    file.path(config[["predictors_raw_dir"]], "socio_economic"),
    full.names = TRUE
  )

  # check that all files have District, department, and province columns
  required_cols <- c("DEPARTMENT", "PROVINCE", "DISTRICT")
  for (f in inei_files) {
    dat <- read.csv(f, nrows = 1)
    if (!all(required_cols %in% colnames(dat))) {
      stop(paste("File", f, "is missing required columns"))
    }
  }

  # loop over the files and read, clean, and reshape each
    inei_dat <- sapply(inei_files, function(f) {
    # Read CSV file
    dat <- read.csv(f)
    
    # Remove 'X' from start of column names if present - use ^ to match only at the start
    colnames(dat) <- sub("^X", "", colnames(dat))
    
    # Remove leading/trailing whitespace from column names
    colnames(dat) <- trimws(colnames(dat))
    
    # Check if required columns exist
    if (!all(c("DEPARTMENT", "PROVINCE", "DISTRICT") %in% colnames(dat))) {
      warning(paste("File", basename(f), "missing required columns"))
      return(dat)  # Return original data
    }
    
    # remove any columns with all NA values
    dat <- dat[, colSums(is.na(dat)) < nrow(dat)]
      
    # Convert DEPARTMENT, PROVINCE, DISTRICT to lowercase
    dat$DEPARTMENT <- tolower(dat$DEPARTMENT)
    dat$PROVINCE <- tolower(dat$PROVINCE)
    dat$DISTRICT <- tolower(dat$DISTRICT)
    
    # Identify year columns with proper regex match
    year_cols <- colnames(dat)[grepl("^[0-9]{4}$", colnames(dat))]
      
    # subset to only year_cols that are in pre_modelling_years
    relevant_years <- unique(unlist(pre_modelling_years))
    year_cols <- intersect(year_cols, relevant_years)
      
    # Check if year columns exist
    if (length(year_cols) == 0) {
      warning(paste("File", basename(f), "has no year columns"))
      return(dat)
    }
      
    # remove cols that are not year_cols or required_cols
    dat <- dat[, c(required_cols, year_cols)]
    
    # Handle empty spaces in data before conversion to numeric
    dat[year_cols] <- lapply(dat[year_cols], function(x) {
      x <- trimws(x)
      x[x == "" | x == " "] <- NA
      as.numeric(as.character(x))
    })
      
    # use the basename minus extension as the variable name
    var_name <- tools::file_path_sans_ext(basename(f))
            
    # Use try-catch to safely perform pivot
    tryCatch({
      # Reshape to long format
      dat <- tidyr::pivot_longer(
        dat,
        cols = dplyr::all_of(year_cols),
        names_to = "year",
        values_to = var_name,
      )
      # Convert year to integer
      dat$year <- as.integer(dat$year)
    }, error = function(e) {
      warning(paste("Error pivoting file", basename(f), ":", e$message))
    })
    
    return(dat)
    }, simplify = FALSE)
  
  
  # get DISTRICT values from df with highest number of unique districts
  district_counts <- sapply(inei_dat, function(df) length(unique(df$DISTRICT)))
  
  # sort descending
  district_counts <- sort(district_counts, decreasing = TRUE)

  # Load in the shapefile of districts in Peru
  districts_shp <- terra::vect(file.path(
    config[["predictors_raw_dir"]],
    "administrative_boundaries", "districts",
    "per_admbnda_adm3_ign_20200714.shp"
  )
  )

  # get names of columns in the shapefile
  district_shp_df <- as.data.frame(districts_shp)







}


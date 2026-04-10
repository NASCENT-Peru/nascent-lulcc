# Fast LULC counts (SUM-aggregated) across years for ALL Peru and polygons (Workshop_r).
# No CRS checks, no reproject. Comments explain code only.

suppressPackageStartupMessages({
  library(terra)
  library(data.table)
  library(tools)
})

# --------------------------- Options ------------------------------------------
terraOptions(threads = 8, parallel = TRUE, memfrac = 0.8, progress = 1)
# terraOptions(tempdir = "D:/terra_tmp")  # optional SSD scratch

# --------------------------- Paths --------------------------------------------
lulc_dir <- "C:/Users/mankurma/Scratch/Peru/LULC"
shp_path <- "Workshop-regions.shp" # must contain attribute 'Workshop_r'
attr_col <- "Workshop_r"

out_dir_steps <- file.path(lulc_dir, "counts_per_step")
out_dir_summary <- file.path(lulc_dir, "counts_summary")
dir.create(out_dir_steps, showWarnings = FALSE, recursive = TRUE)
dir.create(out_dir_summary, showWarnings = FALSE, recursive = TRUE)

# --------------------------- Aggregation mapping (SUM) ------------------------
# Put numeric source classes here; they will be summed into the target ID.
map_numeric <- list(
  `101` = c(3, 4, 5, 6),
  `102` = c(11, 12, 32, 13),
  `103` = integer(0), # add numeric codes here for Pasture/Mosaic if numeric
  `104` = c(18, 9, 35, 31),
  `105` = c(24, 25),
  `106` = c(30),
  `107` = c(33, 34)
)

class_names <- c(
  `0` = "Nodata",
  `101` = "Forested areas",
  `102` = "Natural Grasslands and Shrublands",
  `103` = "Low-Intensity Agricultural Areas",
  `104` = "High-Intensity Agricultural Areas",
  `105` = "Built-Up and Barren Lands",
  `106` = "Mining",
  `107` = "Water body"
)
target_ids <- as.integer(names(class_names))

# --------------------------- Helpers ------------------------------------------
sanitize_label <- function(x) {
  # Safe filename from a single attribute value.
  x <- trimws(as.character(x))
  if (is.na(x) || x == "") {
    x <- "region"
  }
  gsub("[^A-Za-z0-9_\\-]+", "_", x)
}

extract_year <- function(path) {
  m <- regexec("peru_coverage_(\\d{4})\\.tif$", basename(path))
  y <- regmatches(basename(path), m)[[1]]
  as.integer(if (length(y) == 2) y[2] else NA_integer_)
}

freq_dt <- function(r) {
  # Streamed frequency of raw classes for a (masked) raster.
  fr <- try(freq(r, digits = 0), silent = TRUE)
  if (inherits(fr, "try-error")) {
    fr <- freq(r)
  }
  if (is.null(fr) || nrow(fr) == 0) {
    return(data.table(value = integer(), count = integer()))
  }
  dt <- as.data.table(fr)
  if (!"value" %in% names(dt)) {
    setnames(dt, names(dt)[1], "value")
  }
  if (!"count" %in% names(dt)) {
    setnames(dt, names(dt)[2], "count")
  }
  dt[!is.na(value) & count > 0L]
}

aggregate_counts <- function(dt_counts) {
  # Assign aggregate IDs from numeric mapping only; SUM aggregation.
  dt_counts[, agg := NA_integer_]

  for (agg_id in names(map_numeric)) {
    vals <- map_numeric[[agg_id]]
    if (length(vals)) dt_counts[value %in% vals, agg := as.integer(agg_id)]
  }

  # Map value==0 to agg 0 if present.
  dt_counts[is.na(agg) & value == 0, agg := 0L]

  out <- dt_counts[!is.na(agg), .(count = sum(as.integer(count))), by = agg]

  # Ensure all target classes appear; fill missing with 0.
  out <- merge(data.table(agg = target_ids), out, by = "agg", all.x = TRUE)
  out[is.na(count), count := 0L]
  out[, class_name := unname(class_names[as.character(agg)])]
  setorder(out, agg)
  out
}

write_step_csv <- function(dt_agg, csv_path) {
  fwrite(dt_agg[, .(class_id = agg, class_name, count)], csv_path)
}

# --------------------------- Discover rasters ---------------------------------
lulc_files <- list.files(
  lulc_dir,
  pattern = "^peru_coverage_\\d{4}\\.tif$",
  full.names = TRUE
)
years <- vapply(lulc_files, extract_year, integer(1))
ord <- order(years, na.last = NA)
lulc_files <- lulc_files[ord]
years <- years[ord]
years_sorted <- unique(years)

# --------------------------- Load polygons (no CRS checks) --------------------
v <- vect(shp_path)
if (!(attr_col %in% names(v))) {
  stop(sprintf("Attribute column '%s' not found in %s", attr_col, shp_path))
}

# --------------------------- Main loop ----------------------------------------
long_acc <- list()

for (i in seq_along(lulc_files)) {
  f <- lulc_files[i]
  yr <- years[i]
  r <- rast(f)

  # -------- ALL Peru (no mask) ----------
  dt_all_raw <- freq_dt(r)
  dt_all_agg <- aggregate_counts(dt_all_raw)
  write_step_csv(
    dt_all_agg,
    file.path(out_dir_steps, sprintf("allPeru_%d.csv", yr))
  )

  long_acc[[paste0("ALL_", yr)]] <- data.table(
    region = "allPeru",
    year = yr,
    agg = dt_all_agg$agg,
    class_name = dt_all_agg$class_name,
    count = dt_all_agg$count
  )

  # -------- Per polygon ----------
  n <- nrow(v)
  for (j in seq_len(n)) {
    poly <- v[j, ]
    label <- sanitize_label(poly[[attr_col]][1])
    if (label == "region") {
      label <- paste0("region_", j)
    }

    xi <- crop(r, poly, snap = "out") # reduce IO
    xm <- mask(xi, poly) # cell-center-in-polygon

    dt_raw <- freq_dt(xm)
    dt_agg <- aggregate_counts(dt_raw)

    write_step_csv(
      dt_agg,
      file.path(out_dir_steps, sprintf("%s_%d.csv", label, yr))
    )

    long_acc[[paste0(label, "_", yr)]] <- data.table(
      region = label,
      year = yr,
      agg = dt_agg$agg,
      class_name = dt_agg$class_name,
      count = dt_agg$count
    )
  }
}

# --------------------------- Summaries (wide per region) ----------------------
all_long <- rbindlist(long_acc, use.names = TRUE, fill = TRUE)

mk_summary <- function(reg) {
  dt <- all_long[region == reg, .(agg, year, count)]
  base <- CJ(agg = target_ids, year = years_sorted, unique = TRUE)
  dtw <- merge(base, dt, by = c("agg", "year"), all.x = TRUE)
  dtw[is.na(count), count := 0L]
  dtw[, class_name := unname(class_names[as.character(agg)])]
  wide <- dcast(dtw, agg + class_name ~ year, value.var = "count")
  setorder(wide, agg)
  setcolorder(wide, c("agg", "class_name", as.character(years_sorted)))
  setnames(wide, c("agg", "class_name"), c("class_id", "class_name"))
  wide
}

for (reg in unique(all_long$region)) {
  fwrite(
    mk_summary(reg),
    file.path(out_dir_summary, sprintf("%s_summary.csv", reg))
  )
}

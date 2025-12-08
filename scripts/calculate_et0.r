#!/usr/bin/env Rscript
# ET0 (Turc) – skip-existing, LERC_ZSTD(MAX_Z_ERROR=1), larger GDAL block cache
# - Uses terra algebra with on-disk backing (todisk=TRUE)
# - Skips existing monthly/annual outputs
# - On-disk via vrt()/rast()
# - Multithreaded GTiff I/O
# - GDAL block cache sized from SLURM node memory (25%, capped)

suppressPackageStartupMessages({
  library(terra)
  library(lubridate)
  library(stringr)
})

# --------------------------- Config (env toggles) ------------------------------
# Inputs are guaranteed to be in °C; no Kelvin logic.
skip_existing <- identical(Sys.getenv("SKIP_EXISTING","1"), "1")

# --------------------------- Paths --------------------------------------------
tas_dir     <- "chelsa_cmip6_processed/aggregated_reprojected/monthly"
rsds_dir    <- "rsds/monthly_averages_1981_2010"  # single RSDS climatology for all ET0
base_et0    <- "et0"
monthly_out <- file.path(base_et0, "monthly")
yearly_out  <- file.path(base_et0, "yearly")
dir.create(monthly_out, recursive = TRUE, showWarnings = FALSE)
dir.create(yearly_out , recursive = TRUE, showWarnings = FALSE)

# --------------------------- Runtime / IO -------------------------------------
# Use a per-job temp folder on the large /cluster/scratch (avoid node-local /scratch).
user_scratch <- Sys.getenv("SCRATCH")
if (!nzchar(user_scratch)) {
  user_scratch <- file.path("/cluster/scratch", Sys.getenv("USER", "user"))
}

job_id  <- Sys.getenv("SLURM_JOB_ID", as.character(Sys.getpid()))
scratch <- file.path(user_scratch, "process_chelsa_tmp", paste0("job_", job_id))
dir.create(scratch, recursive = TRUE, showWarnings = FALSE)

# Size GDAL block cache (MB): 25% of SLURM_MEM_PER_NODE, with floor/ceiling
cache_mb <- 16384L
mem_node <- suppressWarnings(as.integer(Sys.getenv("SLURM_MEM_PER_NODE", "0")))
if (!is.na(mem_node) && mem_node > 0) {
  cache_mb <- max(4096L, min(65536L, as.integer(mem_node * 0.25)))
}

# On-disk processing and multi-threaded GTiff I/O
Sys.setenv(
  TMPDIR = scratch, TEMP = scratch, TMP = scratch, CPL_TMPDIR = scratch,
  OMP_NUM_THREADS = "1", OPENBLAS_NUM_THREADS = "1", MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1", NUMEXPR_NUM_THREADS = "1",
  GDAL_NUM_THREADS = "ALL_CPUS",
  GTIFF_NUM_THREADS = "ALL_CPUS",
  GDAL_CACHEMAX = as.character(cache_mb),   # MB
  GDAL_PAM_ENABLED = "NO"                   # avoid .aux.xml sidecars
)
terraOptions(tempdir = scratch, memfrac = 0.40, todisk = TRUE, progress = 0)

# Debug: verify where temps go
cat("PWD: ", getwd(), "\n")
cat("TMPDIR: ", Sys.getenv("TMPDIR"), "\n")
cat("CPL_TMPDIR: ", Sys.getenv("CPL_TMPDIR"), "\n")
cat("base tempdir(): ", tempdir(), "\n")
cat("terra tempdir  : ", terraOptions()$tempdir, "\n")

# Cleanup on exit to avoid temp accumulation
on.exit({
  try(terra::deleteTemp(), silent = TRUE)
  try({
    # Remove all contents of the job temp folder, keep folder itself
    leftovers <- list.files(scratch, all.files = TRUE, full.names = TRUE, recursive = FALSE, include.dirs = TRUE)
    if (length(leftovers)) unlink(leftovers, recursive = TRUE, force = TRUE)
  }, silent = TRUE)
}, add = TRUE)

# --------------------------- GeoTIFF options ----------------------------------
gt_co <- c(
  "TILED=YES",
  "COMPRESS=LERC_ZSTD",
  "MAX_Z_ERROR=1",
  "ZSTD_LEVEL=9",      # lower transient pressure; raise later if desired
  "BIGTIFF=YES",
  "SPARSE_OK=TRUE",
  "NUM_THREADS=ALL_CPUS",
  "BLOCKXSIZE=512","BLOCKYSIZE=512"
)
wopt_f32 <- list(datatype = "FLT4S", gdal = gt_co)

# --------------------------- Helpers ------------------------------------------
get_mm <- function(f) as.integer(stringr::str_extract(basename(f), "\\d{2}(?=\\.tif$)"))

vrast <- function(x) {
  # Prefer VRT-backed; fall back to direct raster
  tryCatch(vrt(x), error = function(e) rast(x))
}

rename_window_to_midyear <- function(dir, vars = c("tas","tasmin","tasmax","pr")) {
  # Rename window-style files to midyear names
  pat <- sprintf("^(%s)_(\\d{4})[-_](\\d{4})_(ssp\\d{3})_(\\d{2})\\.tif$",
                 paste(vars, collapse = "|"))
  files <- list.files(dir, pattern = pat, full.names = TRUE)
  if (!length(files)) { message("No window-style files found to rename."); return(invisible(NULL)) }
  n_ren <- 0L; n_skip <- 0L
  for (f in files) {
    m <- str_match(basename(f), pat); if (is.na(m[1])) next
    var <- m[2]; yfrom <- as.integer(m[3]); ssp <- m[5]; mm <- m[6]
    midY <- yfrom + 15L
    target <- file.path(dir, sprintf("%s_%04d_%s_%s.tif", var, midY, ssp, mm))
    if (file.exists(target)) { n_skip <- n_skip + 1L; next }
    if (!file.rename(f, target)) warning("Failed to rename: ", f, " -> ", basename(target)) else n_ren <- n_ren + 1L
  }
  message("Rename summary: ", n_ren, " renamed, ", n_skip, " skipped.")
}

dpm_nonleap <- function(m) lubridate::days_in_month(as.Date(sprintf("2001-%02d-15", m)))

quick_stats <- function(r, label) {
  # Print min/mean/max for a SpatRaster
  g <- tryCatch(global(r, c("min","mean","max"), na.rm = TRUE), error = function(e) NULL)
  if (!is.null(g)) message(sprintf("  %-14s min=%.3f mean=%.3f max=%.3f", label, g$min, g$mean, g$max))
}

purge_job_tmp <- function(path) {
  # Remove *all* contents inside the job temp folder, keep the folder itself.
  # Adds a tiny delay after writes to let GDAL close handles.
  try(terra::deleteTemp(), silent = TRUE)
  Sys.sleep(0.2)
  # Extra safety: only purge if under /cluster/scratch
  if (!startsWith(normalizePath(path, winslash = "/", mustWork = FALSE), "/cluster/scratch/")) return(invisible())
  # List direct children and nuke them recursively
  kids <- tryCatch(list.files(path, all.files = TRUE, full.names = TRUE, recursive = FALSE, include.dirs = TRUE), error = function(e) character(0))
  if (length(kids)) {
    try(unlink(kids, recursive = TRUE, force = TRUE), silent = TRUE)
  }
  # Recreate in case a child was in use and got removed later
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
}

# --------------------------- RSDS file list -----------------------------------
rsds_files <- list.files(
  rsds_dir,
  pattern    = "^rsds_1981_2010_\\d{2}\\.tif$",
  full.names = TRUE
)
if (length(rsds_files) != 12) stop("Expected 12 RSDS climatology files in: ", rsds_dir)
rsds_files <- rsds_files[order(get_mm(rsds_files))]

# --------------------------- Rename pass (idempotent) -------------------------
rename_window_to_midyear(tas_dir)

# --------------------------- Discover (midY, SSP) combos ----------------------
all_tn <- list.files(
  tas_dir,
  pattern    = "^tasmin_\\d{4}_ssp\\d{3}_\\d{2}\\.tif$",
  full.names = TRUE
)
if (!length(all_tn)) stop("No tasmin midyear-named files found in: ", tas_dir)

meta <- str_match(basename(all_tn), "^tasmin_(\\d{4})_(ssp\\d{3})_")[, 2:3]
combos <- unique(data.frame(
  midY = as.integer(meta[, 1]),
  ssp  = meta[, 2],
  stringsAsFactors = FALSE
))
combos <- combos[order(combos$ssp, combos$midY), , drop = FALSE]

# --------------------------- Main loop ----------------------------------------
for (i in seq_len(nrow(combos))) {

  midY <- combos$midY[i]
  ssp  <- combos$ssp[i]
  message("\n── ET0 for ", ssp, " / midY ", midY, " ──")

  patt_tn  <- sprintf("^tasmin_%04d_%s_\\d{2}\\.tif$", midY, ssp)
  patt_tx  <- sprintf("^tasmax_%04d_%s_\\d{2}\\.tif$", midY, ssp)
  tn_files <- list.files(tas_dir, pattern = patt_tn, full.names = TRUE)
  tx_files <- list.files(tas_dir, pattern = patt_tx, full.names = TRUE)

  if (length(tn_files) != 12 || length(tx_files) != 12) {
    warning("Skipping ", ssp, " / ", midY, ": need 12 tasmin AND 12 tasmax.")
    next
  }
  tn_files <- tn_files[order(get_mm(tn_files))]
  tx_files <- tx_files[order(get_mm(tx_files))]

  mon_paths <- file.path(monthly_out, sprintf("ET0_%04d_%s_%02d.tif", midY, ssp, 1:12))
  ann_path  <- file.path(yearly_out , sprintf("ET0_%04d_%s.tif",      midY, ssp))

  # Geometry check once using month 01 (always perform, even if skipping write)
  {
    tn1 <- vrast(tn_files[1])
    tx1 <- vrast(tx_files[1])
    rs1 <- vrast(rsds_files[1])
    if (!compareGeom(tn1, tx1, stopOnError = FALSE) || !compareGeom(tn1, rs1, stopOnError = FALSE)) {
      stop("Geometry mismatch for ", ssp, " / ", midY, " at month 01 (no warping in this ET0 script).")
    }
    rm(tn1, tx1, rs1); gc()
  }

  for (m in 1:12) {
    out_mon <- mon_paths[m]

    if (skip_existing && file.exists(out_mon)) {
      message("  • Skip existing month ", sprintf("%02d", m))
      next
    }

    # Open inputs for month m
    tn <- vrast(tn_files[m])
    tx <- vrast(tx_files[m])
    rs <- vrast(rsds_files[m])
    rs_term <- 0.3107 * rs + 0.65

    # Compute ET0 for month m (inputs are already in °C)
    tmean <- (tn + tx) / 2
    ratio <- (tmean > 0) * (tmean / (tmean + 15))
    et0_m <- (ratio * rs_term) * as.numeric(dpm_nonleap(m))

    if (m == 1) quick_stats(et0_m, "ET0_m[01]")

    writeRaster(et0_m, filename = out_mon, overwrite = TRUE, wopt = wopt_f32)
    message("  ✓ Month ", sprintf("%02d", m), " written.")

    # Close handles, purge terra temps, then nuke all contents of the job temp dir
    rm(tn, tx, rs, rs_term, tmean, ratio, et0_m); gc()
    purge_job_tmp(scratch)
  }

  # Annual sum (same as before)
  if (skip_existing && file.exists(ann_path)) {
    message("  • Skip existing annual: ", basename(ann_path))
  } else {
    have_all <- all(file.exists(mon_paths))
    if (!have_all) {
      message("  • Annual not computed (missing months).")
    } else {
      app(rast(mon_paths), sum, filename = ann_path, overwrite = TRUE, wopt = wopt_f32)
      message("  ✓ Annual ET0 written: ", basename(ann_path))
    }
  }
  purge_job_tmp(scratch)  # extra cleanup after annual aggregation
}

message("\nAll done.")

#!/usr/bin/env Rscript
# CHELSA CMIP6 processing on SLURM HPC (GDAL-warp only, lowercase-safe)
# - Step 0: Rename all .nc files to lowercase (recursive, collision-safe)
# - Yearly: VRT (PixelFunctionType="sum"+ScaleRatio) -> single gdalwarp
# - Monthly 30-yr means: one file per month per window; name uses mid-year (yy+15)
# - Supports mixed inputs (12-band climatologies or 360-band monthlies)
# - pr: for 360-band inputs, treats units as mm/day and multiplies by days_in_month
# - Writes to $TMPDIR then moves; GeoTIFF compression via LERC_ZSTD (controlled lossy)
# - No terra::project anywhere; terra only for metadata
# - Historic Peru add-on: reads single-file timesteps (midyears 2010/2014/2018/2022),
#   no GCM averaging; produces yearly and monthly outputs with the same naming scheme,
#   using SSP token "historical".

suppressPackageStartupMessages({
  library(terra)
  library(stringr)
  library(dplyr)
  library(purrr)
  library(lubridate)
  library(tibble)
})

# ------------------------- HPC / env ------------------------------------------
scratch <- Sys.getenv("TMPDIR")
if (!nzchar(scratch)) {
  scratch <- file.path(
    "/scratch",
    Sys.getenv("USER", "user"),
    Sys.getenv("SLURM_JOB_ID", as.character(Sys.getpid()))
  )
}
dir.create(scratch, recursive = TRUE, showWarnings = FALSE)

threads_env <- Sys.getenv(
  "SLURM_CPUS_PER_TASK",
  Sys.getenv("SLURM_CPUS_ON_NODE", "")
)
threads_num <- suppressWarnings(as.integer(threads_env))
threads_str <- if (!is.na(threads_num) && threads_num > 0) {
  as.character(threads_num)
} else {
  "ALL_CPUS"
}

cache_mb <- 16384L
mem_node <- suppressWarnings(as.integer(Sys.getenv("SLURM_MEM_PER_NODE", "")))
if (!is.na(mem_node) && mem_node > 0) {
  cache_mb <- max(4096L, min(65536L, as.integer(mem_node * 0.25)))
}

Sys.setenv(
  TMPDIR = scratch,
  TEMP = scratch,
  TMP = scratch,
  CPL_TMPDIR = scratch,
  GDAL_NUM_THREADS = if (threads_str == "ALL_CPUS") "ALL_CPUS" else threads_str,
  GDAL_CACHEMAX = as.character(cache_mb),
  OMP_NUM_THREADS = "1",
  OPENBLAS_NUM_THREADS = "1",
  MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1",
  NUMEXPR_NUM_THREADS = "1",
  GTIFF_NUM_THREADS = "ALL_CPUS"
)

terraOptions(tempdir = scratch, memfrac = 0.6, progress = 1)

# ------------------------- Paths & constants ----------------------------------
nc_folder <- "./chelsa_cmip6_downloads/future"
hist_folder <- "./chelsa_cmip6_downloads/historic_peru" # NEW: historic input folder

out_yearly <- "./chelsa_cmip6_processed/aggregated_reprojected/yearly"
out_monthly <- "./chelsa_cmip6_processed/aggregated_reprojected/monthly"
track_file <- file.path(out_yearly, "missing_combinations.txt")
ref_grid_path <- "./ref_grid.tif"

# Allow-list of GCMs (lowercase canonical forms)
gcms <- c(
  "ipsl-cm6a-lr",
  "gfdl-esm4",
  "mri-esm2-0",
  "ukesm1-0-ll",
  "mpi-esm1-2-lr"
)
ssps <- c("ssp126", "ssp245", "ssp585")

# Require complete GCM coverage for aggregations
require_full_gcm <- TRUE

vars_yearly <- c(paste0("bio", 1:19), "gdd", "pr", "tas", "tasmin", "tasmax")
vars_monthly <- c("tas", "tasmin", "tasmax", "pr")

dir.create(out_yearly, recursive = TRUE, showWarnings = FALSE)
dir.create(out_monthly, recursive = TRUE, showWarnings = FALSE)
dir.create("logs", recursive = TRUE, showWarnings = FALSE)

# ------------------------- Step 0: lowercase filenames ------------------------
lowercase_nc_filenames <- function(root_dir) {
  # Lowercase all .nc files; skip case-collision risks
  ncs <- list.files(
    root_dir,
    pattern = "\\.nc$",
    recursive = TRUE,
    full.names = TRUE
  )
  if (!length(ncs)) {
    return(invisible(NULL))
  }
  by_dir <- split(ncs, dirname(ncs))
  for (d in names(by_dir)) {
    files <- by_dir[[d]]
    bns <- basename(files)
    lbs <- tolower(bns)
    if (any(duplicated(lbs))) {
      # Warn and skip renaming when names collide in case-insensitive FS
      dups <- unique(lbs[duplicated(lbs)])
      msg <- sprintf(
        "WARNING: Case-insensitive name collisions in %s: %s. Skipping rename for these.",
        d,
        paste(dups, collapse = ", ")
      )
      message(msg)
      next
    }
    for (i in seq_along(files)) {
      if (bns[i] != lbs[i]) {
        src <- files[i]
        dst <- file.path(d, lbs[i])
        if (file.exists(dst)) {
          message("Skip lowercasing (target exists): ", src, " -> ", dst)
        } else {
          ok <- tryCatch(file.rename(src, dst), error = function(e) FALSE)
          if (ok) {
            message("Renamed: ", bns[i], " -> ", lbs[i])
          } else {
            message("Failed to rename: ", src)
          }
        }
      }
    }
  }
  invisible(NULL)
}
lowercase_nc_filenames(nc_folder)
lowercase_nc_filenames(hist_folder) # NEW: also lowercase historic inputs

# ------------------------- Compression (LERC_ZSTD) ----------------------------
# Custom per-variable MAX_Z_ERROR (data units)
custom_lerc_tol <- c(
  bio12 = 1,
  bio13 = 1,
  bio16 = 1,
  bio17 = 1,
  bio18 = 1,
  bio19 = 1
)

use_lerc <- TRUE
lerc_tol_temp <- 0.01 # °C
lerc_tol_pr <- 0.10 # mm (or mm/day depending on semantics)
lerc_tol_other <- 0.01
lerc_tol_gdd <- 1.0

co_gtiff_for <- function(var) {
  # Choose GTiff creation options with LERC tolerance per variable
  v <- tolower(var)
  tol <- if (v %in% names(custom_lerc_tol)) {
    custom_lerc_tol[[v]]
  } else if (v == "pr") {
    lerc_tol_pr
  } else if (v %in% c("tas", "tasmin", "tasmax")) {
    lerc_tol_temp
  } else if (v == "gdd") {
    lerc_tol_gdd
  } else {
    lerc_tol_other
  }
  if (use_lerc) {
    c(
      "TILED=YES",
      "COMPRESS=LERC_ZSTD",
      sprintf("MAX_Z_ERROR=%.15g", tol),
      "ZSTD_LEVEL=12",
      "BIGTIFF=IF_SAFER",
      "NUM_THREADS=ALL_CPUS",
      "BLOCKXSIZE=512",
      "BLOCKYSIZE=512"
    )
  } else {
    c(
      "TILED=YES",
      "COMPRESS=ZSTD",
      "ZSTD_LEVEL=12",
      "BIGTIFF=IF_SAFER",
      "NUM_THREADS=ALL_CPUS",
      "BLOCKXSIZE=512",
      "BLOCKYSIZE=512"
    )
  }
}

# ----------------------------- Helpers ----------------------------------------
xml_escape <- function(x) {
  # Minimal XML escaping for embedding file paths in VRT XML
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  x <- gsub(">", "&gt;", x, fixed = TRUE)
  x <- gsub('"', "&quot;", x, fixed = TRUE)
  x <- gsub("'", "&apos;", x, fixed = TRUE)
  x
}

ensure_wgs84 <- function(r) {
  if (is.na(crs(r))) {
    crs(r) <- "EPSG:4326"
  }
  r
}
days_in_yymm <- function(y, mm) {
  lubridate::days_in_month(as.Date(sprintf("%04d-%02d-15", y, as.integer(mm))))
}
has_gdalwarp <- function() nzchar(Sys.which("gdalwarp"))

# Helper: check that all required GCMs are present
has_full_gcm <- function(gcms_present, gcms_required) {
  # Returns TRUE when all required GCM names are present
  length(unique(gcms_present)) == length(gcms_required) &&
    all(sort(unique(gcms_present)) %in% sort(gcms_required))
}

.nly_cache <- new.env(parent = emptyenv())
get_nly <- function(nc_path, varname) {
  # Cache number of layers per (file, var) to avoid repeated openings
  key <- paste0(nc_path, "::", varname)
  if (exists(key, envir = .nly_cache, inherits = FALSE)) {
    return(get(key, envir = .nly_cache, inherits = FALSE))
  }
  n <- tryCatch(
    nlyr(rast(sprintf(
      'NETCDF:"%s":%s',
      normalizePath(nc_path, winslash = "/"),
      varname
    ))),
    error = function(e) NA_integer_
  )
  assign(key, n, envir = .nly_cache)
  n
}

build_vrt_sum_xml <- function(
  width,
  height,
  xmin,
  xmax,
  ymin,
  ymax,
  srs = "EPSG:4326",
  band_specs
) {
  # Build a VRT with PixelFunctionType="sum" and optional ScaleRatio/ScaleOffset per source
  dx <- (xmax - xmin) / width
  dy <- (ymax - ymin) / height
  geotr <- sprintf("%.15f,%.15f,0,%.15f,0,%.15f", xmin, dx, ymax, -dy)

  band_xml <- vapply(
    seq_along(band_specs),
    function(i) {
      df <- band_specs[[i]]
      if (is.null(df) || !nrow(df)) {
        return(
          '<VRTRasterBand dataType="Float32" subClass="VRTDerivedRasterBand"><PixelFunctionType>sum</PixelFunctionType></VRTRasterBand>'
        )
      }
      df$srcband <- as.integer(df$srcband)
      srcs <- apply(df, 1, function(row) {
        so <- if (!is.na(as.numeric(row[["scale_offset"]]))) {
          sprintf(
            "<ScaleOffset>%.10f</ScaleOffset>",
            as.numeric(row[["scale_offset"]])
          )
        } else {
          ""
        }
        sr <- if (!is.na(as.numeric(row[["scale_ratio"]]))) {
          sprintf(
            "<ScaleRatio>%.10f</ScaleRatio>",
            as.numeric(row[["scale_ratio"]])
          )
        } else {
          ""
        }
        sprintf(
          '
        <ComplexSource>
          <SourceFilename relativeToVRT="0">%s</SourceFilename>
          <SourceBand>%d</SourceBand>
          <SrcRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>
          <DstRect xOff="0" yOff="0" xSize="%d" ySize="%d"/>
          %s
          %s
        </ComplexSource>',
          xml_escape(row[["srcfile"]]),
          as.integer(row[["srcband"]]),
          as.integer(width),
          as.integer(height),
          as.integer(width),
          as.integer(height),
          so,
          sr
        )
      })
      sprintf(
        '<VRTRasterBand dataType="Float32" subClass="VRTDerivedRasterBand"><PixelFunctionType>sum</PixelFunctionType>%s</VRTRasterBand>',
        paste(srcs, collapse = "\n")
      )
    },
    character(1)
  )

  sprintf(
    '<VRTDataset rasterXSize="%d" rasterYSize="%d"><SRS>%s</SRS><GeoTransform>%s</GeoTransform>%s</VRTDataset>',
    as.integer(width),
    as.integer(height),
    srs,
    geotr,
    paste(band_xml, collapse = "\n")
  )
}
write_vrt <- function(xml_txt, out_path) {
  # Write VRT XML to file (binary-safe); return path
  con <- file(out_path, open = "wb")
  on.exit(close(con), add = TRUE)
  writeBin(charToRaw(xml_txt), con)
  out_path
}
netcdf_gdal_path <- function(nc_path, varname) {
  sprintf('NETCDF:"%s":%s', normalizePath(nc_path, winslash = "/"), varname)
}

# ------------------------ NEW helper: historic parsing ------------------------
detect_varname <- function(filename) {
  # Extracts one of: bio1..bio19, gdd, tasmin, tasmax, tas, pr
  m <- stringr::str_match(
    basename(filename),
    "(bio\\d{1,2}|gdd|tasmin|tasmax|tas|pr)"
  )[, 2]
  tolower(m)
}

extract_midyear_hist <- function(filename) {
  # Extract midyear; prefer 2010/2014/2018/2022 if present, else try range mid or first year
  allowed <- c(2010L, 2014L, 2018L, 2022L)
  yrs <- stringr::str_extract_all(basename(filename), "\\d{4}")[[1]]
  yrs <- suppressWarnings(as.integer(yrs))
  hit <- yrs[yrs %in% allowed]
  if (length(hit) >= 1) {
    return(hit[1])
  }
  if (length(yrs) >= 2 && is.finite(yrs[1]) && is.finite(yrs[2])) {
    return(as.integer(round((yrs[1] + yrs[2]) / 2)))
  }
  if (length(yrs) >= 1) {
    return(yrs[1])
  }
  NA_integer_
}

# ------------------------ gdalwarp wrapper (logs + retries) -------------------
safe_gdalwarp_to_ref_cli <- function(
  src_vrt,
  ref,
  outpath,
  co,
  add_dstnodata = FALSE,
  tag = NULL
) {
  # Run gdalwarp against a reference grid; retry once with -dstnodata if first run fails
  if (!has_gdalwarp()) {
    stop("gdalwarp not found on PATH.")
  }
  dir.create(dirname(outpath), showWarnings = FALSE, recursive = TRUE)

  ex <- ext(ref)
  ncols <- ncol(ref)
  nrows <- nrow(ref)

  tmpfile <- file.path(
    scratch,
    paste0("gw_", sprintf("%08x", as.integer(runif(1, 0, 2^31 - 1))), ".tif")
  )
  on.exit(try(unlink(tmpfile), silent = TRUE), add = TRUE)

  # Creation options
  co_args <- as.vector(rbind(rep("-co", length(co)), co))

  # Warp memory configuration (use bytes for robustness)
  wm_mb_env <- suppressWarnings(as.integer(Sys.getenv(
    "GDAL_WARP_MEMORY_MB",
    ""
  )))
  wm_mb_fbk <- suppressWarnings(as.integer(Sys.getenv("GDAL_CACHEMAX", "4096")))
  wm_mb <- if (!is.na(wm_mb_env) && wm_mb_env > 0) {
    wm_mb_env
  } else if (!is.na(wm_mb_fbk) && wm_mb_fbk > 0) {
    wm_mb_fbk
  } else {
    4096L
  }
  wm_bytes <- as.character(as.double(wm_mb) * 1048576.0)

  args <- c(
    "-overwrite",
    "-r",
    "bilinear",
    "-multi",
    "-t_srs",
    "EPSG:4326",
    "-te_srs",
    "EPSG:4326",
    "-te",
    sprintf("%.15f", ex$xmin),
    sprintf("%.15f", ex$ymin),
    sprintf("%.15f", ex$xmax),
    sprintf("%.15f", ex$ymax),
    "-ts",
    as.character(ncols),
    as.character(nrows),
    "-wm",
    wm_bytes,
    "-wo",
    "NUM_THREADS=ALL_CPUS",
    co_args
  )
  if (isTRUE(add_dstnodata)) {
    args <- c(args, "-dstnodata", "nan")
  }
  args <- c(args, src_vrt, tmpfile)

  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  outname <- if (!is.null(tag)) tag else basename(outpath)
  logf <- file.path("logs", paste0("gdalwarp_", ts, "_", outname, ".log"))

  status <- system2(
    command = Sys.which("gdalwarp"),
    args = args,
    stdout = logf,
    stderr = logf
  )
  if (status != 0L || !file.exists(tmpfile)) {
    if (!isTRUE(add_dstnodata)) {
      message("gdalwarp failed (see ", logf, "). Retrying with -dstnodata …")
      return(safe_gdalwarp_to_ref_cli(
        src_vrt,
        ref,
        outpath,
        co,
        add_dstnodata = TRUE,
        tag = outname
      ))
    }
    stop("gdalwarp failed for: ", src_vrt, " (see ", logf, ")")
  }

  if (file.exists(outpath)) {
    try(unlink(outpath), silent = TRUE)
  }
  ok <- tryCatch(
    file.copy(tmpfile, outpath, overwrite = TRUE),
    error = function(e) FALSE
  )
  if (!ok) {
    Sys.sleep(0.5)
    if (file.exists(outpath)) {
      try(unlink(outpath), silent = TRUE)
    }
    ok2 <- tryCatch(
      file.copy(tmpfile, outpath, overwrite = TRUE),
      error = function(e) FALSE
    )
    if (!ok2) stop(sprintf("Failed to copy '%s' to '%s'.", tmpfile, outpath))
  }
  invisible(TRUE)
}

# ----------------------------- Reference --------------------------------------
ref <- rast(ref_grid_path)
ref <- ensure_wgs84(ref)

# ------------------------ YEARLY aggregation via VRT + gdalwarp ---------------
run_yearly <- function() {
  # Determine which outputs already exist
  existing <- list.files(out_yearly, "\\.tif$", full.names = FALSE)
  m_exist <- str_match(existing, "^(.+?)_(\\d{4})_(ssp\\d{3})\\.tif$")
  done_y <- tibble(
    variable = tolower(m_exist[, 2]),
    mid_year = suppressWarnings(as.integer(m_exist[, 3])),
    ssp = tolower(m_exist[, 4])
  ) %>%
    filter(!is.na(variable))

  nc_files <- list.files(
    nc_folder,
    "\\.nc$",
    recursive = TRUE,
    full.names = TRUE
  )
  meta_list <- lapply(nc_files, function(fp) {
    # Parse metadata from filename
    bn <- basename(fp)
    parts <- str_split(bn, "_", simplify = TRUE)
    yrs <- str_match(bn, "_(\\d{4})-\\d{2}-\\d{2}_(\\d{4})-")[, 2:3]
    tibble(
      path = fp,
      gcm = tolower(parts[3]),
      variable = tolower(parts[4]),
      ssp = tolower(parts[5]),
      year_from = suppressWarnings(as.integer(yrs[1])),
      year_to = suppressWarnings(as.integer(yrs[2]))
    )
  })
  dfy <- bind_rows(meta_list) %>%
    filter(gcm %in% gcms, variable %in% tolower(vars_yearly), ssp %in% ssps)

  combos <- dfy %>%
    distinct(variable, ssp, year_from, year_to) %>%
    mutate(mid_year = year_from + 15L)
  to_do_y <- anti_join(combos, done_y, by = c("variable", "mid_year", "ssp"))

  if (file.exists(track_file)) {
    file.remove(track_file)
  }
  file.create(track_file)

  for (i in seq_len(nrow(to_do_y))) {
    vr <- to_do_y$variable[i]
    sp <- to_do_y$ssp[i]
    yf <- to_do_y$year_from[i]
    yt <- to_do_y$year_to[i]
    midY <- to_do_y$mid_year[i]

    sub <- filter(
      dfy,
      variable == vr,
      ssp == sp,
      year_from == yf,
      year_to == yt
    )
    if (!nrow(sub)) {
      next
    }

    # Log missing GCMs
    missing_gc <- setdiff(gcms, unique(sub$gcm))
    if (length(missing_gc) > 0) {
      msg <- sprintf(
        "%s %d-%d %s missing: %s (proceeding with %d/%d GCMs)",
        vr,
        yf,
        yt,
        sp,
        paste(missing_gc, collapse = ", "),
        length(unique(sub$gcm)),
        length(gcms)
      )
      write(msg, file = track_file, append = TRUE)
      message(msg)
    }
    # Skip when full GCM coverage is required and missing
    if (isTRUE(require_full_gcm) && !has_full_gcm(sub$gcm, gcms)) {
      msg <- sprintf(
        "%s %d-%d %s skipped (requires full GCM set): missing %s",
        vr,
        yf,
        yt,
        sp,
        paste(missing_gc, collapse = ", ")
      )
      write(msg, file = track_file, append = TRUE)
      message(msg)
      next
    }

    # Probe geometry from first dataset
    first_ds <- netcdf_gdal_path(sub$path[1], vr)
    r0 <- rast(first_ds)
    r0 <- ensure_wgs84(r0)
    width <- ncol(r0)
    height <- nrow(r0)
    e <- ext(r0)
    rm(r0)

    if (vr == "pr") {
      rows <- list()
      for (p in sub$path) {
        nly <- get_nly(p, vr)
        if (is.na(nly) || nly < 1) {
          next
        }
        for (b in seq_len(nly)) {
          rows[[length(rows) + 1L]] <- data.frame(
            srcfile = netcdf_gdal_path(p, vr),
            srcband = b,
            scale_offset = NA_real_,
            scale_ratio = 1.0 / length(unique(sub$gcm)),
            stringsAsFactors = FALSE
          )
        }
      }
      band_specs <- list(if (length(rows)) do.call(rbind, rows) else NULL)
    } else if (vr %in% c("tas", "tasmin", "tasmax")) {
      rows <- list()
      n_gcm <- length(unique(sub$gcm))
      for (p in sub$path) {
        nly <- get_nly(p, vr)
        if (is.na(nly) || nly < 1) {
          next
        }
        ratio <- 1.0 / (nly * n_gcm) # average across (months × GCMs) over the 30-yr climatology
        for (b in seq_len(nly)) {
          rows[[length(rows) + 1L]] <- data.frame(
            srcfile = netcdf_gdal_path(p, vr),
            srcband = b,
            scale_offset = NA_real_,
            scale_ratio = ratio,
            stringsAsFactors = FALSE
          )
        }
      }
      band_specs <- list(if (length(rows)) do.call(rbind, rows) else NULL)
    } else {
      rows <- lapply(sub$path, function(p) {
        data.frame(
          srcfile = netcdf_gdal_path(p, vr),
          srcband = 1L,
          scale_offset = NA_real_,
          scale_ratio = 1.0 / length(unique(sub$gcm)),
          stringsAsFactors = FALSE
        )
      })
      band_specs <- list(if (length(rows)) do.call(rbind, rows) else NULL)
    }

    vrt_path <- file.path(scratch, sprintf("yr_%s_%s_%d.vrt", vr, sp, midY))
    vrt_xml <- build_vrt_sum_xml(
      width,
      height,
      e$xmin,
      e$xmax,
      e$ymin,
      e$ymax,
      srs = "EPSG:4326",
      band_specs = band_specs
    )
    write_vrt(vrt_xml, vrt_path)

    outname <- sprintf("%s_%d_%s.tif", vr, midY, sp)
    outpath <- file.path(out_yearly, outname)
    co <- co_gtiff_for(vr)

    message("Yearly warp ", vr, "/", sp, " midY ", midY)
    safe_gdalwarp_to_ref_cli(vrt_path, ref, outpath, co = co, tag = outname)

    try(unlink(vrt_path), silent = TRUE)
    invisible(gc())
    message("Wrote: ", outname)
  }
  message("Yearly aggregation done.")
}

# -------- MONTHLY 30-year MEANS (midyear naming): one output per month --------
run_monthly_30yr_means <- function() {
  scale_days_for_pr <- TRUE # only used for pr with 360-band inputs

  # Helper: choose exactly one file per GCM deterministically
  pick_one_per_gcm <- function(df) {
    # Prefer entries that look like a primary member; else lexicographic first
    df %>%
      mutate(
        pref = ifelse(grepl("r1i1", basename(path), ignore.case = TRUE), 0L, 1L)
      ) %>%
      arrange(gcm, pref, path) %>%
      distinct(gcm, .keep_all = TRUE) %>%
      select(path, gcm, variable, ssp, year_from)
  }

  nc_files <- list.files(
    nc_folder,
    "\\.nc$",
    recursive = TRUE,
    full.names = TRUE
  )
  nc_meta <- purrr::map_dfr(nc_files, function(fp) {
    # Parse meta per file for monthly stage
    bn <- basename(fp)
    parts <- stringr::str_split(bn, "_", simplify = TRUE)
    yfrom <- suppressWarnings(as.integer(stringr::str_match(bn, "_(\\d{4})-")[,
      2
    ]))
    tibble(
      path = fp,
      gcm = tolower(parts[3]),
      variable = tolower(parts[4]),
      ssp = tolower(parts[5]),
      year_from = yfrom
    )
  }) %>%
    dplyr::filter(variable %in% vars_monthly, ssp %in% ssps, gcm %in% gcms)

  for (var in vars_monthly) {
    for (ssp in ssps) {
      sub_all <- dplyr::filter(nc_meta, variable == var, ssp == ssp)
      if (!nrow(sub_all)) {
        next
      }

      yrs <- sort(unique(sub_all$year_from))
      for (yfrom in yrs) {
        # Start from all files that match var/ssp/window
        sub0 <- dplyr::filter(sub_all, year_from == yfrom)
        if (!nrow(sub0)) {
          next
        }

        # Enforce exactly one file per GCM
        sub <- pick_one_per_gcm(sub0)

        # Require full 5/5 GCM coverage
        missing_gc <- setdiff(gcms, unique(sub$gcm))
        if (length(missing_gc) > 0) {
          msg <- sprintf(
            "%s %d-%d %s missing: %s (have %d/%d) -> SKIP",
            var,
            yfrom,
            yfrom + 29L,
            ssp,
            paste(missing_gc, collapse = ", "),
            length(unique(sub$gcm)),
            length(gcms)
          )
          write(msg, file = track_file, append = TRUE)
          message(msg)
          next
        }

        # Probe geometry from the first dataset
        ds_path <- netcdf_gdal_path(sub$path[1], var)
        r0 <- rast(ds_path)
        r0 <- ensure_wgs84(r0)
        width <- ncol(r0)
        height <- nrow(r0)
        e <- ext(r0)
        rm(r0)

        n_gcm <- length(unique(sub$gcm))
        midY <- yfrom + 15L

        # Hard assertion for tas/tasmin/tasmax: exactly 12 bands
        if (var %in% c("tas", "tasmin", "tasmax")) {
          nly_vec <- vapply(
            sub$path,
            function(p) as.integer(get_nly(p, var)),
            integer(1),
            USE.NAMES = FALSE
          )
          bad12 <- which(is.na(nly_vec) | nly_vec != 12L)
          if (length(bad12)) {
            msg <- sprintf(
              "%s %d-%d %s SKIP: non-12-band file(s): %s",
              var,
              yfrom,
              yfrom + 29L,
              ssp,
              paste(basename(sub$path[bad12]), collapse = ", ")
            )
            write(msg, file = track_file, append = TRUE)
            message(msg)
            next
          }
        }

        for (m in 1:12) {
          outname <- sprintf("%s_%04d_%s_%02d.tif", var, midY, ssp, m)
          outpath <- file.path(out_monthly, outname)
          if (file.exists(outpath)) {
            next
          }

          rows_m <- list()

          if (var == "pr") {
            # pr: allow both 12-band (already month means/totals) and 360-band inputs
            for (p in sub$path) {
              nly <- as.integer(get_nly(p, var))
              if (is.na(nly) || nly < 1) {
                next
              }
              if (nly <= 12) {
                rows_m[[length(rows_m) + 1L]] <- data.frame(
                  srcfile = netcdf_gdal_path(p, var),
                  srcband = m,
                  scale_offset = NA_real_,
                  scale_ratio = 1.0 / n_gcm, # average across GCMs
                  stringsAsFactors = FALSE
                )
              } else {
                # 360-band: monthly mm/day -> monthly total via days_in_month; then 30-yr mean
                for (yy in yfrom:(yfrom + 29L)) {
                  bidx <- ((yy - yfrom) * 12L + m)
                  base <- if (scale_days_for_pr) {
                    as.numeric(days_in_yymm(yy, m))
                  } else {
                    1.0
                  }
                  rows_m[[length(rows_m) + 1L]] <- data.frame(
                    srcfile = netcdf_gdal_path(p, var),
                    srcband = bidx,
                    scale_offset = NA_real_,
                    scale_ratio = base / (n_gcm * 30.0),
                    stringsAsFactors = FALSE
                  )
                }
              }
            }
          } else if (var %in% c("tas", "tasmin", "tasmax")) {
            # tas*: strictly 12-band inputs; equal-weight mean across the 5 GCMs
            for (p in sub$path) {
              rows_m[[length(rows_m) + 1L]] <- data.frame(
                srcfile = netcdf_gdal_path(p, var),
                srcband = m,
                scale_offset = NA_real_, # no unit conversion
                scale_ratio = 1.0 / n_gcm, # equal weights across GCMs
                stringsAsFactors = FALSE
              )
            }
          } else {
            # Fallback for other variables (not expected here)
            for (p in sub$path) {
              rows_m[[length(rows_m) + 1L]] <- data.frame(
                srcfile = netcdf_gdal_path(p, var),
                srcband = m,
                scale_offset = NA_real_,
                scale_ratio = 1.0 / n_gcm,
                stringsAsFactors = FALSE
              )
            }
          }

          if (!length(rows_m)) {
            msg <- sprintf(
              "No sources for 30yr mean: %s %s midY=%d m=%02d",
              var,
              ssp,
              midY,
              m
            )
            write(msg, file = track_file, append = TRUE)
            message(msg)
            next
          }

          df_m <- do.call(rbind, rows_m)

          # Safety: for tas* the weights must sum to 1 (unitless average). For pr(360) this check is not applicable.
          if (var %in% c("tas", "tasmin", "tasmax")) {
            wsum <- sum(df_m$scale_ratio)
            if (!is.finite(wsum) || abs(wsum - 1) > 1e-6) {
              msg <- sprintf(
                "%s %d-%d %s m=%02d SKIP: weights sum = %.6f (expected 1.0)",
                var,
                yfrom,
                yfrom + 29L,
                ssp,
                m,
                wsum
              )
              write(msg, file = track_file, append = TRUE)
              message(msg)
              next
            }
          }

          vrt_path <- file.path(
            scratch,
            sprintf("mo30_%s_%04d_%s_%02d.vrt", var, midY, ssp, m)
          )
          vrt_xml <- build_vrt_sum_xml(
            width,
            height,
            e$xmin,
            e$xmax,
            e$ymin,
            e$ymax,
            srs = "EPSG:4326",
            band_specs = list(df_m)
          )
          write_vrt(vrt_xml, vrt_path)

          co <- co_gtiff_for(var)
          message(
            "Monthly 30yr mean ",
            var,
            "/",
            ssp,
            " midY ",
            midY,
            " m=",
            sprintf("%02d", m)
          )
          t0 <- Sys.time()
          safe_gdalwarp_to_ref_cli(
            vrt_path,
            ref,
            outpath,
            co = co,
            tag = outname
          )
          dt <- as.numeric(difftime(Sys.time(), t0, units = "mins"))
          message(sprintf("Wrote: %s (%.1f min)", outname, dt))

          try(unlink(vrt_path), silent = TRUE)
          invisible(gc())
        }
        message("Finished 30yr means ", var, "/", ssp, " midY ", midY)
      }
    }
  }
  message("Monthly 30yr mean aggregation done.")
}

# ------------------------ HISTORIC (Peru) processing --------------------------
run_historic_peru <- function() {
  # Process single-file timesteps from historic_peru; no GCM averaging; SSP token "historical"
  if (!dir.exists(hist_folder)) {
    message("Historic folder not found: ", hist_folder, " — skipping.")
    return(invisible(NULL))
  }

  nc_files <- list.files(
    hist_folder,
    "\\.nc$",
    recursive = TRUE,
    full.names = TRUE
  )
  if (!length(nc_files)) {
    message("No historic .nc files found in ", hist_folder, " — skipping.")
    return(invisible(NULL))
  }

  meta <- purrr::map_dfr(nc_files, function(fp) {
    var <- detect_varname(fp)
    mid <- extract_midyear_hist(fp)
    tibble(path = fp, variable = var, mid_year = mid)
  }) %>%
    dplyr::filter(!is.na(variable), !is.na(mid_year))

  if (!nrow(meta)) {
    message(
      "No usable historic files after parsing variable/midyear — skipping."
    )
    return(invisible(NULL))
  }

  sp_hist <- "historical"

  for (i in seq_len(nrow(meta))) {
    fp <- meta$path[i]
    vr <- meta$variable[i]
    mid <- meta$mid_year[i]

    ds <- netcdf_gdal_path(fp, vr)
    r0 <- tryCatch(rast(ds), error = function(e) NULL)
    if (is.null(r0)) {
      message("Could not open ", fp, ":", vr)
      next
    }
    r0 <- ensure_wgs84(r0)
    width <- ncol(r0)
    height <- nrow(r0)
    e <- ext(r0)
    nly <- nlyr(r0)
    rm(r0)

    # ---------------- Yearly output ----------------
    outname_y <- sprintf("%s_%d_%s.tif", vr, mid, sp_hist)
    outpath_y <- file.path(out_yearly, outname_y)
    if (!file.exists(outpath_y)) {
      if (vr %in% c("tas", "tasmin", "tasmax")) {
        if (is.finite(nly) && nly >= 12L) {
          rows <- data.frame(
            srcfile = ds,
            srcband = 1:12,
            scale_offset = NA_real_,
            scale_ratio = rep(1.0 / 12.0, 12L), # mean of 12 months
            stringsAsFactors = FALSE
          )
        } else {
          rows <- data.frame(
            srcfile = ds,
            srcband = 1L,
            scale_offset = NA_real_,
            scale_ratio = 1.0,
            stringsAsFactors = FALSE
          )
        }
      } else if (vr == "pr") {
        if (is.finite(nly) && nly >= 12L) {
          rows <- data.frame(
            srcfile = ds,
            srcband = 1:12,
            scale_offset = NA_real_,
            scale_ratio = 1.0, # sum of 12 months
            stringsAsFactors = FALSE
          )
        } else {
          rows <- data.frame(
            srcfile = ds,
            srcband = 1L,
            scale_offset = NA_real_,
            scale_ratio = 1.0,
            stringsAsFactors = FALSE
          )
        }
      } else {
        rows <- data.frame(
          srcfile = ds,
          srcband = 1L,
          scale_offset = NA_real_,
          scale_ratio = 1.0, # single-band
          stringsAsFactors = FALSE
        )
      }

      vrt_y <- file.path(scratch, sprintf("hist_y_%s_%d.vrt", vr, mid))
      xml_y <- build_vrt_sum_xml(
        width,
        height,
        e$xmin,
        e$xmax,
        e$ymin,
        e$ymax,
        srs = "EPSG:4326",
        band_specs = list(rows)
      )
      write_vrt(xml_y, vrt_y)
      co <- co_gtiff_for(vr)
      message("Historic yearly warp ", vr, " midY ", mid)
      safe_gdalwarp_to_ref_cli(vrt_y, ref, outpath_y, co = co, tag = outname_y)
      try(unlink(vrt_y), silent = TRUE)
      invisible(gc())
      message("Wrote: ", outname_y)
    }

    # ---------------- Monthly outputs (if 12-band) ----------------
    if (
      vr %in% c("tas", "tasmin", "tasmax", "pr") && is.finite(nly) && nly >= 12L
    ) {
      for (m in 1:12) {
        outname_m <- sprintf("%s_%04d_%s_%02d.tif", vr, mid, sp_hist, m)
        outpath_m <- file.path(out_monthly, outname_m)
        if (file.exists(outpath_m)) {
          next
        }

        rows_m <- data.frame(
          srcfile = ds,
          srcband = m,
          scale_offset = NA_real_,
          scale_ratio = 1.0, # pass-through for month m
          stringsAsFactors = FALSE
        )

        vrt_m <- file.path(
          scratch,
          sprintf("hist_m_%s_%d_%02d.vrt", vr, mid, m)
        )
        xml_m <- build_vrt_sum_xml(
          width,
          height,
          e$xmin,
          e$xmax,
          e$ymin,
          e$ymax,
          srs = "EPSG:4326",
          band_specs = list(rows_m)
        )
        write_vrt(xml_m, vrt_m)
        co <- co_gtiff_for(vr)
        message(
          "Historic monthly warp ",
          vr,
          " midY ",
          mid,
          " m=",
          sprintf("%02d", m)
        )
        safe_gdalwarp_to_ref_cli(
          vrt_m,
          ref,
          outpath_m,
          co = co,
          tag = outname_m
        )
        try(unlink(vrt_m), silent = TRUE)
        invisible(gc())
      }
      message("Finished historic monthly ", vr, " midY ", mid)
    }
  }

  message("Historic Peru processing done.")
}

# ----------------------------- Driver -----------------------------------------
#run_yearly()
#run_monthly_30yr_means()
run_historic_peru() # NEW: run historic processing

#!/usr/bin/env Rscript
#' Post-hoc national-mosaic assembler (Phase 3.6 D-03 / D-05).
#'
#' Stitches the per-region `region_<suffix>/posterior.tif` files produced by the
#' per-region parallel allocation jobs (Plan 02 launcher) into a national
#' `posterior_<year>.tif` for EVERY timestep in `simulation_year_steps`.
#'
#' Under per-region parallel jobs (D-01) the single-region driver path (Plan 01
#' D-05) deliberately no longer writes the shared national mosaic — each region
#' chains on its OWN posterior so concurrent regions cannot clobber a shared
#' path. This assembler therefore becomes the SOLE writer of the national
#' `posterior_<year>.tif` (D-03). It lifts the merge/extend/write logic that was
#' formerly inline in `run_allocation_one_timestep` (`src/allocation.r`) verbatim
#' (same datatype="INT2U" / COMPRESS=LZW) so the national series is
#' byte-comparable to the inline writer it replaces.
#'
#' It is the `afterok` target the Plan 02 launcher
#' (`scripts/submit_allocation_scenario.sh`) queues after all region jobs finish,
#' invoked via the thin wrapper `scripts/submit_assemble_mosaic.sh`.
#'
#' Usage:
#'   Rscript scripts/assemble_national_mosaic.r [<scenario>] [--scenario <name>] [--year <year>]
#'
#'   <scenario> / --scenario  Scenario name (default: BAU). Positional or flag.
#'   --year                   Assemble only this single posterior year
#'                            (default: every posterior year in the schedule).

# ---------------------------------------------------------------------------
# Set working directory to project root (must happen before sourcing src/*.r).
# Mirrors scripts/diagnose_allocation_saturation.r:26-37.
# ---------------------------------------------------------------------------
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") {
    project_root <- dirname(project_root)
  }
}
setwd(project_root)

# ---------------------------------------------------------------------------
# Source helpers. Order matters: setup gives get_config(), utils gives
# log_msg()/ensure_dir(), allocation gives the write_raster_atomic() helper
# (Plan 01 Task 3) reused here for atomic .tmp.tif -> file.rename writes.
# Mirrors scripts/diagnose_allocation_saturation.r:43-60.
# ---------------------------------------------------------------------------
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/allocation.r"
)

for (src_file in src_files) {
  cat(sprintf("Sourcing %s...\n", src_file))
  tryCatch(
    {
      source(src_file)
      cat(sprintf("%s sourced successfully.\n", src_file))
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, e$message))
    }
  )
}
cat("\n")

usage <- paste(
  "Usage: Rscript scripts/assemble_national_mosaic.r",
  "[<scenario>] [--scenario <name>] [--year <year>]"
)

# ---------------------------------------------------------------------------
# Argument parsing: positional <scenario> + --scenario / --year flags.
# Mirrors the positional + while-loop flag parser shape of
# scripts/diagnose_allocation_saturation.r:63-87.
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
scenario <- NULL
single_year <- NULL
i <- 1L
while (i <= length(args)) {
  arg <- args[[i]]
  if (identical(arg, "--scenario")) {
    if (i + 1L > length(args)) {
      stop("--scenario requires a name argument", call. = FALSE)
    }
    scenario <- args[[i + 1L]]
    i <- i + 1L
  } else if (startsWith(arg, "--scenario=")) {
    scenario <- sub("^--scenario=", "", arg)
  } else if (identical(arg, "--year")) {
    if (i + 1L > length(args)) {
      stop("--year requires a year argument", call. = FALSE)
    }
    single_year <- as.integer(args[[i + 1L]])
    i <- i + 1L
  } else if (startsWith(arg, "--year=")) {
    single_year <- as.integer(sub("^--year=", "", arg))
  } else if (startsWith(arg, "--")) {
    stop(sprintf("Unknown argument: %s\n%s", arg, usage), call. = FALSE)
  } else if (is.null(scenario)) {
    # First bare positional is the scenario.
    scenario <- arg
  } else {
    stop(sprintf("Unexpected positional argument: %s\n%s", arg, usage),
      call. = FALSE)
  }
  i <- i + 1L
}
if (is.null(scenario)) scenario <- "NAT"
if (!is.null(single_year) && is.na(single_year)) {
  stop("--year must be an integer year", call. = FALSE)
}

# ---------------------------------------------------------------------------
# Resolve config-derived paths + schedule + region labels.
# ---------------------------------------------------------------------------
config <- get_config()

simulation_output_dir <- config[["simulation_output_dir"]]
if (is.null(simulation_output_dir) || !nzchar(simulation_output_dir)) {
  stop("config[[\"simulation_output_dir\"]] is missing or empty.", call. = FALSE)
}

# Authoritative schedule (D-11). Posterior years are the TAIL of the schedule —
# each timestep writes posterior_<year_end>.
year_steps <- config[["simulation_year_steps"]]
if (is.null(year_steps) || length(year_steps) < 2L) {
  stop(paste(
    "config[[\"simulation_year_steps\"]] is missing or has < 2 boundaries.",
    "Add an explicit ordered list to your config YAML — see 03.2-CONTEXT.md D-01."
  ), call. = FALSE)
}
# IN-03: cast to integer to match run_manifest.r (as.integer(tail(...))) so the two
# capstone artifacts agree on year typing even if a non-integral boundary slips in.
posterior_years <- as.integer(tail(year_steps, -1))
if (!is.null(single_year)) {
  if (!single_year %in% posterior_years) {
    stop(sprintf(
      "--year %d is not a posterior year in simulation_year_steps (%s).",
      single_year, paste(posterior_years, collapse = ", ")
    ), call. = FALSE)
  }
  posterior_years <- single_year
}

# Region labels from regions.json under config[["reg_dir"]] (authoritative on
# HPC scratch; empty in the repo checkout). Same source the driver
# (src/allocation.r:1440) and the launcher probe use.
reg_json <- file.path(config[["reg_dir"]], "regions.json")
if (!file.exists(reg_json)) {
  stop(sprintf(
    "regions.json not found at %s — it is authoritative only on HPC scratch.",
    reg_json
  ), call. = FALSE)
}
regions <- jsonlite::fromJSON(reg_json)
region_labels <- regions[["label"]]
region_labels <- region_labels[!is.na(region_labels) & nzchar(region_labels)]
if (length(region_labels) == 0L) {
  stop("regions.json declared zero usable region labels.", call. = FALSE)
}
# region_suffix == gsub(" ", "_", tolower()) (src/allocation.r:862).
region_suffixes <- gsub(" ", "_", tolower(region_labels))

# Full national extent: derive from the initial-LULC raster for the simulation
# start year (config[["aggregated_lulc_dir"]]). This is the SAME national grid
# the inline writer extended every per-region posterior to each timestep (its
# current_lulc_path was the previous national mosaic, itself carrying the
# initial extent), so the national series stays byte-comparable.
start_year <- config[["simulation_start_year"]]
lulc_files <- list.files(
  config[["aggregated_lulc_dir"]],
  pattern = "\\.tif$",
  full.names = TRUE
)
# IN-02: anchor the year match to the file NAME with non-digit boundaries (not a bare
# substring of the full path) and assert a UNIQUE hit, so a path component containing
# the year digits elsewhere cannot silently select the wrong national extent.
initial_lulc_file <- lulc_files[grepl(
  sprintf("(^|[^0-9])%d([^0-9]|$)", as.integer(start_year)),
  basename(lulc_files)
)]
if (length(initial_lulc_file) != 1L) {
  stop(sprintf(
    "Expected exactly one initial LULC raster for start year %d in %s, found %d: %s",
    start_year, config[["aggregated_lulc_dir"]], length(initial_lulc_file),
    paste(basename(initial_lulc_file), collapse = ", ")
  ), call. = FALSE)
}
full_extent <- terra::ext(terra::rast(initial_lulc_file))

log_msg(sprintf(
  "Assembling national mosaics: scenario=%s years=%s regions=%d",
  scenario, paste(posterior_years, collapse = ","), length(region_suffixes)
))

# ---------------------------------------------------------------------------
# Per-timestep assembly.
# ---------------------------------------------------------------------------
assembled <- 0L
skipped <- 0L

for (year in posterior_years) {
  timestep_dir <- file.path(simulation_output_dir, scenario, as.character(year))

  posterior_paths <- file.path(
    timestep_dir,
    paste0("region_", region_suffixes),
    "posterior.tif"
  )
  missing <- !file.exists(posterior_paths)

  if (any(missing)) {
    # Refuse to write a partial national mosaic (T-036-08). Partial completeness
    # is the manifest's job (Plan 04); the assembler logs and skips.
    for (suffix in region_suffixes[missing]) {
      log_msg(sprintf(
        "AUDIT stage=mosaic scenario=%s year=%d region=%s status=missing",
        scenario, year, suffix
      ))
    }
    log_msg(sprintf(
      "AUDIT stage=mosaic scenario=%s year=%d status=skipped missing=%d",
      scenario, year, sum(missing)
    ))
    skipped <- skipped + 1L
    next
  }

  # Lift the inline merge/extend/write block verbatim (src/allocation.r mosaic
  # branch). Read each per-region posterior, extend to the national full extent,
  # then merge (single-region passthrough when only one region).
  region_rasters <- lapply(posterior_paths, terra::rast)
  region_rasters <- lapply(region_rasters, function(r) {
    terra::extend(r, full_extent)
  })

  if (length(region_rasters) == 1L) {
    mosaiced <- region_rasters[[1]]
  } else {
    mosaiced <- do.call(terra::merge, region_rasters)
  }

  # National output path: the region-agnostic posterior_<year>.tif the inline
  # writer used (src/allocation.r). Written atomically (T-036-07) via the
  # write_raster_atomic helper from Plan 01, with identical datatype/compression
  # so the national series is byte-comparable.
  output_path <- file.path(timestep_dir, sprintf("posterior_%d.tif", year))
  write_raster_atomic(
    mosaiced,
    output_path,
    list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
  )

  log_msg(sprintf(
    "AUDIT stage=mosaic scenario=%s year=%d regions=%d output=%s status=ok",
    scenario, year, length(region_rasters), output_path
  ))
  assembled <- assembled + 1L

  # Free rasters + gc() between years to bound memory.
  rm(region_rasters, mosaiced)
  gc(verbose = FALSE)
}

log_msg(sprintf(
  "STATE stage=mosaic scenario=%s assembled=%d skipped=%d total=%d",
  scenario, assembled, skipped, length(posterior_years)
))

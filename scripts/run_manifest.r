#!/usr/bin/env Rscript
#' Run-manifest / completeness + plausibility checker (Phase 3.6 D-08 / D-08b).
#'
#' The durable capstone evidence that the single-scenario end-to-end run is
#' complete -- declared from DATA, not log eyeballing. For the FULL
#' region x timestep matrix it:
#'
#'   1. EXISTENCE (D-06)            -- asserts every region_<suffix>/posterior.tif
#'                                     exists and loads as a valid GeoTIFF, and
#'                                     that count == regions x timesteps.
#'   2. DIFFERS-FROM-ANTERIOR (D-06 blocking bar)
#'                                  -- loads each cell's anterior.tif + posterior.tif
#'                                     and fails the cell if ZERO cells changed
#'                                     (rules out the degenerate "nothing happened").
#'   3. SATURATION (D-07, report-only)
#'                                  -- aggregates each cell's saturation_summary.csv
#'                                     (met/exempt/failed) and emits one
#'                                     `AUDIT stage=saturation ...` line per cell.
#'                                     Saturation NEVER gates the verdict.
#'   4. PLAUSIBILITY (D-08b)        -- per-class LULC areas via terra::freq(): HARD
#'                                     all-one-class degeneracy escalates to
#'                                     INCOMPLETE. (The former SOFT demand-mismatch
#'                                     vs simulation_lulc_areas_2060.csv was removed:
#'                                     achieved areas legitimately differ from that
#'                                     upstream Stage-4 demand input.)
#'   5. NATIONAL MOSAICS (D-03)     -- confirms posterior_<year>.tif exists per timestep.
#'
#' It writes a manifest CSV
#'   <simulation_output_dir>/<scenario>/run_manifest.csv
#' and emits a single structured verdict line
#'   STATE manifest verdict=PASS|INCOMPLETE scenario=... cells=n/expected ...
#' then sets the process exit code (0 = PASS, non-zero = INCOMPLETE) so a wrapping
#' shell (scripts/verify_allocation_run.sh) can gate on it.
#'
#' Usage:
#'   Rscript scripts/run_manifest.r [<scenario>] [--scenario <name>]
#'
#'   <scenario> / --scenario  Scenario name (default: BAU). Positional or flag.

# ---------------------------------------------------------------------------
# Set working directory to project root (must happen before sourcing src/*.r).
# Mirrors scripts/diagnose_allocation_saturation.r:26-37 /
# scripts/assemble_national_mosaic.r:32-43.
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
# log_msg(), saturation_diagnostics gives the AUDIT-line grammar conventions.
# Mirrors scripts/diagnose_allocation_saturation.r:43-60.
# ---------------------------------------------------------------------------
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/saturation_diagnostics.r"
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
  "Usage: Rscript scripts/run_manifest.r",
  "[<scenario>] [--scenario <name>]"
)

# ---------------------------------------------------------------------------
# Argument parsing: positional <scenario> + --scenario flag.
# Mirrors the positional + while-loop flag parser shape of
# scripts/diagnose_allocation_saturation.r:63-87.
# ---------------------------------------------------------------------------
args <- commandArgs(trailingOnly = TRUE)
scenario <- NULL
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
if (is.null(scenario)) scenario <- "BAU"

# ---------------------------------------------------------------------------
# Resolve config-derived paths + schedule + region labels + saturation threshold.
# ---------------------------------------------------------------------------
config <- get_config()

simulation_output_dir <- config[["simulation_output_dir"]]
if (is.null(simulation_output_dir) || !nzchar(simulation_output_dir)) {
  stop("config[[\"simulation_output_dir\"]] is missing or empty.", call. = FALSE)
}

# Authoritative schedule (D-11). Posterior years are the TAIL of the schedule --
# each timestep writes posterior_<year_end>. Built from simulation_year_steps,
# NOT a hardcoded single cell.
year_steps <- config[["simulation_year_steps"]]
if (is.null(year_steps) || length(year_steps) < 2L) {
  stop(paste(
    "config[[\"simulation_year_steps\"]] is missing or has < 2 boundaries.",
    "Add an explicit ordered list to your config YAML -- see 03.2-CONTEXT.md D-01."
  ), call. = FALSE)
}
posterior_years <- as.integer(tail(year_steps, -1))

# Region labels from regions.json under config[["reg_dir"]] (authoritative on
# HPC scratch; empty in the repo checkout). Same source the driver
# (src/allocation.r) and the launcher probe use.
reg_json <- file.path(config[["reg_dir"]], "regions.json")
if (!file.exists(reg_json)) {
  stop(sprintf(
    "regions.json not found at %s -- it is authoritative only on HPC scratch.",
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

# Saturation threshold (D-07, report-only). Same key the inline writer uses
# (src/saturation_diagnostics.r:238).
saturation_threshold <-
  config[["simulation_trans_rates_params"]][["saturation_threshold"]]
if (is.null(saturation_threshold)) saturation_threshold <- 0.90

n_regions <- length(region_suffixes)
n_timesteps <- length(posterior_years)
expected_cells <- n_regions * n_timesteps

log_msg(sprintf(
  paste(
    "STATE manifest stage=start scenario=%s regions=%d timesteps=%d",
    "expected_cells=%d threshold=%.2f"
  ),
  scenario, n_regions, n_timesteps, expected_cells,
  saturation_threshold
))

# ---------------------------------------------------------------------------
# Helper: read one cell's anterior.tif + posterior.tif, return validity + the
# changed-cell count (the D-06 differs-from-anterior blocking bar).
# ---------------------------------------------------------------------------
inspect_cell_posterior <- function(anterior_path, posterior_path) {
  out <- list(
    valid_geotiff = FALSE,
    anterior_exists = file.exists(anterior_path),
    changed_cells = NA_integer_,
    differs_ok = FALSE,
    degenerate = NA,
    n_classes = NA_integer_,
    freq = NULL,
    error = NA_character_
  )

  post <- tryCatch(terra::rast(posterior_path), error = function(e) {
    out$error <<- conditionMessage(e)
    NULL
  })
  if (is.null(post)) return(out)
  out$valid_geotiff <- TRUE

  # Per-class areas via terra::freq() (D-08b). Used for both the degeneracy
  # check here and the national demand roll-up by the caller.
  fr <- tryCatch(terra::freq(post), error = function(e) NULL)
  if (!is.null(fr)) {
    fr <- fr[!is.na(fr$value), , drop = FALSE]
    out$freq <- fr
    out$n_classes <- nrow(fr)
    # HARD degeneracy = the posterior collapsed to a single LULC class.
    out$degenerate <- nrow(fr) <= 1L
  }

  if (out$anterior_exists) {
    ante <- tryCatch(terra::rast(anterior_path), error = function(e) NULL)
    if (!is.null(ante)) {
      changed <- tryCatch(
        {
          diff_mask <- post != ante
          as.numeric(terra::global(diff_mask, "sum", na.rm = TRUE)[1, 1])
        },
        error = function(e) NA_real_
      )
      if (!is.na(changed)) {
        out$changed_cells <- as.integer(changed)
        # D-06 blocking bar: a cell with ZERO changed cells FAILS.
        out$differs_ok <- out$changed_cells > 0L
      }
    }
  }
  out
}

# ---------------------------------------------------------------------------
# Helper: aggregate a cell's saturation_summary.csv (D-07, report-only). Returns
# active/met/exempt/failed counts; emits one AUDIT stage=saturation line per cell
# using the src/saturation_diagnostics.r:315-328 grammar. NEVER gates the verdict.
# ---------------------------------------------------------------------------
aggregate_saturation <- function(work_dir, scenario, region_label, year) {
  out <- list(active = 0L, met = 0L, exempt = 0L, failed = 0L, present = FALSE)
  sat_path <- file.path(work_dir, "saturation_summary.csv")
  if (!file.exists(sat_path)) {
    log_msg(sprintf(
      paste(
        "AUDIT stage=saturation scenario=%s region=%s year_ant=%d",
        "active=0 met=0 exempt=0 failed=0 threshold=%.2f status=no_summary"
      ),
      scenario, region_label, as.integer(year), saturation_threshold
    ))
    return(out)
  }
  sat <- tryCatch(
    utils::read.csv(sat_path, check.names = FALSE),
    error = function(e) NULL
  )
  if (is.null(sat) || !all(c("floor_met", "exempt") %in% colnames(sat))) {
    log_msg(sprintf(
      paste(
        "AUDIT stage=saturation scenario=%s region=%s year_ant=%d",
        "active=0 met=0 exempt=0 failed=0 threshold=%.2f status=bad_summary"
      ),
      scenario, region_label, as.integer(year), saturation_threshold
    ))
    return(out)
  }
  out$present <- TRUE
  out$active <- nrow(sat)
  out$met <- sum(sat$floor_met, na.rm = TRUE)
  out$exempt <- sum(sat$exempt, na.rm = TRUE)
  out$failed <- sum(!sat$floor_met & !sat$exempt, na.rm = TRUE)
  # Same grammar as the inline writer (src/saturation_diagnostics.r:315-328).
  log_msg(sprintf(
    paste(
      "AUDIT stage=saturation scenario=%s region=%s year_ant=%d",
      "active=%d met=%d exempt=%d failed=%d threshold=%.2f"
    ),
    scenario, region_label, as.integer(year),
    as.integer(out$active), as.integer(out$met),
    as.integer(out$exempt), as.integer(out$failed),
    saturation_threshold
  ))
  out
}

# ---------------------------------------------------------------------------
# Walk the FULL region x timestep matrix.
# ---------------------------------------------------------------------------
manifest_rows <- list()
row_i <- 1L

for (idx in seq_along(region_suffixes)) {
  region_label <- region_labels[[idx]]
  region_suffix <- region_suffixes[[idx]]

  for (year in posterior_years) {
    timestep_dir <- file.path(simulation_output_dir, scenario, as.character(year))
    work_dir <- file.path(timestep_dir, paste0("region_", region_suffix))
    posterior_path <- file.path(work_dir, "posterior.tif")
    anterior_path <- file.path(work_dir, "anterior.tif")

    posterior_exists <- file.exists(posterior_path)
    cell <- list(
      valid_geotiff = FALSE,
      anterior_exists = file.exists(anterior_path),
      changed_cells = NA_integer_,
      differs_ok = FALSE,
      degenerate = NA,
      n_classes = NA_integer_,
      freq = NULL,
      error = NA_character_
    )
    if (posterior_exists) {
      cell <- inspect_cell_posterior(anterior_path, posterior_path)
    }

    # Saturation roll-up (D-07, report-only). Never affects the verdict.
    sat <- aggregate_saturation(work_dir, scenario, region_label, year)

    manifest_rows[[row_i]] <- data.frame(
      scenario = scenario,
      region = region_suffix,
      year = as.integer(year),
      posterior_exists = posterior_exists,
      valid_geotiff = isTRUE(cell$valid_geotiff),
      changed_cells = if (is.na(cell$changed_cells)) NA_integer_ else cell$changed_cells,
      differs_ok = isTRUE(cell$differs_ok),
      saturation_met = as.integer(sat$met),
      saturation_exempt = as.integer(sat$exempt),
      saturation_failed = as.integer(sat$failed),
      degenerate = if (is.na(cell$degenerate)) NA else isTRUE(cell$degenerate),
      n_classes = if (is.na(cell$n_classes)) NA_integer_ else cell$n_classes,
      national_mosaic_exists = NA,  # filled per-year below
      stringsAsFactors = FALSE
    )
    row_i <- row_i + 1L
  }
}

manifest <- do.call(rbind, manifest_rows)

# ---------------------------------------------------------------------------
# National mosaic existence per timestep (D-03 output). Stamp every cell row of
# that year with the year's mosaic-existence flag.
# ---------------------------------------------------------------------------
mosaic_present <- 0L
for (year in posterior_years) {
  mosaic_path <- file.path(
    simulation_output_dir, scenario, as.character(year),
    sprintf("posterior_%d.tif", year)
  )
  exists_flag <- file.exists(mosaic_path)
  if (exists_flag) mosaic_present <- mosaic_present + 1L
  manifest$national_mosaic_exists[manifest$year == as.integer(year)] <- exists_flag
  log_msg(sprintf(
    "AUDIT stage=mosaic scenario=%s year=%d national_mosaic_exists=%s",
    scenario, as.integer(year), tolower(as.character(exists_flag))
  ))
}

# ---------------------------------------------------------------------------
# D-08b demand-vs-achieved plausibility check REMOVED (operator decision).
# The scenario_area_mods demand table (tools/simulation_lulc_areas_2060.csv) is an
# UPSTREAM Stage-4 input (src/old/simulation_trans_tables_prep.r) already baked into
# the rate tables, NOT an allocation criterion. The achieved 2060 per-class areas
# legitimately differ from it (what the model can place != demand), so the manifest
# no longer compares against it. The remaining checks (existence, valid GeoTIFF,
# differs-from-anterior D-06, degeneracy, national mosaic presence) are unaffected.
# ---------------------------------------------------------------------------
demand_warnings <- 0L

# ---------------------------------------------------------------------------
# Write the manifest CSV.
# ---------------------------------------------------------------------------
manifest_out <- file.path(simulation_output_dir, scenario, "run_manifest.csv")
ensure_dir(dirname(manifest_out))
utils::write.csv(manifest, manifest_out, row.names = FALSE)
log_msg(sprintf("Wrote run manifest CSV (%d rows): %s", nrow(manifest), manifest_out))
# IN-04: machine-stable path line (exact key) for the verifier to grep, so the CSV
# path is not recovered by parsing the human-readable prose above.
log_msg(sprintf("STATE manifest csv=%s", manifest_out))

# ---------------------------------------------------------------------------
# Verdict (D-08). INCOMPLETE if ANY:
#   - cell fails existence OR valid-GeoTIFF
#   - differs_ok is FALSE for any cell (the D-06 blocking bar)
#   - any national mosaic is missing
#   - cell count != regions x timesteps
#   - any HARD all-one-class degeneracy
# Saturation failures NEVER change the verdict (D-07).
# ---------------------------------------------------------------------------
observed_cells <- nrow(manifest)
# posterior_exists / valid_geotiff / differs_ok are plain logical columns built
# per-row via isTRUE() above, so they carry no NA -- a simple sum() is exact.
existence_fail <- sum(!manifest$posterior_exists | !manifest$valid_geotiff)
differs_failures <- sum(!manifest$differs_ok)
degenerate_count <- sum(manifest$degenerate %in% TRUE)
mosaics_missing <- n_timesteps - mosaic_present
count_mismatch <- observed_cells != expected_cells
sat_failed_total <- sum(manifest$saturation_failed, na.rm = TRUE)

verdict_incomplete <-
  existence_fail > 0L ||
    differs_failures > 0L ||
    mosaics_missing > 0L ||
    count_mismatch ||
    degenerate_count > 0L

verdict <- if (verdict_incomplete) "INCOMPLETE" else "PASS"

log_msg(sprintf(
  paste(
    "STATE manifest verdict=%s scenario=%s cells=%d/%d mosaics=%d/%d",
    "differs_failures=%d degenerate=%d sat_failed_total=%d warnings=%d"
  ),
  verdict, scenario, observed_cells, expected_cells,
  mosaic_present, n_timesteps,
  differs_failures, degenerate_count, sat_failed_total, demand_warnings
))

quit(status = if (verdict_incomplete) 1L else 0L)

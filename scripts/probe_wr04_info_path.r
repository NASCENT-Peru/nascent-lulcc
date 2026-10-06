#!/usr/bin/env Rscript
#' Probe: which run combination fires the WR-04 "Info: ... not counted" path?
#'
#' UAT INSTRUMENT for phase 05.1 human item 1. Read-only: it writes nothing,
#' runs no allocation and takes minutes, not hours.
#'
#' The question it answers
#' -----------------------
#' `scripts/verify_intervention_smoke.r` section (c) downgrades a per-transition
#' probability map to `Info: ... not counted` when
#'
#'     n_zone = count(cells where <zone selection> AND !is.na(r)) == 0
#'
#' i.e. when the mask's selected zone holds none of that transition's from-class
#' cells in this region. Both production combinations checked so far
#' (NAT x costa_peruana x 2032 and NAT x andes x 2032) had every map intersect,
#' so the branch has never fired outside the testthat fixtures.
#'
#' Why an existing run can predict a future one
#' --------------------------------------------
#' `r` is non-NA exactly on the cells holding that transition's from-class
#' (src/allocation.r builds it as setValues(anterior, NA_real_) and writes
#' dt_j$prob at dt_j$cell_id). Under ALLOCATION_YEAR_POST_FILTER the anterior
#' map is the initial 2022 LULC map for EVERY posterior year (README_HPC.md
#' Pitfall 4), so the from-class footprints of a year-filtered smoke run do not
#' change with the year. A donor run's probability maps therefore give the exact
#' footprints for any other year of the same region, and the only thing that
#' changes is WHICH mask the intervention resolves to.
#'
#' That makes the year swap near-exact. The one residual uncertainty is whether
#' the probe year's trans_rates.csv carries the same row set as the donor's;
#' the report states this explicitly rather than hiding it.
#'
#' The n_zone arithmetic below is copied from the verifier (crop, compareGeom,
#' ifel(is.na) -> 0, zone selection, global(sel & !is.na(r))) so a zero here
#' means a zero there.
#'
#' Usage:
#'   Rscript scripts/probe_wr04_info_path.r \
#'     --donor-region andes --donor-year 2032 \
#'     [--donor-scenario NAT] [--scenario NAT] \
#'     [--probe-years 2024,2028,2032,2036] \
#'     [--output-root <path>] [--mask-dir <path>] [--interventions-dir <path>]
#'
#' Exit status: 0 a firing combination was found, 1 none found, 2 usage error.

script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  project_root <- dirname(dirname(sub("--file=", "", script_path)))
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") project_root <- dirname(project_root)
}
setwd(project_root)

usage <- paste(
  "Usage: Rscript scripts/probe_wr04_info_path.r --donor-region <region> --donor-year <year>",
  "[--donor-scenario NAT] [--scenario NAT] [--probe-years 2024,2028,2032,2036]",
  "[--output-root <path>] [--mask-dir <path>] [--interventions-dir <path>]"
)

known_flags <- c(
  "donor-scenario", "donor-region", "donor-year", "scenario", "probe-years",
  "output-root", "mask-dir", "interventions-dir"
)
opts <- list()
args <- commandArgs(trailingOnly = TRUE)
i <- 1L
while (i <= length(args)) {
  arg <- args[[i]]
  if (arg %in% c("--help", "-h")) {
    cat(usage, "\n")
    quit(status = 0)
  }
  if (!startsWith(arg, "--")) {
    cat(sprintf("Unknown argument: %s\n%s\n", arg, usage), file = stderr())
    quit(status = 2)
  }
  key <- sub("^--", "", arg)
  if (!key %in% known_flags) {
    cat(sprintf("Unknown flag: --%s\n%s\n", key, usage), file = stderr())
    quit(status = 2)
  }
  if (i + 1L > length(args)) {
    cat(sprintf("Flag --%s needs a value\n%s\n", key, usage), file = stderr())
    quit(status = 2)
  }
  opts[[key]] <- args[[i + 1L]]
  i <- i + 2L
}

for (src_file in c("src/setup.r", "src/utils.r", "src/implement_spatial_interventions.R")) {
  ok <- tryCatch({ source(src_file); TRUE }, error = function(e) {
    cat(sprintf("ERROR sourcing %s: %s\n", src_file, conditionMessage(e)), file = stderr())
    FALSE
  })
  if (!ok) quit(status = 2)
}

suppressPackageStartupMessages(library(terra))
terra::terraOptions(progress = 0)
config <- get_config()

opt_or <- function(key, default) {
  v <- opts[[key]]
  if (is.null(v) || !nzchar(v)) default else v
}

donor_region <- opts[["donor-region"]]
if (is.null(donor_region) || !nzchar(donor_region)) {
  cat(sprintf("ERROR: --donor-region is required\n%s\n", usage), file = stderr())
  quit(status = 2)
}
if (is.null(opts[["donor-year"]]) || !grepl("^[0-9]{4}$", opts[["donor-year"]])) {
  cat(sprintf("ERROR: --donor-year <posterior year> is required\n%s\n", usage), file = stderr())
  quit(status = 2)
}
donor_year <- as.integer(opts[["donor-year"]])
donor_scenario <- opt_or("donor-scenario", "NAT")
scenario <- opt_or("scenario", donor_scenario)
output_root <- opt_or("output-root", config[["simulation_output_dir"]])
mask_dir <- opt_or("mask-dir", config[["spat_prob_perturb_dir"]])
interventions_dir <- opt_or("interventions-dir", config[["interventions_dir"]])

probe_years <- opt_or("probe-years", "")
probe_years <- if (nzchar(probe_years)) {
  as.integer(trimws(strsplit(probe_years, ",", fixed = TRUE)[[1]]))
} else {
  # Every posterior year the scenario YAML mentions.
  e <- yaml::yaml.load_file(file.path(interventions_dir, paste0(scenario, "_interventions.yml")))
  sort(unique(as.integer(unlist(lapply(e, function(x) x[["Time_steps_implemented"]])))))
}
if (any(is.na(probe_years))) {
  cat(sprintf("ERROR: --probe-years must be a comma-separated list of 4-digit years\n%s\n", usage),
      file = stderr())
  quit(status = 2)
}

donor_dir <- file.path(output_root, donor_scenario, as.character(donor_year),
                       paste0("region_", donor_region))
prob_map_dir <- file.path(donor_dir, "probability_map_dir")
trans_rates_path <- file.path(donor_dir, "trans_rates.csv")

cat("WR-04 INFO-path probe (read-only)\n")
cat(sprintf("donor run:         %s\n", donor_dir))
cat(sprintf("probe scenario:    %s\n", scenario))
cat(sprintf("probe years:       %s\n", paste(probe_years, collapse = ", ")))
cat(sprintf("mask_dir:          %s\n", mask_dir))
cat(sprintf("interventions_dir: %s\n\n", interventions_dir))

stop_hard <- function(msg) {
  cat(sprintf("ERROR: %s\n", msg), file = stderr())
  quit(status = 2)
}
if (!dir.exists(donor_dir)) stop_hard(sprintf("donor region dir does not exist: %s", donor_dir))
if (!dir.exists(prob_map_dir)) stop_hard(sprintf("probability_map_dir missing in %s", donor_dir))
if (!file.exists(trans_rates_path)) stop_hard(sprintf("trans_rates.csv missing in %s", donor_dir))

region_ref_paths <- list.files(prob_map_dir, pattern = "[.]tif$", full.names = TRUE)
if (length(region_ref_paths) == 0L) stop_hard(sprintf("%s holds no .tif", prob_map_dir))
region_ref <- terra::rast(region_ref_paths[[1]])

tr <- utils::read.csv(trans_rates_path, check.names = FALSE, stringsAsFactors = FALSE)
to_col <- grep("^To", names(tr), value = TRUE)[1]
from_col <- grep("^From", names(tr), value = TRUE)[1]
if (is.na(to_col) || is.na(from_col) || !"id_trans" %in% names(tr)) {
  stop_hard(sprintf("trans_rates.csv lacks From*/To*/id_trans columns: %s",
                    paste(names(tr), collapse = ", ")))
}

schema <- jsonlite::fromJSON(config[["lulc_aggregation_path"]], simplifyVector = FALSE)
class_map <- setNames(
  vapply(schema, function(x) as.integer(x$value), integer(1)),
  vapply(schema, function(x) as.character(x$class_name), character(1))
)
name_of <- setNames(names(class_map), as.character(class_map))
# Class value -> class name, falling back to the bare value for anything the
# schema does not name. Avoids base-R %||% (R >= 4.4 only).
label_of <- function(v) {
  nm <- name_of[[as.character(v)]]
  if (is.null(nm) || is.na(nm)) as.character(v) else nm
}
to_vals_of <- function(names_vec) {
  v <- class_map[as.character(unlist(names_vec))]
  if (any(is.na(v))) {
    stop_hard(sprintf("unknown class name(s): %s",
                      paste(unlist(names_vec)[is.na(v)], collapse = ", ")))
  }
  as.integer(v[!is.na(v)])
}

entries <- yaml::yaml.load_file(file.path(interventions_dir, paste0(scenario, "_interventions.yml")))

# Cache the per-(mask, zone, row) n_zone: the same mask is reused across years,
# so a phase2 mask shared by 2036-2060 is scanned once.
n_zone_cache <- new.env(parent = emptyenv())

n_zone_for <- function(mask_path, zone, k, id_trans) {
  key <- paste(mask_path, zone, k, sep = "|")
  if (!is.null(n_zone_cache[[key]])) return(n_zone_cache[[key]])
  tif <- file.path(prob_map_dir, sprintf("%03d_id_trans_%d.tif", k, id_trans))
  if (!file.exists(tif)) {
    res <- list(n_zone = NA_real_, n_r = NA_real_, note = "donor probability map missing")
    n_zone_cache[[key]] <- res
    return(res)
  }
  r <- terra::rast(tif)
  m <- terra::crop(terra::rast(mask_path), r)
  if (!isTRUE(terra::compareGeom(r, m, stopOnError = FALSE))) {
    res <- list(n_zone = NA_real_, n_r = NA_real_, note = "mask does not align after crop")
    n_zone_cache[[key]] <- res
    return(res)
  }
  m0 <- terra::ifel(is.na(m), 0, m)
  sel <- if (identical(zone, "Inside")) (m0 == 1) else (m0 != 1)
  res <- list(
    n_zone = terra::global(sel & !is.na(r), "sum", na.rm = TRUE)[[1]][1],
    n_r    = terra::global(!is.na(r), "sum", na.rm = TRUE)[[1]][1],
    note   = NA_character_
  )
  n_zone_cache[[key]] <- res
  res
}

rows_out <- list()

for (py in probe_years) {
  resolved <- tryCatch(
    resolve_intervention_masks(interventions_dir, mask_dir, scenario, py),
    error = function(e) {
      cat(sprintf("Info: resolve_intervention_masks failed at %d: %s\n", py, conditionMessage(e)))
      NULL
    }
  )
  if (is.null(resolved) || nrow(resolved) == 0L) {
    cat(sprintf("%d: no active interventions\n", py))
    next
  }
  active_ids <- unique(resolved$intervention_id)
  abs0 <- Filter(function(x) {
    as.character(x[["Intervention_ID"]]) %in% active_ids &&
      identical(as.character(x[["Prob_adjust_type"]]), "Absolute") &&
      isTRUE(as.numeric(x[["Prob_adjust_value"]]) == 0)
  }, entries)
  if (length(abs0) == 0L) {
    cat(sprintf("%d: no active Absolute-0 intervention\n", py))
    next
  }

  for (x in abs0) {
    id <- as.character(x[["Intervention_ID"]])
    zone <- as.character(x[["Prob_adjust_zone"]])
    targets <- to_vals_of(x[["Transition_target_classes"]])
    from_filter <- x[["From_lulc_filter"]]
    from_vals <- if (!is.null(from_filter) && length(from_filter) > 0L &&
                     all(unlist(from_filter) != "None")) to_vals_of(from_filter) else NULL
    mask_path <- resolved$mask_path[resolved$intervention_id == id][1]
    if (is.na(mask_path) || !file.exists(mask_path)) {
      cat(sprintf("%d %s: mask not available (%s)\n", py, id, mask_path))
      next
    }
    rows <- which(tr[[to_col]] %in% targets &
                    (is.null(from_vals) | tr[[from_col]] %in% from_vals))
    if (length(rows) == 0L) {
      cat(sprintf("%d %s: no target rows in the DONOR trans_rates.csv (GAP-1 ledger territory, not WR-04)\n",
                  py, id))
      next
    }
    for (k in rows) {
      z <- n_zone_for(mask_path, zone, k, tr[["id_trans"]][k])
      rows_out[[length(rows_out) + 1L]] <- data.frame(
        year = py, intervention = id, zone = zone, mask = basename(mask_path),
        row = k, id_trans = tr[["id_trans"]][k],
        from = label_of(tr[[from_col]][k]),
        to = label_of(tr[[to_col]][k]),
        n_zone = z$n_zone, n_r = z$n_r, note = z$note,
        stringsAsFactors = FALSE
      )
    }
  }
}

if (length(rows_out) == 0L) {
  cat("\nNo Absolute-0 target rows to probe. Nothing to report.\n")
  quit(status = 1)
}

res <- do.call(rbind, rows_out)

cat("\n== per-row n_zone (0 = the WR-04 INFO path fires) ==\n")
print(res[order(res$year, res$intervention, res$row),
          c("year", "intervention", "zone", "mask", "row", "id_trans",
            "from", "to", "n_zone", "n_r", "note")],
      row.names = FALSE)

cat("\n== per (year, intervention) summary ==\n")
key <- paste(res$year, res$intervention, sep = " | ")
summ <- do.call(rbind, lapply(split(res, key), function(d) data.frame(
  year = d$year[1], intervention = d$intervention[1], zone = d$zone[1],
  rows = nrow(d),
  info_rows = sum(!is.na(d$n_zone) & d$n_zone == 0),
  asserted_rows = sum(!is.na(d$n_zone) & d$n_zone > 0),
  stringsAsFactors = FALSE
)))
summ$verdict <- ifelse(
  summ$info_rows > 0 & summ$asserted_rows > 0, "FIRES INFO + still asserts (what item 1 wants)",
  ifelse(summ$info_rows > 0 & summ$asserted_rows == 0, "all rows INFO -> D-06a FAIL, not a PASS",
         "no INFO rows (every map intersects)"))
print(summ[order(-summ$info_rows, summ$year), ], row.names = FALSE)

want <- summ[summ$info_rows > 0 & summ$asserted_rows > 0, ]
cat("\n")
if (nrow(want) > 0L) {
  cat("CANDIDATE RUN COMBINATION(S) for phase 05.1 human item 1:\n")
  for (j in seq_len(nrow(want))) {
    cat(sprintf("  %s x %s x %d  (%s: %d INFO row(s), %d asserted row(s))\n",
                scenario, donor_region, want$year[j], want$intervention[j],
                want$info_rows[j], want$asserted_rows[j]))
  }
  cat(sprintf("\nExpect maps_checked to land near %d, BELOW the %d matching trans_rates.csv row(s).\n",
              sum(summ$asserted_rows), sum(summ$rows)))
  cat("Caveat: row counts come from the DONOR trans_rates.csv. The probe year's own\n")
  cat("trans_rates.csv is written by the run itself and may carry a different row set.\n")
  quit(status = 0)
}
cat("No combination fires the INFO path while still asserting on at least one map.\n")
cat("Widen --probe-years, or try another --donor-region.\n")
quit(status = 1)

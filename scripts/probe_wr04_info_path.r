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
#'     [--donor-scenario NAT] [--scenario NAT,SOC,CUL] \
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
  "[--donor-scenario NAT] [--scenario NAT,SOC,CUL] [--probe-years 2024,2028,2032,2036]",
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
# memfrac caps what terra will hold in RAM before spilling to disk. The default
# (0.6) let a single crop on a big region blow a 32GB job; 0.25 keeps terra's
# own allocations well inside a modest --mem request, and the counting pass
# below is bounded separately by CHUNK_CELLS.
terra::terraOptions(progress = 0, memfrac = 0.25)

# Per-run scratch for the cropped masks. Inside tempdir(), which R unlinks on
# exit, so nothing is left behind on a normal finish; if the job is killed,
# SLURM's TMPDIR cleanup takes it.
scratch_dir <- file.path(tempdir(), "wr04_probe")
dir.create(scratch_dir, recursive = TRUE, showWarnings = FALSE)

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
# --scenario takes a comma-separated LIST. The donor supplies from-class
# footprints only, and those are scenario-independent under
# ALLOCATION_YEAR_POST_FILTER (the anterior map is the initial 2022 LULC map
# whatever the scenario), so one donor run can be probed against every
# scenario's masks and target classes without re-running allocation. This is
# what makes the other scenarios' masks — low_es_value_mask.tif,
# indigenous_lands_mask.tif, CUL's own PA phases — testable for free.
scenarios <- trimws(strsplit(opt_or("scenario", donor_scenario), ",", fixed = TRUE)[[1]])
scenarios <- scenarios[nzchar(scenarios)]
if (length(scenarios) == 0L) {
  cat(sprintf("ERROR: --scenario resolved to nothing\n%s\n", usage), file = stderr())
  quit(status = 2)
}
output_root <- opt_or("output-root", config[["simulation_output_dir"]])
mask_dir <- opt_or("mask-dir", config[["spat_prob_perturb_dir"]])
interventions_dir <- opt_or("interventions-dir", config[["interventions_dir"]])

yaml_for <- function(s) file.path(interventions_dir, paste0(s, "_interventions.yml"))
for (s in scenarios) {
  if (!file.exists(yaml_for(s))) {
    cat(sprintf("ERROR: no interventions YAML for scenario %s at %s\n", s, yaml_for(s)),
        file = stderr())
    quit(status = 2)
  }
}

probe_years_opt <- opt_or("probe-years", "")
probe_years_for <- function(s) {
  if (nzchar(probe_years_opt)) {
    as.integer(trimws(strsplit(probe_years_opt, ",", fixed = TRUE)[[1]]))
  } else {
    # Every posterior year this scenario's YAML mentions.
    e <- yaml::yaml.load_file(yaml_for(s))
    sort(unique(as.integer(unlist(lapply(e, function(x) x[["Time_steps_implemented"]])))))
  }
}
if (nzchar(probe_years_opt) && any(is.na(probe_years_for(scenarios[[1]])))) {
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
cat(sprintf("probe scenarios:   %s\n", paste(scenarios, collapse = ", ")))
cat(sprintf("probe years:       %s\n",
            if (nzchar(probe_years_opt)) probe_years_opt else "<all years in each scenario YAML>"))
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


# Cache the per-(mask, zone, row) n_zone: the same mask is reused across years,
# so a phase2 mask shared by 2036-2060 is scanned once.
n_zone_cache <- new.env(parent = emptyenv())

# Rows of cells held in memory at once. The first version of this script used
# terra::ifel() plus chained boolean algebra (`sel & !is.na(r)`), which
# materialises several full-extent logical rasters simultaneously and was
# OOM-killed at 32GB on andes. Reading a fixed-size window of both rasters and
# reducing it to two integers bounds peak memory by CHUNK_CELLS regardless of
# region size, so the probe costs the same on andes as on costa_peruana.
CHUNK_CELLS <- 4e6

# Every per-transition map in a region is setValues(anterior, ...), so they all
# share one grid. Crop each national mask to that grid ONCE, to a temp file, and
# reuse the disk-backed result across all of the region's maps. Re-cropping per
# map was re-materialising a region-sized raster 16 times over.
mask_crop_cache <- new.env(parent = emptyenv())

cropped_mask <- function(mask_path) {
  key <- mask_path
  if (!is.null(mask_crop_cache[[key]])) return(mask_crop_cache[[key]])
  out <- file.path(scratch_dir, paste0("maskcrop_", tools::file_path_sans_ext(basename(mask_path)), ".tif"))
  m <- tryCatch(
    terra::crop(terra::rast(mask_path), region_ref, filename = out, overwrite = TRUE),
    error = function(e) {
      cat(sprintf("Info: crop of %s failed: %s\n", basename(mask_path), conditionMessage(e)))
      NULL
    }
  )
  mask_crop_cache[[key]] <- m
  m
}

#' Count, in one streaming pass over the two aligned rasters:
#'   n_r  — cells where the probability map is non-NA (the from-class footprint)
#'   n_in — cells where additionally the mask equals 1
#'
#' NA mask cells count as 0, matching the verifier's ifel(is.na(m), 0, m).
count_blockwise <- function(r, m) {
  nc <- terra::ncol(r)
  nr <- terra::nrow(r)
  rows_per <- max(1L, as.integer(floor(CHUNK_CELLS / max(1L, nc))))
  n_r <- 0
  n_in <- 0
  # terra::readValues() on a disk-backed raster needs the file opened first;
  # without readStart() it raises "the file is not open for reading". readStop()
  # must run even on error, or the handles leak across the 16-map loop.
  terra::readStart(r)
  terra::readStart(m)
  on.exit({
    try(terra::readStop(r), silent = TRUE)
    try(terra::readStop(m), silent = TRUE)
  }, add = TRUE)
  start <- 1L
  while (start <= nr) {
    n_this <- min(rows_per, nr - start + 1L)
    rv <- terra::readValues(r, row = start, nrows = n_this, col = 1L, ncols = nc, mat = FALSE)
    mv <- terra::readValues(m, row = start, nrows = n_this, col = 1L, ncols = nc, mat = FALSE)
    ok <- !is.na(rv)
    n_r <- n_r + sum(ok)
    n_in <- n_in + sum(ok & !is.na(mv) & mv == 1)
    rm(rv, mv, ok)
    start <- start + n_this
  }
  list(n_r = n_r, n_in = n_in)
}

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
  m <- cropped_mask(mask_path)
  if (is.null(m)) {
    res <- list(n_zone = NA_real_, n_r = NA_real_, note = "mask crop failed")
    n_zone_cache[[key]] <- res
    return(res)
  }
  if (!isTRUE(terra::compareGeom(r, m, stopOnError = FALSE))) {
    res <- list(n_zone = NA_real_, n_r = NA_real_, note = "mask does not align after crop")
    n_zone_cache[[key]] <- res
    return(res)
  }
  # Progress on stdout: if a future run is killed mid-pass, the log names the
  # exact mask and map it died on instead of just stopping.
  cat(sprintf("  scan %s x row %d (id_trans %s) ... ", basename(mask_path), k,
              as.character(id_trans)))
  utils::flush.console()
  cnt <- count_blockwise(r, m)
  cat(sprintf("n_r=%.0f n_in=%.0f\n", cnt$n_r, cnt$n_in))
  # The verifier maps NA -> 0 before selecting, so zone Outside (`m0 != 1`)
  # INCLUDES every NA mask cell. The Outside count is therefore the complement
  # of the Inside count within the from-class footprint, which means one
  # blockwise pass serves both zones.
  res <- list(
    n_zone = if (identical(zone, "Inside")) cnt$n_in else cnt$n_r - cnt$n_in,
    n_r    = cnt$n_r,
    note   = NA_character_
  )
  n_zone_cache[[key]] <- res
  res
}

rows_out <- list()

for (scenario in scenarios) {
  entries <- yaml::yaml.load_file(yaml_for(scenario))
  probe_years <- probe_years_for(scenario)
  cat(sprintf("
--- scenario %s, years %s ---
", scenario,
              paste(probe_years, collapse = ", ")))
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
        scenario = scenario,
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
}

if (length(rows_out) == 0L) {
  cat("\nNo Absolute-0 target rows to probe. Nothing to report.\n")
  quit(status = 1)
}

res <- do.call(rbind, rows_out)
res$mask_short <- sub("^protected_areas_mask_", "PA_", sub("[.]tif$", "", res$mask))

# The per-row dump is wide and repeats itself: n_zone depends only on
# (mask, zone, from-class), never on the To class, so the three rows sharing a
# from-class always carry the same number. Collapse to the distinct scans.
cat("\n== distinct (scenario, mask, zone, from-class) scans ==\n")
cat("   n_zone = cells of the from-class inside the asserted zone; 0 fires the INFO path\n")
cat("   pct    = n_zone as a share of the from-class footprint (how far from firing)\n\n")
d <- res[!duplicated(res[c("scenario", "mask_short", "zone", "from")]), ]
d$pct <- ifelse(is.na(d$n_r) | d$n_r == 0, NA_real_, 100 * d$n_zone / d$n_r)
print(d[order(d$scenario, d$mask_short, d$pct),
        c("scenario", "mask_short", "zone", "from", "n_zone", "n_r", "pct")],
      row.names = FALSE, digits = 3)

if (any(!is.na(res$note))) {
  cat("\n== rows that could not be scanned ==\n")
  print(unique(res[!is.na(res$note), c("scenario", "mask_short", "row", "note")]),
        row.names = FALSE)
}

cat("\n== per (scenario, year, intervention) summary ==\n")
key <- paste(res$scenario, res$year, res$intervention, sep = " | ")
summ <- do.call(rbind, lapply(split(res, key), function(x) data.frame(
  scenario = x$scenario[1], year = x$year[1], intervention = x$intervention[1],
  zone = x$zone[1], rows = nrow(x),
  info_rows = sum(!is.na(x$n_zone) & x$n_zone == 0),
  asserted_rows = sum(!is.na(x$n_zone) & x$n_zone > 0),
  stringsAsFactors = FALSE
)))
summ$verdict <- ifelse(
  summ$info_rows > 0 & summ$asserted_rows > 0, "FIRES INFO + still asserts (what item 1 wants)",
  ifelse(summ$info_rows > 0 & summ$asserted_rows == 0, "all rows INFO -> D-06a FAIL, not a PASS",
         "no INFO rows (every map intersects)"))
print(summ[order(-summ$info_rows, summ$scenario, summ$year), ], row.names = FALSE)

want <- summ[summ$info_rows > 0 & summ$asserted_rows > 0, ]
cat("\n")
if (nrow(want) > 0L) {
  cat("CANDIDATE RUN COMBINATION(S) for phase 05.1 human item 1:\n")
  for (j in seq_len(nrow(want))) {
    cat(sprintf("  %s x %s x %d  (%s: %d INFO row(s), %d asserted row(s))\n",
                want$scenario[j], donor_region, want$year[j], want$intervention[j],
                want$info_rows[j], want$asserted_rows[j]))
  }
  cat("\nCaveat: row counts come from the DONOR trans_rates.csv. The probe year's own\n")
  cat("trans_rates.csv is written by the run itself and may carry a different row set.\n")
  quit(status = 0)
}

# Nothing fired. Report HOW FAR from firing, so the decision to keep hunting or
# to accept the fixture-only proof rests on a number rather than a hunch.
cat("No combination fires the INFO path while still asserting on at least one map.\n\n")
ok <- d[!is.na(d$pct), ]
if (nrow(ok) > 0L) {
  closest <- ok[order(ok$pct), ][1, ]
  cat(sprintf("Closest approach: %s / %s / zone %s / from %s\n",
              closest$scenario, closest$mask_short, closest$zone, closest$from))
  cat(sprintf("  n_zone = %.0f of %.0f footprint cells (%.3f%%)\n",
              closest$n_zone, closest$n_r, closest$pct))
  cat(sprintf("  smallest from-class footprint scanned: %.0f cells\n", min(ok$n_r)))
  cat("\nEvery mask overlaps every from-class by a wide margin in this donor region.\n")
  cat("Firing the path needs a from-class whose footprint is small enough that this\n")
  cat("overlap rounds to zero cells - a different region, not a different year.\n")
}
cat("\nNext options, cheapest first:\n")
cat("  1. another --scenario (other masks, same donor, no new allocation run)\n")
cat("  2. another --donor-region that has already been run\n")
cat("  3. accept the fixture-only proof, citing this probe output as the evidence\n")
quit(status = 1)

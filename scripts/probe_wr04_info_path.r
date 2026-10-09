#!/usr/bin/env Rscript
#' Probe: which run combination fires the WR-04 "Info: ... not counted" path?
#'
#' UAT INSTRUMENT for phase 05.1 human item 1. Read-only: it writes nothing
#' outside tempdir(), runs no allocation, and streams rather than loading
#' rasters whole.
#'
#' The question it answers
#' -----------------------
#' `scripts/verify_intervention_smoke.r` section (c) downgrades a per-transition
#' probability map to `Info: ... not counted` when
#'
#'     n_zone = count(cells where <zone selection> AND !is.na(r)) == 0
#'
#' i.e. when the mask's selected zone holds none of that transition's from-class
#' cells in this region. Both production combinations checked by hand
#' (NAT x costa_peruana x 2032 and NAT x andes x 2032) had every map intersect,
#' so the branch has never fired outside the testthat fixtures. This script
#' searches the existing output tree for a combination that would fire it.
#'
#' How it works
#' ------------
#' A DONOR is an existing `<scenario>/<year>/region_<slug>` output directory. Its
#' per-transition probability maps give the exact from-class footprints (`r` is
#' non-NA only on that transition's from-class cells, because src/allocation.r
#' builds it as setValues(anterior, NA_real_) and writes dt_j$prob at
#' dt_j$cell_id) and its trans_rates.csv gives the row set.
#'
#' By default each donor is probed against ITS OWN scenario and year, so the
#' answer is faithful — it is the combination the verifier would actually run on,
#' with no cross-year or cross-scenario transplanting. Pass --scenario to probe a
#' donor against a DIFFERENT scenario's masks; that is valid only for
#' year-filtered smoke donors, whose anterior map is the initial 2022 LULC map
#' regardless of scenario (README_HPC.md Pitfall 4), and the report says so.
#'
#' Outputs produced before interventions were wired in are still valid donors:
#' an intervention changes probability VALUES, never the non-NA footprint.
#'
#' The n_zone arithmetic is copied from the verifier (crop, compareGeom,
#' NA -> 0, zone selection, count) so a zero here means a zero there. Because NA
#' mask cells select as Outside, the Outside count is the complement of the
#' Inside count within the footprint, so one streaming pass serves both zones.
#'
#' Usage:
#'   Rscript scripts/probe_wr04_info_path.r \
#'     [--donor-region all|costa_peruana,andes] \
#'     [--donor-year all|2032,2060] \
#'     [--donor-scenario all|NAT,SOC,CUL] \
#'     [--scenario NAT,SOC,CUL]   # cross-probe; default = each donor's own \
#'     [--probe-years 2024,2028]  # default = each donor's own year \
#'     [--census]                 # scan everything instead of stopping at the first hit \
#'     [--max-donors N] \
#'     [--output-root <path>] [--mask-dir <path>] [--interventions-dir <path>]
#'
#' Donors are ordered cheapest region first and the sweep STOPS at the first
#' combination that fires the INFO path while still asserting on another map,
#' because item 1 needs one example, not a census. --census disables that.
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
  "Usage: Rscript scripts/probe_wr04_info_path.r [--donor-region all|<list>]",
  "[--donor-year all|<list>] [--donor-scenario all|<list>] [--scenario <list>]",
  "[--probe-years <list>] [--census] [--max-donors N]",
  "[--output-root <path>] [--mask-dir <path>] [--interventions-dir <path>]"
)

known_flags <- c(
  "donor-scenario", "donor-region", "donor-year", "scenario", "probe-years",
  "output-root", "mask-dir", "interventions-dir", "max-donors"
)
bool_flags <- c("census")
opts <- list()
census <- FALSE
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
  if (key %in% bool_flags) {
    if (identical(key, "census")) census <- TRUE
    i <- i + 1L
    next
  }
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
# memfrac caps what terra holds in RAM before spilling. The default (0.6) let a
# single crop on a big region blow a 32GB job; the counting pass is bounded
# separately by CHUNK_CELLS.
terra::terraOptions(progress = 0, memfrac = 0.25)

scratch_dir <- file.path(tempdir(), "wr04_probe")
dir.create(scratch_dir, recursive = TRUE, showWarnings = FALSE)

config <- get_config()

opt_or <- function(key, default) {
  v <- opts[[key]]
  if (is.null(v) || !nzchar(v)) default else v
}
as_list <- function(s) {
  v <- trimws(strsplit(s, ",", fixed = TRUE)[[1]])
  v[nzchar(v)]
}

output_root <- opt_or("output-root", config[["simulation_output_dir"]])
mask_dir <- opt_or("mask-dir", config[["spat_prob_perturb_dir"]])
interventions_dir <- opt_or("interventions-dir", config[["interventions_dir"]])
max_donors <- suppressWarnings(as.integer(opt_or("max-donors", "0")))
if (is.na(max_donors)) max_donors <- 0L

yaml_for <- function(s) file.path(interventions_dir, paste0(s, "_interventions.yml"))

stop_hard <- function(msg) {
  cat(sprintf("ERROR: %s\n", msg), file = stderr())
  quit(status = 2)
}

# --- Donor discovery -------------------------------------------------------
# Only directories named exactly region_<slug> with no dot in the slug, so
# moved-aside copies such as region_costa_peruana.pre_gapclosure are ignored.
discover_donors <- function() {
  sc_dirs <- list.dirs(output_root, recursive = FALSE, full.names = TRUE)
  out <- list()
  for (sd in sc_dirs) {
    for (yd in list.dirs(sd, recursive = FALSE, full.names = TRUE)) {
      if (!grepl("^[0-9]{4}$", basename(yd))) next
      for (rd in list.dirs(yd, recursive = FALSE, full.names = TRUE)) {
        b <- basename(rd)
        if (!grepl("^region_[A-Za-z0-9_]+$", b)) next
        out[[length(out) + 1L]] <- data.frame(
          scenario = basename(sd), year = as.integer(basename(yd)),
          region = sub("^region_", "", b), stringsAsFactors = FALSE
        )
      }
    }
  }
  if (length(out) == 0L) return(NULL)
  do.call(rbind, out)
}

all_donors <- discover_donors()
if (is.null(all_donors)) stop_hard(sprintf("no donor directories found under %s", output_root))

sel_field <- function(flag, column) {
  v <- opt_or(flag, "all")
  if (identical(tolower(v), "all")) return(unique(all_donors[[column]]))
  as_list(v)
}
want_sc <- sel_field("donor-scenario", "scenario")
want_rg <- sel_field("donor-region", "region")
want_yr <- sel_field("donor-year", "year")

donors <- all_donors[all_donors$scenario %in% want_sc &
                       all_donors$region %in% want_rg &
                       as.character(all_donors$year) %in% as.character(want_yr), ]
if (nrow(donors) == 0L) {
  stop_hard("no donor matched the --donor-scenario/--donor-region/--donor-year selection")
}

# Prune scenarios with no Absolute-0 intervention at all (BAU): section (c)
# only ever asserts on Absolute/0, so such a donor cannot fire the path.
scenario_override <- opts[["scenario"]]
abs0_capable <- function(s) {
  if (!file.exists(yaml_for(s))) return(FALSE)
  e <- yaml::yaml.load_file(yaml_for(s))
  any(vapply(e, function(x) {
    identical(as.character(x[["Prob_adjust_type"]]), "Absolute") &&
      isTRUE(as.numeric(x[["Prob_adjust_value"]]) == 0)
  }, logical(1)))
}
if (is.null(scenario_override)) {
  keep <- vapply(donors$scenario, abs0_capable, logical(1))
  dropped <- unique(donors$scenario[!keep])
  if (length(dropped) > 0L) {
    cat(sprintf("Skipping scenario(s) with no Absolute-0 intervention: %s\n",
                paste(dropped, collapse = ", ")))
  }
  donors <- donors[keep, ]
  if (nrow(donors) == 0L) stop_hard("every selected donor scenario lacks an Absolute-0 intervention")
}

# Cheapest region first, so a hit in a small region costs minutes. Order from
# the measured per-region cost (see the HPC notes): costa << selva < andes < cuenca.
region_cost <- c(costa_peruana = 1, selva_andina = 2, andes = 3, cuenca_del_amazonas = 4)
donors$cost <- ifelse(donors$region %in% names(region_cost),
                      region_cost[donors$region], 9)
donors <- donors[order(donors$cost, donors$scenario, donors$year), ]
if (max_donors > 0L && nrow(donors) > max_donors) donors <- donors[seq_len(max_donors), ]

cat("WR-04 INFO-path probe (read-only)\n")
cat(sprintf("output_root:       %s\n", output_root))
cat(sprintf("mask_dir:          %s\n", mask_dir))
cat(sprintf("interventions_dir: %s\n", interventions_dir))
cat(sprintf("donors selected:   %d\n", nrow(donors)))
cat(sprintf("probe scenario:    %s\n",
            if (is.null(scenario_override)) "<each donor's own>" else scenario_override))
cat(sprintf("probe years:       %s\n",
            if (is.null(opts[["probe-years"]])) "<each donor's own>" else opts[["probe-years"]]))
cat(sprintf("mode:              %s\n\n",
            if (census) "census (scan everything)" else "stop at first firing combination"))

schema <- jsonlite::fromJSON(config[["lulc_aggregation_path"]], simplifyVector = FALSE)
class_map <- setNames(
  vapply(schema, function(x) as.integer(x$value), integer(1)),
  vapply(schema, function(x) as.character(x$class_name), character(1))
)
name_of <- setNames(names(class_map), as.character(class_map))
label_of <- function(v) {
  nm <- name_of[[as.character(v)]]
  if (is.null(nm) || is.na(nm)) as.character(v) else nm
}
to_vals_of <- function(names_vec) {
  v <- class_map[as.character(unlist(names_vec))]
  if (any(is.na(v))) {
    cat(sprintf("Info: unknown class name(s): %s\n",
                paste(unlist(names_vec)[is.na(v)], collapse = ", ")))
  }
  as.integer(v[!is.na(v)])
}

# Rows of cells held at once. An earlier version used terra::ifel() plus chained
# boolean algebra, which materialises several full-extent logical rasters and was
# OOM-killed at 32GB on andes. Streaming bounds peak memory by CHUNK_CELLS
# regardless of region size.
CHUNK_CELLS <- 4e6

#' n_r  — cells where the probability map is non-NA (the from-class footprint)
#' n_in — cells where additionally the mask equals 1 (NA mask cells count as 0,
#'        matching the verifier's ifel(is.na(m), 0, m))
count_blockwise <- function(r, m) {
  nc <- terra::ncol(r)
  nr <- terra::nrow(r)
  rows_per <- max(1L, as.integer(floor(CHUNK_CELLS / max(1L, nc))))
  n_r <- 0
  n_in <- 0
  # readValues() on a disk-backed raster needs the file opened first, or it
  # raises "the file is not open for reading". readStop must run even on error.
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

rows_out <- list()
hit <- NULL
donors_scanned <- 0L
donors_skipped <- list()

for (di in seq_len(nrow(donors))) {
  dn <- donors[di, ]
  donor_dir <- file.path(output_root, dn$scenario, as.character(dn$year),
                         paste0("region_", dn$region))
  prob_map_dir <- file.path(donor_dir, "probability_map_dir")
  trans_rates_path <- file.path(donor_dir, "trans_rates.csv")

  skip <- function(why) {
    donors_skipped[[length(donors_skipped) + 1L]] <<- data.frame(
      scenario = dn$scenario, year = dn$year, region = dn$region,
      why = why, stringsAsFactors = FALSE)
  }
  if (!dir.exists(prob_map_dir)) { skip("no probability_map_dir"); next }
  if (!file.exists(trans_rates_path)) { skip("no trans_rates.csv"); next }
  ref_paths <- list.files(prob_map_dir, pattern = "[.]tif$", full.names = TRUE)
  if (length(ref_paths) == 0L) { skip("probability_map_dir holds no .tif"); next }

  tr <- tryCatch(
    utils::read.csv(trans_rates_path, check.names = FALSE, stringsAsFactors = FALSE),
    error = function(e) NULL)
  if (is.null(tr)) { skip("trans_rates.csv unreadable"); next }
  to_col <- grep("^To", names(tr), value = TRUE)[1]
  from_col <- grep("^From", names(tr), value = TRUE)[1]
  if (is.na(to_col) || is.na(from_col) || !"id_trans" %in% names(tr)) {
    skip("trans_rates.csv lacks From*/To*/id_trans"); next
  }

  region_ref <- terra::rast(ref_paths[[1]])
  donors_scanned <- donors_scanned + 1L
  donor_tag <- sprintf("%s/%d/%s", dn$scenario, dn$year, dn$region)
  cat(sprintf("[%d/%d] donor %s  (%d maps, %d x %d cells)\n",
              di, nrow(donors), donor_tag, length(ref_paths),
              terra::nrow(region_ref), terra::ncol(region_ref)))

  # Caches are per donor: the region grid changes between donors.
  n_zone_cache <- new.env(parent = emptyenv())
  mask_crop_cache <- new.env(parent = emptyenv())

  cropped_mask <- function(mask_path) {
    k <- mask_path
    if (!is.null(mask_crop_cache[[k]])) return(mask_crop_cache[[k]])
    out <- file.path(scratch_dir, sprintf(
      "mc_%d_%s.tif", di, tools::file_path_sans_ext(basename(mask_path))))
    m <- tryCatch(
      terra::crop(terra::rast(mask_path), region_ref, filename = out, overwrite = TRUE),
      error = function(e) {
        cat(sprintf("    Info: crop of %s failed: %s\n", basename(mask_path),
                    conditionMessage(e)))
        NULL
      })
    mask_crop_cache[[k]] <- m
    m
  }

  # n_zone depends only on (mask, zone, from-class) — never on the To class — so
  # the cache key is the row, and sibling rows sharing a from-class are free.
  n_zone_for <- function(mask_path, zone, k, id_trans) {
    ck <- paste(mask_path, zone, k, sep = "|")
    if (!is.null(n_zone_cache[[ck]])) return(n_zone_cache[[ck]])
    tif <- file.path(prob_map_dir, sprintf("%03d_id_trans_%d.tif", k, id_trans))
    if (!file.exists(tif)) {
      res <- list(n_zone = NA_real_, n_r = NA_real_, note = "donor probability map missing")
      n_zone_cache[[ck]] <- res; return(res)
    }
    r <- terra::rast(tif)
    m <- cropped_mask(mask_path)
    if (is.null(m)) {
      res <- list(n_zone = NA_real_, n_r = NA_real_, note = "mask crop failed")
      n_zone_cache[[ck]] <- res; return(res)
    }
    if (!isTRUE(terra::compareGeom(r, m, stopOnError = FALSE))) {
      res <- list(n_zone = NA_real_, n_r = NA_real_, note = "mask does not align after crop")
      n_zone_cache[[ck]] <- res; return(res)
    }
    cnt <- count_blockwise(r, m)
    res <- list(
      n_zone = if (identical(zone, "Inside")) cnt$n_in else cnt$n_r - cnt$n_in,
      n_r = cnt$n_r, note = NA_character_)
    n_zone_cache[[ck]] <- res
    res
  }

  probe_scenarios <- if (is.null(scenario_override)) dn$scenario else as_list(scenario_override)
  for (ps in probe_scenarios) {
    if (!file.exists(yaml_for(ps))) {
      cat(sprintf("    Info: no interventions YAML for %s\n", ps)); next
    }
    entries <- yaml::yaml.load_file(yaml_for(ps))
    pys <- if (is.null(opts[["probe-years"]])) dn$year else as.integer(as_list(opts[["probe-years"]]))
    for (py in pys) {
      resolved <- tryCatch(
        resolve_intervention_masks(interventions_dir, mask_dir, ps, py),
        error = function(e) NULL)
      if (is.null(resolved) || nrow(resolved) == 0L) next
      active_ids <- unique(resolved$intervention_id)
      abs0 <- Filter(function(x) {
        as.character(x[["Intervention_ID"]]) %in% active_ids &&
          identical(as.character(x[["Prob_adjust_type"]]), "Absolute") &&
          isTRUE(as.numeric(x[["Prob_adjust_value"]]) == 0)
      }, entries)
      if (length(abs0) == 0L) next

      for (x in abs0) {
        id <- as.character(x[["Intervention_ID"]])
        zone <- as.character(x[["Prob_adjust_zone"]])
        targets <- to_vals_of(x[["Transition_target_classes"]])
        ff <- x[["From_lulc_filter"]]
        from_vals <- if (!is.null(ff) && length(ff) > 0L && all(unlist(ff) != "None")) {
          to_vals_of(ff)
        } else NULL
        mask_path <- resolved$mask_path[resolved$intervention_id == id][1]
        if (is.na(mask_path) || !file.exists(mask_path)) {
          cat(sprintf("    Info: %s %d %s: mask not available\n", ps, py, id)); next
        }
        rows <- which(tr[[to_col]] %in% targets &
                        (is.null(from_vals) | tr[[from_col]] %in% from_vals))
        if (length(rows) == 0L) next

        n_info <- 0L; n_asserted <- 0L
        for (k in rows) {
          z <- n_zone_for(mask_path, zone, k, tr[["id_trans"]][k])
          if (!is.na(z$n_zone)) {
            if (z$n_zone == 0) n_info <- n_info + 1L else n_asserted <- n_asserted + 1L
          }
          rows_out[[length(rows_out) + 1L]] <- data.frame(
            donor = donor_tag, scenario = ps, year = py, intervention = id, zone = zone,
            mask = basename(mask_path), row = k, id_trans = tr[["id_trans"]][k],
            from = label_of(tr[[from_col]][k]), to = label_of(tr[[to_col]][k]),
            n_zone = z$n_zone, n_r = z$n_r, note = z$note, stringsAsFactors = FALSE)
        }
        cat(sprintf("    %s %d %-42s zone=%-7s rows=%2d INFO=%d asserted=%d\n",
                    ps, py, id, zone, length(rows), n_info, n_asserted))
        if (n_info > 0L && n_asserted > 0L && is.null(hit)) {
          hit <- data.frame(donor = donor_tag, scenario = ps, year = py, region = dn$region,
                            intervention = id, zone = zone, info_rows = n_info,
                            asserted_rows = n_asserted, rows = length(rows),
                            stringsAsFactors = FALSE)
          if (!census) break
        }
      }
      if (!is.null(hit) && !census) break
    }
    if (!is.null(hit) && !census) break
  }
  # Free this donor's cropped masks before moving on.
  unlink(list.files(scratch_dir, pattern = sprintf("^mc_%d_", di), full.names = TRUE))
  if (!is.null(hit) && !census) {
    cat("\nFiring combination found; stopping early (pass --census to scan everything).\n")
    break
  }
}

cat(sprintf("\ndonors scanned: %d of %d selected\n", donors_scanned, nrow(donors)))
if (length(donors_skipped) > 0L) {
  sk <- do.call(rbind, donors_skipped)
  cat(sprintf("donors skipped: %d\n", nrow(sk)))
  print(unique(sk[, c("why")], incomparables = FALSE))
}

if (length(rows_out) == 0L) {
  cat("\nNo Absolute-0 target rows were scanned. Nothing to report.\n")
  quit(status = 1)
}

res <- do.call(rbind, rows_out)
res$mask_short <- sub("^protected_areas_mask_", "PA_", sub("[.]tif$", "", res$mask))

cat("\n== distinct (donor, mask, zone, from-class) scans ==\n")
cat("   n_zone = cells of the from-class inside the asserted zone; 0 fires the INFO path\n")
cat("   pct    = n_zone as a share of the from-class footprint (how far from firing)\n\n")
d <- res[!duplicated(res[c("donor", "mask_short", "zone", "from")]), ]
d$pct <- ifelse(is.na(d$n_r) | d$n_r == 0, NA_real_, 100 * d$n_zone / d$n_r)
print(d[order(d$pct, d$donor),
        c("donor", "mask_short", "zone", "from", "n_zone", "n_r", "pct")],
      row.names = FALSE, digits = 3)

if (any(!is.na(res$note))) {
  cat("\n== rows that could not be scanned ==\n")
  print(unique(res[!is.na(res$note), c("donor", "mask_short", "row", "note")]), row.names = FALSE)
}

cat("\n")
if (!is.null(hit)) {
  cat("CANDIDATE RUN COMBINATION for phase 05.1 human item 1:\n")
  cat(sprintf("  scenario %s x region %s x year %d\n", hit$scenario, hit$region, hit$year))
  cat(sprintf("  intervention %s (zone %s)\n", hit$intervention, hit$zone))
  cat(sprintf("  %d of %d target row(s) fire the INFO path; %d still asserted\n",
              hit$info_rows, hit$rows, hit$asserted_rows))
  cat("\nRun it with:\n")
  cat(sprintf(paste0("  sbatch --partition=%s \\\n",
                     "    --export=ALL,ALLOCATION_PROFILE_SCENARIO=%s,",
                     "ALLOCATION_REGION_FILTER=%s,ALLOCATION_YEAR_POST_FILTER=%d \\\n",
                     "    scripts/submit_allocation_smoke.sh\n"),
              if (hit$region %in% c("andes", "cuenca_del_amazonas", "selva_andina")) "fat" else "highmem",
              hit$scenario, hit$region, hit$year))
  cat("\nCaveat: row counts come from the donor's trans_rates.csv. A fresh run writes\n")
  cat("its own, and with ALLOCATION_YEAR_POST_FILTER the anterior map is the initial\n")
  cat("2022 map, so a donor from a sequential sweep may carry a different row set.\n")
  quit(status = 0)
}

cat("No combination fires the INFO path while still asserting on at least one map.\n\n")
ok <- d[!is.na(d$pct), ]
if (nrow(ok) > 0L) {
  closest <- ok[order(ok$pct), ][1, ]
  cat(sprintf("Closest approach: %s / %s / zone %s / from %s\n",
              closest$donor, closest$mask_short, closest$zone, closest$from))
  cat(sprintf("  n_zone = %.0f of %.0f footprint cells (%.4f%%)\n",
              closest$n_zone, closest$n_r, closest$pct))
  cat(sprintf("  smallest from-class footprint scanned: %.0f cells\n", min(ok$n_r)))
  cat(sprintf("  distinct scans: %d across %d donor(s)\n", nrow(ok), length(unique(ok$donor))))
}
quit(status = 1)

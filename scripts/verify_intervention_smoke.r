#!/usr/bin/env Rscript
#' Post-smoke assertion script for spatial interventions (Phase 5, D-15).
#'
#' Declares an intervention smoke run PASS or FAIL for one
#' scenario x region x posterior-year output directory:
#'
#'   (a) posterior.tif exists in the region work dir.
#'   (b) The newest worker log that carries an "AUDIT stage=intervention_summary"
#'       line for this scenario/year has exactly one
#'       "AUDIT stage=intervention region=" line per Allocation intervention
#'       active at --year (resolved via resolve_intervention_masks()), with the
#'       same set of intervention ids, and exactly one summary line.
#'   (c) For every Absolute intervention with Prob_adjust_value 0, every
#'       per-transition probability map whose To class is a target (and whose
#'       From class passes From_lulc_filter when present) is 0 inside the mask
#'       (zone Inside) or outside the mask (zone Outside). The national mask is
#'       cropped to the region map (same grid, no resampling).
#'   (d) No forbidden marker appears in any log under the region dir (or in
#'       --extra-log, e.g. the SLURM stdout file).
#'
#' Pitfall 4: with ALLOCATION_YEAR_POST_FILTER set, the anterior map is the
#' initial 2022 map rather than the simulated map for year_post - step, so
#' the output is a mechanism proof for the intervention hook, not a
#' scientific result for that year.
#'
#' Usage:
#'   Rscript scripts/verify_intervention_smoke.r --year <posterior year>
#'     [--scenario NAT] [--region costa_peruana] [--output-root <path>]
#'     [--mask-dir <path>] [--extra-log <path>]
#'
#' --year is required: the smoke script default (2026) is not a posterior
#' year (Pitfall 5). --region is the region suffix (lower case, spaces as _).
#' Exit status: 0 PASS, 1 FAIL, 2 usage or sourcing error.

# ---------------------------------------------------------------------------
# Set working directory to project root (before sourcing src/*.r).
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

usage <- paste(
  "Usage: Rscript scripts/verify_intervention_smoke.r --year <posterior year>",
  "[--scenario NAT] [--region costa_peruana] [--output-root <path>]",
  "[--mask-dir <path>] [--extra-log <path>]"
)

known_flags <- c("scenario", "region", "year", "output-root", "mask-dir", "extra-log")
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
  body <- sub("^--", "", arg)
  if (grepl("=", body, fixed = TRUE)) {
    key <- sub("=.*$", "", body)
    val <- sub("^[^=]*=", "", body)
  } else {
    key <- body
    if (i + 1L > length(args)) {
      cat(sprintf("--%s requires a value\n%s\n", key, usage), file = stderr())
      quit(status = 2)
    }
    val <- args[[i + 1L]]
    i <- i + 1L
  }
  if (!key %in% known_flags) {
    cat(sprintf("Unknown flag: --%s\n%s\n", key, usage), file = stderr())
    quit(status = 2)
  }
  opts[[key]] <- val
  i <- i + 1L
}

if (is.null(opts[["year"]]) || !grepl("^[0-9]{4}$", opts[["year"]])) {
  cat(sprintf(
    "ERROR: --year <posterior year> is required (e.g. --year 2032); the smoke default 2026 is not a posterior year.\n%s\n",
    usage
  ), file = stderr())
  quit(status = 2)
}
year <- as.integer(opts[["year"]])

src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/implement_spatial_interventions.R"
)
for (src_file in src_files) {
  ok <- tryCatch(
    {
      source(src_file)
      TRUE
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, conditionMessage(e)), file = stderr())
      FALSE
    }
  )
  if (!ok) quit(status = 2)
}

suppressPackageStartupMessages(library(terra))
terra::terraOptions(progress = 0)

config <- get_config()

opt_or <- function(key, default) {
  v <- opts[[key]]
  if (is.null(v) || !nzchar(v)) default else v
}
scenario <- opt_or("scenario", "NAT")
region <- opt_or("region", "costa_peruana")
output_root <- opt_or("output-root", config[["simulation_output_dir"]])
mask_dir <- opt_or("mask-dir", config[["spat_prob_perturb_dir"]])
extra_log <- opts[["extra-log"]]
interventions_dir <- config[["interventions_dir"]]

region_dir <- file.path(output_root, scenario, as.character(year), paste0("region_", region))
prob_map_dir <- file.path(region_dir, "probability_map_dir")

cat(sprintf("region_dir: %s\n", region_dir))
cat(sprintf("mask_dir:   %s\n\n", mask_dir))

errors <- character(0)
fail <- function(msg) {
  errors <<- c(errors, msg)
  cat(sprintf("ERROR: %s\n", msg), file = stderr())
}

if (!dir.exists(region_dir)) {
  fail(sprintf("region dir does not exist: %s", region_dir))
  quit(status = 1)
}

# (a) posterior.tif ----------------------------------------------------------
if (!file.exists(file.path(region_dir, "posterior.tif"))) {
  fail(sprintf("posterior.tif missing in %s", region_dir))
}

# Active interventions -------------------------------------------------------
yaml_path <- file.path(interventions_dir, paste0(scenario, "_interventions.yml"))
entries <- tryCatch(yaml::yaml.load_file(yaml_path), error = function(e) {
  fail(sprintf("cannot read %s: %s", yaml_path, conditionMessage(e)))
  list()
})
resolved <- tryCatch(
  resolve_intervention_masks(interventions_dir, mask_dir, scenario, year),
  error = function(e) {
    fail(sprintf("resolve_intervention_masks failed: %s", conditionMessage(e)))
    NULL
  }
)
active_ids <- if (is.null(resolved)) character(0) else unique(resolved$intervention_id)
n_active <- length(active_ids)
cat(sprintf("Active Allocation interventions at %d: %d (%s)\n\n", year, n_active,
            paste(active_ids, collapse = ", ")))
if (!is.null(resolved) && any(!resolved$exists)) {
  fail(sprintf("mask file(s) missing: %s",
               paste(resolved$mask_path[!resolved$exists], collapse = ", ")))
}

# Logs -----------------------------------------------------------------------
log_files <- list.files(region_dir, pattern = "[.]log$", recursive = TRUE, full.names = TRUE)
read_lines_safe <- function(p) tryCatch(readLines(p, warn = FALSE), error = function(e) character(0))
log_lines <- lapply(log_files, read_lines_safe)
names(log_lines) <- log_files

# (b) AUDIT lines --------------------------------------------------------------
year_pat <- sprintf(" year=%d ", year)
scen_pat <- sprintf(" scenario=%s ", scenario)
is_summary <- function(l) {
  grepl("AUDIT stage=intervention_summary", l, fixed = TRUE) &
    grepl(year_pat, l, fixed = TRUE) & grepl(scen_pat, l, fixed = TRUE)
}
is_iv <- function(l) {
  grepl("AUDIT stage=intervention region=", l, fixed = TRUE) &
    grepl(year_pat, l, fixed = TRUE) & grepl(scen_pat, l, fixed = TRUE)
}
with_summary <- log_files[vapply(log_lines, function(l) any(is_summary(l)), logical(1))]
if (length(with_summary) == 0L) {
  fail(sprintf(
    "no 'AUDIT stage=intervention_summary' line for scenario=%s year=%d in %d log file(s) under %s",
    scenario, year, length(log_files), region_dir
  ))
} else {
  # Re-runs of the smoke append new worker logs (one per PID); judge the
  # newest log that carries a summary line.
  newest <- with_summary[which.max(file.info(with_summary)$mtime)]
  if (length(with_summary) > 1L) {
    cat(sprintf("Note: %d logs carry a summary line; using newest: %s\n",
                length(with_summary), newest))
  }
  l <- log_lines[[newest]]
  iv_lines <- l[is_iv(l)]
  sum_lines <- l[is_summary(l)]
  cat(sprintf("AUDIT lines from %s:\n", newest))
  for (x in c(iv_lines, sum_lines)) cat("  ", x, "\n", sep = "")
  cat("\n")
  if (length(iv_lines) != n_active) {
    fail(sprintf(
      "expected %d 'AUDIT stage=intervention region=' lines for year=%d, found %d",
      n_active, year, length(iv_lines)
    ))
  }
  seen_ids <- sub("^.* id=([^ ]+) .*$", "\\1", iv_lines)
  if (!setequal(seen_ids, active_ids)) {
    fail(sprintf(
      "AUDIT intervention ids {%s} differ from active ids {%s}",
      paste(sort(seen_ids), collapse = ","), paste(sort(active_ids), collapse = ",")
    ))
  }
  if (length(sum_lines) != 1L) {
    fail(sprintf("expected 1 'AUDIT stage=intervention_summary' line, found %d", length(sum_lines)))
  }
}

# (c) Absolute-0 raster assertions --------------------------------------------
maps_checked <- 0L
schema <- jsonlite::fromJSON(config[["lulc_aggregation_path"]], simplifyVector = FALSE)
class_map <- setNames(
  vapply(schema, function(x) as.integer(x$value), integer(1)),
  vapply(schema, function(x) as.character(x$class_name), character(1))
)
to_vals_of <- function(names_vec) {
  v <- class_map[as.character(unlist(names_vec))]
  if (any(is.na(v))) {
    fail(sprintf("unknown class name(s): %s",
                 paste(unlist(names_vec)[is.na(v)], collapse = ", ")))
  }
  as.integer(v[!is.na(v)])
}

trans_rates_path <- file.path(region_dir, "trans_rates.csv")
abs0 <- Filter(function(x) {
  id <- as.character(x[["Intervention_ID"]])
  id %in% active_ids &&
    identical(as.character(x[["Prob_adjust_type"]]), "Absolute") &&
    isTRUE(as.numeric(x[["Prob_adjust_value"]]) == 0)
}, entries)

if (length(abs0) > 0L) {
  if (!file.exists(trans_rates_path)) {
    fail(sprintf("trans_rates.csv missing in %s", region_dir))
  } else if (!dir.exists(prob_map_dir)) {
    fail(sprintf("probability_map_dir missing in %s", region_dir))
  } else {
    tr <- utils::read.csv(trans_rates_path, check.names = FALSE, stringsAsFactors = FALSE)
    to_col <- grep("^To", names(tr), value = TRUE)[1]
    from_col <- grep("^From", names(tr), value = TRUE)[1]
    if (is.na(to_col) || is.na(from_col) || !"id_trans" %in% names(tr)) {
      fail(sprintf("trans_rates.csv lacks From*/To*/id_trans columns: %s",
                   paste(names(tr), collapse = ", ")))
    } else {
      for (x in abs0) {
        id <- as.character(x[["Intervention_ID"]])
        zone <- as.character(x[["Prob_adjust_zone"]])
        targets <- to_vals_of(x[["Transition_target_classes"]])
        from_filter <- x[["From_lulc_filter"]]
        from_vals <- if (!is.null(from_filter) && length(from_filter) > 0L &&
                         all(unlist(from_filter) != "None")) {
          to_vals_of(from_filter)
        } else {
          NULL
        }
        mask_path <- resolved$mask_path[resolved$intervention_id == id][1]
        if (is.na(mask_path) || !file.exists(mask_path)) {
          fail(sprintf("mask for %s not available: %s", id, mask_path))
          next
        }
        m_nat <- terra::rast(mask_path)
        rows <- which(tr[[to_col]] %in% targets &
                        (is.null(from_vals) | tr[[from_col]] %in% from_vals))
        if (length(rows) == 0L) {
          cat(sprintf("Info: %s has no target rows in trans_rates.csv for this region\n", id))
          next
        }
        for (k in rows) {
          id_trans <- tr[["id_trans"]][k]
          tif <- file.path(prob_map_dir, sprintf("%03d_id_trans_%d.tif", k, id_trans))
          if (!file.exists(tif)) {
            fail(sprintf("probability map missing: %s", tif))
            next
          }
          r <- terra::rast(tif)
          m <- terra::crop(m_nat, r)
          if (!isTRUE(terra::compareGeom(r, m, stopOnError = FALSE))) {
            fail(sprintf("mask %s does not align with %s after crop (no resampling allowed)",
                         basename(mask_path), basename(tif)))
            next
          }
          m0 <- terra::ifel(is.na(m), 0, m)
          sel <- if (identical(zone, "Inside")) (m0 == 1) else (m0 != 1)
          mx <- terra::global(r * sel, "max", na.rm = TRUE)[[1]][1]
          if (!is.finite(mx)) mx <- 0
          maps_checked <- maps_checked + 1L
          status <- if (mx == 0) "ok" else "FAIL"
          cat(sprintf("  %s zone=%s %s: max prob in zone = %g [%s]\n",
                      id, zone, basename(tif), mx, status))
          if (mx != 0) {
            fail(sprintf("%s: %s has max probability %g %s %s (expected 0)",
                         id, basename(tif), mx, tolower(zone), basename(mask_path)))
          }
        }
      }
      if (maps_checked == 0L) {
        cat("Info: no Absolute-0 target maps present for this region\n")
      }
    }
  }
}
cat("\n")

# (d) Forbidden markers -------------------------------------------------------
forbidden <- c(
  "missing value where TRUE/FALSE needed",
  "could not find function",
  "intervention mask missing"
)
scan_sets <- log_lines
if (!is.null(extra_log)) {
  if (file.exists(extra_log)) {
    scan_sets[[extra_log]] <- read_lines_safe(extra_log)
  } else {
    fail(sprintf("--extra-log not found: %s", extra_log))
  }
}
# Known benign line: allocation always logs the mlr3 predict_newdata()
# failure (".__Task__col_info" missing) before its deterministic direct
# ranger fallback. It contains "could not find function" but is expected.
benign <- c(".__Task__col_info", "falling back to direct model prediction")
for (p in names(scan_sets)) {
  lines_p <- scan_sets[[p]]
  is_benign <- Reduce(`|`, lapply(benign, function(b) grepl(b, lines_p, fixed = TRUE)),
                      logical(length(lines_p)))
  lines_p <- lines_p[!is_benign]
  for (f in forbidden) {
    hits <- grep(f, lines_p, fixed = TRUE, value = TRUE)
    if (length(hits) > 0L) {
      fail(sprintf("forbidden marker '%s' in %s: %s", f, p, hits[[1]]))
    }
  }
}

# Information only: placed vs demanded -----------------------------------------
sat_path <- file.path(region_dir, "saturation_summary.csv")
if (file.exists(sat_path)) {
  sat <- tryCatch(utils::read.csv(sat_path, stringsAsFactors = FALSE), error = function(e) NULL)
  if (!is.null(sat) && all(c("id_trans", "from_val", "to_val", "demanded_cells", "placed_cells") %in% names(sat))) {
    cat("Info (not assessed): placed vs demanded per transition (interventions shrink feasible area)\n")
    for (j in seq_len(nrow(sat))) {
      cat(sprintf("  id_trans=%d %d->%d demanded=%d placed=%d\n",
                  as.integer(sat$id_trans[j]), as.integer(sat$from_val[j]),
                  as.integer(sat$to_val[j]), as.integer(sat$demanded_cells[j]),
                  as.integer(sat$placed_cells[j])))
    }
    cat("\n")
  }
}

if (length(errors) > 0L) {
  cat(sprintf("FAIL verify_intervention_smoke scenario=%s region=%s year=%d errors=%d\n",
              scenario, region, year, length(errors)), file = stderr())
  quit(status = 1)
}
cat(sprintf(
  "PASS verify_intervention_smoke scenario=%s region=%s year=%d interventions=%d maps_checked=%d\n",
  scenario, region, year, n_active, maps_checked
))
quit(status = 0)

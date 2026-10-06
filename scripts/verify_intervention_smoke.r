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
#'       cropped to the region map (same grid, no resampling). Because
#'       `r * sel` is 0 wherever `sel` is FALSE, an empty zone would report a
#'       vacuous [ok]; non-vacuity is therefore asserted at three levels:
#'         - per map: a zone holding no non-NA probability cell is INFO and is
#'           NOT counted in maps_checked. `r` is a PER-TRANSITION map, non-NA
#'           only on that transition's from-class cells, so a mask that does
#'           not intersect that from-class in this region is an ordinary,
#'           correct-engine outcome, not a defect (WR-04);
#'         - per intervention: zero checked maps is a FAIL. That is the real
#'           WR-07 condition -- a mask that does not intersect this region at
#'           all. An intervention that is skipped earlier, because this
#'           region's trans_rates.csv holds none of its target rows, never
#'           reaches that check; it is recorded in an unasserted-id ledger and
#'           FAILs at run level instead, because the run asserted nothing about
#'           it and is therefore not evidence for it (GAP-1);
#'         - per run: maps_checked == 0 while at least one active Absolute-0
#'           intervention is configured is a FAIL, because the run then
#'           asserted nothing at all.
#'       For zone Outside the mask itself must additionally hold at least one
#'       cell equal to 1 over this region: the Outside zone is the complement
#'       of the mask, so an empty or wrongly-coded mask would satisfy the
#'       assertion trivially across the whole region (CR-01). That proof runs
#'       once per intervention against the region grid and BEFORE the
#'       target-row lookup, because it depends only on the mask and the grid --
#'       never on what this region's trans_rates.csv happens to contain
#'       (GAP-1).
#'   (d) No forbidden marker appears in any log under the region dir (or in
#'       --extra-log, e.g. the SLURM stdout file).
#'   (e) The plan 05-13 telemetry CSV
#'       <region_dir>/intervention_prob_deltas_<scenario>_<region>_<year>.csv
#'       exists, carries the 25-column contract in order, covers exactly the
#'       active intervention ids, and reconciles with the AUDIT lines of (b):
#'       per intervention its target_class set equals the AUDIT to_vals set,
#'       sum(n_changed) equals rows_changed and sum(sum_abs_delta) equals the
#'       AUDIT sum_abs_delta. sum(n_target) is compared to rows_target for
#'       Absolute interventions ONLY: relative_prob_adjust() adds the target
#'       rows to rows_target BEFORE its skip paths, so a skipped class
#'       legitimately shows rows_target > 0 with n_target = 0 (05-13). Finally
#'       every Absolute Prob_adjust_value 0 row with n_target > 0 must report
#'       mean_after = 0 (D-22). Note the CSV `region` column is the slug while
#'       the AUDIT `region=` field is the verbatim region label; they are not
#'       compared.
#'
#' Pitfall 4: with ALLOCATION_YEAR_POST_FILTER set, the anterior map is the
#' initial 2022 map rather than the simulated map for year_post - step, so
#' the output is a mechanism proof for the intervention hook, not a
#' scientific result for that year.
#'
#' Usage:
#'   Rscript scripts/verify_intervention_smoke.r --year <posterior year>
#'     [--scenario NAT] [--region costa_peruana] [--output-root <path>]
#'     [--mask-dir <path>] [--interventions-dir <path>] [--extra-log <path>]
#'
#' --year is required: the smoke script default (2026) is not a posterior
#' year (Pitfall 5). --region is the region suffix (lower case, spaces as _).
#' --interventions-dir overrides the directory holding
#' <scenario>_interventions.yml (default: config[["interventions_dir"]]); it
#' mirrors the existing --mask-dir / --output-root overrides and lets the
#' verifier be pointed at a fixture config.
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
  "[--mask-dir <path>] [--interventions-dir <path>] [--extra-log <path>]"
)

known_flags <- c(
  "scenario", "region", "year", "output-root", "mask-dir",
  "interventions-dir", "extra-log"
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
interventions_dir <- opt_or("interventions-dir", config[["interventions_dir"]])

region_dir <- file.path(output_root, scenario, as.character(year), paste0("region_", region))
prob_map_dir <- file.path(region_dir, "probability_map_dir")

cat(sprintf("region_dir: %s\n", region_dir))
cat(sprintf("mask_dir:   %s\n", mask_dir))
# Echoed because --interventions-dir selects WHICH policy set is asserted on
# (T-05-54): the resolved value must be visible in the verifier's own output.
cat(sprintf("interventions_dir: %s\n\n", interventions_dir))

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
# Declared before the branch so section (e) can consume the AUDIT lines rather
# than re-parse the logs, even on the "no summary line" path.
iv_lines <- character(0)
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
      # GAP-1: the Outside non-degeneracy proof (D-04) needs a reference grid
      # for this region, and ANY probability map in the directory is a valid
      # one: allocation builds every per-transition map as
      # terra::setValues(anterior, ...), so all of them share one geometry. The
      # previous code relied on exactly this already when it latched D-04 on
      # the first map it happened to visit. Taking the reference from the
      # directory instead decouples the proof from trans_rates.csv content.
      region_ref_paths <- list.files(prob_map_dir, pattern = "[.]tif$", full.names = TRUE)
      region_ref <- if (length(region_ref_paths) > 0L) {
        terra::rast(region_ref_paths[[1]])
      } else {
        NULL
      }
      if (is.null(region_ref)) {
        fail(sprintf(
          "probability_map_dir %s holds no .tif, so the region grid for the Outside non-degeneracy proof cannot be established",
          prob_map_dir
        ))
      }
      # GAP-1 ledger: every active Absolute-0 intervention that this run could
      # not assert anything about because the region's trans_rates.csv holds
      # none of its target rows. Checked at run level below — a sibling
      # intervention with rows keeps maps_checked > 0, so the D-06b floor alone
      # would let the whole intervention pass unasserted.
      unasserted_ids <- character(0)
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
        # D-04 (CR-01): for zone Outside the asserted zone is the COMPLEMENT of
        # the mask, so "prob is 0 over the complement" is satisfied trivially
        # across the whole region by an all-zero or wrongly-coded mask — with a
        # large, non-vacuous n_zone. The per-map n_zone guard below only ever
        # proved the complement is non-empty; it never proved the mask holds a
        # single cell equal to 1. Prove that separately, or the evidence cannot
        # distinguish a correctly 1-coded mask from an empty one. Only the
        # Outside branch needs it: for zone Inside `sel` IS (m0 == 1), so a
        # degenerate mask already yields n_zone == 0 on every map and is
        # reported by the per-intervention D-06a check.
        #
        # GAP-1: this proof is a property of the mask and the region grid
        # ALONE, so it runs here — once per intervention, before the target-row
        # lookup. Sited below that lookup (as it was) it was skipped entirely
        # whenever this region's trans_rates.csv held none of the
        # intervention's target rows, which is the normal case for a
        # mining-only Outside intervention.
        if (identical(zone, "Outside") && !is.null(region_ref)) {
          m_reg <- terra::crop(m_nat, region_ref)
          if (!isTRUE(terra::compareGeom(region_ref, m_reg, stopOnError = FALSE))) {
            fail(sprintf("mask %s does not align with %s after crop (no resampling allowed)",
                         basename(mask_path), basename(region_ref_paths[[1]])))
          } else {
            m0r <- terra::ifel(is.na(m_reg), 0, m_reg)
            n_ones <- terra::global(m0r == 1, "sum", na.rm = TRUE)[[1]][1]
            if (!is.finite(n_ones) || n_ones == 0) {
              fail(sprintf(
                "%s: mask %s has 0 cells equal to 1 over this region but Prob_adjust_zone=Outside (degenerate mask: the Outside zone is then the whole region, so the assertion cannot distinguish a correct mask from an empty one)",
                id, basename(mask_path)
              ))
            }
          }
        }
        rows <- which(tr[[to_col]] %in% targets &
                        (is.null(from_vals) | tr[[from_col]] %in% from_vals))
        if (length(rows) == 0L) {
          # GAP-1: this intervention asserted nothing in this region, so the
          # run is not evidence for it. Record it; the ledger is checked at run
          # level after this loop. D-06a below is unreachable from here, and
          # the D-06b floor is satisfied by any sibling intervention that does
          # have rows.
          unasserted_ids <- c(unasserted_ids, id)
          cat(sprintf("Info: %s has no target rows in trans_rates.csv for this region\n", id))
          next
        }
        # D-06a: count the maps this intervention actually asserted on, so the
        # per-map INFO downgrade above cannot become a blanket escape hatch.
        n_checked_id <- 0L
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
          # WR-04: `r` is a PER-TRANSITION probability map — allocation builds
          # it as setValues(anterior, NA_real_) and then writes dt_j$prob at
          # dt_j$cell_id (src/allocation.r:3196-3200), so it is non-NA only on
          # the cells holding THIS transition's from-class. `n_zone` therefore
          # counts "cells in the selected zone that also hold this transition's
          # from-class", and zero is an ordinary correct-engine outcome: a mask
          # that holds none of a rare from-class in this region simply has
          # nothing to adjust for that transition. Report it as INFO, do not
          # count it in maps_checked (there is nothing to assert on), and do
          # NOT fail here — the meaningful non-vacuity assertions are one and
          # two levels up (D-06a / D-06b). A non-finite global() result on an
          # empty selection is the same "nothing to assert on" state.
          n_zone <- terra::global(sel & !is.na(r), "sum", na.rm = TRUE)[[1]][1]
          if (!is.finite(n_zone) || n_zone == 0) {
            cat(sprintf(
              "  Info: %s %s has no non-NA cells in zone=%s (mask does not intersect this transition's from-class); not counted\n",
              id, basename(tif), zone
            ))
            next
          }
          mx <- terra::global(r * sel, "max", na.rm = TRUE)[[1]][1]
          if (!is.finite(mx)) {
            # The zone is provably non-empty, so a non-finite max means the
            # product yielded no usable value. Never convert that to a pass.
            fail(sprintf(
              "%s: %s has a non-finite max over %d non-NA cell(s) in zone=%s for mask %s",
              id, basename(tif), as.integer(n_zone), zone, basename(mask_path)
            ))
            next
          }
          maps_checked <- maps_checked + 1L
          n_checked_id <- n_checked_id + 1L
          status <- if (mx == 0) "ok" else "FAIL"
          cat(sprintf("  %s zone=%s %s: n_zone=%d max prob in zone = %g [%s]\n",
                      id, zone, basename(tif), as.integer(n_zone), mx, status))
          if (mx != 0) {
            fail(sprintf("%s: %s has max probability %g %s %s (expected 0)",
                         id, basename(tif), mx, tolower(zone), basename(mask_path)))
          }
        }
        # D-06a: this is the real WR-07 condition. Individual transitions may
        # legitimately miss the zone (INFO above), but an intervention NONE of
        # whose target maps could be asserted on means the mask does not
        # intersect this region at all — nothing about that intervention was
        # proved, and the run must not report PASS on it. The
        # `length(rows) == 0L` branch above `next`s before the map loop, so
        # this check is correctly unreachable for "this region has no target
        # rows in trans_rates.csv"; that case is caught by the `unasserted_ids`
        # ledger below (GAP-1). D-06b remains only the floor for "not a single
        # map was asserted on anywhere in this region".
        if (n_checked_id == 0L) {
          fail(sprintf(
            "%s: no probability map could be asserted on (0 of %d target map(s) had a non-NA probability cell in zone=%s for mask %s)",
            id, length(rows), zone, basename(mask_path)
          ))
        }
      }
      # GAP-1: run-level ledger. An intervention skipped for want of target
      # rows never reaches D-06a, and a sibling intervention that does have
      # rows keeps the D-06b floor satisfied — which is how a green banner came
      # to be read as evidence for an intervention nothing was asserted about.
      # Note this deliberately does NOT also collect the D-06a ids: D-06a
      # already calls fail() naming them, and double-reporting would inflate
      # errors=N (the defect R51-IN-01 describes).
      if (length(unasserted_ids) > 0L) {
        fail(sprintf(
          "no probability map could be asserted on for active Absolute-0 intervention(s) %s (no target rows in trans_rates.csv for this region); this run is not evidence for them",
          paste(unasserted_ids, collapse = ", ")
        ))
      }
      # D-06b: run-level floor. This block is already inside
      # `if (length(abs0) > 0L)`, so reaching it with maps_checked == 0 means
      # active Absolute-0 interventions are configured and yet not a single
      # probability map was asserted on. This deliberately also fires when
      # every active Absolute-0 intervention had no target rows in
      # trans_rates.csv: the run then asserted nothing at all, and a PASS
      # banner reporting maps_checked=0 would be evidence of nothing.
      if (maps_checked == 0L) {
        fail(sprintf(
          "maps_checked=0: no Absolute-0 probability map could be asserted on in this region, but %d active Absolute-0 intervention(s) are configured",
          length(abs0)
        ))
      }
    }
  }
}
cat("\n")

# (e) telemetry CSV (D-22) -----------------------------------------------------
# Reuses region_dir, active_ids, iv_lines and abs0 from the sections above: the
# point of this section is to prove the telemetry describes the SAME run the
# AUDIT lines describe, so it must not re-derive either side.
tel_cols <- c(
  "scenario", "region", "year", "intervention_id", "rank", "type", "zone",
  "mask", "target_class", "n_target", "n_changed", "mean_before", "mean_after",
  "sd_before", "sd_after", "p05_delta", "p25_delta", "p50_delta", "p75_delta",
  "p95_delta", "min_delta", "max_delta", "sum_abs_delta", "prob_mass_before",
  "prob_mass_after"
)
tel_path <- file.path(
  region_dir,
  sprintf("intervention_prob_deltas_%s_%s_%d.csv", scenario, region, year)
)
# Same extraction style as seen_ids above: the key appears once per AUDIT line
# and no value contains a space.
audit_field <- function(lines, key) {
  pat <- sprintf("^.* %s=([^ ]*).*$", key)
  out <- rep(NA_character_, length(lines))
  hit <- grepl(pat, lines)
  out[hit] <- sub(pat, "\\1", lines[hit])
  out
}
tel_errs0 <- length(errors)
telemetry_rows <- 0L
if (n_active == 0L) {
  cat("Info: no active Allocation interventions; telemetry CSV not asserted\n")
} else if (!file.exists(tel_path)) {
  fail(sprintf("telemetry CSV missing: %s", tel_path))
} else {
  tel <- tryCatch(
    utils::read.csv(tel_path, check.names = FALSE, stringsAsFactors = FALSE),
    error = function(e) {
      fail(sprintf("cannot read telemetry CSV %s: %s", tel_path, conditionMessage(e)))
      NULL
    }
  )
  if (!is.null(tel) && !identical(names(tel), tel_cols)) {
    fail(sprintf(
      "telemetry CSV %s violates the 25-column contract: expected [%s], found [%s]",
      basename(tel_path), paste(tel_cols, collapse = ","),
      paste(names(tel), collapse = ",")
    ))
    tel <- NULL
  }
  if (!is.null(tel)) {
    telemetry_rows <- nrow(tel)
    tel_ids <- unique(as.character(tel[["intervention_id"]]))
    if (!setequal(tel_ids, active_ids)) {
      fail(sprintf(
        "telemetry intervention ids {%s} differ from active ids {%s}",
        paste(sort(tel_ids), collapse = ","), paste(sort(active_ids), collapse = ",")
      ))
    }
    a_id <- audit_field(iv_lines, "id")
    a_type <- audit_field(iv_lines, "type")
    a_to <- audit_field(iv_lines, "to_vals")
    a_rows_target <- suppressWarnings(as.numeric(audit_field(iv_lines, "rows_target")))
    a_rows_changed <- suppressWarnings(as.numeric(audit_field(iv_lines, "rows_changed")))
    a_sum_abs <- suppressWarnings(as.numeric(audit_field(iv_lines, "sum_abs_delta")))
    close_enough <- function(a, b) abs(a - b) <= 1e-5 * max(1, abs(a), abs(b))
    for (j in seq_along(a_id)) {
      idj <- a_id[[j]]
      rows_j <- which(as.character(tel[["intervention_id"]]) == idj)
      if (length(rows_j) == 0L) {
        fail(sprintf("telemetry CSV has no rows for AUDIT intervention id=%s", idj))
        next
      }
      want <- suppressWarnings(as.integer(
        strsplit(as.character(a_to[[j]]), ",", fixed = TRUE)[[1]]
      ))
      got <- suppressWarnings(as.integer(tel[["target_class"]][rows_j]))
      if (!setequal(want, got)) {
        fail(sprintf(
          "%s: telemetry target_class {%s} differs from AUDIT to_vals {%s}",
          idj, paste(sort(got), collapse = ","), paste(sort(want), collapse = ",")
        ))
      }
      if (anyDuplicated(got) > 0L) {
        fail(sprintf(
          "%s: telemetry has duplicate (intervention_id, target_class) rows", idj
        ))
      }
      n_tgt <- sum(suppressWarnings(as.numeric(tel[["n_target"]][rows_j])), na.rm = TRUE)
      n_chg <- sum(suppressWarnings(as.numeric(tel[["n_changed"]][rows_j])), na.rm = TRUE)
      s_abs <- sum(suppressWarnings(as.numeric(tel[["sum_abs_delta"]][rows_j])), na.rm = TRUE)
      if (is.finite(a_rows_changed[[j]]) && n_chg != a_rows_changed[[j]]) {
        fail(sprintf(
          "%s: telemetry sum(n_changed)=%g differs from AUDIT rows_changed=%g",
          idj, n_chg, a_rows_changed[[j]]
        ))
      }
      if (is.finite(a_sum_abs[[j]]) && !close_enough(s_abs, a_sum_abs[[j]])) {
        fail(sprintf(
          "%s: telemetry sum(sum_abs_delta)=%g differs from AUDIT sum_abs_delta=%g",
          idj, s_abs, a_sum_abs[[j]]
        ))
      }
      # Absolute only: relative_prob_adjust() increments rows_target BEFORE its
      # three `next` paths, so a skipped class contributes to the AUDIT
      # rows_target while its stats row correctly reports n_target = 0. Both
      # numbers are correct under their own definitions (05-13), so equality
      # here would be a false alarm on every Relative intervention.
      if (identical(a_type[[j]], "Absolute") &&
            is.finite(a_rows_target[[j]]) && n_tgt != a_rows_target[[j]]) {
        fail(sprintf(
          "%s: telemetry sum(n_target)=%g differs from AUDIT rows_target=%g",
          idj, n_tgt, a_rows_target[[j]]
        ))
      }
    }
    # D-22: an Absolute Prob_adjust_value 0 intervention must have driven every
    # target class it actually touched to exactly 0.
    for (x in abs0) {
      idx <- as.character(x[["Intervention_ID"]])
      n_t <- suppressWarnings(as.numeric(tel[["n_target"]]))
      rows_x <- which(as.character(tel[["intervention_id"]]) == idx &
                        !is.na(n_t) & n_t > 0)
      for (rr in rows_x) {
        ma <- suppressWarnings(as.numeric(tel[["mean_after"]][rr]))
        if (!is.finite(ma) || abs(ma) > 1e-12) {
          fail(sprintf(
            "%s: telemetry mean_after=%s for target_class=%s (expected 0 for Absolute Prob_adjust_value 0)",
            idx, as.character(tel[["mean_after"]][rr]),
            as.character(tel[["target_class"]][rr])
          ))
        }
      }
    }
    if (length(errors) == tel_errs0) {
      cat(sprintf(
        "telemetry: %d rows, %d interventions, %d classes [ok]\n",
        nrow(tel), length(tel_ids), length(unique(tel[["target_class"]]))
      ))
    }
  }
}
cat("\n")

# (d) Forbidden markers -------------------------------------------------------
forbidden <- c(
  # Generic R crash signatures. The length-zero message below is the EXACT
  # text the missing-Prob_adjust_* defect produced end to end (CR-03); the
  # out-of-bounds one is its sibling for a mis-shaped index.
  "missing value where TRUE/FALSE needed",
  "argument is of length zero",
  "subscript out of bounds",
  "could not find function",
  # Runtime intervention stops introduced by plan 05-09 (CR-02, IN-07, WR-10).
  # The two geometry strings are deliberately prefix-free so they also catch
  # the Stage 7 pre-flight wording, which differs only in its "intervention
  # mask: " prefix and the absent trailing clause.
  "intervention mask missing",
  "intervention ref grid unreadable",
  "is not on the reference grid",
  "layers (expected 1)",
  "cell_index$cell_id must be",
  "cell_index$ref_cell_id values outside",
  # Stage 7 pre-flight rejections (plan 05-10). Every string carrying either of
  # these two prefixes is an error; no success line uses them.
  "intervention mask: ",
  "intervention config: ",
  # Mask value-domain rejections (plan 05.1-01, CR-01). The D-01 runtime stop
  # in .mask_inside_lut() reads "intervention mask <path> has value(s) outside
  # {0,1,NA}: ..." (no colon after "mask"), while the D-02 Stage 7 pre-flight
  # variants carry the existing "intervention mask: " prefix above. These two
  # substrings are deliberately prefix-free so they catch BOTH wordings; they
  # are a frozen contract with plan 05.1-01 and must not be paraphrased.
  # Matching is grep(f, lines_p, fixed = TRUE), so the braces are literal.
  # No success line anywhere in the engine emits either substring.
  "outside {0,1,NA}",
  "is categorical/non-numeric",
  # Telemetry degradation (plan 05-13). The success line is
  # "intervention telemetry: wrote N rows to ...", so only the WARN form and
  # the pre-flight form are forbidden.
  "WARN intervention telemetry:",
  "intervention telemetry: output directory not writable"
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
# failure (its task column-info attribute is missing) before its deterministic direct
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
  "PASS verify_intervention_smoke scenario=%s region=%s year=%d interventions=%d maps_checked=%d telemetry_rows=%d\n",
  scenario, region, year, n_active, maps_checked, telemetry_rows
))
quit(status = 0)

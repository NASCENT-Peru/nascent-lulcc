# Phase 5 Plan 14 (D-23): end-to-end fixture tests for
# scripts/verify_intervention_smoke.r.
#
# The verifier is the phase's regression gate for every future scenario and
# region, so the thing it must never do is say PASS about nothing (WR-07). These
# blocks drive the real script as a subprocess against a synthetic run output
# tree and pin:
#   - D-06a  an intervention NONE of whose target maps could be asserted on is
#            a FAIL, not a vacuous PASS (the real WR-07 condition);
#   - WR-07b maps_checked counts only assertions that had cells to assert on;
#   - WR-04  a single transition whose from-class misses the mask is INFO and
#            is not counted, NOT a FAIL — the guard that over-fired on correct
#            runs in every region but the one the phase gated on;
#   - D-06b  an active Absolute-0 intervention with no target rows leaves the
#            run asserting nothing, so maps_checked == 0 is a FAIL;
#   - D-04   an Outside-zone mask with no cell equal to 1 is a FAIL: the
#            complement assertion is otherwise satisfied trivially (CR-01);
#   - CR-03  "argument is of length zero" / "subscript out of bounds" are fatal
#            forbidden markers (the exact crash signature of the missing
#            Prob_adjust_* defect);
#   - CR-01  the mask value-domain stops ("outside {0,1,NA}",
#            "is categorical/non-numeric") are fatal forbidden markers;
#   - D-22   the 05-13 telemetry CSV exists, matches the AUDIT lines and shows
#            mean_after = 0 for Absolute-to-0 target rows;
#   - GAP-1  an active Absolute-0 intervention that this region's
#            trans_rates.csv holds no target rows for FAILs the run by name,
#            even when a sibling intervention keeps maps_checked above the
#            run-level floor — and its Outside mask is still proved
#            non-degenerate. A two-intervention run where both have rows is
#            the control that the ledger does not false-FAIL;
#   - R51-WR-04
#            a probability map with no non-NA cell anywhere in the region is a
#            FAIL unless the judged worker log carries the engine's own
#            "has no predictions; wrote empty TIF" WARN for that row;
#   - R51-IN-01
#            one log line matching two forbidden markers yields errors=1;
#   - D-03   the forbidden vector carries "has no cell equal to 1" and parses
#            standalone (frozen contract with plans 05.1-05 and 05.1-06).
#
# IN-06: the repository root is resolved WITHOUT base R's null-coalescing
# operator, which would be taken from base at file-source time, before
# src/utils.r is available, and which the engine deliberately avoids.

library(testthat)

.smoke_repo_root <- local({
  has_script <- function(p) {
    !is.null(p) && nzchar(p) &&
      file.exists(file.path(p, "scripts", "verify_intervention_smoke.r"))
  }
  cand <- tryCatch(
    normalizePath(testthat::test_path("..", ".."), winslash = "/", mustWork = FALSE),
    error = function(e) NULL
  )
  if (is.null(cand) || identical(cand, "")) cand <- NULL
  if (!has_script(cand)) {
    alt <- normalizePath(file.path(getwd(), "..", ".."), winslash = "/", mustWork = FALSE)
    if (has_script(alt)) {
      cand <- alt
    } else {
      here <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
      if (has_script(here)) cand <- here
    }
  }
  if (is.null(cand)) cand <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  cand
})

.smoke_script <- file.path(.smoke_repo_root, "scripts", "verify_intervention_smoke.r")
.smoke_rscript <- file.path(R.home("bin"), "Rscript")

# The 25-column telemetry contract published by plan 05-13.
.smoke_tel_cols <- c(
  "scenario", "region", "year", "intervention_id", "rank", "type", "zone",
  "mask", "target_class", "n_target", "n_changed", "mean_before", "mean_after",
  "sd_before", "sd_after", "p05_delta", "p25_delta", "p50_delta", "p75_delta",
  "p95_delta", "min_delta", "max_delta", "sum_abs_delta", "prob_mass_before",
  "prob_mass_after"
)

# --------------------------------------------------------------------------
# Raster fixtures: the same 4x5 grid idiom as test-spatial-interventions.R, but
# the verifier crops the mask to the probability map and compares geometry
# directly (no cell_index), so mask and map only have to share this one grid.

.smoke_grid <- function() {
  terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
}

# Cells the mask marks as inside. Chosen so the zone is non-empty on the happy
# path and trivially emptied (cells = integer(0)) for the WR-07 fixture.
.smoke_mask_cells <- c(2L, 7L, 13L)

.smoke_write_raster <- function(path, values) {
  r <- .smoke_grid()
  terra::values(r) <- values
  suppressWarnings(terra::writeRaster(r, path, overwrite = TRUE))
  path
}

.smoke_write_mask <- function(path, cells = .smoke_mask_cells) {
  v <- rep(NA_real_, 20L)
  if (length(cells) > 0L) v[cells] <- 1
  .smoke_write_raster(path, v)
}

# A probability map that is exactly 0 on the cells of the adjusted zone and
# positive elsewhere: what an Absolute-to-0 intervention is supposed to
# produce. `na_cells` makes the map non-NA-free on those cells, which is how a
# per-transition map whose from-class does not intersect the mask looks (WR-04).
.smoke_write_prob <- function(path, zero_cells = .smoke_mask_cells, fill = 0.5,
                              na_cells = integer(0)) {
  v <- rep(fill, 20L)
  if (length(zero_cells) > 0L) v[zero_cells] <- 0
  if (length(na_cells) > 0L) v[na_cells] <- NA_real_
  .smoke_write_raster(path, v)
}

# The intervention-identifying tokens are formals so a fixture can emit a
# SECOND AUDIT line. The defaults reproduce the single-intervention line
# byte for byte.
.smoke_audit_iv <- function(region_label, scenario, year, zone = "Inside",
                            id = "iv_abs", rank = 1L, to_vals = "105,104",
                            mask = "mask_a.tif", rows_target = 6,
                            rows_changed = 6, sum_abs_delta = 1.62) {
  sprintf(
    paste0(
      "2026-01-01 00:00:00 AUDIT stage=intervention region=%s scenario=%s ",
      "year=%d id=%s rank=%d type=Absolute zone=%s to_vals=%s ",
      "mask=%s rows_target=%g rows_changed=%g delta_mean=-0.27 ",
      "delta_med=-0.19 delta_sd=0.192146 delta_min=-0.6 delta_max=-0.1 ",
      "n_inc=0 n_dec=6 sum_abs_delta=%g"
    ),
    region_label, scenario, as.integer(year), id, as.integer(rank), zone,
    to_vals, mask, rows_target, rows_changed, sum_abs_delta
  )
}

.smoke_audit_summary <- function(region_label, scenario, year,
                                 n_interventions = 1L) {
  sprintf(
    paste0(
      "2026-01-01 00:00:01 AUDIT stage=intervention_summary region=%s ",
      "scenario=%s year=%d n_interventions=%d cells_sum_gt1=0"
    ),
    region_label, scenario, as.integer(year), as.integer(n_interventions)
  )
}

# Emits one row per target class. The distribution columns are not asserted on
# by the verifier, so they are recycled to the requested row count; the columns
# section (e) reconciles against the AUDIT line are formals.
.smoke_telemetry <- function(scenario, region, year, zone = "Inside",
                             id = "iv_abs", rank = 1L, mask = "mask_a.tif",
                             target_classes = c(105L, 104L),
                             n_target = c(3L, 3L), n_changed = c(3L, 3L),
                             sum_abs_delta = c(1.2, 0.42)) {
  n <- length(target_classes)
  sum_abs_delta <- rep_len(as.numeric(sum_abs_delta), n)
  data.frame(
    scenario = rep(scenario, n),
    region = rep(region, n),
    year = rep(as.integer(year), n),
    intervention_id = rep(id, n),
    rank = rep(as.integer(rank), n),
    type = rep("Absolute", n),
    zone = rep(zone, n),
    mask = rep(mask, n),
    target_class = as.integer(target_classes),
    n_target = rep_len(as.integer(n_target), n),
    n_changed = rep_len(as.integer(n_changed), n),
    mean_before = rep_len(c(0.4, 0.14), n),
    mean_after = rep(0, n),
    sd_before = rep_len(c(0.2, 0.04), n),
    sd_after = rep(0, n),
    p05_delta = rep_len(c(-0.58, -0.176), n),
    p25_delta = rep_len(c(-0.5, -0.16), n),
    p50_delta = rep_len(c(-0.4, -0.14), n),
    p75_delta = rep_len(c(-0.3, -0.12), n),
    p95_delta = rep_len(c(-0.22, -0.104), n),
    min_delta = rep_len(c(-0.6, -0.18), n),
    max_delta = rep_len(c(-0.2, -0.1), n),
    sum_abs_delta = sum_abs_delta,
    prob_mass_before = abs(sum_abs_delta),
    prob_mass_after = rep(0, n),
    stringsAsFactors = FALSE
  )
}

.smoke_write_telemetry <- function(path, tel) {
  utils::write.csv(tel, path, row.names = FALSE, quote = FALSE, na = "")
  path
}

# --------------------------------------------------------------------------
# The full run-output fixture.

#   zone             Prob_adjust_zone, threaded into the YAML entry, the AUDIT
#                    line and the telemetry `zone` column so the three sources
#                    stay mutually consistent. The probability maps follow: the
#                    cells written as 0 are the mask cells for "Inside" and
#                    their complement for "Outside", so the fixture is a
#                    correct Absolute-to-0 run under either zone.
#   na_in_zone_maps  subset of c(1L, 2L) naming which probability maps also get
#                    NA_real_ at mask_cells. Such a map has no non-NA cell in an
#                    Inside zone: the WR-04 shape.
#   trans_to         the To_lulc column of trans_rates.csv, so a fixture can
#                    present an intervention with no matching target rows.
#   all_na_maps      subset of c(1L, 2L) naming which probability maps are
#                    written entirely NA. That is what allocation writes for an
#                    active transition with no predictions, so the shape is
#                    legitimate only when the worker log explains it.
#   empty_tif_warn_maps
#                    subset of c(1L, 2L) for which the engine's exact
#                    "WARN id_trans=.. row=.. has no predictions; wrote empty
#                    TIF" line is appended to the judged worker log.
#   second           when a list, adds a SECOND active Allocation intervention.
#                    This reproduces the shipped config/NAT_interventions.yml
#                    shape — Conservation_expansion_and_preservation
#                    (Absolute/0/Inside, three target classes, almost always
#                    has rows) paired with Mining_freeze_post_2030
#                    (Absolute/0/Outside, mining alone, often has none). No
#                    fixture covered that shape, which is why an intervention
#                    could go entirely unasserted behind a sibling's green
#                    banner. Its sum_abs_delta field is the per-telemetry-row
#                    vector; the second AUDIT line carries its sum, which is
#                    the number section (e) reconciles against.
.smoke_fixture <- function(root, scenario = "BAU", region = "r1", year = 2028L,
                           region_label = "R1", mask_cells = .smoke_mask_cells,
                           zone = "Inside", na_in_zone_maps = integer(0),
                           trans_to = c(105L, 104L),
                           all_na_maps = integer(0),
                           empty_tif_warn_maps = integer(0),
                           second = NULL) {
  # Wholesale field replacement, not modifyList(): modifyList() recurses into
  # list-valued fields and merges them BY NAME, so an unnamed override such as
  # targets = list("a", "b") would silently leave the default list("mining") in
  # place.
  sec <- NULL
  if (!is.null(second)) {
    sec <- list(
      id = "iv_mine", rank = 2L, mask_name = "mask_b.tif",
      mask_cells = integer(0), zone = "Outside",
      targets = list("mining"), to_vals = "106", target_classes = 106L,
      rows_target = 0, rows_changed = 0, sum_abs_delta = 0,
      n_target = 0L, n_changed = 0L
    )
    for (nm in names(second)) sec[[nm]] <- second[[nm]]
  }
  output_root <- file.path(root, "out")
  region_dir <- file.path(
    output_root, scenario, as.character(year), paste0("region_", region)
  )
  prob_dir <- file.path(region_dir, "probability_map_dir")
  mask_dir <- file.path(root, "masks")
  iv_dir <- file.path(root, "interventions")
  dir.create(prob_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(mask_dir, recursive = TRUE, showWarnings = FALSE)
  dir.create(iv_dir, recursive = TRUE, showWarnings = FALSE)

  .smoke_write_raster(file.path(region_dir, "posterior.tif"), rep(1, 20L))
  mask_path <- .smoke_write_mask(file.path(mask_dir, "mask_a.tif"), mask_cells)
  if (!is.null(sec)) {
    .smoke_write_mask(file.path(mask_dir, sec$mask_name), sec$mask_cells)
  }

  # One row per target class. The verifier reconstructs the map filename from
  # the 1-based row index and id_trans, so row order is load-bearing.
  tr <- data.frame(
    From_lulc = c(101L, 101L),
    To_lulc = as.integer(trans_to),
    Rate = c(0.1, 0.2),
    id_trans = c(11L, 12L),
    stringsAsFactors = FALSE
  )
  utils::write.csv(tr, file.path(region_dir, "trans_rates.csv"), row.names = FALSE)
  zero_cells <- if (identical(zone, "Inside")) {
    mask_cells
  } else {
    setdiff(seq_len(20L), mask_cells)
  }
  na_cells_for <- function(j) {
    if (j %in% all_na_maps) {
      seq_len(20L)
    } else if (j %in% na_in_zone_maps) {
      mask_cells
    } else {
      integer(0)
    }
  }
  .smoke_write_prob(
    file.path(prob_dir, "001_id_trans_11.tif"), zero_cells,
    na_cells = na_cells_for(1L)
  )
  .smoke_write_prob(
    file.path(prob_dir, "002_id_trans_12.tif"), zero_cells,
    na_cells = na_cells_for(2L)
  )

  # Map j carries id_trans 10 + j and sits at trans_rates row j, which is the
  # pair the engine prints and the verifier rebuilds the filename from.
  warn_lines <- vapply(
    sort(as.integer(empty_tif_warn_maps)),
    function(j) {
      sprintf(
        "2026-01-01 00:00:00 WARN id_trans=%d row=%d has no predictions; wrote empty TIF",
        10L + j, j
      )
    },
    character(1)
  )

  log_path <- file.path(region_dir, "worker_1.log")
  writeLines(
    c(
      "2026-01-01 00:00:00 Applying intervention: iv_abs",
      .smoke_audit_iv(region_label, scenario, year, zone = zone),
      if (is.null(sec)) {
        character(0)
      } else {
        .smoke_audit_iv(
          region_label, scenario, year, zone = sec$zone, id = sec$id,
          rank = sec$rank, to_vals = sec$to_vals, mask = sec$mask_name,
          rows_target = sec$rows_target, rows_changed = sec$rows_changed,
          # sec$sum_abs_delta is the per-telemetry-row vector; the AUDIT line
          # carries the intervention total, which is what section (e)
          # reconciles against.
          sum_abs_delta = sum(sec$sum_abs_delta)
        )
      },
      .smoke_audit_summary(
        region_label, scenario, year,
        n_interventions = if (is.null(sec)) 1L else 2L
      ),
      warn_lines
    ),
    log_path
  )

  entry <- list(
    Intervention_stage = "Allocation",
    Intervention_ID = "iv_abs",
    Intervention_ranking = 1L,
    Mask_type = "Static",
    Intervention_mask = "mask_a.tif",
    Time_steps_implemented = list(as.integer(year)),
    Prob_adjust_type = "Absolute",
    Prob_adjust_value = 0,
    Prob_adjust_zone = zone,
    Transition_target_classes = list(
      "built_up_and_barren_lands", "high_intensity_agricultural_areas"
    )
  )
  entries <- list(entry)
  if (!is.null(sec)) {
    entries[[2L]] <- list(
      Intervention_stage = "Allocation",
      Intervention_ID = sec$id,
      Intervention_ranking = as.integer(sec$rank),
      Mask_type = "Static",
      Intervention_mask = sec$mask_name,
      Time_steps_implemented = list(as.integer(year)),
      Prob_adjust_type = "Absolute",
      Prob_adjust_value = 0,
      Prob_adjust_zone = sec$zone,
      Transition_target_classes = sec$targets
    )
  }
  yaml_path <- file.path(iv_dir, paste0(scenario, "_interventions.yml"))
  yaml::write_yaml(entries, yaml_path)

  csv_path <- file.path(
    region_dir,
    sprintf("intervention_prob_deltas_%s_%s_%d.csv", scenario, region, as.integer(year))
  )
  tel <- .smoke_telemetry(scenario, region, year, zone = zone)
  if (!is.null(sec)) {
    tel <- rbind(tel, .smoke_telemetry(
      scenario, region, year, zone = sec$zone, id = sec$id, rank = sec$rank,
      mask = sec$mask_name, target_classes = sec$target_classes,
      n_target = sec$n_target, n_changed = sec$n_changed,
      sum_abs_delta = sec$sum_abs_delta
    ))
  }
  .smoke_write_telemetry(csv_path, tel)

  list(
    scenario = scenario, region = region, year = as.integer(year),
    region_label = region_label, output_root = output_root,
    region_dir = region_dir, prob_dir = prob_dir, mask_dir = mask_dir,
    iv_dir = iv_dir, mask_path = mask_path, log_path = log_path,
    yaml_path = yaml_path, csv_path = csv_path
  )
}

# --------------------------------------------------------------------------
# Driving the real script.

.run_verifier <- function(fx, extra = character(0)) {
  args <- c(
    shQuote(.smoke_script),
    "--output-root", shQuote(fx$output_root),
    "--mask-dir", shQuote(fx$mask_dir),
    "--interventions-dir", shQuote(fx$iv_dir),
    "--scenario", fx$scenario,
    "--region", fx$region,
    "--year", as.character(fx$year),
    extra
  )
  out <- suppressWarnings(
    system2(.smoke_rscript, args, stdout = TRUE, stderr = TRUE)
  )
  status <- attr(out, "status")
  if (is.null(status)) status <- 0L
  list(status = as.integer(status), out = paste(out, collapse = "\n"))
}

# Exit status 2 is the script's "usage or sourcing error" code. It is a genuine
# environment problem ONLY when the project config could not be loaded; an
# unknown-flag or usage rejection is a real result and must be allowed to fail
# the expectations below.
.skip_if_config_unavailable <- function(res) {
  if (identical(res$status, 2L) &&
        grepl("ERROR sourcing|get_config|Loaded configuration for environment",
              res$out) &&
        !grepl("Unknown flag|Unknown argument|requires a value", res$out)) {
    testthat::skip(
      "verify_intervention_smoke could not load the project config in this environment"
    )
  }
  invisible(NULL)
}

.smoke_read_csv <- function(path) {
  utils::read.csv(path, check.names = FALSE, stringsAsFactors = FALSE)
}

# --------------------------------------------------------------------------

test_that("CR-03s: the forbidden marker list carries both length-zero messages", {
  # Static: runs even when the subprocess blocks skip.
  src <- paste(readLines(.smoke_script, warn = FALSE), collapse = "\n")
  expect_true(grepl("argument is of length zero", src, fixed = TRUE))
  expect_true(grepl("subscript out of bounds", src, fixed = TRUE))
})

test_that("D-06a: an intervention whose maps all miss the zone fails at intervention level", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, mask_cells = integer(0))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "no probability map could be asserted on", fixed = TRUE)
  # The per-map path is now INFO; the FAIL comes from the level above it.
  expect_match(res$out, "not counted", fixed = TRUE)
})

test_that("WR-07b: maps_checked counts only non-vacuous assertions", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, mask_cells = integer(0))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_false(grepl("maps_checked=1", res$out, fixed = TRUE))
  expect_false(grepl("maps_checked=2", res$out, fixed = TRUE))
})

test_that("WR-04: a transition whose from-class misses the mask is INFO, not FAIL", {
  # `r` is a per-transition map, so a mask holding none of that transition's
  # from-class is an ordinary correct-engine outcome. Under the HEAD guard this
  # exited 1 — it is the regression that would have failed the andes run.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, na_in_zone_maps = 1L)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "not counted", fixed = TRUE)
  expect_match(res$out, "maps_checked=1", fixed = TRUE)
  expect_match(res$out, "PASS verify_intervention_smoke", fixed = TRUE)
})

test_that("D-06b: an active Absolute-0 intervention with no target rows fails the run", {
  # No trans_rates row matches the target classes, so the intervention is
  # skipped before the map loop and the run asserts nothing at all.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, trans_to = c(101L, 102L))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "maps_checked=0", fixed = TRUE)
})

test_that("D-04: a degenerate Outside mask fails instead of passing trivially", {
  # The Outside zone is the complement of the mask, so an empty mask satisfies
  # "prob is 0 over the complement" across the whole region with a large,
  # non-vacuous n_zone (CR-01).
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, zone = "Outside", mask_cells = integer(0))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "degenerate mask", fixed = TRUE)
})

test_that("D-04: a 1-coded Outside mask still passes", {
  # No-regression control for the shipped Absolute/0/Outside entries.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, zone = "Outside")
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "maps_checked=2", fixed = TRUE)
})

# The CR-01s source-text grep for "outside {0,1,NA}" / "is categorical/non-numeric"
# is retired (R51-IN-05): it passed on the rationale comments above `forbidden`.
# See test-intervention-wording-contract.R and the D-03 block near the end of this file.

test_that("CR-01: a mask value-domain stop in the worker log is fatal", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat(paste0(
    "2026-01-01 00:00:02 ERROR: intervention mask ",
    "/inputs/mining_concessions_mask.tif has value(s) outside {0,1,NA}: 255\n"
  ), file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "forbidden marker", fixed = TRUE)
})

test_that("CR-03: 'argument is of length zero' is a forbidden marker", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat("2026-01-01 00:00:02 ERROR: argument is of length zero\n",
      file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "forbidden marker", fixed = TRUE)
  expect_match(res$out, "argument is of length zero", fixed = TRUE)
})

test_that("CR-03: 'subscript out of bounds' is a forbidden marker", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat("2026-01-01 00:00:02 ERROR: subscript out of bounds\n",
      file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "forbidden marker", fixed = TRUE)
  expect_match(res$out, "subscript out of bounds", fixed = TRUE)
})

test_that("D-22: a missing telemetry CSV fails the verifier", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  file.remove(fx$csv_path)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "intervention_prob_deltas", fixed = TRUE)
})

test_that("D-22b: telemetry rows must match the AUDIT lines", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)

  # (i) a missing (intervention_id, target_class) pair.
  tel <- .smoke_read_csv(fx$csv_path)
  .smoke_write_telemetry(fx$csv_path, tel[tel$target_class != 104L, , drop = FALSE])
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "iv_abs", fixed = TRUE)

  # (ii) restored rows, but sum(n_target) now disagrees with AUDIT rows_target.
  tel <- .smoke_telemetry(fx$scenario, fx$region, fx$year)
  tel$n_target[[1]] <- 2L
  .smoke_write_telemetry(fx$csv_path, tel)
  res2 <- .run_verifier(fx)
  .skip_if_config_unavailable(res2)
  expect_identical(res2$status, 1L)
  expect_match(res2$out, "rows_target", fixed = TRUE)
})

test_that("D-22c: Absolute-to-0 rows must report mean_after = 0", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  tel <- .smoke_telemetry(fx$scenario, fx$region, fx$year)
  tel$mean_after[[1]] <- 0.3
  .smoke_write_telemetry(fx$csv_path, tel)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "mean_after", fixed = TRUE)
})

test_that("GAP-1: a sibling Absolute-0 intervention with a degenerate Outside mask and no target rows fails the run", {
  # The shipped config shape: an Inside intervention that has rows paired with
  # an Outside mining-only intervention that has none. Before the hoist and the
  # ledger this exited 0 — the sibling kept maps_checked above the run-level
  # floor, so the green banner was read as evidence for both.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, second = list())
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  # The hoisted D-04 fired even though the intervention has zero target rows.
  expect_match(res$out, "degenerate mask", fixed = TRUE)
  expect_match(res$out, "iv_mine", fixed = TRUE)
  expect_match(res$out, "this run is not evidence for them", fixed = TRUE)
  expect_false(grepl("PASS verify_intervention_smoke", res$out, fixed = TRUE))
})

test_that("GAP-1: the unasserted ledger fires even when the skipped intervention's mask is fine", {
  # A correctly 1-coded Inside mask, so D-04 does not apply and only the ledger
  # can catch that nothing was asserted about this intervention.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(
    root,
    second = list(zone = "Inside", mask_cells = .smoke_mask_cells)
  )
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "this run is not evidence for them", fixed = TRUE)
  expect_match(res$out, "iv_mine", fixed = TRUE)
  expect_match(res$out, "Info: iv_mine has no target rows", fixed = TRUE)
})

test_that("GAP-1 control: two Absolute-0 interventions that both have rows still PASS", {
  # Regression guard: the ledger must not false-FAIL a correct
  # multi-intervention run.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, second = list(
    zone = "Inside", mask_cells = .smoke_mask_cells, mask_name = "mask_b.tif",
    targets = list("built_up_and_barren_lands",
                   "high_intensity_agricultural_areas"),
    to_vals = "105,104", target_classes = c(105L, 104L),
    rows_target = 6, rows_changed = 6, sum_abs_delta = c(1.2, 0.42),
    n_target = c(3L, 3L), n_changed = c(3L, 3L)
  ))
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "maps_checked=4", fixed = TRUE)
  expect_match(res$out, "interventions=2", fixed = TRUE)
  expect_false(grepl("this run is not evidence", res$out, fixed = TRUE))
})

test_that("R51-WR-04: an all-NA probability map explained by the engine's WARN is INFO, not FAIL", {
  # src/allocation.r writes an all-NA TIF on purpose for an active transition
  # with no predictions, to keep the %03d sequence Dinamica binds to gap-free.
  # Failing on that shape would reject a correct run.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, all_na_maps = 1L, empty_tif_warn_maps = 1L)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "the engine logged no predictions for id_trans", fixed = TRUE)
  expect_match(res$out, "maps_checked=1", fixed = TRUE)
  expect_match(res$out, "PASS verify_intervention_smoke", fixed = TRUE)
})

test_that("R51-WR-04: an unexplained all-NA probability map FAILs", {
  # Same raster, no engine WARN to explain it: nothing accounts for a map that
  # holds no probability anywhere, so it must not be downgraded to INFO.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root, all_na_maps = 1L)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(
    res$out, "has no non-NA probability cell anywhere in this region",
    fixed = TRUE
  )
})

test_that("R51-IN-01: a single mis-coded-mask log line produces one forbidden error, not two", {
  # The Stage 7 pre-flight wording carries both "intervention mask: " and
  # "outside {0,1,NA}", which used to be counted twice in the FAIL banner.
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  cat(paste0(
    "2026-01-01 00:00:02 ERROR: intervention mask: /inputs/m.tif ",
    "has value(s) outside {0,1,NA} (observed min 0, max 255)\n"
  ), file = fx$log_path, append = TRUE)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 1L)
  expect_match(res$out, "FAIL verify_intervention_smoke", fixed = TRUE)
  expect_match(res$out, "errors=1", fixed = TRUE)
})

test_that("D-03: the forbidden vector carries the no-1-cell substring and parses standalone", {
  # Static: runs even when the subprocess blocks skip. Asserts on the PARSED
  # vector, not a source grep, so an occurrence in a comment cannot satisfy it.
  # Frozen cross-file contract with plans 05.1-05 and 05.1-06.
  src <- readLines(.smoke_script, warn = FALSE)
  i <- grep("^forbidden <- c\\(", src)
  expect_length(i, 1L)
  j <- i + which(grepl("^\\)[[:space:]]*$", src[(i + 1):length(src)]))[1]
  fb <- eval(parse(text = paste(src[i:j], collapse = "\n")))
  expect_true(is.character(fb))
  expect_true("has no cell equal to 1" %in% fb)
  expect_true("outside {0,1,NA}" %in% fb)
  expect_true("is categorical/non-numeric" %in% fb)
})

test_that("the intact fixture is a real PASS with a non-zero maps_checked", {
  root <- withr::local_tempdir()
  fx <- .smoke_fixture(root)
  res <- .run_verifier(fx)
  .skip_if_config_unavailable(res)
  expect_identical(res$status, 0L)
  expect_match(res$out, "PASS verify_intervention_smoke", fixed = TRUE)
  expect_match(res$out, "maps_checked=2", fixed = TRUE)
  expect_match(res$out, "n_zone=3", fixed = TRUE)
})

# Tests for the active prep-script path repairs in Phase 1 Plan 1
# (PIPE-01, PIPE-03 + canonical_refs D-12, D-13).
#
# Contract:
#   * src/calibration_predictor_prep.r resolves terra temp via the shared
#     Stage 7 contract (Sys.getenv("TERRA_TEMP", unset = tempdir())) instead
#     of the hardcoded `E:/terra_temp` literal.
#   * src/simulation_trans_rates_prep.r reads LULC demand from the
#     config-driven CSV path (`config[["lulc_demand_path"]]`) instead of
#     the hardcoded Windows xlsx path.

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

read_file <- function(rel) {
  paste(
    readLines(file.path(.repo_root, rel), warn = FALSE),
    collapse = "\n"
  )
}

test_that("calibration_predictor_prep.r no longer hardcodes E:/terra_temp", {
  src <- read_file("src/calibration_predictor_prep.r")
  expect_false(
    grepl("E:/terra_temp", src, fixed = TRUE),
    info = "Hardcoded E:/terra_temp literal must be removed (PIPE-03)."
  )
})

test_that("calibration_predictor_prep.r resolves TERRA_TEMP via env/config", {
  src <- read_file("src/calibration_predictor_prep.r")
  expect_true(
    grepl("TERRA_TEMP", src, fixed = TRUE),
    info = "Must reference TERRA_TEMP env override per the shared contract."
  )
})

test_that("simulation_trans_rates_prep.r drops the hardcoded LULC_demand xlsx", {
  src <- read_file("src/simulation_trans_rates_prep.r")
  expect_false(
    grepl("LULC_demand_results.xlsx", src, fixed = TRUE),
    info = "Hardcoded Windows xlsx demand path must be removed (PIPE-01)."
  )
  # Belt & braces: also check no E:/ literal anywhere in the file.
  expect_false(
    grepl("E:/nascent-lulcc", src, fixed = TRUE),
    info = "Hardcoded E:/ data-basepath literal must be removed."
  )
})

test_that("simulation_trans_rates_prep.r reads demand via config[[\"lulc_demand_path\"]]", {
  src <- read_file("src/simulation_trans_rates_prep.r")
  expect_true(
    grepl("lulc_demand_path", src, fixed = TRUE),
    info = "Must read demand path from config[[\"lulc_demand_path\"]] per PIPE-01."
  )
})

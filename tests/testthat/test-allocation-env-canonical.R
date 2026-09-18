# Tests for the canonical Stage 7 execution environment contract introduced in
# Phase 1 Plan 2 (MEM-06 + canonical_refs A4 / Pitfall 2).
#
# Contract:
#   - environments/allocation_env.yml MUST contain the full prediction-time
#     package set required by MEM-06: r-parsnip, r-recipes, r-ranger,
#     r-xgboost (pinned 1.7.x), r-tidypredict, r-butcher, r-ps, r-lobstr,
#     r-bundle, r-qs, r-rhpcblasctl.
#   - The two allocation submit scripts MUST activate `allocation_env`, not
#     `transition_model_env` — otherwise MEM-06 is satisfied on paper but the
#     batch run still uses a drifted environment (RESEARCH Pitfall 2).
#
# These tests intentionally read the YAML and shell scripts as text rather
# than parsing them, because Rscript may not be present in the executor host
# (matching the 01-01 precedent) and because the assertion is about checked-in
# contract content, not runtime behaviour.

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (file.info(here)$isdir %||% FALSE) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.read_text <- function(rel) {
  paste(readLines(file.path(.repo_root, rel), warn = FALSE), collapse = "\n")
}

# --------------------------------------------------------------------------
# allocation_env.yml: prediction-time package set (MEM-06)
# --------------------------------------------------------------------------

test_that("allocation_env.yml declares the full MEM-06 prediction-time package set", {
  yml <- .read_text("environments/allocation_env.yml")

  required_packages <- c(
    "r-parsnip",
    "r-recipes",
    "r-ranger",
    "r-tidypredict",
    "r-butcher",
    "r-ps",
    "r-lobstr",
    "r-bundle",
    "r-qs",
    "r-rhpcblasctl"
  )
  for (pkg in required_packages) {
    expect_true(
      grepl(paste0("(?m)^\\s*-\\s*", pkg, "(\\s|=|$)"), yml, perl = TRUE),
      info = sprintf(
        "environments/allocation_env.yml must include MEM-06 package %s",
        pkg
      )
    )
  }
})

test_that("allocation_env.yml pins r-xgboost to the 1.7.x line", {
  yml <- .read_text("environments/allocation_env.yml")
  # MEM-06 requires r-xgboost pinned 1.7.x; transition_model_env.yml uses the
  # same major.minor pin and we must not let allocation_env drift below that.
  expect_true(
    grepl("(?m)^\\s*-\\s*r-xgboost\\s*=\\s*1\\.7", yml, perl = TRUE),
    info = "environments/allocation_env.yml must pin r-xgboost to 1.7.x"
  )
})

# --------------------------------------------------------------------------
# Submit scripts: canonical activation target (RESEARCH Pitfall 2)
# --------------------------------------------------------------------------

test_that("submit_allocation.sh activates allocation_env, not transition_model_env", {
  sh <- .read_text("scripts/submit_allocation.sh")
  expect_true(
    grepl('(?m)^\\s*ENV_NAME\\s*=\\s*"?allocation_env"?\\s*$', sh, perl = TRUE),
    info = "scripts/submit_allocation.sh must set ENV_NAME=allocation_env"
  )
  expect_false(
    grepl('ENV_NAME\\s*=\\s*"?transition_model_env"?', sh, perl = TRUE),
    info = "scripts/submit_allocation.sh must not activate transition_model_env"
  )
})

test_that("submit_allocation_profile.sh activates allocation_env, not transition_model_env", {
  sh <- .read_text("scripts/submit_allocation_profile.sh")
  expect_true(
    grepl('(?m)^\\s*ENV_NAME\\s*=\\s*"?allocation_env"?\\s*$', sh, perl = TRUE),
    info = "scripts/submit_allocation_profile.sh must set ENV_NAME=allocation_env"
  )
  expect_false(
    grepl('ENV_NAME\\s*=\\s*"?transition_model_env"?', sh, perl = TRUE),
    info = "scripts/submit_allocation_profile.sh must not activate transition_model_env"
  )
})

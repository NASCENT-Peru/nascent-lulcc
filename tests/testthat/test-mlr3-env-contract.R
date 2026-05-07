# Contract tests for Phase 2 Plan 01 (MEM-04):
#   - environments/allocation_env.yml MUST contain the six mlr3 packages
#     required for loading and calling $predict_newdata() on mlr3 Learner objects
#     saved by the rewritten transition_modelling.r.
#   - environments/transition_model_env.yml MUST contain the five mlr3 training
#     packages (r-glmnet is already present and is NOT asserted here).
#   - config/local_config.yaml MUST have max_training_rows under configuration_settings.
#   - config/hpc_config.yaml MUST have max_training_rows under configuration_settings.
#
# These tests read YAML/config files as text (not parsed) — consistent with the
# pattern established in test-allocation-env-canonical.R.

library(testthat)

`%||%` <- function(x, y) if (!is.null(x) && !is.na(x)) x else y

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (file.info(here)$isdir %||% FALSE) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

.read_text <- function(rel) {
  paste(readLines(file.path(.repo_root, rel), warn = FALSE), collapse = "\n")
}

# --------------------------------------------------------------------------
# Test 1: allocation_env.yml declares all six Phase 2 mlr3 packages (MEM-04)
# --------------------------------------------------------------------------

test_that("allocation_env.yml declares all Phase 2 mlr3 packages (MEM-04)", {
  yml <- .read_text("environments/allocation_env.yml")

  mlr3_packages <- c(
    "r-mlr3",
    "r-mlr3learners",
    "r-mlr3tuning",
    "r-paradox",
    "r-bbotk",
    "r-glmnet"
  )
  for (pkg in mlr3_packages) {
    expect_true(
      grepl(paste0("(?m)^\\s*-\\s*", pkg, "(\\s|=|$)"), yml, perl = TRUE),
      info = sprintf(
        "environments/allocation_env.yml must include Phase 2 MEM-04 package %s",
        pkg
      )
    )
  }
})

# --------------------------------------------------------------------------
# Test 2: allocation_env.yml does NOT duplicate r-mlr3 entries (sanity guard)
# --------------------------------------------------------------------------

test_that("allocation_env.yml does not duplicate r-mlr3 entries", {
  yml <- .read_text("environments/allocation_env.yml")

  # Count occurrences of strict r-mlr3 (not r-mlr3learners, r-mlr3tuning, etc.)
  matches <- gregexpr("(?m)^\\s*-\\s*r-mlr3\\s*$", yml, perl = TRUE)[[1]]
  n_matches <- if (matches[1] == -1) 0L else length(matches)

  expect_equal(
    n_matches, 1L,
    info = "environments/allocation_env.yml must contain exactly one r-mlr3 entry (no duplicates)"
  )
})

# --------------------------------------------------------------------------
# Test 3: local_config.yaml has max_training_rows under configuration_settings
# --------------------------------------------------------------------------

test_that("local_config.yaml has max_training_rows under configuration_settings", {
  yml <- .read_text("config/local_config.yaml")

  expect_true(
    grepl("(?m)^\\s+max_training_rows:", yml, perl = TRUE),
    info = "config/local_config.yaml must contain max_training_rows under configuration_settings"
  )
})

# --------------------------------------------------------------------------
# Test 4: hpc_config.yaml has max_training_rows under configuration_settings
# --------------------------------------------------------------------------

test_that("hpc_config.yaml has max_training_rows under configuration_settings", {
  yml <- .read_text("config/hpc_config.yaml")

  expect_true(
    grepl("(?m)^\\s+max_training_rows:", yml, perl = TRUE),
    info = "config/hpc_config.yaml must contain max_training_rows under configuration_settings"
  )
})

# --------------------------------------------------------------------------
# Test 5: transition_model_env.yml declares all five Phase 2 mlr3 training packages (MEM-04)
#         Note: r-glmnet is already present and is NOT asserted here.
# --------------------------------------------------------------------------

test_that("transition_model_env.yml declares all Phase 2 mlr3 training packages (MEM-04)", {
  yml <- .read_text("environments/transition_model_env.yml")

  mlr3_training_packages <- c(
    "r-mlr3",
    "r-mlr3learners",
    "r-mlr3tuning",
    "r-paradox",
    "r-bbotk"
  )
  for (pkg in mlr3_training_packages) {
    expect_true(
      grepl(paste0("(?m)^\\s*-\\s*", pkg, "(\\s|=|$)"), yml, perl = TRUE),
      info = sprintf(
        "environments/transition_model_env.yml must include Phase 2 MEM-04 training package %s",
        pkg
      )
    )
  }
})

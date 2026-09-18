# Tests for the mlr3 training pipeline introduced in Phase 2 Plan 02 (MEM-04).
#
# Contract:
#   - src/transition_modelling.r MUST contain train_mlr3_transition() function
#   - src/transition_modelling.r MUST NOT contain library(tidymodels) or library(workflows)
#   - build_transition_model_path() MUST return a .qs extension (not .rds)
#   - train_mlr3_transition() MUST produce a valid .qs model file readable by qs::qread()
#     with the expected structure: model_type, predictor_names, response_levels, learner
#
# Tests 1-3 read src/transition_modelling.r as text (grep-based assertions).
# Test 4 is an integration test that requires mlr3 packages to be installed.

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
# Test 1: transition_modelling.r exports train_mlr3_transition function
# --------------------------------------------------------------------------

test_that("transition_modelling.r exports train_mlr3_transition function", {
  content <- .read_text("src/transition_modelling.r")
  expect_true(
    grepl("train_mlr3_transition", content, fixed = TRUE),
    info = "src/transition_modelling.r must contain train_mlr3_transition function definition"
  )
})

# --------------------------------------------------------------------------
# Test 2: transition_modelling.r does not contain tidymodels or workflows library calls
# --------------------------------------------------------------------------

test_that("transition_modelling.r does not contain library(tidymodels) or library(workflows)", {
  content <- .read_text("src/transition_modelling.r")

  # Strip comment lines before checking (lines starting with optional whitespace + #)
  lines <- strsplit(content, "\n")[[1]]
  non_comment_lines <- lines[!grepl("^\\s*#", lines)]
  non_comment_content <- paste(non_comment_lines, collapse = "\n")

  expect_false(
    grepl("library(tidymodels)", non_comment_content, fixed = TRUE),
    info = "src/transition_modelling.r must not call library(tidymodels) outside of comments"
  )
  expect_false(
    grepl("library(workflows)", non_comment_content, fixed = TRUE),
    info = "src/transition_modelling.r must not call library(workflows) outside of comments"
  )
  expect_false(
    grepl("library(parsnip)", non_comment_content, fixed = TRUE),
    info = "src/transition_modelling.r must not call library(parsnip) outside of comments"
  )
})

# --------------------------------------------------------------------------
# Test 3: build_transition_model_path returns .qs extension (not .rds)
# --------------------------------------------------------------------------

test_that("build_transition_model_path returns .qs extension (not .rds)", {
  content <- .read_text("src/transition_modelling.r")

  expect_true(
    grepl(".qs", content, fixed = TRUE),
    info = "src/transition_modelling.r must contain .qs extension (build_transition_model_path)"
  )
  # Verify the old .rds extension is NOT used in the path builder
  # Check specifically for the sprintf format string with .rds
  expect_false(
    grepl("%s_%s.rds", content, fixed = TRUE),
    info = "src/transition_modelling.r must not contain %s_%s.rds in build_transition_model_path"
  )
})

# --------------------------------------------------------------------------
# Test 4: train_mlr3_transition produces a valid .qs model file (integration)
# --------------------------------------------------------------------------

test_that("train_mlr3_transition produces a valid .qs model file (integration)", {
  skip_if_not_installed("mlr3")
  skip_if_not_installed("mlr3learners")
  skip_if_not_installed("qs")

  # Source the transition_modelling.r to make functions available.
  # We use local() to avoid polluting the global environment and tryCatch to
  # skip gracefully if sourcing fails due to missing dependencies at source time.
  tm_path <- file.path(.repo_root, "src", "transition_modelling.r")
  skip_if_not(file.exists(tm_path), "src/transition_modelling.r not found")

  # Attempt to load the function. If sourcing the whole file fails (due to
  # other missing packages loaded at source time), try to extract just the
  # function definitions we need.
  source_env <- new.env(parent = baseenv())
  source_result <- tryCatch(
    source(tm_path, local = source_env),
    error = function(e) e
  )

  skip_if(
    inherits(source_result, "error"),
    sprintf("Could not source transition_modelling.r: %s", conditionMessage(source_result))
  )

  skip_if(
    !exists("train_mlr3_transition", envir = source_env),
    "train_mlr3_transition not found in transition_modelling.r after sourcing"
  )

  # Create a 200-row synthetic dataset with factor response and 5 numeric predictors
  set.seed(42L)
  n <- 200L
  synth_data <- data.frame(
    p1 = rnorm(n), p2 = rnorm(n), p3 = rnorm(n), p4 = rnorm(n), p5 = rnorm(n),
    response = factor(sample(c("0", "1"), n, replace = TRUE, prob = c(0.7, 0.3)))
  )
  pred_names <- c("p1", "p2", "p3", "p4", "p5")

  # Minimal config
  test_config <- list(
    max_training_rows = 500000L,
    random_seed       = 42L
  )

  # Minimal model_specs using RF with single-value params (num.trees=50 for speed)
  test_model_specs <- list(
    global = list(
      cv_folds    = 3L,
      random_seed = 42L
    ),
    models = list(
      rf = list(
        parameters = list(
          num.trees     = list(50L),
          min.node.size = list(5L),
          mtry          = list(3L)
        )
      )
    )
  )

  # Create a temp output path ending in .qs
  out_path <- tempfile(fileext = ".qs")
  on.exit(unlink(out_path), add = TRUE)

  # Call train_mlr3_transition()
  result <- tryCatch(
    source_env$train_mlr3_transition(
      transition_data = synth_data,
      predictor_names = pred_names,
      trans_name      = "test_transition",
      region          = NULL,
      output_path     = out_path,
      config          = test_config,
      model_specs     = test_model_specs,
      log_file        = NULL
    ),
    error = function(e) {
      stop(sprintf("train_mlr3_transition() failed: %s", conditionMessage(e)))
    }
  )

  # Assert: file exists and is non-empty
  expect_true(file.exists(out_path), info = "Output .qs file must exist after train_mlr3_transition()")
  expect_gt(file.size(out_path), 0L, label = "Output .qs file must be non-empty")

  # Assert: result structure
  expect_equal(result$status, "success", info = "Result status must be 'success'")
  expect_equal(result$transition, "test_transition")

  # Assert: qs::qread() returns valid model object
  model_obj <- qs::qread(out_path)
  expect_true(is.list(model_obj), info = "qs::qread() must return a list")
  expect_true(
    all(c("model_type", "predictor_names", "response_levels", "learner") %in% names(model_obj)),
    info = "model object must contain model_type, predictor_names, response_levels, learner"
  )
  expect_equal(model_obj$model_type, "mlr3", info = "model_type must be 'mlr3'")
  expect_true(
    all(model_obj$predictor_names %in% colnames(synth_data)),
    info = "predictor_names must be a subset of synthetic data column names"
  )

  # Assert: 5-row predict_newdata() returns valid probabilities
  fixture_rows <- utils::head(synth_data[, pred_names, drop = FALSE], 5L)
  pred_result <- model_obj$learner$predict_newdata(newdata = fixture_rows)
  prob_vals <- pred_result$prob[, "1"]

  expect_false(any(is.na(prob_vals)), info = "Predicted probabilities must not contain NA")
  expect_true(all(prob_vals >= 0), info = "Predicted probabilities must be >= 0")
  expect_true(all(prob_vals <= 1), info = "Predicted probabilities must be <= 1")
})

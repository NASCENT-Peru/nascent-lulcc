# Tests for the mlr3 dispatch branch in predict_saved_transition_prob() and
# the extension-detecting model loader introduced in Phase 2 Plan 3 (MEM-04).
#
# Contract:
#   - predict_saved_transition_prob() MUST dispatch to the mlr3 branch when
#     model_obj$model_type == "mlr3", and that branch MUST be checked BEFORE
#     the tidypredict_ branch.
#   - The mlr3 branch MUST call predict_newdata() and return data.frame(.pred_0,
#     .pred_1) with values in [0, 1].
#   - The model loader at ~line 1514 in allocation.r MUST call qs::qread() when
#     the file path has a .qs extension.
#
# Tests 1-3 and 5 are grep-based (read allocation.r as text) so they can run
# without sourcing the full allocation.r dependency chain.
# Test 4 is an integration test that sources allocation.r and exercises the
# dispatch with a real mlr3 Learner trained on synthetic data — guarded by
# skip_if_not_installed("mlr3") and a tryCatch around source().

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
# Test 1: allocation.r contains the mlr3 dispatch branch
# --------------------------------------------------------------------------

test_that("allocation.r contains mlr3 dispatch branch", {
  content <- .read_text("src/allocation.r")
  expect_true(
    grepl('model_obj$model_type == "mlr3"', content, fixed = TRUE),
    info = "allocation.r must contain exact-equality check: model_obj$model_type == \"mlr3\""
  )
})

# --------------------------------------------------------------------------
# Test 2: mlr3 branch appears BEFORE the tidypredict_ check
# --------------------------------------------------------------------------

test_that("mlr3 branch appears before tidypredict_ check in allocation.r", {
  content <- .read_text("src/allocation.r")
  pos_mlr3 <- regexpr('model_type == "mlr3"', content, fixed = TRUE)
  pos_tidypredict <- regexpr('grepl("^tidypredict_"', content, fixed = TRUE)
  expect_true(
    pos_mlr3 > 0L,
    info = "allocation.r must contain 'model_type == \"mlr3\"'"
  )
  expect_true(
    pos_tidypredict > 0L,
    info = "allocation.r must contain 'grepl(\"^tidypredict_\"'"
  )
  expect_true(
    pos_mlr3 < pos_tidypredict,
    info = "mlr3 dispatch check must appear BEFORE the tidypredict_ check in allocation.r"
  )
})

# --------------------------------------------------------------------------
# Test 3: mlr3 branch uses predict_newdata and named prob column
# --------------------------------------------------------------------------

test_that("mlr3 branch uses predict_newdata and named prob column", {
  content <- .read_text("src/allocation.r")
  expect_true(
    grepl("predict_newdata", content, fixed = TRUE),
    info = "allocation.r must contain predict_newdata (the mlr3 prediction call)"
  )
  expect_true(
    grepl('prob[, "1"]', content, fixed = TRUE),
    info = "allocation.r must use named column indexing: prob[, \"1\"] (not positional)"
  )
})

# --------------------------------------------------------------------------
# Test 4: mlr3 dispatch returns .pred_0 and .pred_1 with mock model object
# --------------------------------------------------------------------------

test_that("mlr3 dispatch returns .pred_0 and .pred_1 with mock model object (integration)", {
  skip_if_not_installed("mlr3")
  skip_if_not_installed("mlr3learners")

  library(mlr3)
  library(mlr3learners)

  set.seed(42L)
  n <- 50L
  synth <- data.frame(
    p1       = rnorm(n),
    p2       = rnorm(n),
    p3       = rnorm(n),
    response = factor(sample(c("0", "1"), n, replace = TRUE))
  )
  task <- mlr3::as_task_classif(synth, target = "response", positive = "1")
  lrn_rf <- mlr3::lrn(
    "classif.ranger",
    predict_type = "prob",
    num.trees    = 10L,
    save.memory  = TRUE,
    importance   = "none",
    num.threads  = 1L
  )
  lrn_rf$train(task)

  mock_model_obj <- list(
    model_type      = "mlr3",
    predictor_names = c("p1", "p2", "p3"),
    response_levels = c("0", "1"),
    learner         = lrn_rf
  )
  new_rows <- data.frame(p1 = rnorm(5L), p2 = rnorm(5L), p3 = rnorm(5L))

  # Source allocation.r — skip this test if the dependency chain is unavailable
  tryCatch(
    source(file.path(.repo_root, "src/allocation.r")),
    error = function(e) skip(e$message)
  )

  result <- predict_saved_transition_prob(mock_model_obj, new_rows)

  expect_equal(names(result), c(".pred_0", ".pred_1"))
  expect_true(all(result$.pred_0 >= 0 & result$.pred_0 <= 1))
  expect_true(all(result$.pred_1 >= 0 & result$.pred_1 <= 1))
  expect_true(all(abs(result$.pred_0 + result$.pred_1 - 1) < 1e-10),
    info = ".pred_0 + .pred_1 must equal 1 for each row"
  )
})

# --------------------------------------------------------------------------
# Test 5: allocation.r model loader detects .qs extension and calls qs::qread()
# --------------------------------------------------------------------------

test_that("allocation.r model loader detects .qs extension and calls qs::qread()", {
  content <- .read_text("src/allocation.r")
  expect_true(
    grepl("qs::qread", content, fixed = TRUE),
    info = "allocation.r model loader must call qs::qread() for .qs model files"
  )
})

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

allocation_text <- paste(
  readLines(file.path(.repo_root, "src", "allocation.r"), warn = FALSE),
  collapse = "\n"
)
run_script_text <- paste(
  readLines(file.path(.repo_root, "scripts", "run_allocation.r"), warn = FALSE),
  collapse = "\n"
)

test_that("allocation.r defines automatic backend selection", {
  expect_match(allocation_text, "select_allocation_plan <- function\\(")
  expect_match(allocation_text, "\"multicore\"")
  expect_match(allocation_text, "\"multisession\"")
})

test_that("run_allocation pins native threads before first future plan", {
  pin_pos <- regexpr("pin_native_threads_to_one", run_script_text, fixed = TRUE)[[1]]
  plan_pos <- regexpr("future::plan(", run_script_text, fixed = TRUE)[[1]]
  expect_gt(pin_pos, 0L)
  expect_gt(plan_pos, 0L)
  expect_lt(pin_pos, plan_pos)
})

test_that("strict-globals gate is wired to the documented env var", {
  expect_match(run_script_text, "ALLOCATION_DEV_STRICT_GLOBALS", fixed = TRUE)
  expect_match(run_script_text, "future.globals.onReference = \"error\"", fixed = TRUE)
})

test_that("allocation smoke filters exist for region and posterior year", {
  expect_match(allocation_text, "ALLOCATION_REGION_FILTER", fixed = TRUE)
  expect_match(allocation_text, "ALLOCATION_YEAR_POST_FILTER", fixed = TRUE)
})

test_that("cgroup snapshot helper logs both current and max counters", {
  expect_match(allocation_text, "prof_cgroup_snapshot <- function\\(")
  expect_match(allocation_text, "memory.current", fixed = TRUE)
  expect_match(allocation_text, "memory.max", fixed = TRUE)
})

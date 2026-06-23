# Phase 3.5 Plan 01: thread-count resolver for scoped multi-threaded ranger
# prediction (Goal 2). Covers get_allocation_predict_num_threads(), the resolver
# that decides how many threads the per-transition ranger predict call may use,
# bounded by `threads x effective_workers <= cores`.
#
# Contract (from 03.5-RESEARCH.md Q3 "Safe num.threads formula"):
#   - Explicit ALLOCATION_PREDICT_NUM_THREADS=N (positive int) returns N,
#     hard-clamped so threads x effective_workers never exceeds cores.
#   - Empty/unset -> auto: floor(cores / effective_workers), clamped >= 1.
#   - ALLOCATION_PARALLEL_STRATEGY=sequential -> effective_workers = 1 (ignore
#     ALLOCATION_NUM_WORKERS), so auto = full cores. THIS is the denominator trap
#     the smoke script trips (it sets ALLOCATION_NUM_WORKERS=cores even when
#     sequential): the resolver must NOT divide by ALLOCATION_NUM_WORKERS then.
#   - Non-sequential -> effective_workers = max(1, ALLOCATION_NUM_WORKERS).
#   - cores = SLURM_CPUS_PER_TASK, else parallel::detectCores(), else 1.
#   - Return value is always a single integer >= 1L; never NA, never 0,
#     never > cores.
#
# This test sources src/allocation.r (the same way test-mlr3-predict-dispatch.R
# does) so it exercises the real resolver. Before Plan 01 Task 1 lands the
# function, the source defines no get_allocation_predict_num_threads() and every
# call below errors with "could not find function" — the intended RED state.

library(testthat)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

# Source the real allocation helpers. allocation.r is pure function definitions
# (no top-level library()/source()), so this succeeds under a plain R install.
tryCatch(
  source(file.path(.repo_root, "src/allocation.r")),
  error = function(e) skip(paste("could not source allocation.r:", conditionMessage(e)))
)

# Run `code` with the given env vars set, restoring prior state afterwards.
# Vars whose value is NA are UNSET for the duration (so we can test absence).
.with_env <- function(vars, code) {
  names_v <- names(vars)
  prev <- lapply(names_v, function(n) Sys.getenv(n, unset = NA_character_))
  names(prev) <- names_v
  on.exit(
    for (n in names_v) {
      if (is.na(prev[[n]])) Sys.unsetenv(n) else do.call(Sys.setenv, setNames(list(prev[[n]]), n))
    },
    add = TRUE
  )
  for (n in names_v) {
    if (is.na(vars[[n]])) Sys.unsetenv(n) else do.call(Sys.setenv, setNames(list(vars[[n]]), n))
  }
  force(code)
}

# --------------------------------------------------------------------------
# Assertion 1: explicit ALLOCATION_PREDICT_NUM_THREADS=N returns N
# (here cores >= N so no clamp engages)
# --------------------------------------------------------------------------
test_that("explicit ALLOCATION_PREDICT_NUM_THREADS returns the requested value", {
  res <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "8",
      ALLOCATION_PARALLEL_STRATEGY = "sequential",
      ALLOCATION_NUM_WORKERS = "4",
      SLURM_CPUS_PER_TASK = "40"
    ),
    get_allocation_predict_num_threads()
  )
  expect_identical(res, 8L)
})

# --------------------------------------------------------------------------
# Assertion 2: sequential strategy => effective workers = 1 => full cores,
# NOT cores/ALLOCATION_NUM_WORKERS even though ALLOCATION_NUM_WORKERS=4 is set.
# This is the denominator trap from RESEARCH Q3.
# --------------------------------------------------------------------------
test_that("sequential strategy treats effective workers as 1 (auto = full cores)", {
  res <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "",
      ALLOCATION_PARALLEL_STRATEGY = "sequential",
      ALLOCATION_NUM_WORKERS = "4",
      SLURM_CPUS_PER_TASK = "40"
    ),
    get_allocation_predict_num_threads()
  )
  expect_identical(res, 40L)
})

# --------------------------------------------------------------------------
# Assertion 3: non-sequential strategy divides cores by ALLOCATION_NUM_WORKERS:
# floor(40 / 4) = 10.
# --------------------------------------------------------------------------
test_that("non-sequential strategy auto = floor(cores / workers)", {
  res <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "",
      ALLOCATION_PARALLEL_STRATEGY = "multisession",
      ALLOCATION_NUM_WORKERS = "4",
      SLURM_CPUS_PER_TASK = "40"
    ),
    get_allocation_predict_num_threads()
  )
  expect_identical(res, 10L)
})

# --------------------------------------------------------------------------
# Assertion 4: a misconfigured explicit value greater than the per-worker core
# budget is clamped so threads x effective_workers never exceeds cores.
# Non-sequential, cores=40, workers=4 => budget=10; asking for 999 clamps to 10.
# --------------------------------------------------------------------------
test_that("explicit value above the per-worker core budget is clamped", {
  res <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "999",
      ALLOCATION_PARALLEL_STRATEGY = "multisession",
      ALLOCATION_NUM_WORKERS = "4",
      SLURM_CPUS_PER_TASK = "40"
    ),
    get_allocation_predict_num_threads()
  )
  expect_identical(res, 10L)
})

# --------------------------------------------------------------------------
# Assertion 5: absent SLURM_CPUS_PER_TASK falls back to detectCores() and the
# resolver still returns a sane integer >= 1.
# --------------------------------------------------------------------------
test_that("absent SLURM_CPUS_PER_TASK falls back to detectCores and stays >= 1", {
  res <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "",
      ALLOCATION_PARALLEL_STRATEGY = "sequential",
      ALLOCATION_NUM_WORKERS = NA_character_,
      SLURM_CPUS_PER_TASK = NA_character_
    ),
    get_allocation_predict_num_threads()
  )
  expect_true(is.integer(res))
  expect_length(res, 1L)
  expect_gte(res, 1L)
})

# --------------------------------------------------------------------------
# Assertion 6: the return value is always a single integer >= 1 — never 0, NA,
# or fractional — even when the knob is garbage and workers oversubscribe cores.
# cores=2, workers=8 => floor(2/8)=0 must clamp UP to 1.
# --------------------------------------------------------------------------
test_that("resolver always returns a single integer >= 1 (never 0 or NA)", {
  res_garbage <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "not-a-number",
      ALLOCATION_PARALLEL_STRATEGY = "multisession",
      ALLOCATION_NUM_WORKERS = "4",
      SLURM_CPUS_PER_TASK = "40"
    ),
    get_allocation_predict_num_threads()
  )
  expect_true(is.integer(res_garbage))
  expect_length(res_garbage, 1L)
  expect_false(is.na(res_garbage))
  expect_gte(res_garbage, 1L)

  res_oversubscribed <- .with_env(
    list(
      ALLOCATION_PREDICT_NUM_THREADS = "",
      ALLOCATION_PARALLEL_STRATEGY = "multisession",
      ALLOCATION_NUM_WORKERS = "8",
      SLURM_CPUS_PER_TASK = "2"
    ),
    get_allocation_predict_num_threads()
  )
  expect_identical(res_oversubscribed, 1L)
})

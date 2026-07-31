# Session-lived transition-model cache (.transition_model_cache /
# load_transition_model_file_cached). The model set is identical across a
# scenario's timesteps, so load_allocation_models() must deserialize each
# model file from disk AT MOST ONCE per R session (per file mtime/size), while
# still rebuilding the model_info table per call so the per-timestep
# active-transition filter keeps working unchanged.
#
# Contract:
#   - First load_allocation_models() call reads every matching file from disk.
#   - A repeat call with the same active set reads NOTHING from disk.
#   - Widening the active set on a later "timestep" reads ONLY the newly
#     activated model files.
#   - Replacing a model file on disk (new mtime/size) invalidates that entry.
#   - The returned model_info table is fresh per call: mutating it by
#     reference (:=) must not leak into later calls.
#
# Fixture models are .rds (qs is absent on local dev hosts; the loader
# dispatches on extension, and the cache sits above the loader, so the cached
# path is identical for .qs).

library(testthat)
library(data.table)

.repo_root <- (function() {
  here <- tryCatch(normalizePath(sys.frame(1)$ofile %||% "."), error = function(e) ".")
  if (is.null(here) || identical(here, "")) here <- "."
  is_dir <- tryCatch(file.info(here)$isdir, error = function(e) NA)
  if (isTRUE(is_dir)) here <- file.path(here, "x")
  normalizePath(file.path(dirname(dirname(dirname(here)))), mustWork = FALSE)
})()

tryCatch(
  source(file.path(.repo_root, "src/allocation.r")),
  error = function(e) skip(paste("could not source allocation.r:", conditionMessage(e)))
)

skip_if_not_installed("jsonlite")

# --------------------------------------------------------------------------
# Fixture: a transition_model_dir with three fitted-model artifacts for one
# region, plus the lulc aggregation JSON that load_allocation_class_map()
# resolves class names against. Model objects carry predictor_names so
# get_saved_transition_predictors() takes its cheap first branch.
.build_model_fixture <- function(root) {
  calibration_period <- "2018_2022"
  model_dir <- file.path(root, "transition_models", calibration_period)
  dir.create(model_dir, recursive = TRUE, showWarnings = FALSE)

  schema_path <- file.path(root, "lulc_agg.json")
  jsonlite::write_json(
    list(
      list(class_name = "forest", value = 1L),
      list(class_name = "cropland", value = 2L),
      list(class_name = "urban", value = 3L)
    ),
    schema_path,
    auto_unbox = TRUE
  )

  region_label <- "Test Region"
  region_suffix <- "test_region"
  transitions <- list(
    c("forest", "cropland"),
    c("forest", "urban"),
    c("cropland", "urban")
  )
  for (tr in transitions) {
    saveRDS(
      list(
        model_type = "fixture",
        trans_name = paste(tr, collapse = "-"),
        predictor_names = c("p1", "p2")
      ),
      file.path(
        model_dir,
        sprintf("%s-%s_%s.rds", tr[[1]], tr[[2]], region_suffix)
      )
    )
  }

  list(
    config = list(
      transition_model_dir = file.path(root, "transition_models"),
      lulc_aggregation_path = schema_path
    ),
    calibration_period = calibration_period,
    region_label = region_label,
    region_suffix = region_suffix,
    model_dir = model_dir
  )
}

# Count real disk reads by shadowing load_transition_model_file (the cached
# wrapper resolves it lexically in the sourcing environment, so a same-named
# counting shim there intercepts every cache MISS).
.with_read_counter <- function(code) {
  counter <- new.env(parent = emptyenv())
  counter$n <- 0L
  original <- load_transition_model_file
  assign(
    "load_transition_model_file",
    function(file_path) {
      counter$n <- counter$n + 1L
      original(file_path)
    },
    envir = globalenv()
  )
  on.exit(assign("load_transition_model_file", original, envir = globalenv()))
  force(code)
  counter$n
}

.active <- function(...) {
  pairs <- list(...)
  data.table::data.table(
    from_val = vapply(pairs, `[[`, integer(1), 1L),
    to_val = vapply(pairs, `[[`, integer(1), 2L)
  )
}

test_that("repeat loads hit the cache instead of disk", {
  fx <- .build_model_fixture(withr::local_tempdir())
  clear_transition_model_cache()
  on.exit(clear_transition_model_cache(), add = TRUE)

  cold <- NULL
  n_cold <- .with_read_counter({
    cold <- suppressMessages(load_allocation_models(
      region_labels = fx$region_label,
      calibration_period = fx$calibration_period,
      config = fx$config
    ))
  })
  expect_equal(n_cold, 3L)
  expect_equal(nrow(cold[[fx$region_suffix]]), 3L)

  second <- NULL
  n_warm <- .with_read_counter({
    second <- suppressMessages(load_allocation_models(
      region_labels = fx$region_label,
      calibration_period = fx$calibration_period,
      config = fx$config
    ))
  })
  expect_equal(n_warm, 0L)

  # Cached objects are the real deserialized models, not placeholders.
  tbl <- second[[fx$region_suffix]]
  expect_setequal(
    vapply(tbl$model_obj, `[[`, character(1), "trans_name"),
    c("forest-cropland", "forest-urban", "cropland-urban")
  )
  expect_true(all(vapply(
    tbl$predictor_names,
    function(p) identical(p, c("p1", "p2")),
    logical(1)
  )))
})

test_that("a widened active set on a later timestep reads only new files", {
  fx <- .build_model_fixture(withr::local_tempdir())
  clear_transition_model_cache()
  on.exit(clear_transition_model_cache(), add = TRUE)

  # Timestep 1: only forest->cropland (1,2) is active.
  n_t1 <- .with_read_counter({
    suppressMessages(load_allocation_models(
      region_labels = fx$region_label,
      calibration_period = fx$calibration_period,
      config = fx$config,
      active_transitions = .active(c(1L, 2L))
    ))
  })
  expect_equal(n_t1, 1L)

  # Timestep 2: rates activate all three transitions -> only the two new
  # files are deserialized.
  t2 <- NULL
  n_t2 <- .with_read_counter({
    t2 <- suppressMessages(load_allocation_models(
      region_labels = fx$region_label,
      calibration_period = fx$calibration_period,
      config = fx$config,
      active_transitions = .active(c(1L, 2L), c(1L, 3L), c(2L, 3L))
    ))
  })
  expect_equal(n_t2, 2L)
  expect_equal(nrow(t2[[fx$region_suffix]]), 3L)

  # Timestep 3: a narrowed set still returns the right subset, zero reads.
  t3 <- NULL
  n_t3 <- .with_read_counter({
    t3 <- suppressMessages(load_allocation_models(
      region_labels = fx$region_label,
      calibration_period = fx$calibration_period,
      config = fx$config,
      active_transitions = .active(c(1L, 3L))
    ))
  })
  expect_equal(n_t3, 0L)
  expect_equal(
    t3[[fx$region_suffix]]$model_obj[[1L]]$trans_name,
    "forest-urban"
  )
})

test_that("a replaced model file (new mtime/size) is re-read, not served stale", {
  fx <- .build_model_fixture(withr::local_tempdir())
  clear_transition_model_cache()
  on.exit(clear_transition_model_cache(), add = TRUE)

  target <- file.path(fx$model_dir, "forest-cropland_test_region.rds")
  suppressMessages(load_allocation_models(
    region_labels = fx$region_label,
    calibration_period = fx$calibration_period,
    config = fx$config
  ))

  saveRDS(
    list(
      model_type = "fixture",
      trans_name = "forest-cropland",
      version = 2L,
      predictor_names = c("p1", "p2", "p3")
    ),
    target
  )
  Sys.setFileTime(target, Sys.time() + 5) # force a distinct mtime

  refreshed <- suppressMessages(load_allocation_models(
    region_labels = fx$region_label,
    calibration_period = fx$calibration_period,
    config = fx$config,
    active_transitions = .active(c(1L, 2L))
  ))
  obj <- refreshed[[fx$region_suffix]]$model_obj[[1L]]
  expect_equal(obj$version, 2L)
  expect_equal(obj$predictor_names, c("p1", "p2", "p3"))
})

test_that("mutating a returned model table does not poison later loads", {
  fx <- .build_model_fixture(withr::local_tempdir())
  clear_transition_model_cache()
  on.exit(clear_transition_model_cache(), add = TRUE)

  first <- suppressMessages(load_allocation_models(
    region_labels = fx$region_label,
    calibration_period = fx$calibration_period,
    config = fx$config
  ))
  # Simulate what a downstream consumer might do to its own copy of the table.
  first[[fx$region_suffix]][, model_obj := list(list(NULL))]
  first[[fx$region_suffix]][, poisoned := TRUE]

  again <- suppressMessages(load_allocation_models(
    region_labels = fx$region_label,
    calibration_period = fx$calibration_period,
    config = fx$config
  ))
  tbl <- again[[fx$region_suffix]]
  expect_false("poisoned" %in% names(tbl))
  expect_true(all(!vapply(tbl$model_obj, is.null, logical(1))))
  expect_setequal(
    vapply(tbl$model_obj, `[[`, character(1), "trans_name"),
    c("forest-cropland", "forest-urban", "cropland-urban")
  )
})

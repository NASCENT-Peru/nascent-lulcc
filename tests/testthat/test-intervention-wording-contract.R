# Phase 05.1 Plan 07 (R51-IN-05): the frozen cross-file wording contract (D-03),
# asserted so that it can actually fail.
#
# THE DEFECT THIS FILE CLOSES. D-03 freezes a handful of substrings that three
# independently-edited files must agree on. Until now the agreement was asserted
# twice, and neither assertion read the other:
#   - test-verify-intervention-smoke.R grepped the verifier's whole SOURCE TEXT
#     for the substrings, and
#   - test-spatial-interventions.R expected the engine's message to contain them.
# The verifier's own rationale comment quotes every frozen substring, so deleting
# an entry from its `forbidden` vector left the source grep GREEN while the
# verifier stopped treating that condition as fatal. The gate did not prove what
# it claimed. That grep is retired by this plan; blocks 10 and 11 below
# demonstrate the defect on mutated copies of the real files.
#
# WHY A PARSE AND NOT A GREP. `tests/testthat/helper-intervention-contract.R`
# reads string literals through the R parser (`utils::getParseData()`
# `STR_CONST` tokens) and evaluates the verifier's `forbidden <- c(...)` block
# via its srcref. The parser discards comments, so a comment can never satisfy
# an assertion here, and a substring split across two adjacent literals is not
# one emittable message. A `^[[:space:]]*#` line filter would have been fooled by
# both (see blocks 7 and 8).
#
# WHO MUST EMIT WHAT (the frozen contract, all substrings prefix-free and matched
# by the verifier with `grep(f, line, fixed = TRUE)`):
#
#   S1  "outside {0,1,NA}"               pre-existing (plans 05.1-01 / 05.1-02)
#   S2  "is categorical/non-numeric"     pre-existing (plans 05.1-01 / 05.1-02)
#   S3  "has no cell equal to 1"         plan 05.1-04 forbids it; 05 and 06 emit it
#   S4  "values could not be read"       PRE-FLIGHT ONLY; deliberately NOT forbidden
#
#   scripts/verify_intervention_smoke.r    `forbidden` holds S1, S2, S3 and the
#                                          marker "intervention mask: "; NOT S4
#   src/allocation.r (Stage 7 pre-flight)  literals holding S1..S4, each STARTING
#                                          with "intervention mask: " (colon form)
#   src/implement_spatial_interventions.R  literals holding S1..S3, each containing
#                                          "intervention mask " and NOT
#                                          "intervention mask: " (colon-free form);
#                                          NOT S4 anywhere
#
# The engine's legitimate non-fatal WARN (plan 05.1-06) must match NO forbidden
# marker once rendered, or a correct run would FAIL the verifier.
#
# All five literals below were read back from the merged tree at this plan's base
# commit and are byte-identical to the wordings recorded in the 05.1-04, 05.1-05
# and 05.1-06 SUMMARYs; there is no drift to note.

library(testthat)

# `.repo_root` lives in helper-spatial-interventions.R, which testthat loads
# before this file under both `test_file()` and `test_dir()`. The guard keeps a
# bare `source()` of this file working - see that helper's header for why the old
# sourcing-frame bootstrap was wrong.
if (!exists(".repo_root", inherits = TRUE)) {
  source(testthat::test_path("helper-spatial-interventions.R"))
}
if (!exists(".contract_string_literals", inherits = TRUE)) {
  source(testthat::test_path("helper-intervention-contract.R"))
}

# The frozen constants. Paraphrasing any of these is the thing the contract
# forbids, so they are spelled once, here.
.S1 <- "outside {0,1,NA}"
.S2 <- "is categorical/non-numeric"
.S3 <- "has no cell equal to 1"
.S4 <- "values could not be read"
.MARK <- "intervention mask: "
.STEM <- "intervention mask "
.WARN_PREFIX <- "WARN intervention zone: "
.WARN_TEMPLATE <- "WARN intervention zone: %s mask %s does not intersect region %s; Prob_adjust_zone=%s therefore applies to %s of the region"

.verifier_path <- function() .contract_path("scripts", "verify_intervention_smoke.r")
.engine_path <- function() .contract_path("src", "implement_spatial_interventions.R")
.preflight_path <- function() .contract_path("src", "allocation.r")


# --- the contract, asserted over parsed sources ------------------------------

test_that("D-03 contract: the verifier's parsed forbidden vector carries the frozen markers and not the pre-flight-only one", {
  fb <- .contract_forbidden_vector(.verifier_path())
  expect_type(fb, "character")
  expect_true(length(fb) > 0L)
  expect_false(anyNA(fb))

  # The marker spelling the helper's two form rules use must be the same string
  # the verifier greps for, or the rules below would police a different contract.
  expect_identical(.MARK, .contract_mark())
  expect_identical(.STEM, .contract_stem())

  expect_true(.S1 %in% fb)
  expect_true(.S2 %in% fb)
  expect_true(.S3 %in% fb)
  expect_true(.MARK %in% fb)

  # S4 is deliberately absent (plan 05.1-04): the Stage 7 pre-flight is its only
  # emitter and that wording already carries the "intervention mask: " marker
  # above, so a second entry would report the SAME log line twice and inflate the
  # errors=N in the FAIL banner (R51-IN-01). This assertion stops a later plan
  # from "completing" the list and reintroducing the double-report.
  expect_false(.S4 %in% fb)
})

test_that("D-03 contract: the engine emits S1-S3 colon-free, in string literals, and never S4", {
  lits <- .contract_string_literals(.engine_path())
  expect_true(length(lits) > 0L)
  expect_identical(.contract_missing(lits, c(.S1, .S2, .S3)), character(0))

  for (s in c(.S1, .S2, .S3)) {
    expect_true(
      .engine_form_ok(lits, s, mark = .MARK, stem = .STEM),
      info = sprintf(
        "engine literals carrying '%s' must all contain '%s' and none may contain '%s': %s",
        s, .STEM, .MARK,
        paste(.contract_literals_with(lits, s), collapse = " | ")
      )
    )
  }

  # The engine never emits S4: the pre-flight owns that wording (plan 05.1-06).
  # Note this is a check over LITERALS, so the roxygen prose above
  # `.mask_inside_lut()` cannot satisfy or violate it.
  expect_identical(.contract_literals_with(lits, .S4), character(0))
})

test_that("D-03 contract: the Stage 7 pre-flight emits S1-S4 behind the colon marker, in string literals", {
  lits <- .contract_string_literals(.preflight_path())
  expect_true(length(lits) > 0L)
  expect_identical(.contract_missing(lits, c(.S1, .S2, .S3, .S4)), character(0))

  for (s in c(.S1, .S2, .S3, .S4)) {
    expect_true(
      .preflight_form_ok(lits, s, mark = .MARK),
      info = sprintf(
        "pre-flight literals carrying '%s' must all start with '%s': %s",
        s, .MARK,
        paste(.contract_literals_with(lits, s), collapse = " | ")
      )
    )
  }
})

test_that("D-03 contract: the engine's legitimate WARN is pinned and matches no forbidden marker", {
  fb <- .contract_forbidden_vector(.verifier_path())
  lits <- .contract_string_literals(.engine_path())
  warns <- lits[startsWith(lits, .WARN_PREFIX)]
  expect_length(warns, 1L)
  expect_identical(warns[[1L]], .WARN_TEMPLATE)

  # A mask that is validly 1-coded but simply misses this region is NOT an error.
  # If any forbidden marker matched this sentence, every correct run of a national
  # mask over a region it does not cover would FAIL the smoke verifier.
  rendered <- sprintf(.WARN_TEMPLATE, "iv_abs", "mask_a.tif", "R1", "Outside", "all")
  matched <- vapply(fb, function(f) grepl(f, rendered, fixed = TRUE), logical(1))
  expect_false(any(matched))
  # The same for the Inside rendering, whose trailing clause differs.
  rendered_inside <- sprintf(
    .WARN_TEMPLATE, "iv_abs", "mask_a.tif", "R1", "Inside", "none"
  )
  expect_false(any(vapply(
    fb, function(f) grepl(f, rendered_inside, fixed = TRUE), logical(1)
  )))
})

test_that("D-03 contract: real engine stops for 255, categorical and all-zero masks match the verifier's parsed markers", {
  # The review's own IN-05 fix, made executable: real `.mask_inside_lut()` error
  # messages are matched against the verifier's EVALUATED vector, so a paraphrase
  # on either side breaks this block. Fixtures are built inline on purpose - this
  # file must not depend on a helper defined in another test file.
  fb <- .contract_forbidden_vector(.verifier_path())

  eng <- new.env(parent = globalenv())
  sys.source(.contract_path("src", "utils.r"), envir = eng)
  sys.source(.engine_path(), envir = eng)

  scratch <- withr::local_tempdir()
  grid <- function() {
    terra::rast(nrows = 4, ncols = 5, xmin = 0, xmax = 5, ymin = 0, ymax = 4)
  }
  mask_cells <- c(2L, 7L, 13L)
  write_numeric <- function(name, value) {
    r <- grid()
    v <- rep(NA_real_, terra::ncell(r))
    v[mask_cells] <- value
    terra::values(r) <- v
    p <- file.path(scratch, name)
    terra::writeRaster(r, p, overwrite = TRUE)
    p
  }
  write_categorical <- function(name) {
    r <- grid()
    v <- rep(0L, terra::ncell(r))
    v[mask_cells] <- 1L
    terra::values(r) <- v
    levels(r) <- data.frame(value = c(0L, 1L), label = c("outside", "inside"))
    p <- file.path(scratch, name)
    terra::writeRaster(r, p, overwrite = TRUE)
    p
  }
  ref_path <- local({
    r <- grid()
    terra::values(r) <- seq_len(terra::ncell(r))
    p <- file.path(scratch, "ref_grid.tif")
    terra::writeRaster(r, p, overwrite = TRUE)
    p
  })
  cell_index <- data.frame(
    cell_id = 1:6, ref_cell_id = c(2L, 3L, 7L, 8L, 13L, 20L)
  )
  stop_message <- function(mask_path) {
    tryCatch(
      {
        eng$.mask_inside_lut(
          mask_path, cell_index, new.env(parent = emptyenv()),
          ref_grid = terra::rast(ref_path)
        )
        NA_character_
      },
      error = conditionMessage
    )
  }

  msgs <- c(
    burned_255 = stop_message(write_numeric("mask_255.tif", 255)),
    categorical = stop_message(write_categorical("mask_cat.tif")),
    all_zero = stop_message(write_numeric("mask_zero.tif", 0))
  )
  # Each fixture must really have stopped; an NA here would make the matching
  # assertions below vacuous.
  expect_false(anyNA(msgs))

  for (nm in names(msgs)) {
    hit <- vapply(fb, function(f) grepl(f, msgs[[nm]], fixed = TRUE), logical(1))
    expect_true(
      any(hit),
      info = sprintf("no forbidden marker matches the %s stop: %s", nm, msgs[[nm]])
    )
  }
  expect_true(grepl(.S1, msgs[["burned_255"]], fixed = TRUE))
  expect_true(grepl(.S2, msgs[["categorical"]], fixed = TRUE))
  expect_true(grepl(.S3, msgs[["all_zero"]], fixed = TRUE))

  # The engine's form, on the real messages rather than on its literals.
  expect_true(grepl(.STEM, msgs[["all_zero"]], fixed = TRUE))
  expect_false(grepl(.MARK, msgs[["all_zero"]], fixed = TRUE))
})

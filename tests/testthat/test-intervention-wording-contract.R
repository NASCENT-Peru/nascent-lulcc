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


# --- the contract test is itself proven non-vacuous (R51-IN-05) ---
#
# A test of a guard is only worth having if it can fail. Every block below feeds
# the SAME helper functions the real blocks above use (no reimplemented rule) a
# source that violates the contract, and asserts the violation is reported. Five
# classes are covered: a full-line comment, a trailing comment, a substring split
# over two literals, a `forbidden` block that is not a literal expression, and
# mutated copies of the three real files.

# Mutate `from` into `to` on CODE lines only, leaving comment lines intact: the
# shape of the R51-IN-05 defect, where the behaviour is paraphrased away while a
# comment keeps quoting the frozen wording. `startsWith(trimws(.), "#")` is used
# here for the MUTATION only - the assertions never decide "comment vs code" by
# line shape, which is the whole reason the real rule goes through the parser.
.mutate_code_lines <- function(lines, from, to) {
  is_comment <- startsWith(trimws(lines), "#")
  lines[!is_comment] <- gsub(from, to, lines[!is_comment], fixed = TRUE)
  lines
}

# Write `lines` to a temp file whose lifetime is the calling test block.
.write_source <- function(lines, env = parent.frame(), ext = ".R") {
  path <- withr::local_tempfile(fileext = ext, .local_envir = env)
  writeLines(lines, path)
  path
}

test_that("non-vacuity: a comment-only occurrence of a frozen substring is reported missing", {
  f <- .write_source(c(
    "# has no cell equal to 1",
    "x <- \"other\""
  ))
  lits <- .contract_string_literals(f)
  # The parser discards the comment, so the only literal is "other".
  expect_identical(lits, "other")
  expect_identical(.contract_missing(lits, .S3), .S3)
  # The retired whole-text grep would have called this file compliant.
  expect_true(grepl(.S3, paste(readLines(f, warn = FALSE), collapse = "\n"), fixed = TRUE))
})

test_that("non-vacuity: a trailing comment after real code does not count", {
  # A `^[[:space:]]*#` line filter - the obvious cheap repair - passes this file,
  # because the line carries real code. The parser does not.
  f <- .write_source("x <- \"other\" # has no cell equal to 1")
  lits <- .contract_string_literals(f)
  expect_identical(lits, "other")
  expect_identical(.contract_missing(lits, .S3), .S3)
  expect_true(grepl(.S3, paste(readLines(f, warn = FALSE), collapse = "\n"), fixed = TRUE))
  # And the line-filter repair really would have been fooled:
  kept <- readLines(f, warn = FALSE)
  kept <- kept[!startsWith(trimws(kept), "#")]
  expect_true(grepl(.S3, paste(kept, collapse = "\n"), fixed = TRUE))
})

test_that("non-vacuity: a substring split across two string literals does not count", {
  # Two literals are two messages. Neither can ever reach a log line as the
  # frozen substring, so the contract must not accept the pair.
  split_file <- .write_source("x <- c(\"has no cell \", \"equal to 1\")")
  expect_identical(.contract_missing(.contract_string_literals(split_file), .S3), .S3)
  # A whole-text grep is not fooled here (the comma and quotes sit between the
  # halves), but a check that pasted every literal together WOULD be: that
  # concatenation does contain the frozen substring, and no log line ever can.
  expect_true(grepl(
    .S3, paste(.contract_string_literals(split_file), collapse = ""), fixed = TRUE
  ))

  # Control: the same text inside ONE literal is present.
  whole_file <- .write_source("x <- \"intervention mask %s has no cell equal to 1\"")
  expect_identical(.contract_missing(.contract_string_literals(whole_file), .S3), character(0))
})

test_that("non-vacuity: a forbidden block that is not a literal expression is rejected, and a commented-out element is absent", {
  # A `forbidden` vector built with a call could not be lifted out and evaluated
  # on its own, so the extraction refuses it loudly instead of guessing.
  not_literal <- .write_source(c(
    "suffix <- \"x\"",
    "forbidden <- c(paste0(\"a\", suffix), \"b\")"
  ))
  expect_error(.contract_forbidden_vector(not_literal), "self-contained")

  # An element demoted to a comment is simply not in the evaluated vector - which
  # is exactly the deletion the retired source grep could not see.
  commented_out <- .write_source(c(
    "forbidden <- c(",
    "  # \"has no cell equal to 1\" used to be an element and is now only this",
    "  \"other\"",
    ")"
  ))
  fb <- .contract_forbidden_vector(commented_out)
  expect_identical(fb, "other")
  expect_false(.S3 %in% fb)

  # Two assignments mean the asserted vector is not the one the script runs with.
  two_assigns <- .write_source(c(
    "forbidden <- c(\"a\")",
    "forbidden <- c(\"b\")"
  ))
  expect_error(.contract_forbidden_vector(two_assigns), "exactly one")
})

test_that("non-vacuity: a mutated copy of the real verifier that keeps S3 only in a comment fails the contract", {
  # THIS IS THE R51-IN-05 DEFECT, DEMONSTRATED ON THE REAL FILE. The mutation is
  # the commit a future plan could plausibly make: paraphrase the `forbidden`
  # entry while the rationale comment above it keeps quoting the frozen wording.
  # The verifier would stop treating an all-zero mask as fatal, and the retired
  # whole-text grep would have stayed green through it.
  src <- readLines(.verifier_path(), warn = FALSE)
  mutated <- c(.mutate_code_lines(src, .S3, "has no 1-coded cell"), paste("#", .S3))
  tmp <- .write_source(mutated, ext = ".r")

  # (a) the retired test's behaviour: still GREEN on the mutated copy.
  expect_true(grepl(.S3, paste(mutated, collapse = "\n"), fixed = TRUE))
  # (b) the parse-based contract: the evaluated vector no longer carries it.
  fb_mut <- .contract_forbidden_vector(tmp)
  expect_false(.S3 %in% fb_mut)
  expect_true("has no 1-coded cell" %in% fb_mut)
  # (c) the unmutated file does carry it, so the mutation is the only difference.
  expect_true(.S3 %in% .contract_forbidden_vector(.verifier_path()))
})

test_that("non-vacuity: mutated copies of the real engine and pre-flight that keep S3 only in a comment fail the contract", {
  # Same demonstration on the two emitting files: a paraphrased message with the
  # frozen wording left behind in a comment. Whole-text grep green, contract red.
  for (real in c(.engine_path(), .preflight_path())) {
    src <- readLines(real, warn = FALSE)
    mutated <- c(.mutate_code_lines(src, .S3, "has no 1-coded cell"), paste("#", .S3))
    tmp <- .write_source(mutated)
    expect_true(
      grepl(.S3, paste(mutated, collapse = "\n"), fixed = TRUE),
      info = sprintf("whole-text grep must stay green on the mutated %s", basename(real))
    )
    expect_identical(
      .contract_missing(.contract_string_literals(tmp), .S3), .S3
    )
    # Control: the real file passes the same check.
    expect_identical(
      .contract_missing(.contract_string_literals(real), .S3), character(0)
    )
  }

  # The pre-flight is the only emitter of S4, so deleting its literal while a
  # comment keeps the phrase must also be caught.
  pre <- readLines(.preflight_path(), warn = FALSE)
  mutated4 <- c(
    .mutate_code_lines(pre, .S4, "values were unreadable"), paste("#", .S4)
  )
  tmp4 <- .write_source(mutated4)
  expect_true(grepl(.S4, paste(mutated4, collapse = "\n"), fixed = TRUE))
  expect_identical(.contract_missing(.contract_string_literals(tmp4), .S4), .S4)
})

test_that("non-vacuity: the colon-form rule rejects the wrong form in each file", {
  # The two forms are not interchangeable: the verifier's single prefix-free
  # entry catches both, but its "intervention mask: " marker is what catches the
  # pre-flight-only "values could not be read", so the pre-flight must keep the
  # colon and the engine must not grow one.
  colon <- .contract_string_literals(
    .write_source("stop(sprintf(\"intervention mask: %s has no cell equal to 1\", p))")
  )
  colon_free <- .contract_string_literals(
    .write_source("stop(sprintf(\"intervention mask %s has no cell equal to 1\", p))")
  )

  expect_false(.engine_form_ok(colon, .S3, mark = .MARK, stem = .STEM))
  expect_true(.engine_form_ok(colon_free, .S3, mark = .MARK, stem = .STEM))
  expect_false(.preflight_form_ok(colon_free, .S3, mark = .MARK))
  expect_true(.preflight_form_ok(colon, .S3, mark = .MARK))

  # Absence is not compliance: a file that never emits the substring fails both.
  absent <- .contract_string_literals(.write_source("x <- \"unrelated\""))
  expect_false(.engine_form_ok(absent, .S3, mark = .MARK, stem = .STEM))
  expect_false(.preflight_form_ok(absent, .S3, mark = .MARK))
})

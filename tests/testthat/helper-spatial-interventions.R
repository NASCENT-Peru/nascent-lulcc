# Shared repo-root resolution for the spatial-interventions test files
# (05-REVIEW.md IN-06).
#
# Both test-spatial-interventions.R and test-spatial-interventions-wiring.R
# used to carry a verbatim copy of a bootstrap that normalised the sourcing
# frame's `ofile` entry, null-coalesced against "." with base R's null-default
# operator, and then took dirname() three times. That bootstrap had two
# independent defects, both of which this helper exists to avoid. The literal
# spellings are deliberately NOT repeated below so that a grep guard over this
# directory stays clean.
#
# 1. Base R's null-default operator only exists from R 4.4.0, while DESCRIPTION
#    declares `Depends: R (>= 4.2)`. The expression ran while the test file was
#    being SOURCED, i.e. before `src/utils.r` was loaded, so the project's own
#    fallback definition could not possibly have covered it: the files silently
#    required a newer R than the package claims. The engine under test
#    deliberately avoids that operator for exactly this reason, and nothing in
#    this helper may reintroduce the dependency. Use an explicit
#    `if (is.null(x) || !nzchar(x)) <default> else x` instead.
#
# 2. The sourcing frame's `ofile` entry is NULL under `testthat::test_dir()`,
#    so the root came out as the wrong directory. That is not hypothetical: it
#    is precisely why tests/testthat/test-prep-paths.R errors four times in
#    every full-suite run. Nothing below reads `ofile`.
#
# testthat sets the working directory to tests/testthat for the duration of
# BOTH `test_file()` and `test_dir()` (helpers included), so
# `test_path("..", "..")` is the repo root under either invocation. The
# `getwd()`-upward walk is the standalone fallback for a bare
# `source("tests/testthat/helper-spatial-interventions.R")` from a console.
# Every candidate is confirmed by a marker file, so a wrong answer is never
# returned silently.

# A path that only exists at the repo root.
.repo_root_marker <- file.path("src", "implement_spatial_interventions.R")

.is_repo_root <- function(path) {
  if (is.null(path)) return(FALSE)
  if (length(path) != 1L) return(FALSE)
  if (is.na(path) || !nzchar(path)) return(FALSE)
  file.exists(file.path(path, .repo_root_marker))
}

.find_repo_root <- function() {
  # Preferred: two levels up from tests/testthat, resolved by testthat itself.
  candidate <- tryCatch(
    normalizePath(testthat::test_path("..", ".."), mustWork = FALSE),
    error = function(e) NULL
  )
  if (.is_repo_root(candidate)) return(candidate)

  # Fallback: walk up from the working directory until the marker appears.
  here <- tryCatch(
    normalizePath(getwd(), mustWork = FALSE),
    error = function(e) NULL
  )
  if (is.null(here) || length(here) != 1L || is.na(here) || !nzchar(here)) {
    here <- "."
  }
  repeat {
    if (.is_repo_root(here)) return(here)
    parent <- dirname(here)
    if (identical(parent, here)) break
    here <- parent
  }

  # Neither worked. Return the conventional guess so the caller's own
  # file.exists() / readLines() reports a concrete path rather than "NULL".
  if (is.null(candidate) || length(candidate) != 1L || is.na(candidate) ||
        !nzchar(candidate)) {
    "."
  } else {
    candidate
  }
}

.repo_root <- .find_repo_root()

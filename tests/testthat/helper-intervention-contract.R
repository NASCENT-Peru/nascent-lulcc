# Parse-based extraction for the frozen cross-file wording contract (D-03),
# used by tests/testthat/test-intervention-wording-contract.R.
#
# Why a parser and not a grep (R51-IN-05). The contract between the intervention
# engine, the Stage 7 pre-flight and the smoke verifier used to be asserted by
# grepping the verifier's SOURCE TEXT for the frozen substrings. The verifier's
# own rationale comment quotes every one of those substrings, so deleting an
# entry from its `forbidden` vector left the assertion green while the verifier
# stopped treating that condition as fatal. A comment is not behaviour, and a
# test that reads one as evidence manufactures confidence.
#
# Two line-oriented repairs were considered and rejected:
#   - filtering out lines matching `^[[:space:]]*#` misses a TRAILING comment
#     (`x <- 1 # has no cell equal to 1`);
#   - a whole-text grep cannot tell `c("has no cell ", "equal to 1")` (two
#     literals, never emitted as one message) from one contiguous literal.
# The R parser answers both exactly: it DISCARDS comments, and it reports one
# `STR_CONST` token per string literal. A substring found in a `STR_CONST` is
# therefore text the file can really emit, and nothing else can satisfy it.
#
# No `library()` calls and no top-level side effects: testthat sources every
# `helper-*.R` in this directory before every test file in it. Helper files load
# in alphabetical order, so this file loads BEFORE
# `helper-spatial-interventions.R`, which is where `.repo_root` is defined.
# `.contract_path()` therefore resolves `.repo_root` at CALL time (by lexical
# lookup when it runs), never at load time.


# The repo root, resolved when called rather than when this file is sourced.
.contract_path <- function(...) {
  if (!exists(".repo_root", inherits = TRUE)) {
    stop(
      "`.repo_root` is not available; source helper-spatial-interventions.R first",
      call. = FALSE
    )
  }
  file.path(.repo_root, ...)
}


# The two spellings the contract splits on. The engine says
# "intervention mask <path> ..." (no colon); the Stage 7 pre-flight says
# "intervention mask: <path> ...". The verifier's markers are deliberately
# prefix-free so one entry catches both.
.contract_mark <- function() "intervention mask: "
.contract_stem <- function() "intervention mask "


# Every string literal of an R file, unescaped, in source order.
#
# A comment can NEVER appear in the result: `parse()` throws comments away, and
# `getParseData()` emits a `STR_CONST` token only for a string literal. The
# token's `text` is the literal AS WRITTEN (quotes and backslash escapes
# included), so each one is re-parsed to recover the characters it denotes -
# otherwise `\"` and `\\` would be compared as two characters each.
.contract_string_literals <- function(path) {
  exprs <- parse(file = path, keep.source = TRUE, encoding = "UTF-8")
  pd <- utils::getParseData(exprs)
  if (is.null(pd) || nrow(pd) == 0L) return(character(0))
  txt <- pd$text[pd$token == "STR_CONST"]
  if (length(txt) == 0L) return(character(0))
  vapply(
    txt,
    function(s) as.character(parse(text = s, keep.source = FALSE)[[1L]]),
    character(1),
    USE.NAMES = FALSE
  )
}


# The verifier's `forbidden` vector, EVALUATED - the set the verifier actually
# runs with, not a grep over its source text.
#
# Plan 05.1-04 kept `forbidden <- c(...)` a self-contained literal expression
# (string literals and comments only) for exactly this reason, so the block can
# be lifted out by srcref and evaluated on its own. That guarantee is asserted,
# not assumed: every argument of the `c()` call must be a character literal, and
# the evaluation happens in an environment whose parent is `baseenv()`, so any
# variable the block referenced would error rather than silently resolve.
.contract_forbidden_vector <- function(path) {
  exprs <- parse(file = path, keep.source = TRUE, encoding = "UTF-8")
  srcrefs <- attr(exprs, "srcref")
  if (is.null(srcrefs)) {
    stop(sprintf("no srcref recorded for %s", path), call. = FALSE)
  }
  is_forbidden_assign <- vapply(
    seq_along(exprs),
    function(i) {
      e <- exprs[[i]]
      is.call(e) && length(e) == 3L && identical(e[[1L]], as.name("<-")) &&
        is.name(e[[2L]]) && identical(as.character(e[[2L]]), "forbidden")
    },
    logical(1)
  )
  idx <- which(is_forbidden_assign)
  if (length(idx) != 1L) {
    stop(sprintf(
      "expected exactly one top-level `forbidden <-` assignment in %s, found %d",
      path, length(idx)
    ), call. = FALSE)
  }
  rhs <- exprs[[idx]][[3L]]
  literal_args <- is.call(rhs) && identical(rhs[[1L]], as.name("c")) &&
    length(rhs) >= 2L &&
    all(vapply(
      as.list(rhs)[-1L],
      function(a) is.character(a) && length(a) == 1L,
      logical(1)
    ))
  if (!literal_args) {
    stop(sprintf(
      paste0(
        "forbidden is not a self-contained literal expression in %s: every ",
        "argument of c(...) must be a length-1 character literal (no variables, ",
        "no function calls)"
      ),
      path
    ), call. = FALSE)
  }
  block <- paste(as.character(srcrefs[[idx]], useSource = TRUE), collapse = "\n")
  e <- new.env(parent = baseenv())
  eval(parse(text = block), envir = e)
  get("forbidden", envir = e)
}


# The elements of `substrings` that occur in NO element of `literals`.
# character(0) when every one is present.
.contract_missing <- function(literals, substrings) {
  present <- vapply(
    substrings,
    function(s) any(grepl(s, literals, fixed = TRUE)),
    logical(1),
    USE.NAMES = FALSE
  )
  substrings[!present]
}


# The literals that contain `substring`.
.contract_literals_with <- function(literals, substring) {
  literals[grepl(substring, literals, fixed = TRUE)]
}


# The engine's form: `substring` occurs, and every literal carrying it uses the
# colon-FREE stem. One implementation, called both by the real contract
# assertion and by the non-vacuity proof, so the proof cannot drift from the
# rule it proves.
.engine_form_ok <- function(literals, substring, mark = .contract_mark(),
                            stem = .contract_stem()) {
  hits <- .contract_literals_with(literals, substring)
  length(hits) > 0L &&
    all(grepl(stem, hits, fixed = TRUE)) &&
    !any(grepl(mark, hits, fixed = TRUE))
}


# The Stage 7 pre-flight's form: `substring` occurs, and every literal carrying
# it STARTS with the colon marker the verifier greps for.
.preflight_form_ok <- function(literals, substring, mark = .contract_mark()) {
  hits <- .contract_literals_with(literals, substring)
  length(hits) > 0L && all(startsWith(hits, mark))
}

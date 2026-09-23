library(testthat)

# Phase 5 Plan 08 repo-consistency gate for WR-11.
#
# The authoritative design source for the four scenario intervention YAMLs
# (spatial_interventions_integration_protocol.md) is .gitignore'd and lives on
# the scenario author's machine, so the parameter choices it justifies cannot be
# reviewed from this repository. D-16 closes that by requiring an in-repo
# provenance record. These tests bind the YAMLs to that record:
#   1. every scenario YAML cites the record,
#   2. no protocol citation is left dangling without it,
#   3. every shipped Allocation intervention has a rationale entry in it.
#
# Test 3 is the would-have-caught-it gate: it fails if an intervention is added
# to a YAML without a corresponding rationale row in the provenance record.
#
# Repo root is resolved by walking up from the working directory until the
# config/ tree is found. This deliberately avoids base R's null-coalescing
# operator (R >= 4.4) at file-source time (IN-06): the engine under test does
# not rely on it, and neither should the tests.

.repo_root <- local({
  candidate <- normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  for (i in seq_len(8L)) {
    if (file.exists(file.path(candidate, "config", "BAU_interventions.yml"))) {
      break
    }
    parent <- dirname(candidate)
    if (identical(parent, candidate)) {
      break
    }
    candidate <- parent
  }
  candidate
})

.scenarios <- c("BAU", "NAT", "CUL", "SOC")

.provenance_rel <- "docs/spatial_interventions/parameter_provenance.md"
.provenance_path <- file.path(.repo_root, .provenance_rel)

.yaml_path <- function(scn) {
  file.path(.repo_root, "config", paste0(scn, "_interventions.yml"))
}

.yaml_lines <- function(scn) {
  readLines(.yaml_path(scn), warn = FALSE, encoding = "UTF-8")
}

# --- WR-11: the YAMLs point at an in-repo record -----------------------------

test_that("WR-11: every scenario YAML cites the in-repo provenance record", {
  for (scn in .scenarios) {
    lines <- .yaml_lines(scn)
    expect_true(
      any(grepl(.provenance_rel, lines, fixed = TRUE)),
      info = sprintf(
        "%s_interventions.yml does not cite %s", scn, .provenance_rel
      )
    )
  }
})

test_that("WR-11: no dangling protocol citation without the in-repo record", {
  section_sign <- "§"
  for (scn in .scenarios) {
    lines <- .yaml_lines(scn)
    cites_protocol <- grepl(
      "spatial_interventions_integration_protocol", lines, fixed = TRUE
    )
    cites_section <- grepl(section_sign, lines, fixed = TRUE)
    dangling <- lines[cites_protocol | cites_section]

    # Every such citation is a comment line in the header block, and the file
    # that carries it must also name the in-repo record.
    if (length(dangling) > 0L) {
      expect_true(
        all(grepl("^\\s*#", dangling)),
        info = sprintf(
          "%s_interventions.yml: protocol citation outside a comment: %s",
          scn, paste(dangling[!grepl("^\\s*#", dangling)], collapse = " | ")
        )
      )
      expect_true(
        any(grepl(.provenance_rel, lines, fixed = TRUE)),
        info = sprintf(
          "%s_interventions.yml cites the unversioned protocol (%s) but not %s",
          scn, paste(trimws(dangling), collapse = " | "), .provenance_rel
        )
      )
    }
  }
})

# --- D-23: the record covers every shipped Allocation intervention -----------

test_that("WR-11: the provenance record exists and covers every Allocation intervention", {
  expect_true(
    file.exists(.provenance_path),
    info = sprintf("missing in-repo provenance record: %s", .provenance_rel)
  )

  record <- paste(
    readLines(.provenance_path, warn = FALSE, encoding = "UTF-8"),
    collapse = "\n"
  )

  covered <- 0L
  for (scn in .scenarios) {
    entries <- yaml::yaml.load_file(.yaml_path(scn))
    expect_true(
      length(entries) > 0L,
      info = sprintf("%s_interventions.yml parsed to nothing", scn)
    )

    for (entry in entries) {
      if (!identical(entry[["Intervention_stage"]], "Allocation")) {
        next
      }
      id <- entry[["Intervention_ID"]]
      expect_true(
        is.character(id) && nzchar(id),
        info = sprintf("%s_interventions.yml: entry without Intervention_ID", scn)
      )
      expect_true(
        grepl(id, record, fixed = TRUE),
        info = sprintf(
          "%s intervention '%s' has no rationale entry in %s",
          scn, id, .provenance_rel
        )
      )
      covered <- covered + 1L
    }
  }

  # Guard against the assertions above passing vacuously.
  expect_gt(covered, 0L)
})

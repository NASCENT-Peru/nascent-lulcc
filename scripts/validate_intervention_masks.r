#!/usr/bin/env Rscript
#' Standalone spatial-intervention mask validator (Phase 5, D-12 / D-13).
#'
#' For every scenario's `<scenario>_interventions.yml` and every posterior
#' simulation year, checks that each referenced mask exists under the mask
#' directory (Static masks and every Dynamic year), that every existing mask
#' sits on exactly the reference grid (terra::compareGeom, single layer) and
#' that its value domain is within {0, 1, NA}. Top-level *.tif files not
#' referenced by any YAML are listed as orphans (WARN); non-tif entries and
#' subdirectories are listed as local-only, not staged (INFO, D-10).
#'
#' Any grid mismatch is a hard FAIL. The validator never resamples or
#' reprojects, and never uses the configured reference CRS (a stale leftover); the
#' reference is the ref-grid raster itself (D-13).
#'
#' Writes a Markdown report and exits 0 on PASS, 1 on FAIL, 2 on usage or
#' sourcing errors.
#'
#' Usage:
#'   Rscript scripts/validate_intervention_masks.r [--out <report.md>]
#'     [--mask-dir <path>] [--ref-grid <path>] [--interventions-dir <path>]
#'     [--scenarios <A,B,...>]
#'
#' Defaults: --out logs/intervention_mask_validation_<YYYYmmdd-HHMMSS>.md,
#' --mask-dir config$spat_prob_perturb_dir, --ref-grid config$ref_grid_path,
#' --interventions-dir config$interventions_dir, --scenarios
#' config$scenario_names. Both `--flag value` and `--flag=value` work.

# ---------------------------------------------------------------------------
# Set working directory to project root (before sourcing src/*.r).
# ---------------------------------------------------------------------------
script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") {
    project_root <- dirname(project_root)
  }
}
setwd(project_root)

usage <- paste(
  "Usage: Rscript scripts/validate_intervention_masks.r [--out <report.md>]",
  "[--mask-dir <path>] [--ref-grid <path>] [--interventions-dir <path>]",
  "[--scenarios <A,B,...>]"
)

# ---------------------------------------------------------------------------
# Parse arguments before sourcing so --help / bad flags fail fast.
# ---------------------------------------------------------------------------
known_flags <- c("out", "mask-dir", "ref-grid", "interventions-dir", "scenarios")
opts <- list()
args <- commandArgs(trailingOnly = TRUE)
i <- 1L
while (i <= length(args)) {
  arg <- args[[i]]
  if (arg %in% c("--help", "-h")) {
    cat(usage, "\n")
    quit(status = 0)
  }
  if (!startsWith(arg, "--")) {
    cat(sprintf("Unknown argument: %s\n%s\n", arg, usage))
    quit(status = 2)
  }
  body <- sub("^--", "", arg)
  if (grepl("=", body, fixed = TRUE)) {
    key <- sub("=.*$", "", body)
    val <- sub("^[^=]*=", "", body)
  } else {
    key <- body
    if (i + 1L > length(args)) {
      cat(sprintf("--%s requires a value\n%s\n", key, usage))
      quit(status = 2)
    }
    val <- args[[i + 1L]]
    i <- i + 1L
  }
  if (!key %in% known_flags) {
    cat(sprintf("Unknown flag: --%s\n%s\n", key, usage))
    quit(status = 2)
  }
  opts[[key]] <- val
  i <- i + 1L
}

# ---------------------------------------------------------------------------
# Source helpers; any failure is fatal here (unlike run_allocation.r).
# ---------------------------------------------------------------------------
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/implement_spatial_interventions.R"
)
for (src_file in src_files) {
  ok <- tryCatch(
    {
      source(src_file)
      TRUE
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, conditionMessage(e)))
      FALSE
    }
  )
  if (!ok) quit(status = 2)
}

suppressPackageStartupMessages(library(terra))
terra::terraOptions(progress = 0)

config <- get_config()

opt_or <- function(key, default) {
  v <- opts[[key]]
  if (is.null(v) || !nzchar(v)) default else v
}
out_path <- opt_or(
  "out",
  file.path("logs", sprintf(
    "intervention_mask_validation_%s.md",
    format(Sys.time(), "%Y%m%d-%H%M%S")
  ))
)
mask_dir <- opt_or("mask-dir", config[["spat_prob_perturb_dir"]])
ref_grid_path <- opt_or("ref-grid", config[["ref_grid_path"]])
interventions_dir <- opt_or("interventions-dir", config[["interventions_dir"]])
scenarios <- if (!is.null(opts[["scenarios"]])) {
  trimws(strsplit(opts[["scenarios"]], ",", fixed = TRUE)[[1]])
} else {
  as.character(unlist(config[["scenario_names"]]))
}
years <- tail(as.integer(unlist(config[["simulation_year_steps"]])), -1L)

cat(sprintf("mask_dir:          %s\n", mask_dir))
cat(sprintf("ref_grid:          %s\n", ref_grid_path))
cat(sprintf("interventions_dir: %s\n", interventions_dir))
cat(sprintf("scenarios:         %s\n", paste(scenarios, collapse = ", ")))
cat(sprintf("posterior years:   %s\n\n", paste(years, collapse = ", ")))

# ---------------------------------------------------------------------------
# Result collection. Never stop mid-way; every problem becomes a row.
# ---------------------------------------------------------------------------
results <- data.frame(
  level = character(0), check = character(0), subject = character(0),
  detail = character(0), stringsAsFactors = FALSE
)
add_result <- function(level, check, subject, detail) {
  results[nrow(results) + 1L, ] <<- list(level, check, subject, detail)
}

# Reference grid ------------------------------------------------------------
ref <- NULL
ref_summary <- "UNAVAILABLE"
if (!file.exists(ref_grid_path)) {
  add_result("FAIL", "ref-grid", ref_grid_path, "reference grid raster missing")
} else {
  ref <- tryCatch(terra::rast(ref_grid_path), error = function(e) {
    add_result("FAIL", "ref-grid", ref_grid_path, conditionMessage(e))
    NULL
  })
}
fmt_ext <- function(r) paste(signif(as.vector(terra::ext(r)), 10), collapse = ", ")
fmt_crs <- function(r) {
  d <- tryCatch(terra::crs(r, describe = TRUE), error = function(e) NULL)
  if (is.null(d) || is.na(d$code)) {
    return("unknown")
  }
  paste0(d$authority, ":", d$code)
}
if (!is.null(ref)) {
  ref_summary <- sprintf(
    "%s, crs %s, res %s, extent (%s), %d rows x %d cols",
    ref_grid_path, fmt_crs(ref),
    paste(signif(terra::res(ref), 10), collapse = " x "),
    fmt_ext(ref), terra::nrow(ref), terra::ncol(ref)
  )
}

# (1) References per scenario -------------------------------------------------
ref_rows <- list()
for (sc in scenarios) {
  resolved <- tryCatch(
    resolve_intervention_masks(interventions_dir, mask_dir, sc, years),
    error = function(e) {
      add_result("FAIL", "config", sc, conditionMessage(e))
      NULL
    }
  )
  if (is.null(resolved)) next
  if (nrow(resolved) == 0L) {
    add_result("INFO", "config", sc, "no Allocation-stage interventions at posterior years")
    next
  }
  ref_rows[[length(ref_rows) + 1L]] <- resolved
  miss <- resolved[!resolved$exists, , drop = FALSE]
  for (j in seq_len(nrow(miss))) {
    add_result(
      "FAIL", "missing",
      sprintf("%s/%s/%d", miss$scenario[j], miss$intervention_id[j], miss$year[j]),
      miss$mask_path[j]
    )
  }
}
all_refs <- if (length(ref_rows) > 0L) do.call(rbind, ref_rows) else NULL

# (2) Grid and value domain per distinct existing mask ----------------------
mask_rows <- list()
referenced_names <- if (is.null(all_refs)) character(0) else unique(all_refs$mask_name)
existing <- if (is.null(all_refs)) {
  character(0)
} else {
  sort(unique(all_refs$mask_name[all_refs$exists]))
}
for (nm in existing) {
  path <- file.path(mask_dir, nm)
  cat(sprintf("Checking %s ...\n", nm))
  row <- list(
    mask = nm, compareGeom = "NA", crs = "NA", res = "NA", dims = "NA",
    values = "NA", ones = NA_real_, ones_outside_ref = NA_real_
  )
  r <- tryCatch(terra::rast(path), error = function(e) {
    add_result("FAIL", "grid", nm, sprintf("unreadable: %s", conditionMessage(e)))
    NULL
  })
  if (is.null(r)) {
    mask_rows[[length(mask_rows) + 1L]] <- row
    next
  }
  row$crs <- fmt_crs(r)
  row$res <- paste(signif(terra::res(r), 10), collapse = " x ")
  row$dims <- sprintf("%d x %d x %d", terra::nrow(r), terra::ncol(r), terra::nlyr(r))

  geom_ok <- FALSE
  if (terra::nlyr(r) != 1L) {
    add_result("FAIL", "grid", nm, sprintf("nlyr = %d (expected 1)", terra::nlyr(r)))
  } else if (is.null(ref)) {
    add_result("FAIL", "grid", nm, "no reference grid to compare against")
  } else {
    geom_ok <- isTRUE(terra::compareGeom(r, ref, stopOnError = FALSE))
    if (!geom_ok) {
      add_result(
        "FAIL", "grid", nm,
        sprintf("compareGeom FALSE (crs %s, res %s, extent (%s))", row$crs, row$res, fmt_ext(r))
      )
    }
  }
  row$compareGeom <- if (geom_ok) "TRUE" else "FALSE"

  fq <- tryCatch(terra::freq(r), error = function(e) {
    add_result("FAIL", "values", nm, sprintf("freq failed: %s", conditionMessage(e)))
    NULL
  })
  if (!is.null(fq)) {
    vals <- fq$value
    row$values <- paste(c(sort(vals), "NA"), collapse = ", ")
    bad <- vals[!vals %in% c(0, 1)]
    if (length(bad) > 0L) {
      add_result(
        "FAIL", "values", nm,
        sprintf("values outside {0,1,NA}: %s", paste(head(sort(bad), 10), collapse = ", "))
      )
    }
    row$ones <- sum(fq$count[fq$value == 1])
  }

  if (geom_ok) {
    outside <- tryCatch(
      terra::global((r == 1) & is.na(ref), "sum", na.rm = TRUE)[[1]][1],
      error = function(e) NA_real_
    )
    row$ones_outside_ref <- outside
    if (!is.na(outside) && outside > 0) {
      add_result(
        "INFO", "outside-ref", nm,
        sprintf("%s mask==1 cells where the ref grid is NA (outside model domain, harmless)",
                format(outside, big.mark = ",", scientific = FALSE))
      )
    }
  }
  mask_rows[[length(mask_rows) + 1L]] <- row
}

# (3) Orphans and local-only entries ----------------------------------------
orphans <- character(0)
local_only <- character(0)
if (!dir.exists(mask_dir)) {
  add_result("FAIL", "mask-dir", mask_dir, "mask directory does not exist")
} else {
  top <- list.files(mask_dir, all.files = TRUE, no.. = TRUE, include.dirs = TRUE)
  is_dir <- dir.exists(file.path(mask_dir, top))
  tifs <- top[!is_dir & grepl("[.]tif$", top)]
  orphans <- sort(setdiff(tifs, referenced_names))
  for (o in orphans) {
    add_result("WARN", "orphan", o, "top-level .tif not referenced by any scenario YAML")
  }
  others <- top[is_dir | !grepl("[.]tif$", top)]
  local_only <- sort(ifelse(is_dir[match(others, top)], paste0(others, "/"), others))
}

# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------
git_sha <- tryCatch(
  suppressWarnings(system2("git", c("rev-parse", "--short", "HEAD"), stdout = TRUE, stderr = FALSE))[1],
  error = function(e) NA_character_
)
if (length(git_sha) == 0L || is.na(git_sha)) git_sha <- "unknown"

n_fail <- sum(results$level == "FAIL")
verdict <- if (n_fail == 0L) "VERDICT: PASS" else sprintf("VERDICT: FAIL (%d failures)", n_fail)

md <- c(
  "# Intervention Mask Validation",
  "",
  sprintf("- Run: %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
  sprintf("- Git: %s", git_sha),
  sprintf("- Environment: %s", if (is.null(config$environment)) "unknown" else config$environment),
  sprintf("- mask_dir: `%s`", mask_dir),
  sprintf("- interventions_dir: `%s`", interventions_dir),
  sprintf("- Reference grid: %s", ref_summary),
  sprintf("- Scenarios: %s", paste(scenarios, collapse = ", ")),
  sprintf("- Posterior years: %s", paste(years, collapse = ", ")),
  "- Checks: existence (D-12), compareGeom + nlyr == 1 hard fail, no resampling (D-13), values within {0,1,NA}, orphan top-level .tif",
  "",
  "## Summary",
  "",
  sprintf("- Referenced (scenario, intervention, year) entries: %d", if (is.null(all_refs)) 0L else nrow(all_refs)),
  sprintf("- Distinct referenced masks: %d", length(referenced_names)),
  sprintf("- Distinct referenced masks present: %d", length(existing)),
  sprintf("- Grid match (compareGeom TRUE): %d / %d",
          sum(vapply(mask_rows, function(x) identical(x$compareGeom, "TRUE"), logical(1))),
          length(mask_rows)),
  sprintf("- Orphan top-level .tif: %d", length(orphans)),
  sprintf("- FAIL: %d, WARN: %d, INFO: %d",
          n_fail, sum(results$level == "WARN"), sum(results$level == "INFO")),
  "",
  "## References per scenario",
  "",
  "| scenario | id | year | mask | exists |",
  "|---|---|---|---|---|"
)
if (!is.null(all_refs)) {
  md <- c(md, sprintf(
    "| %s | %s | %d | %s | %s |",
    all_refs$scenario, all_refs$intervention_id, all_refs$year,
    all_refs$mask_name, ifelse(all_refs$exists, "yes", "**NO**")
  ))
}
md <- c(
  md, "",
  "## Grid and values per mask",
  "",
  "| mask | compareGeom | crs | res | dims (rows x cols x lyr) | values | ones | ones_outside_ref |",
  "|---|---|---|---|---|---|---|---|"
)
fmt_n <- function(x) if (is.na(x)) "NA" else format(x, big.mark = ",", scientific = FALSE)
for (row in mask_rows) {
  md <- c(md, sprintf(
    "| %s | %s | %s | %s | %s | %s | %s | %s |",
    row$mask, row$compareGeom, row$crs, row$res, row$dims, row$values,
    fmt_n(row$ones), fmt_n(row$ones_outside_ref)
  ))
}
md <- c(md, "", "## Orphans and local-only entries", "")
md <- c(md, if (length(orphans) == 0L) "- Orphan top-level .tif: none" else
  sprintf("- ORPHAN (WARN): %s", orphans))
md <- c(md, if (length(local_only) == 0L) "- Local-only entries: none" else
  sprintf("- %s: local-only, not staged (INFO, D-10)", local_only))
md <- c(md, "", "## Findings", "")
if (nrow(results) == 0L) {
  md <- c(md, "- none")
} else {
  md <- c(md, "| level | check | subject | detail |", "|---|---|---|---|",
          sprintf("| %s | %s | %s | %s |", results$level, results$check,
                  results$subject, gsub("|", "\\|", results$detail, fixed = TRUE)))
}
md <- c(md, "", verdict)

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
writeLines(md, out_path)
cat(sprintf("\nReport written: %s\n", out_path))
if (nrow(results) > 0L) {
  for (j in seq_len(nrow(results))) {
    cat(sprintf("%s %s %s: %s\n", results$level[j], results$check[j],
                results$subject[j], results$detail[j]))
  }
}
cat(verdict, "\n")
quit(status = if (n_fail == 0L) 0 else 1)

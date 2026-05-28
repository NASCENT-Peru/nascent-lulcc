#!/usr/bin/env Rscript
# run_allocation.r
# Run the LULC allocation simulations using Dinamica EGO.
#
# Phase 1 Plan 01-03 contract:
#   - Pre-flight is the ONLY gate. We do NOT install missing packages at
#     runtime — environment provisioning is done by `setup_environments.sh`
#     and the `allocation_env.yml` contract. Self-healing installs hide
#     env drift instead of failing fast (RESEARCH Pitfall 3).
#   - Two operator-facing flags:
#         --preflight-only           Run the consolidated pre-flight gate
#                                    and exit (zero on PASS, non-zero on
#                                    FAIL). Does not load region data,
#                                    set up workers, or do any allocation.
#         --preflight-fixture FILE   Optional JSON file describing a
#                                    synthetic prerequisite world (env vars,
#                                    packages, files, Dinamica backend
#                                    expectations). When provided, the
#                                    pre-flight uses the fixture exclusively
#                                    so verification can assert one
#                                    consolidated failure list without
#                                    mutating the host.
#
# Capture start time
start_time <- Sys.time()

# ---------------------------------------------------------------------------
# Argument parsing (very small, only the flags this entry script supports)
# ---------------------------------------------------------------------------
.cli_args <- commandArgs(trailingOnly = TRUE)
preflight_only <- FALSE
preflight_fixture_path <- NULL
i <- 1L
while (i <= length(.cli_args)) {
  a <- .cli_args[[i]]
  if (identical(a, "--preflight-only")) {
    preflight_only <- TRUE
  } else if (identical(a, "--preflight-fixture")) {
    if (i + 1L > length(.cli_args)) {
      stop("--preflight-fixture requires a path argument")
    }
    preflight_fixture_path <- .cli_args[[i + 1L]]
    i <- i + 1L
  } else if (startsWith(a, "--preflight-fixture=")) {
    preflight_fixture_path <- sub("^--preflight-fixture=", "", a)
  }
  i <- i + 1L
}

cat("\n========================================\n")
cat("Starting LULC Allocation Simulations\n")
cat("========================================\n\n")

# Diagnostics: show R used and library paths
cat("R version and path:\n")
print(R.version.string)
cat("R executable:\n")
print(Sys.which("R"))
cat(".libPaths():\n")
print(.libPaths())
cat("\n")

# ---------------------------------------------------------------------------
# Set working directory to project root (needs to happen before sourcing
# `src/*.r`).
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
cat(sprintf("Working directory set to: %s\n", getwd()))

# ---------------------------------------------------------------------------
# Source allocation source files.
#
# We deliberately do NOT call install.packages() here (Plan 01-03, D-01..D-04
# + RESEARCH Pitfall 3). If a required package is missing, the pre-flight
# gate below will surface it as ONE actionable failure line.
# ---------------------------------------------------------------------------
src_files <- c(
  "src/setup.r",
  "src/utils.r",
  "src/dinamica_utils.r",
  "src/saturation_diagnostics.r",
  "src/allocation.r"
)

for (src_file in src_files) {
  cat(sprintf("Sourcing %s...\n", src_file))
  tryCatch(
    {
      source(src_file)
      cat(sprintf("%s sourced successfully.\n", src_file))
    },
    error = function(e) {
      cat(sprintf("ERROR sourcing %s: %s\n", src_file, e$message))
    }
  )
}
cat("\n")

# ---------------------------------------------------------------------------
# Pre-flight-only mode: load fixture (optional), run validate_allocation_runtime(),
# print one consolidated failure list (or PASS), and exit. Does NOT load config,
# set up workers, or do any allocation work.
# ---------------------------------------------------------------------------
run_preflight_and_print <- function(fixture = NULL, config = NULL) {
  errors <- validate_allocation_runtime(config = config, fixture = fixture)
  if (length(errors) == 0L) {
    cat("Allocation pre-flight passed.\n")
    return(0L)
  }
  cat("Allocation pre-flight failed:\n")
  for (line in errors) {
    cat(sprintf("  - %s\n", line))
  }
  return(1L)
}

if (preflight_only) {
  fixture <- NULL
  if (!is.null(preflight_fixture_path)) {
    if (!file.exists(preflight_fixture_path)) {
      cat(sprintf(
        "ERROR: --preflight-fixture path does not exist: %s\n",
        preflight_fixture_path
      ))
      quit(status = 2)
    }
    if (!requireNamespace("jsonlite", quietly = TRUE)) {
      cat("ERROR: package 'jsonlite' is required to read --preflight-fixture\n")
      quit(status = 2)
    }
    fixture <- jsonlite::fromJSON(
      preflight_fixture_path,
      simplifyVector = TRUE,
      simplifyDataFrame = FALSE,
      simplifyMatrix = FALSE
    )
  }

  # Pre-flight-only path intentionally does not call get_config(); the
  # fixture is the canonical input when supplied, and a no-fixture run uses
  # the documented Stage 7 contract defaults from src/allocation.r.
  exit_code <- run_preflight_and_print(fixture = fixture, config = NULL)
  quit(status = exit_code)
}

# ---------------------------------------------------------------------------
# Normal allocation run.
# ---------------------------------------------------------------------------
cat("Loading configuration...\n")
config <- tryCatch(
  {
    get_config()
  },
  error = function(e) {
    cat(sprintf("ERROR getting config: %s\n", e$message))
    quit(status = 1)
  }
)

cat("Configuration loaded successfully.\n\n")

profile_scenario <- Sys.getenv("ALLOCATION_PROFILE_SCENARIO", unset = "")
if (nzchar(profile_scenario)) {
  if (!(profile_scenario %in% config[["scenario_names"]])) {
    stop(sprintf(
      "ALLOCATION_PROFILE_SCENARIO='%s' is not in config$scenario_names",
      profile_scenario
    ))
  }
  config[["scenario_names"]] <- profile_scenario
  cat(sprintf(
    "Smoke filter: restricting to scenario '%s'\n",
    profile_scenario
  ))
}

region_filter <- Sys.getenv("ALLOCATION_REGION_FILTER", unset = "")
if (nzchar(region_filter)) {
  cat(sprintf("Smoke filter: ALLOCATION_REGION_FILTER=%s\n", region_filter))
}
year_post_filter <- Sys.getenv("ALLOCATION_YEAR_POST_FILTER", unset = "")
if (nzchar(year_post_filter)) {
  cat(sprintf("Smoke filter: ALLOCATION_YEAR_POST_FILTER=%s\n", year_post_filter))
}

profile_enabled <- isTRUE(as.logical(
  Sys.getenv("ALLOCATION_PROFILE", unset = "FALSE")
))
if (profile_enabled) {
  profile_timestep_index <- Sys.getenv(
    "ALLOCATION_PROFILE_TIMESTEP_INDEX",
    unset = ""
  )
  if (nzchar(profile_timestep_index)) {
    config[["profile_timestep_index"]] <- as.integer(profile_timestep_index)
    if (is.na(config[["profile_timestep_index"]])) {
      stop("ALLOCATION_PROFILE_TIMESTEP_INDEX must be an integer")
    }
    cat(sprintf(
      "Profile mode: restricting to timestep index %d\n",
      config[["profile_timestep_index"]]
    ))
  }
  cat("Profile mode enabled.\n\n")
}

# Set up parallel processing — but ONLY after run_allocation()'s pre-flight
# would otherwise pass. We run the gate explicitly here so an early failure
# does not leak into future::plan(). run_allocation() will run it again
# (idempotent) before any region work; the redundant call is intentional so
# that direct callers of run_allocation() (e.g. tests) still get gated.
preflight_exit <- run_preflight_and_print(config = config)
if (preflight_exit != 0L) {
  quit(status = preflight_exit)
}

t_pin_threads <- prof_tic()
pin_native_threads_to_one(verbose = TRUE)
prof_toc(t_pin_threads, "stage=pin_native_threads_to_one")

plan_choice <- select_allocation_plan()
if (identical(plan_choice$strategy, "multicore")) {
  options(parallelly.fork.enable = TRUE)
  future::plan(future::multicore, workers = plan_choice$workers)
  # externalptr objects (ranger forests, terra rasters) are safe under fork —
  # workers inherit the parent address space without serialization.
  if (isTRUE(as.logical(Sys.getenv("ALLOCATION_DEV_STRICT_GLOBALS", "FALSE")))) {
    options(future.globals.onReference = "warning")
    cat("DEV MODE: future.globals.onReference = 'warning' (multicore fork is safe for externalptr)\n")
  }
} else if (identical(plan_choice$strategy, "multisession")) {
  future::plan(future::multisession, workers = plan_choice$workers)
  if (isTRUE(as.logical(Sys.getenv("ALLOCATION_DEV_STRICT_GLOBALS", "FALSE")))) {
    options(future.globals.onReference = "error")
    cat("DEV MODE: future.globals.onReference = 'error' enabled\n")
  }
} else {
  future::plan(future::sequential)
}
cat(sprintf(
  "Parallel: strategy=%s workers=%d\n",
  plan_choice$strategy,
  plan_choice$workers
))

# Run allocation simulations
cat("\n========================================\n")
cat("Running Allocation Simulations\n")
cat("========================================\n")

result <- tryCatch(
  {
    run_allocation(config)
    list(status = "success", error = NA)
  },
  error = function(e) {
    cat(sprintf("ERROR in allocation simulations: %s\n", e$message))
    cat(sprintf("Traceback:\n"))
    traceback()
    list(status = "error", error = e$message)
  }
)

# Clean up parallel workers
future::plan(future::sequential)

# Report results
end_time <- Sys.time()
total_elapsed <- difftime(end_time, start_time, units = "hours")

cat("\n========================================\n")
cat("Allocation Simulations Summary\n")
cat("========================================\n")
cat(sprintf("Total runtime: %.2f hours\n", as.numeric(total_elapsed)))
cat(sprintf("Status: %s\n", result$status))

if (result$status == "error") {
  cat(sprintf("Error: %s\n", result$error))
}

# Save summary
summary_file <- file.path(
  "logs",
  sprintf(
    "allocation_summary_%s%s.txt",
    Sys.getenv("SLURM_JOB_ID", unset = "local"),
    ifelse(
      nzchar(Sys.getenv("ALLOCATION_PROFILE_RUN_LABEL", unset = "")),
      paste0("_", Sys.getenv("ALLOCATION_PROFILE_RUN_LABEL")),
      ""
    )
  )
)

if (!dir.exists("logs")) {
  dir.create("logs", recursive = TRUE)
}

sink(summary_file)
cat(sprintf("Job ID: %s\n", Sys.getenv("SLURM_JOB_ID", unset = "local")))
cat(sprintf("Start time: %s\n", start_time))
cat(sprintf("End time: %s\n", end_time))
cat(sprintf("Runtime: %.2f hours\n", as.numeric(total_elapsed)))
cat(sprintf("Status: %s\n", result$status))
if (result$status == "error") {
  cat(sprintf("Error: %s\n", result$error))
}
sink()

cat(sprintf("\nSummary saved to: %s\n", summary_file))

# Exit with appropriate code
exit_code <- ifelse(result$status == "error", 1, 0)
cat(sprintf(
  "\nAllocation simulations completed with exit code: %d\n",
  exit_code
))
quit(status = exit_code)

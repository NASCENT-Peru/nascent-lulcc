#!/usr/bin/env Rscript
# run_allocation_dinamica_only.r
#
# Smoke test for the Dinamica allocation step only.
# Skips probability map generation by reusing an existing probability_map_dir
# from a prior full run. Useful for validating the apptainer launch contract
# without re-running 3+ hours of ranger prediction.
#
# Required env vars:
#   DINAMICA_EGO_8_HOME   — absolute path to the Dinamica .sif image (HPC) or
#                           install directory (local)
#   ALLOCATION_WORK_DIR   — path to an existing region work_dir that already
#                           contains anterior.tif and probability_map_dir/
#
# Optional env vars:
#   DINAMICA_BACKEND      — "auto" (default), "hpc", or "local"

start_time <- Sys.time()

script_path <- commandArgs(trailingOnly = FALSE)
script_path <- script_path[grepl("--file=", script_path)]
if (length(script_path) > 0) {
  script_dir <- dirname(sub("--file=", "", script_path))
  project_root <- dirname(script_dir)
} else {
  project_root <- getwd()
  if (basename(project_root) == "scripts") project_root <- dirname(project_root)
}
setwd(project_root)
cat(sprintf("Working directory: %s\n", getwd()))

for (src_file in c("src/setup.r", "src/utils.r", "src/dinamica_utils.r", "src/allocation.r")) {
  source(src_file)
}

work_dir <- Sys.getenv("ALLOCATION_WORK_DIR", unset = "")
if (!nzchar(work_dir)) {
  stop("ALLOCATION_WORK_DIR is not set. Point it at an existing region work_dir.")
}
if (!dir.exists(work_dir)) {
  stop(sprintf("ALLOCATION_WORK_DIR does not exist: %s", work_dir))
}

anterior_path <- file.path(work_dir, "anterior.tif")
prob_map_dir  <- file.path(work_dir, "probability_map_dir")

if (!file.exists(anterior_path)) {
  stop(sprintf("anterior.tif not found in work_dir: %s", anterior_path))
}
if (!dir.exists(prob_map_dir)) {
  stop(sprintf("probability_map_dir not found in work_dir: %s", prob_map_dir))
}

n_tifs <- length(list.files(prob_map_dir, pattern = "\\.tif$"))
cat(sprintf("\nwork_dir     : %s\n", work_dir))
cat(sprintf("anterior.tif : %s\n", anterior_path))
cat(sprintf("prob_map_dir : %s (%d TIFs)\n", prob_map_dir, n_tifs))
cat(sprintf("backend      : %s\n", detect_dinamica_backend()))
cat(sprintf("DINAMICA_EGO_8_HOME: %s\n\n", Sys.getenv("DINAMICA_EGO_8_HOME", "<unset>")))

log_file <- file.path(
  "logs",
  sprintf("dinamica_only_%s.log", format(Sys.time(), "%Y%m%d_%H%M%S"))
)
if (!dir.exists("logs")) dir.create("logs", recursive = TRUE)

cat("Launching Dinamica...\n")
result <- tryCatch(
  {
    posterior <- run_allocation_dinamica(
      work_dir = work_dir,
      log_file  = log_file
    )
    list(status = "success", posterior = posterior)
  },
  error = function(e) {
    list(status = "error", message = e$message)
  }
)

end_time <- Sys.time()
elapsed  <- as.numeric(difftime(end_time, start_time, units = "secs"))

cat(sprintf("\n--- Result ---\n"))
cat(sprintf("Status  : %s\n", result$status))
cat(sprintf("Elapsed : %.1fs\n", elapsed))

if (result$status == "success") {
  posterior_path <- result$posterior
  if (!is.null(posterior_path) && file.exists(posterior_path)) {
    cat(sprintf("Posterior: %s (%.1f MB)\n",
                posterior_path,
                file.info(posterior_path)$size / 1e6))
  } else {
    cat("WARNING: posterior.tif path returned but file not found\n")
  }
  quit(status = 0)
} else {
  cat(sprintf("Error: %s\n", result$message))
  quit(status = 1)
}

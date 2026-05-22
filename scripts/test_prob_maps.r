#!/usr/bin/env Rscript
# Targeted test: probability map generation for BAU / costa_peruana / 2022->2026
# Run from project root via: Rscript scripts/test_prob_maps.r

setwd(dirname(dirname(normalizePath(sys.frames()[[1]]$ofile %||% commandArgs(FALSE)[grep("--file=", commandArgs(FALSE))] |> sub("--file=", "", x=_) |> (\(x) if (length(x)) x else ".")()))))

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0L) y else x

# Resolve project root robustly
args <- commandArgs(FALSE)
file_arg <- grep("--file=", args, value = TRUE)
if (length(file_arg)) {
  project_root <- dirname(dirname(sub("--file=", "", file_arg)))
} else {
  project_root <- getwd()
}
setwd(project_root)
cat("Project root:", getwd(), "\n")

source("src/setup.r")
source("src/utils.r")
source("src/allocation.r")

config <- get_config()

scenario     <- "BAU"
year_ant     <- 2022L
year_post    <- 2026L
region_label <- "costa_peruana"
region_val   <- 3L
region_suffix <- "costa_peruana"
cal_period   <- config[["data_periods"]][length(config[["data_periods"]])]
work_dir     <- "E:/nascent-lulcc-agg/outputs/simulations/BAU/2026/region_costa_peruana"

log_file <- file.path(work_dir, "worker_logs", "test_prob_maps.log")
dir.create(dirname(log_file), recursive = TRUE, showWarnings = FALSE)

anterior_path <- file.path(work_dir, "anterior.tif")
if (!file.exists(anterior_path)) {
  stop("anterior.tif not found at: ", anterior_path,
       "\nRun the full allocation once first to generate it.")
}
cat("anterior.tif found:", anterior_path, "\n")

cat("\nLoading models for", region_label, "...\n")
models_list   <- load_allocation_models(
  region_labels      = region_label,
  calibration_period = cal_period,
  config             = config
)
region_models <- models_list[[region_suffix]]
cat("Models loaded:", nrow(region_models), "transitions\n")

cat("\nBuilding nhood_paths from downloaded TIFs (skipping recompute)...\n")
nhood_cache_dir <- file.path(
  Sys.getenv("TERRA_TEMP", unset = tempdir()),
  "nhood_cache",
  paste0(scenario, "_", year_post, "_", region_suffix)
)
nhood_tifs <- list.files(nhood_cache_dir, pattern = "\\.tif$", full.names = TRUE)
if (!length(nhood_tifs)) {
  stop("No TIF files found in nhood cache dir: ", nhood_cache_dir,
       "\nCheck TERRA_TEMP is set correctly.")
}
nhood_paths <- setNames(nhood_tifs, tools::file_path_sans_ext(basename(nhood_tifs)))
cat("Nhood paths found:", length(nhood_paths), "\n")

scalar_str     <- sprintf("%.1f", config[["selected_scalar"]])
trans_rate_src <- file.path(
  config[["trans_rate_table_dir"]],
  paste0("simulation-lulc-areas-scalar-", scalar_str, "x"),
  scenario, region_label,
  paste0(scenario, "-", region_label, "-trans_rates-", year_ant, ".csv")
)
if (!file.exists(trans_rate_src)) {
  stop("Transition rates file not found: ", trans_rate_src)
}
trans_rates_df <- read.csv(trans_rate_src, check.names = FALSE)
trans_rates_df <- trans_rates_df[order(trans_rates_df[["id_trans"]]), ]
cat("Transition rates loaded:", nrow(trans_rates_df), "rows\n")

cat("\n--- Running generate_probability_maps ---\n")
result <- tryCatch({
  generate_probability_maps(
    work_dir           = work_dir,
    region_label       = region_label,
    region_val         = region_val,
    scenario           = scenario,
    year_ant           = year_ant,
    calibration_period = cal_period,
    anterior_path      = anterior_path,
    trans_rates_df     = trans_rates_df,
    config             = config,
    log_file           = log_file,
    models_list        = region_models,
    nhood_paths        = nhood_paths
  )
  "SUCCESS"
}, error = function(e) {
  cat("\nFAILED:", conditionMessage(e), "\n")
  traceback()
  "FAILED"
})

cat("\nResult:", result, "\n")
cat("Worker log:", log_file, "\n")

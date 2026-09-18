#!/usr/bin/env Rscript
# Test just the probability map SAVE step using synthetic predictions.
# Bypasses the 3+ hour prediction phase by building a dummy normalized table
# from the real anterior cell indices and real trans_rates.
#
# Run from project root: Rscript scripts/test_prob_map_save.r

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0L) y else x

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
source("src/allocation.r")

config      <- get_config()
scalar_str  <- sprintf("%.1f", config[["selected_scalar"]])
scenario    <- "BAU"
year_ant    <- 2022L
region_label <- "costa_peruana"

work_dir <- "E:/nascent-lulcc-agg/outputs/simulations/BAU/2026/region_costa_peruana"
prob_map_dir <- file.path(work_dir, "prob_maps_save_test")
dir.create(prob_map_dir, recursive = TRUE, showWarnings = FALSE)

anterior_path <- file.path(work_dir, "anterior.tif")
if (!file.exists(anterior_path)) stop("anterior.tif not found: ", anterior_path)

trans_rate_src <- file.path(
  config[["trans_rate_table_dir"]],
  paste0("simulation-lulc-areas-scalar-", scalar_str, "x"),
  scenario, region_label,
  paste0(scenario, "-", region_label, "-trans_rates-", year_ant, ".csv")
)
if (!file.exists(trans_rate_src)) stop("trans_rates not found: ", trans_rate_src)

# ---------------------------------------------------------------------------
cat("\nLoading anterior raster...\n")
anterior <- terra::rast(anterior_path)

cat("Extracting cell index from anterior...\n")
t0 <- proc.time()[["elapsed"]]
anterior_dt <- data.table::setDT(
  terra::as.data.frame(anterior, cells = TRUE, xy = TRUE, na.rm = TRUE)
)
data.table::setnames(anterior_dt, c("cell_id", "x", "y", "lulc_class"))
cat(sprintf("  %d non-NA cells extracted (%.1fs)\n",
            nrow(anterior_dt), proc.time()[["elapsed"]] - t0))

# ---------------------------------------------------------------------------
cat("\nLoading transition rates...\n")
trans_rates_df <- read.csv(trans_rate_src, check.names = FALSE)
trans_rates_df <- trans_rates_df[order(trans_rates_df[["id_trans"]]), ]
trans_rates_dt <- data.table::as.data.table(trans_rates_df)
trans_rates_dt[, row_idx := seq_len(.N)]
cat(sprintf("  %d transitions\n", nrow(trans_rates_dt)))

# ---------------------------------------------------------------------------
cat("\nBuilding synthetic normalized table (same shape as real predictions)...\n")
t0 <- proc.time()[["elapsed"]]

gather <- vector("list", nrow(trans_rates_dt))
j <- 0L
for (k in seq_len(nrow(trans_rates_dt))) {
  from_val <- trans_rates_dt[["From*"]][k]
  to_val   <- trans_rates_dt[["To*"]][k]
  rate     <- trans_rates_dt[["Rate"]][k]
  if (from_val == to_val || rate == 0) next

  from_cells <- anterior_dt[lulc_class == from_val]
  if (nrow(from_cells) == 0L) next

  j <- j + 1L
  gather[[j]] <- data.table::data.table(
    row_idx  = k,
    from_val = from_val,
    to_val   = to_val,
    cell_id  = from_cells$cell_id,
    x        = from_cells$x,
    y        = from_cells$y,
    prob     = runif(nrow(from_cells), 0, 0.3)
  )
}
gather <- gather[seq_len(j)]

normalized <- data.table::rbindlist(gather, use.names = TRUE, fill = TRUE)
cat(sprintf("  %d rows in normalized (%.1fs)\n",
            nrow(normalized), proc.time()[["elapsed"]] - t0))

# ---------------------------------------------------------------------------
cat("\nNormalizing probabilities...\n")
t0 <- proc.time()[["elapsed"]]
normalized[, tot_prob := sum(prob), by = cell_id]
normalized[tot_prob > 1, prob := prob / tot_prob]
normalized[, tot_prob := NULL]
cat(sprintf("  Done (%.1fs)\n", proc.time()[["elapsed"]] - t0))

cat("\nSetting key on normalized (row_idx)...\n")
t0 <- proc.time()[["elapsed"]]
data.table::setkey(normalized, row_idx)
cat(sprintf("  Done (%.1fs)\n", proc.time()[["elapsed"]] - t0))

# ---------------------------------------------------------------------------
cat("\nSaving probability maps...\n")
t_write_total <- proc.time()[["elapsed"]]

for (k in seq_len(nrow(trans_rates_dt))) {
  from_val <- trans_rates_dt[["From*"]][k]
  to_val   <- trans_rates_dt[["To*"]][k]
  rate     <- trans_rates_dt[["Rate"]][k]
  id_trans <- trans_rates_dt[["id_trans"]][k]

  if (from_val == to_val || rate == 0) next

  dt_j <- normalized[.(k)]
  if (nrow(dt_j) == 0L) next

  tif_path <- file.path(prob_map_dir, sprintf("%03d_id_trans_%d.tif", k, id_trans))

  t_rw <- proc.time()[["elapsed"]]
  r <- terra::setValues(anterior, NA_real_)
  r[dt_j[["cell_id"]]] <- dt_j[["prob"]]
  terra::writeRaster(r, filename = tif_path, overwrite = TRUE, NAflag = -999)

  cat(sprintf("  %d -> %d: %d cells  %.1fs\n",
              from_val, to_val, nrow(dt_j),
              proc.time()[["elapsed"]] - t_rw))
}

cat(sprintf("\nAll maps saved in %.1fs\n", proc.time()[["elapsed"]] - t_write_total))
cat(sprintf("Output: %s\n", prob_map_dir))
cat(sprintf("Files:  %d\n", length(list.files(prob_map_dir, pattern = "\\.tif$"))))

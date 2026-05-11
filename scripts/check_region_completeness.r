#!/usr/bin/env Rscript
model_dir <- "E:/nascent-lulcc-agg/outputs/transition_models/2018_2022"
csv_path  <- "E:/nascent-lulcc-agg/outputs/transition_identification/viable_transitions_lists.csv"

vt <- read.csv(csv_path, stringsAsFactors = FALSE)
vt <- vt[vt$region_name != "whole_map" & vt$from_lulc != vt$to_lulc, ]
vt <- vt[!is.na(vt$rate_2018_2022) & vt$rate_2018_2022 != 0, ]

slug <- function(r) tolower(gsub(" ", "_", r))
vt$model_name <- paste0(vt$from_lulc, "-", vt$to_lulc, "_", slug(vt$region_name))
saved <- sub("\\.qs$", "", list.files(model_dir, pattern = "\\.qs$"))

for (r in sort(unique(vt$region_name))) {
  expected <- vt$model_name[vt$region_name == r]
  missing  <- setdiff(expected, saved)
  cat(sprintf("%s: %d expected, %d saved, %d missing\n",
              r, length(expected), length(expected) - length(missing), length(missing)))
  if (length(missing)) for (m in missing) cat("  MISSING:", m, "\n")
}

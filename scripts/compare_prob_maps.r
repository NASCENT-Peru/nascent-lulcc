#!/usr/bin/env Rscript
# Compare two probability_map_dir TIF sets cell-by-cell and report max|delta|
# per file plus an overall maximum — the Phase 3.5 numerical-equivalence check
# (plan 03.5-03). Lazy vs eager at 1 thread should be bit-for-bit (max|delta|=0);
# at matched thread counts it should be within FP-reorder tolerance (< 1e-6).
#
# Usage (run on the cluster, inside allocation_env so terra is available):
#   Rscript scripts/compare_prob_maps.r <dirA> <dirB>
# e.g.
#   Rscript scripts/compare_prob_maps.r \
#     validation_outputs/573721/lazy-threads/probability_map_dir \
#     validation_outputs/<eagerjob>/eager-threads/probability_map_dir

suppressMessages(library(terra))

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2L) {
  stop("usage: compare_prob_maps.r <dirA> <dirB>")
}
a <- args[[1L]]
b <- args[[2L]]
stopifnot(dir.exists(a), dir.exists(b))

fa <- sort(list.files(a, pattern = "\\.tif$"))
fb <- sort(list.files(b, pattern = "\\.tif$"))
common <- intersect(fa, fb)

cat(sprintf("dirA = %s  (%d tifs)\n", a, length(fa)))
cat(sprintf("dirB = %s  (%d tifs)\n", b, length(fb)))
cat(sprintf("common=%d  onlyA=%d  onlyB=%d\n\n",
            length(common), length(setdiff(fa, fb)), length(setdiff(fb, fa))))

overall <- 0
worst_file <- NA_character_
for (f in common) {
  va <- terra::values(terra::rast(file.path(a, f)))
  vb <- terra::values(terra::rast(file.path(b, f)))
  na_mismatch <- sum(xor(is.na(va), is.na(vb)))
  d <- abs(va - vb)
  m <- suppressWarnings(max(d, na.rm = TRUE))
  if (!is.finite(m)) m <- NA_real_
  cat(sprintf("  %-44s max|delta|=%-10.3e NA-mismatch=%d\n", f, m, na_mismatch))
  if (is.finite(m) && m > overall) { overall <- m; worst_file <- f }
}

cat(sprintf("\nOVERALL max|delta| over %d common tifs = %.3e  (worst: %s)\n",
            length(common), overall, worst_file))
verdict <- if (overall == 0) {
  "BIT-FOR-BIT IDENTICAL"
} else if (overall < 1e-6) {
  "EQUIVALENT within < 1e-6 (FP-reorder tolerance)"
} else {
  "DIVERGENT (>= 1e-6) — investigate"
}
cat(sprintf("VERDICT: %s\n", verdict))

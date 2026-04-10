# Cache Refresh Behavior

## Overview

The `calibrate_allocation_parameters` workflow now supports a `refresh_cache` parameter that controls whether cached intermediate files should be reused or regenerated.

## Usage

```r
# Reuse existing cached files (default, faster)
calibrate_allocation_parameters(config = cfg, refresh_cache = FALSE)

# Force regeneration of all cached files
calibrate_allocation_parameters(config = cfg, refresh_cache = TRUE)
```

## Cached Files

The `refresh_cache` parameter controls the behavior of the following cached files:

### 1. Region Masked Rasters
**Location:** `temp_dir/yr1_region_{region_id}_{period}.tif` and `yr2_region_{region_id}_{period}.tif`

**Purpose:** LULC rasters masked and trimmed to specific regions

**Behavior:**
- `refresh_cache = FALSE`: If files exist, they are loaded from disk
- `refresh_cache = TRUE`: Files are always regenerated, even if they exist

### 2. Post-Class Patches Cache
**Location:** `temp_dir/post_class_patches_region_{region}_{to_class}_{period}.tif`

**Purpose:** Binary raster indicating where destination class exists at posterior timepoint

**Behavior:**
- `refresh_cache = FALSE`: Created only if doesn't exist
- `refresh_cache = TRUE`: Always regenerated

### 3. Neighbor Count Cache
**Location:** `temp_dir/neighbor_count_region_{region}_{to_class}_{period}.tif`

**Purpose:** Count of neighboring cells of destination class (3x3 focal window)

**Behavior:**
- `refresh_cache = FALSE`: Created only if doesn't exist
- `refresh_cache = TRUE`: Always regenerated

## When to Use Each Mode

### Use `refresh_cache = FALSE` (Default)
- Normal workflow execution
- When resuming failed jobs
- When reprocessing only specific transitions
- When you want maximum performance

**Advantages:**
- Faster execution (no redundant I/O)
- Lower memory usage
- Enables resuming from failures

### Use `refresh_cache = TRUE`
- After modifying input LULC rasters
- After changing region definitions
- When you suspect cache corruption
- For debugging or validation
- First run after code updates that affect cache creation

**Advantages:**
- Ensures cache consistency with current inputs
- Eliminates stale data issues
- Clean slate for testing

## Implementation Details

The `refresh_cache` parameter is propagated through the entire function call chain:

```
calibrate_allocation_parameters(refresh_cache)
  ↓
calculate_allocation_params_for_periods(refresh_cache)
  ↓
process_all_regions(refresh_cache)
  ↓
process_single_region(refresh_cache)
  ├─ Creates/loads region masked rasters
  ├─ Pre-computes cache files in parallel
  └─ Calls process_region_transitions(refresh_cache)
       ↓
     calculate_single_transition_params(refresh_cache)
       └─ Loads pre-computed cache files
```

## Performance Considerations

### First Run (`refresh_cache = TRUE` or no cache exists)
- Region masked rasters: ~10-30 seconds per region
- Cache pre-computation: ~5-15 seconds per destination class (parallelized)
- Total overhead: Depends on number of regions and unique destination classes

### Subsequent Runs (`refresh_cache = FALSE` with existing cache)
- Region masked rasters: ~1-2 seconds to load from disk
- Cache files: Already exist, workers just load them
- Total overhead: Minimal (>90% reduction in preprocessing time)

## HPC Recommendations

On HPC systems, use `refresh_cache = FALSE` (default) to:
1. Avoid race conditions (files created before parallel processing)
2. Enable job resumption after SLURM time limits
3. Reduce memory pressure during parallel execution
4. Minimize redundant I/O on shared filesystems

## Cache Location

All cache files are stored in `config$temp_dir`. To clear all caches:

```r
# Remove all cached files
unlink(config$temp_dir, recursive = TRUE)

# Next run will regenerate everything
calibrate_allocation_parameters(config = cfg)
```

Or use `refresh_cache = TRUE` to regenerate without manual deletion.

## Troubleshooting

### Problem: Getting unexpected results
**Solution:** Run with `refresh_cache = TRUE` to ensure cache is current

### Problem: "File not found" errors in parallel workers
**Solution:** This should not happen if using the updated code. Cache files are pre-computed before parallel processing starts.

### Problem: Disk space issues
**Solution:** Clean up `temp_dir` periodically, or use a more aggressive cleanup strategy in your workflow

### Problem: Want to reprocess specific regions/transitions
**Solution:** Delete specific cache files manually, or use `refresh_cache = TRUE` for a complete refresh

#' @title Resolve intervention mask paths for a scenario and set of years
#' @description
#' Shared resolver used by the allocation engine, the Stage 7 pre-flight and
#' the standalone validator. Reads `<scenario>_interventions.yml` from
#' `interventions_dir`, keeps Allocation-stage entries, and returns one row per
#' (intervention, year) where the year is in both `years` and the entry's
#' `Time_steps_implemented`. Mask names must be bare filenames and always
#' resolve to `file.path(mask_dir, name)` (D-05). An implemented year with no
#' Dynamic mask entry is an error (D-14). Missing mask files are NOT an error
#' here: they are reported via `exists = FALSE` and callers decide.
#'
#' Uses base R and `yaml::` only (no data.table syntax) so it can be sourced
#' into baseenv-parented environments.
#' @param interventions_dir Directory containing `<scenario>_interventions.yml`.
#' @param mask_dir Directory containing the intervention mask rasters.
#' @param scenario Scenario identifier.
#' @param years Integer vector of simulation years to resolve.
#' @return data.frame with columns scenario, intervention_id, rank, year,
#'   mask_type, mask_name, mask_path, exists, entry_index (zero rows when
#'   nothing is active). `entry_index` is the 1-based index of the row's source
#'   entry in the Allocation-filtered entry list, and that list is attached to
#'   every return value (including the zero-row ones) as
#'   `attr(<return>, "entries")` — `list()` when the YAML holds no Allocation
#'   entries. Callers MUST consume `attr(., "entries")` rather than re-parsing
#'   the YAML: this function is the only place the Allocation/year filter, the
#'   `Mask_type` domain and the `Prob_adjust_*` schema are enforced, and every
#'   entry is validated whether or not it is active in `years`
#'   (05-REVIEW.md CR-01, CR-03, WR-06, IN-01).
resolve_intervention_masks <- function(interventions_dir, mask_dir, scenario, years) {
  yaml_path <- file.path(interventions_dir, paste0(scenario, "_interventions.yml"))
  empty <- data.frame(
    scenario = character(0),
    intervention_id = character(0),
    rank = numeric(0),
    year = integer(0),
    mask_type = character(0),
    mask_name = character(0),
    mask_path = character(0),
    exists = logical(0),
    entry_index = integer(0),
    stringsAsFactors = FALSE
  )
  # Every return path carries the Allocation-filtered entries (WR-06).
  with_entries <- function(out, entries) {
    attr(out, "entries") <- entries
    out
  }
  if (!file.exists(yaml_path)) {
    stop(sprintf("interventions YAML missing: %s", yaml_path))
  }
  entries <- yaml::yaml.load_file(yaml_path)
  if (is.null(entries) || length(entries) == 0L) {
    return(with_entries(empty, list()))
  }
  is_alloc <- vapply(entries, function(x) {
    st <- x[["Intervention_stage"]]
    !is.null(st) && identical(as.character(st), "Allocation")
  }, logical(1))
  entries <- entries[is_alloc]
  if (length(entries) == 0L) {
    return(with_entries(empty, list()))
  }
  # Identity is a validated requirement, not a best-effort derivation: a NULL,
  # non-scalar, NA or empty Intervention_ID used to survive as a single
  # NA_character_ (not `duplicated()`), resolve a mask, and then silently drop
  # the whole policy entry in the applier (CR-01).
  ids <- vapply(entries, function(x) {
    v <- x[["Intervention_ID"]]
    if (is.null(v) || length(v) != 1L) NA_character_ else as.character(v)
  }, character(1))
  bad <- which(is.na(ids) | !nzchar(ids))
  if (length(bad) > 0L) {
    stop(sprintf(
      "Allocation entry %s in %s has no usable Intervention_ID",
      paste(bad, collapse = ", "), yaml_path
    ))
  }
  dup <- unique(ids[duplicated(ids)])
  if (length(dup) > 0L) {
    stop(sprintf(
      "duplicate Intervention_ID in %s: %s",
      yaml_path, paste(dup, collapse = ", ")
    ))
  }

  # Per-entry validation pass. Runs for every Allocation entry regardless of
  # `years` so a malformed entry is reported by the pre-flight and the
  # standalone validator, not by a hard crash mid-run after higher-ranked
  # interventions have already rewritten the probability surface (CR-03), and
  # so an unknown Mask_type is caught even when the entry is not active in
  # `years` (IN-01).
  mask_types <- character(length(entries))
  for (i in seq_along(entries)) {
    x <- entries[[i]]
    id <- ids[[i]]

    mt_raw <- x[["Mask_type"]]
    mt <- if (is.null(mt_raw)) NA_character_ else as.character(mt_raw)
    if (length(mt) != 1L || is.na(mt) || !(mt %in% c("Static", "Dynamic"))) {
      stop(sprintf(
        "Unknown Mask_type: %s (scenario=%s id=%s)",
        paste(mt, collapse = ", "), scenario, id
      ))
    }
    mask_types[[i]] <- mt

    adj_raw <- x[["Prob_adjust_type"]]
    adj <- if (is.null(adj_raw) || length(adj_raw) != 1L) {
      NA_character_
    } else {
      as.character(adj_raw)
    }
    req <- if (identical(adj, "Absolute")) {
      c("Prob_adjust_value", "Prob_adjust_zone")
    } else if (identical(adj, "Relative")) {
      c(
        "Prob_adjust_valency", "Prob_adjust_zone", "Prob_adjust_threshold",
        "Prob_adjust_intervention_percentile",
        "Prob_adjust_non_intervention_percentile"
      )
    } else {
      stop(sprintf(
        "Unknown Prob_adjust_type: %s (scenario=%s id=%s)",
        if (is.na(adj)) "<missing>" else adj, scenario, id
      ))
    }
    missing_keys <- req[vapply(req, function(k) length(x[[k]]) != 1L, logical(1))]
    if (length(missing_keys) > 0L) {
      stop(sprintf(
        "scenario=%s id=%s missing/!scalar: %s",
        scenario, id, paste(missing_keys, collapse = ", ")
      ))
    }
    num_keys <- intersect(req, c(
      "Prob_adjust_value", "Prob_adjust_threshold",
      "Prob_adjust_intervention_percentile",
      "Prob_adjust_non_intervention_percentile"
    ))
    bad_num <- num_keys[vapply(
      num_keys,
      function(k) is.na(suppressWarnings(as.numeric(x[[k]]))),
      logical(1)
    )]
    if (length(bad_num) > 0L) {
      stop(sprintf(
        "scenario=%s id=%s non-numeric: %s",
        scenario, id, paste(bad_num, collapse = ", ")
      ))
    }
    pct_keys <- intersect(req, c(
      "Prob_adjust_intervention_percentile",
      "Prob_adjust_non_intervention_percentile"
    ))
    if (length(pct_keys) > 0L) {
      pct <- suppressWarnings(as.numeric(unlist(x[pct_keys])))
      if (any(pct < 0 | pct > 100)) {
        stop(sprintf(
          "scenario=%s id=%s percentile outside 0-100", scenario, id
        ))
      }
    }
    # The applier hard-errors on an unknown or absent target class list, so
    # require it here too.
    tgt <- as.character(unlist(x[["Transition_target_classes"]]))
    if (length(tgt) == 0L || any(is.na(tgt)) || !all(nzchar(tgt))) {
      stop(sprintf(
        "scenario=%s id=%s Transition_target_classes missing or empty",
        scenario, id
      ))
    }
  }

  years <- as.integer(years)
  rows <- list()
  for (i in seq_along(entries)) {
    x <- entries[[i]]
    id <- ids[[i]]
    rank <- if (is.null(x[["Intervention_ranking"]])) {
      NA_real_
    } else {
      as.numeric(x[["Intervention_ranking"]])
    }
    implemented <- as.integer(unlist(x[["Time_steps_implemented"]]))
    active_years <- intersect(years, implemented)
    mask_type <- mask_types[[i]]
    for (y in active_years) {
      if (identical(mask_type, "Static")) {
        name <- x[["Intervention_mask"]]
      } else {
        # Dynamic: the domain is already validated above (IN-01).
        name <- x[["Intervention_mask"]][[as.character(y)]]
        if (is.null(name)) {
          stop(sprintf(
            "no Dynamic Intervention_mask entry for year %d (scenario=%s id=%s)",
            y, scenario, id
          ))
        }
      }
      name <- as.character(unlist(name))
      if (length(name) != 1L || is.na(name) || !nzchar(name)) {
        stop(sprintf(
          "Intervention_mask must be a single bare filename (scenario=%s id=%s year=%d)",
          scenario, id, y
        ))
      }
      if (grepl("[/\\\\]", name) || grepl("..", name, fixed = TRUE)) {
        stop(sprintf("Intervention_mask must be a bare filename: %s", name))
      }
      mask_path <- file.path(mask_dir, name)
      rows[[length(rows) + 1L]] <- data.frame(
        scenario = as.character(scenario),
        intervention_id = id,
        rank = rank,
        year = as.integer(y),
        mask_type = mask_type,
        mask_name = name,
        mask_path = mask_path,
        exists = file.exists(mask_path),
        entry_index = i,
        stringsAsFactors = FALSE
      )
    }
  }
  if (length(rows) == 0L) {
    return(with_entries(empty, entries))
  }
  out <- do.call(rbind, rows)
  rownames(out) <- NULL
  with_entries(out, entries)
}

#' @title Cached inside-mask lookup table indexed by region cell_id
#' @description
#' Reads `mask_path` once per call-scoped `cache`, samples it at the national
#' cell numbers `cell_index$ref_cell_id` (never an xy matrix), and returns a
#' logical vector of length `max(cell_index$cell_id)` that is TRUE where the
#' mask value equals 1.
#'
#' A national cell number is only meaningful on the exact grid that produced it
#' (`config$ref_grid_path`, via `terra::cellFromXY()` in the allocation hook), so
#' this function refuses to trust a mask it has not proven is on that grid: a
#' multi-layer file, or any crs/resolution/extent mismatch, is a hard stop
#' (CR-02, D-13). Nothing is ever resampled or reprojected at runtime. The
#' caller's `cell_index` is validated too, because `max(cell_id)` drives a vector
#' allocation and a `cell_id` of 0 silently mis-selects rows downstream (WR-10).
#'
#' The cache is keyed on the normalised path, the mask's modification time
#' (sub-second), its size in bytes and `length(cell_index$cell_id)` — not on the
#' path alone (IN-03). An overwritten mask therefore cannot be served stale, so a
#' longer-lived (e.g. session-level) cache is safe here, not just the call-scoped
#' `new.env()` the engine currently passes.
#' @param mask_path Path to the mask raster.
#' @param cell_index data.table/data.frame with columns cell_id (region) and
#'   ref_cell_id (national cell number on the mask grid).
#' @param cache Environment created with `new.env(parent = emptyenv())`.
#' @param ref_grid SpatRaster for the reference grid the `ref_cell_id` values
#'   are defined on. REQUIRED: there is no fallback that skips the geometry
#'   check.
#' @return Logical vector indexed by region cell_id.
.mask_inside_lut <- function(mask_path, cell_index, cache, ref_grid) {
  # WR-10: validate before anything depends on max(cell_id). Done first so a
  # degenerate call fails identically whether or not the mask file exists.
  cid <- cell_index$cell_id
  if (length(cid) == 0L || anyNA(cid) || any(cid < 1L)) {
    stop("cell_index$cell_id must be non-empty, non-NA and >= 1", call. = FALSE)
  }

  # IN-07: a mask can vanish (or its mount drop) between the resolver's
  # file.exists() and this read. Re-raise on the marker the smoke verifier
  # already treats as fatal instead of a bare normalizePath() error.
  key_path <- tryCatch(
    normalizePath(mask_path, mustWork = TRUE),
    error = function(e) {
      stop(sprintf(
        "intervention mask missing: %s (disappeared after pre-flight)", mask_path
      ), call. = FALSE)
    }
  )

  # IN-03: path alone is not a safe cache key.
  key <- paste(
    key_path,
    format(file.mtime(key_path), "%Y-%m-%d %H:%M:%OS6"),
    file.size(key_path),
    length(cid),
    sep = "|"
  )
  if (exists(key, envir = cache, inherits = FALSE)) {
    return(get(key, envir = cache, inherits = FALSE))
  }

  m <- terra::rast(mask_path)
  # Checked before compareGeom(), which ignores layer count by default and would
  # let a multi-band file through to a silent `[[1L]]`.
  if (terra::nlyr(m) != 1L) {
    stop(sprintf(
      "intervention mask %s has %d layers (expected 1)",
      mask_path, terra::nlyr(m)
    ), call. = FALSE)
  }
  # Same assertion scripts/validate_intervention_masks.r makes offline, so the
  # runtime and the validator report the same condition (CR-02).
  if (!isTRUE(terra::compareGeom(m, ref_grid, stopOnError = FALSE))) {
    stop(sprintf(
      "intervention mask %s is not on the reference grid (crs/res/extent mismatch); cell-number lookup would be silently wrong",
      mask_path
    ), call. = FALSE)
  }

  # CR-02(b): terra::extract() only warns on out-of-range cell numbers, to
  # stderr, and the NA it returns reads as "outside". Reject deterministically.
  rcid <- cell_index$ref_cell_id
  n_ref <- terra::ncell(m)
  if (
    length(rcid) != length(cid) || anyNA(rcid) ||
      any(rcid < 1) || any(rcid > n_ref)
  ) {
    stop(sprintf(
      "cell_index$ref_cell_id values outside the reference grid 1..%.0f (or not aligned with cell_id) for mask %s",
      n_ref, mask_path
    ), call. = FALSE)
  }

  v <- terra::extract(m, rcid)[[1L]]
  lut <- logical(max(cid))
  lut[cid[!is.na(v) & v == 1]] <- TRUE
  assign(key, lut, envir = cache)
  lut
}


# Count selected rows whose probability differs between two snapshots
# (NA-aware). Internal helper for rows_changed accounting.
.count_prob_changes <- function(before, after) {
  na_b <- is.na(before)
  na_a <- is.na(after)
  sum((na_b != na_a) | (!na_b & !na_a & before != after))
}

# The 19-column per-target-class statistics contract (D-18), in its
# "this class was never adjusted" shape. Every element of Target_classes
# contributes exactly one row, including the classes an adjuster skips, so the
# stats table always lines up 1:1 with Target_classes and a downstream consumer
# can tell "no rows" apart from "rows that did not move".
.delta_stats_skipped <- function(target_class) {
  out <- data.frame(
    target_class = as.integer(target_class),
    n_target = 0L,
    n_changed = 0L,
    mean_before = NA_real_,
    mean_after = NA_real_,
    sd_before = NA_real_,
    sd_after = NA_real_,
    p05_delta = NA_real_,
    p25_delta = NA_real_,
    p50_delta = NA_real_,
    p75_delta = NA_real_,
    p95_delta = NA_real_,
    min_delta = NA_real_,
    max_delta = NA_real_,
    sum_abs_delta = 0,
    prob_mass_before = NA_real_,
    prob_mass_after = NA_real_,
    n_inc = 0L,
    n_dec = 0L,
    stringsAsFactors = FALSE
  )
  attr(out, "delta") <- numeric(0)
  out
}

# Per-target-class probability-change statistics for ONE intervention (D-18).
#
# `before` and `after` are the probability values of the rows this intervention
# targeted (`Target_area_idx` for Absolute, `sub_idx` for Relative — the same
# index sets rows_target counts), sampled immediately before and immediately
# after that single intervention's adjustment. Interventions are sequential, so
# a later intervention's `before` is the earlier one's output (D-19).
#
# D-20: the only memory this holds is the three length(rows_target) vectors
# `before`, `after` and `d`. It never sees, and must never be given, a copy of
# the probability table. The class's delta vector is returned as
# `attr(<row>, "delta")` so the caller can pool it for the intervention-level
# AUDIT roll-up without recomputing it from a second snapshot.
#
# `n_changed` is a lazily-defaulted argument: callers that already computed
# `.count_prob_changes(before, after)` for their rows_changed accumulator pass
# it in, so the comparison runs once per class rather than twice.
.delta_stats <- function(
  target_class, before, after,
  n_changed = .count_prob_changes(before, after)
) {
  ok <- !is.na(before) & !is.na(after)
  d <- after[ok] - before[ok]
  if (length(d) == 0L) return(.delta_stats_skipped(target_class))
  q <- stats::quantile(
    d, c(0.05, 0.25, 0.5, 0.75, 0.95), names = FALSE, na.rm = TRUE
  )
  out <- data.frame(
    target_class = as.integer(target_class),
    n_target = length(before),
    n_changed = as.integer(n_changed),
    mean_before = mean(before, na.rm = TRUE),
    mean_after = mean(after, na.rm = TRUE),
    sd_before = stats::sd(before, na.rm = TRUE),
    sd_after = stats::sd(after, na.rm = TRUE),
    p05_delta = q[[1L]],
    p25_delta = q[[2L]],
    p50_delta = q[[3L]],
    p75_delta = q[[4L]],
    p95_delta = q[[5L]],
    min_delta = min(d),
    max_delta = max(d),
    sum_abs_delta = sum(abs(d)),
    prob_mass_before = sum(before, na.rm = TRUE),
    prob_mass_after = sum(after, na.rm = TRUE),
    n_inc = sum(d > 0),
    n_dec = sum(d < 0),
    stringsAsFactors = FALSE
  )
  attr(out, "delta") <- d
  out
}

# Bind the per-class rows into the stats table an adjuster returns, pooling the
# per-class delta vectors onto `attr(., "delta")` exactly once (D-20: one
# unlist, no per-class growing vector).
.bind_delta_stats <- function(stats_rows, delta_parts) {
  out <- do.call(rbind, stats_rows)
  rownames(out) <- NULL
  pooled <- unlist(delta_parts, use.names = FALSE)
  if (is.null(pooled)) pooled <- numeric(0)
  attr(out, "delta") <- pooled
  out
}

# Render one numeric AUDIT field. Every appended field must stay a single
# whitespace-delimited token or the shipped log parsers break, so a non-finite
# statistic is written as the literal NA and formatC()'s "g" format is used
# (no thousands separator, no embedded space).
#
# `width = 1L` is NOT cosmetic: formatC() defaults `width` to `digits` for the
# "g" format, so the default would render -0.4 as "   -0.4" and split one field
# into four tokens. trimws() is kept as a second guard so the invariant holds
# whatever a future formatC()/locale does.
.fmt_num <- function(v) {
  if (length(v) != 1L || !is.finite(v)) return("NA")
  trimws(formatC(v, format = "g", digits = 6, width = 1L))
}

#' @title Implement Spatial Interventions on per-transition Probabilities
#' @description
#' Implement all Allocation-stage spatial interventions whose
#' `Time_steps_implemented` contains `simulation_time_step` on the long-format
#' data.table of per-transition probabilities. The caller passes the
#' posterior (target) year (D-07).
#'
#' Mask paths come from [resolve_intervention_masks()] (bare filenames under
#' `mask_dir`, D-05). Any referenced mask file that does not exist stops the
#' call before any probability is edited (D-14). Mask membership is looked up
#' by national cell number through a per-call cached LUT
#' ([.mask_inside_lut()]), which aborts the call if the mask is not on
#' `ref_grid_path`'s grid or is not single-layer (CR-02, D-13). Each applied
#' intervention writes one
#' intervention AUDIT line and each call writes one intervention summary
#' AUDIT line to `log_file` (D-15). Probabilities
#' are NOT renormalised; the number of cells whose summed probability exceeds 1
#' is only logged (`cells_sum_gt1`).
#'
#' @section Intervention AUDIT line contract:
#' The per-intervention line is
#' \preformatted{
#' AUDIT stage=intervention region=<r> scenario=<s> year=<y> id=<id> rank=<n>
#'   type=<t> zone=<z> to_vals=<v,..> mask=<f> rows_target=<n> rows_changed=<n>
#'   delta_mean=<x> delta_med=<x> delta_sd=<x> delta_min=<x> delta_max=<x>
#'   n_inc=<n> n_dec=<n> sum_abs_delta=<x>
#' }
#' written on one physical line. The fields up to and including `rows_changed`
#' are FROZEN: `scripts/verify_intervention_smoke.r` parses them by fixed
#' string and by `sub("^.* id=([^ ]+) .*$", ...)`, so they must never be
#' reordered, renamed or removed. The eight `delta_*` / `n_inc` / `n_dec` /
#' `sum_abs_delta` fields (D-18) are ADDITIVE and are appended at the end;
#' any future telemetry is appended the same way.
#'
#' The eight appended fields describe the distribution of the probability
#' change THIS intervention made, pooled across its target classes, over the
#' rows it targeted (`Target_area_idx` for Absolute, `sub_idx` for Relative —
#' the same index sets `rows_target` counts) against the values immediately
#' before it ran. Interventions are applied in rank order, so a later
#' intervention's "before" is the earlier one's output (D-19). Every numeric
#' field is rendered by [.fmt_num()], which emits the literal `NA` for a
#' non-finite value and never emits a space, so each field stays a single
#' whitespace-delimited token. When every target class was skipped the five
#' `delta_*` fields are `NA` and `n_inc`, `n_dec` and `sum_abs_delta` are `0`.
#' `AUDIT stage=intervention_summary` remains the unchanged whole-call roll-up.
#' @param normalized data.table with columns row_idx, from_val, to_val,
#'   cell_id (region), prob. Modified in place by reference. The engine never
#'   reads `x`/`y`: mask membership is looked up by national cell number
#'   (IN-02).
#' @param cell_index data.table with columns cell_id (region cell id) and
#'   ref_cell_id (national cell number on the mask grid).
#' @param class_name_to_value named integer vector mapping
#'   lulc_schema class_name to raster integer value.
#' @param interventions_dir Directory containing `<scenario>_interventions.yml`.
#' @param mask_dir Directory containing the intervention mask rasters.
#' @param ref_grid_path Path to the reference grid raster whose cell numbers
#'   `cell_index$ref_cell_id` was computed against. Every mask is checked
#'   against it at runtime and a mismatch aborts the call (CR-02, D-13): the
#'   engine never resamples or reprojects.
#' @param scenario Identifier for the scenario to apply interventions.
#' @param simulation_time_step The (posterior) year at which to apply the
#'   interventions.
#' @param log_file Path to per-region log file used by log_msg(...).
#' @param region_label Region label written into the AUDIT lines.
#' @return The `normalized` data.table with updated probabilities.
implement_spatial_interventions <- function(
  normalized,
  cell_index,
  class_name_to_value,
  interventions_dir,
  mask_dir,
  ref_grid_path,
  scenario,
  simulation_time_step,
  log_file,
  region_label = NA_character_
) {
  year <- as.integer(simulation_time_step)
  region_label <- as.character(region_label)

  # Resolve every mask referenced for this year up front and fail fast on any
  # missing file before touching probabilities (D-05, D-14).
  resolved <- resolve_intervention_masks(
    interventions_dir = interventions_dir,
    mask_dir = mask_dir,
    scenario = scenario,
    years = year
  )
  missing_rows <- resolved[!resolved$exists, , drop = FALSE]
  if (nrow(missing_rows) > 0L) {
    msg <- paste(
      sprintf(
        "intervention mask missing: %s (scenario=%s id=%s year=%d)",
        missing_rows$mask_path,
        missing_rows$scenario,
        missing_rows$intervention_id,
        missing_rows$year
      ),
      collapse = "; "
    )
    log_msg(msg, log_file)
    stop(msg, call. = FALSE)
  }

  # Read the reference grid once. Every mask must sit on it exactly: the mask
  # lookup indexes by national cell number, which is meaningless on any other
  # grid (CR-02). Read here, not per mask, so the terra pointer lifetime stays
  # inside this one scope.
  ref_grid_fail <- function(why) {
    m <- sprintf(
      "intervention ref grid unreadable: %s (%s)",
      paste(ref_grid_path, collapse = ", "), why
    )
    log_msg(m, log_file)
    stop(m, call. = FALSE)
  }
  if (
    length(ref_grid_path) != 1L || is.na(ref_grid_path) ||
      !nzchar(ref_grid_path)
  ) {
    ref_grid_fail("not a single non-empty path")
  }
  # Checked before terra::rast() so a missing file reports here rather than as a
  # bare GDAL warning on stderr.
  if (!file.exists(ref_grid_path)) ref_grid_fail("file does not exist")
  ref_grid <- tryCatch(
    terra::rast(ref_grid_path),
    error = function(e) ref_grid_fail(conditionMessage(e))
  )

  write_summary <- function(n_applied) {
    sums <- normalized[, list(s = sum(prob, na.rm = TRUE)), by = cell_id]
    cells_sum_gt1 <- sum(sums$s > 1 + 1e-9)
    log_msg(
      sprintf(
        "AUDIT stage=intervention_summary region=%s scenario=%s year=%d n_interventions=%d cells_sum_gt1=%d",
        region_label, scenario, year, as.integer(n_applied), as.integer(cells_sum_gt1)
      ),
      log_file
    )
  }

  # Drive everything below off the resolver's rows. It has already parsed the
  # YAML once, filtered to Intervention_stage == "Allocation", intersected
  # Time_steps_implemented with `year` and validated identity, mask type and
  # the Prob_adjust_* schema. Re-parsing and re-filtering here is exactly what
  # let a typo'd Intervention_ID silently drop a policy entry (WR-06, CR-01).
  entries <- attr(resolved, "entries")

  # If no interventions are found, return the normalized DT unchanged
  if (nrow(resolved) == 0L) {
    log_msg(
      paste(
        "No interventions found for scenario", scenario,
        "at time step", year, "- returning original probabilities."
      ),
      log_file
    )
    write_summary(0L)
    return(normalized)
  }

  log_msg(
    paste(
      "Found", nrow(resolved),
      "allocation stage interventions for scenario", scenario,
      "at time step", year
    ),
    log_file
  )

  # order interventions by Intervention_ranking putting NAs last
  ord <- order(resolved$rank, na.last = TRUE)

  cache <- new.env(parent = emptyenv())
  n_applied <- 0L

  # loop over interventions
  for (k in ord) {
    entry_index <- resolved$entry_index[k]
    if (
      is.null(entries) || length(entry_index) != 1L || is.na(entry_index) ||
        entry_index < 1L || entry_index > length(entries)
    ) {
      stop(sprintf(
        "resolver contract violation: entry_index=%s has no parsed entry (id=%s year=%d scenario=%s)",
        paste(entry_index, collapse = ","),
        paste(resolved$intervention_id[k], collapse = ","),
        year, scenario
      ), call. = FALSE)
    }
    intervention <- entries[[entry_index]]
    iv_id <- resolved$intervention_id[k]
    mask_path <- resolved$mask_path[k]
    mask_name <- resolved$mask_name[k]
    rank_k <- resolved$rank[k]
    log_msg(paste("Applying intervention:", iv_id), log_file)

    # Translate Transition_target_classes (class_name strings) to integer
    # to_val values via lulc_schema.
    Target_classes <- as.integer(
      class_name_to_value[unlist(intervention[["Transition_target_classes"]])]
    )
    if (length(Target_classes) == 0L || any(is.na(Target_classes))) {
      stop(paste(
        "Unknown class_name in Transition_target_classes:",
        paste(intervention[["Transition_target_classes"]], collapse = ", ")
      ))
    }

    # Mask_type is validated by the resolver for every Allocation entry
    # (IN-01); the mask path is the resolver row's own, so there is no join and
    # no way for an intervention to be skipped here.
    inside_lut <- .mask_inside_lut(
      mask_path, cell_index, cache, ref_grid = ref_grid
    )

    # If the Intervention requires filtering by LULC classes then translate
    # the From_lulc_filter class names to integer from_val values, to be
    # passed through to the helpers (which apply the filter on the long DT).
    From_filter_vals <- NULL
    if (
      !is.null(intervention$From_lulc_filter) &&
        length(intervention$From_lulc_filter) > 0 &&
        all(intervention$From_lulc_filter != "None")
    ) {
      From_filter_vals <- as.integer(
        class_name_to_value[unlist(intervention[["From_lulc_filter"]])]
      )
      if (any(is.na(From_filter_vals))) {
        stop(paste(
          "Unknown class_name in From_lulc_filter:",
          paste(intervention[["From_lulc_filter"]], collapse = ", ")
        ))
      }
      log_msg(
        paste(
          "Filtering to only cells that are currently LULC class values:",
          paste(From_filter_vals, collapse = ", ")
        ),
        log_file
      )
    }

    # Apply different functions based on whether the intervention specifies
    # absolute or relative adjustments to probabilities
    if (identical(intervention$Prob_adjust_type, "Absolute")) {
      log_msg(
        paste(
          "Applying absolute probability adjustment to cells:",
          intervention[["Prob_adjust_zone"]],
          "the intervention area, adjusting probability values to:",
          intervention[["Prob_adjust_value"]]
        ),
        log_file
      )
      res <- absolute_prob_adjust(
        normalized = normalized,
        Prob_adjust_zone = intervention$Prob_adjust_zone,
        Prob_adjust_value = as.numeric(intervention$Prob_adjust_value),
        Target_classes = Target_classes,
        From_filter_vals = From_filter_vals,
        inside_lut = inside_lut,
        log_file = log_file
      )
    } else if (identical(intervention$Prob_adjust_type, "Relative")) {
      # percentile values are converted to numeric decimals (YAML gives 0-100)
      res <- relative_prob_adjust(
        Prob_adjust_valency = intervention[["Prob_adjust_valency"]],
        Prob_adjust_intervention_percentile = as.numeric(intervention[[
          "Prob_adjust_intervention_percentile"
        ]]) / 100,
        Prob_adjust_non_intervention_percentile = as.numeric(intervention[[
          "Prob_adjust_non_intervention_percentile"
        ]]) / 100,
        Prob_adjust_threshold = as.numeric(intervention[["Prob_adjust_threshold"]]),
        Prob_adjust_zone = intervention[["Prob_adjust_zone"]],
        Target_classes = Target_classes,
        From_filter_vals = From_filter_vals,
        inside_lut = inside_lut,
        normalized = normalized,
        log_file = log_file
      )
    } else {
      stop(paste(
        "Unknown Prob_adjust_type:",
        intervention[["Prob_adjust_type"]]
      ))
    }
    normalized <- res$normalized
    n_applied <- n_applied + 1L

    # D-18: roll the per-target-class statistics up to one intervention-level
    # record. The pooled delta vector rides on attr(res$stats, "delta") — it is
    # the concatenation of the per-class deltas the adjuster already computed,
    # so nothing is recomputed and nothing beyond O(rows_target) doubles is
    # held; it is dropped as soon as the line is written (D-20).
    iv_stats <- res$stats
    d_pool <- attr(iv_stats, "delta")
    if (is.null(d_pool)) d_pool <- numeric(0)
    if (length(d_pool) == 0L) {
      # Every target class was skipped: there is no distribution to describe.
      d_mean <- NA_real_
      d_med <- NA_real_
      d_sd <- NA_real_
      d_min <- NA_real_
      d_max <- NA_real_
    } else {
      d_mean <- mean(d_pool)
      d_med <- stats::median(d_pool)
      d_sd <- stats::sd(d_pool)
      d_min <- min(d_pool)
      d_max <- max(d_pool)
    }
    n_inc_k <- as.integer(sum(iv_stats$n_inc))
    n_dec_k <- as.integer(sum(iv_stats$n_dec))
    sum_abs_k <- sum(iv_stats$sum_abs_delta)
    rm(d_pool)

    log_msg(
      sprintf(
        "AUDIT stage=intervention region=%s scenario=%s year=%d id=%s rank=%s type=%s zone=%s to_vals=%s mask=%s rows_target=%d rows_changed=%d delta_mean=%s delta_med=%s delta_sd=%s delta_min=%s delta_max=%s n_inc=%d n_dec=%d sum_abs_delta=%s",
        region_label,
        scenario,
        year,
        iv_id,
        if (is.na(rank_k)) "NA" else as.character(rank_k),
        intervention[["Prob_adjust_type"]],
        intervention[["Prob_adjust_zone"]],
        paste(Target_classes, collapse = ","),
        mask_name,
        as.integer(res$rows_target),
        as.integer(res$rows_changed),
        .fmt_num(d_mean),
        .fmt_num(d_med),
        .fmt_num(d_sd),
        .fmt_num(d_min),
        .fmt_num(d_max),
        n_inc_k,
        n_dec_k,
        .fmt_num(sum_abs_k)
      ),
      log_file
    )
    # The per-class table is the only telemetry that outlives the line, and it
    # is one small row per target class. Drop the pooled delta with it.
    attr(iv_stats, "delta") <- NULL
    rm(iv_stats)
  } # end of intervention loop

  write_summary(n_applied)

  # return the updated normalized data.table
  return(normalized)
}

#' @title Perform absolute adjustment of probabilities of change in target classes
#' @description
#' Perform absolute adjustment of probabilities of change in target classes
#' either inside or outside an intervention mask. Only rows with prob > 0 are
#' set to `Prob_adjust_value` (zeros stay zero).
#' @param normalized A long-format data.table with columns to_val, from_val,
#'   cell_id, prob. Modified by reference.
#' @param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Prob_adjust_value A numeric value to set the probabilities in the target area.
#' @param Target_classes An integer vector of target to_val class values.
#' @param From_filter_vals Optional integer vector of from_val classes to restrict to.
#' @param inside_lut Logical vector indexed by region cell_id (TRUE = inside
#'   the mask), from [.mask_inside_lut()].
#' @param log_file Optional per-region log file for log_msg(...).
#' @return list(normalized, rows_target, rows_changed, stats): rows_target
#'   counts the rows in the target zone, rows_changed those whose prob changed.
#'   `stats` is the 19-column per-target-class delta table from
#'   [.delta_stats()] with exactly `length(Target_classes)` rows in
#'   `Target_classes` order (skipped classes included), carrying the pooled
#'   delta vector on `attr(stats, "delta")` (D-18, D-19).
absolute_prob_adjust <- function(
  normalized,
  Prob_adjust_zone,
  Prob_adjust_value,
  Target_classes,
  From_filter_vals = NULL,
  inside_lut,
  log_file = NULL
) {
  if (!Prob_adjust_zone %in% c("Inside", "Outside")) {
    stop(paste("Unknown Prob_adjust_zone:", Prob_adjust_zone))
  }
  rows_target <- 0L
  rows_changed <- 0L
  # D-18: one statistics row and one delta vector slot per target class,
  # allocated up front so the table always lines up 1:1 with Target_classes.
  stats_rows <- vector("list", length(Target_classes))
  delta_parts <- vector("list", length(Target_classes))

  # loop over the target classes
  for (i in seq_along(Target_classes)) {
    lulc_class <- Target_classes[[i]]
    # Seeded with the skipped-class shape before any `next` can fire, so a
    # class with no rows still contributes its row (D-18).
    stats_rows[[i]] <- .delta_stats_skipped(lulc_class)
    delta_parts[[i]] <- numeric(0)

    log_msg(
      paste(
        "Adjusting pixels values of class:", lulc_class, ",",
        Prob_adjust_zone, "mask to:", Prob_adjust_value
      ),
      log_file
    )

    # Subset to rows of this target class (and from-filter if applicable)
    hit <- normalized$to_val == lulc_class
    if (!is.null(From_filter_vals)) {
      hit <- hit & (normalized$from_val %in% From_filter_vals)
    }
    sub_idx <- which(hit)
    if (length(sub_idx) == 0L) next

    # Look up mask membership by region cell_id (cached LUT)
    inside_flag <- inside_lut[normalized$cell_id[sub_idx]]
    inside_flag[is.na(inside_flag)] <- FALSE

    if (Prob_adjust_zone == "Inside") {
      Target_area_idx <- sub_idx[inside_flag]
    } else {
      # invert the mask to get the non-intersecting area
      Target_area_idx <- sub_idx[!inside_flag]
    }
    before <- normalized$prob[Target_area_idx]

    # Adjust the probabilities in the target area
    ix <- Target_area_idx[!is.na(before) & before > 0]
    normalized[ix, prob := Prob_adjust_value]

    # WR-02: clamp ONLY the rows this intervention just wrote. The previous
    # table-wide clamp reached rows of every other transition, in both zones,
    # uncounted by rows_changed. Clamping must never touch a row outside the
    # intervention's declared target class and zone. pmin/pmax propagate NA, so
    # NA rows stay NA exactly as the old `!is.na(prob)` guard intended.
    normalized[ix, prob := pmin(pmax(prob, 0), 1)]

    # D-19/D-20: read the post-adjustment values of the targeted rows ONCE and
    # reuse that one vector for both the delta statistics and the rows_changed
    # accounting, instead of indexing the table a second time. Peak extra
    # memory for this class is the three length(Target_area_idx) vectors
    # `before`, `after` and the delta - never a copy of the table.
    after <- normalized$prob[Target_area_idx]
    n_changed_k <- .count_prob_changes(before, after)
    st <- .delta_stats(lulc_class, before, after, n_changed_k)
    rm(after)
    delta_parts[[i]] <- attr(st, "delta")
    attr(st, "delta") <- NULL
    stats_rows[[i]] <- st

    rows_target <- rows_target + length(Target_area_idx)
    rows_changed <- rows_changed + n_changed_k
  }

  list(
    normalized = normalized,
    rows_target = rows_target,
    rows_changed = rows_changed,
    stats = .bind_delta_stats(stats_rows, delta_parts)
  )
}

#' @title Perform relative probability adjustment for target land use classes
#' @description
#' Perform relative probability adjustment for target lulc classes based upon
#' the % difference in average probabilities above specified percentiles for
#' the intervention and non-intervention pixels with the option to specify
#' target pixels as those outside or inside the intervention mask areas.
#' Classes where one zone has no positive probabilities (or the percentage
#' difference is not finite) are skipped and logged instead of crashing.
#' @param Prob_adjust_valency A string indicating the valency of the adjustment, either "Increase", "Decrease" or "Increase_inside_decrease_outside".
#' @param Prob_adjust_intervention_percentile A numeric value (0-1) indicating the percentile for the intervention area.
#' @param Prob_adjust_non_intervention_percentile A numeric value (0-1) indicating the percentile for the non-intervention area.
#' @param Prob_adjust_threshold A numeric value indicating the threshold for the percentage difference.
#' @param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Target_classes An integer vector of target to_val class values.
#' @param From_filter_vals Optional integer vector of from_val classes to restrict to.
#' @param inside_lut Logical vector indexed by region cell_id (TRUE = inside
#'   the mask), from [.mask_inside_lut()].
#' @param normalized A long-format data.table with columns to_val, from_val,
#'   cell_id, prob. Modified by reference.
#' @param log_file Optional per-region log file for log_msg(...).
#' @return list(normalized, rows_target, rows_changed, stats): rows_target
#'   counts the rows of the target classes (both zones), rows_changed those
#'   whose prob changed. `stats` is the 19-column per-target-class delta table
#'   from [.delta_stats()] with exactly `length(Target_classes)` rows in
#'   `Target_classes` order (all three skip paths included), carrying the
#'   pooled delta vector on `attr(stats, "delta")` (D-18, D-19).
relative_prob_adjust <- function(
  Prob_adjust_valency,
  Prob_adjust_intervention_percentile,
  Prob_adjust_non_intervention_percentile,
  Prob_adjust_threshold,
  Prob_adjust_zone,
  Target_classes,
  From_filter_vals = NULL,
  inside_lut,
  normalized,
  log_file = NULL
) {
  # check that of Prob_adjust_valency == Increase_inside_decrease_outside that Prob_adjust_zone is "Inside"
  if (
    Prob_adjust_valency == "Increase_inside_decrease_outside" &&
      Prob_adjust_zone != "Inside"
  ) {
    stop(
      "If Prob_adjust_valency is 'Increase_inside_decrease_outside', then Prob_adjust_zone must be 'Inside'."
    )
  }
  if (!Prob_adjust_zone %in% c("Inside", "Outside")) {
    stop(paste("Unknown Prob_adjust_zone:", Prob_adjust_zone))
  }
  if (!Prob_adjust_valency %in% c("Increase", "Decrease", "Increase_inside_decrease_outside")) {
    stop(paste("Unknown Prob_adjust_valency:", Prob_adjust_valency))
  }

  rows_target <- 0L
  rows_changed <- 0L
  # D-18: one statistics row and one delta vector slot per target class,
  # allocated up front so the table always lines up 1:1 with Target_classes.
  stats_rows <- vector("list", length(Target_classes))
  delta_parts <- vector("list", length(Target_classes))

  threshold_msg <- function(lulc_class, value) {
    log_msg(
      paste0(
        "The Percentage difference is below the threshold for ", lulc_class,
        ", setting to threshold value: ", value
      ),
      log_file
    )
  }
  # IN-04: the explanation used to be logged as a bare sentence fragment with no
  # target class, so a line read as an orphan next to the `to_val=`-prefixed
  # skip lines and could not be attributed to a class from the log alone.
  because_msg <- function(lulc_class, tail) {
    log_msg(
      paste0(
        "to_val=", lulc_class, ": ",
        paste("because the Prob_adjust_valency is", Prob_adjust_valency, tail)
      ),
      log_file
    )
  }

  # loop over the target classes
  for (i in seq_along(Target_classes)) {
    lulc_class <- Target_classes[[i]]
    # D-18: seeded with the skipped-class shape before any of this loop's three
    # `next` paths (no rows, one empty zone, NaN guard) can fire, so every
    # target class contributes exactly one statistics row.
    stats_rows[[i]] <- .delta_stats_skipped(lulc_class)
    delta_parts[[i]] <- numeric(0)

    # Subset to rows of this target class (and from-filter if applicable)
    hit <- normalized$to_val == lulc_class
    if (!is.null(From_filter_vals)) {
      hit <- hit & (normalized$from_val %in% From_filter_vals)
    }
    sub_idx <- which(hit)
    if (length(sub_idx) == 0L) next

    # Look up mask membership by region cell_id (cached LUT)
    inside_flag <- inside_lut[normalized$cell_id[sub_idx]]
    inside_flag[is.na(inside_flag)] <- FALSE

    # If Prob_adjust_zone is Inside, then the intervention area is inside the
    # mask and non-intersecting area is outside the mask (and vice versa).
    if (Prob_adjust_zone == "Inside") {
      Intervention_idx <- sub_idx[inside_flag]
      Non_intervention_idx <- sub_idx[!inside_flag]
    } else {
      Intervention_idx <- sub_idx[!inside_flag]
      Non_intervention_idx <- sub_idx[inside_flag]
    }

    # seperate probability values
    Intervention_vals <- normalized$prob[Intervention_idx]
    Non_Intervention_vals <- normalized$prob[Non_intervention_idx]
    rows_target <- rows_target + length(sub_idx)

    if (length(Intervention_vals) == 0L || length(Non_Intervention_vals) == 0L) {
      log_msg(
        sprintf("intervention skip: to_val=%s one zone has no rows", lulc_class),
        log_file
      )
      next
    }

    before <- normalized$prob[sub_idx]

    # get percentile values
    Intervention_ptile_val <- quantile(
      Intervention_vals[Intervention_vals > 0],
      probs = Prob_adjust_intervention_percentile,
      na.rm = TRUE
    )
    Non_intervention_ptile_val <- quantile(
      Non_Intervention_vals[Non_Intervention_vals > 0],
      probs = Prob_adjust_non_intervention_percentile,
      na.rm = TRUE
    )

    #get the means of the values above the percentile
    Intervention_ptile_mean <- mean(
      Intervention_vals[Intervention_vals >= Intervention_ptile_val],
      na.rm = TRUE
    )
    Non_intervention_ptile_mean <- mean(
      Non_Intervention_vals[
        Non_Intervention_vals >= Non_intervention_ptile_val
      ],
      na.rm = TRUE
    )

    #mean difference
    Mean_diff <- Intervention_ptile_mean - Non_intervention_ptile_mean

    #Average of means
    Average_mean <- (Intervention_ptile_mean + Non_intervention_ptile_mean) / 2

    #calculate percentage difference
    Perc_diff <- (Mean_diff / Average_mean) * 100

    # NaN guard: with no positive probabilities in one zone the percentile /
    # mean chain yields NA/NaN and `if (Perc_diff >= 0)` would crash with
    # "missing value where TRUE/FALSE needed". Skip and log instead.
    if (
      !any(Intervention_vals > 0, na.rm = TRUE) ||
        !any(Non_Intervention_vals > 0, na.rm = TRUE) ||
        !is.finite(Perc_diff)
    ) {
      log_msg(
        sprintf(
          "intervention skip: to_val=%s no positive probabilities in one zone",
          lulc_class
        ),
        log_file
      )
      next
    }

    log_msg(
      paste0(
        "The Percentage difference in average probability above the ",
        Prob_adjust_intervention_percentile, " and ",
        Prob_adjust_non_intervention_percentile,
        " percentiles of the intervention & non-intervention areas respectively for ",
        lulc_class, " is : ", Perc_diff
      ),
      log_file
    )

    if (Prob_adjust_valency == "Increase") {
      # Goal: increase the probability of change for the target class in the
      # intervention area. Perc_diff >= 0 -> increase intervention pixels above
      # the percentile; Perc_diff < 0 -> decrease non-intervention pixels.
      # WR-01(b): `>= 0`, not `> 0`. At exactly zero neither branch used to run,
      # so Prob_adjust_threshold - whose purpose is to guarantee a nudge when
      # the two zones are indistinguishable - was silently skipped, while
      # Increase_inside_decrease_outside applied it. All three valencies now
      # agree at the boundary.
      if (Perc_diff >= 0) {
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          threshold_msg(lulc_class, Prob_adjust_threshold)
          Perc_diff <- Prob_adjust_threshold
        }
        because_msg(lulc_class, "and the percentage difference is >0 then increasing the probability of the intervention pixels")
        # Increase the probability of instances above the specified percentile.
        # WR-01(a): the selection must mirror the `>=` used for
        # Intervention_ptile_mean / Non_intervention_ptile_mean above; with a
        # strict `>` the mean was taken over rows that were then never adjusted.
        ix <- Intervention_idx[Intervention_vals >= Intervention_ptile_val]
        normalized[ix, prob := prob + (prob / 100) * Perc_diff]
        # WR-02: clamp only the rows just written, never rows outside this
        # intervention's declared target class and zone.
        normalized[ix, prob := pmin(pmax(prob, 0), 1)]
      } else if (Perc_diff < 0) {
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          # WR-03: log the signed value actually assigned on the next line.
          threshold_msg(lulc_class, -(Prob_adjust_threshold))
          Perc_diff <- -(Prob_adjust_threshold)
        }
        because_msg(lulc_class, "and the percentage difference is <0 then decreasing the probability of the non-intervention pixels")
        # Decrease the probability of instances above the specified percentile
        ix <- Non_intervention_idx[
          Non_Intervention_vals >= Non_intervention_ptile_val
        ]
        normalized[ix, prob := prob + (prob / 100) * Perc_diff]
        # WR-02: clamp only the rows just written, never rows outside this
        # intervention's declared target class and zone.
        normalized[ix, prob := pmin(pmax(prob, 0), 1)]
      }
    } else if (Prob_adjust_valency == "Decrease") {
      # Goal: decrease the probability of change for the target class in the
      # intervention area. Perc_diff >= 0 -> decrease intervention pixels above
      # the percentile; Perc_diff < 0 -> increase non-intervention pixels.
      # WR-01(b): `>= 0` for the same reason as the Increase branch above.
      if (Perc_diff >= 0) {
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          threshold_msg(lulc_class, Prob_adjust_threshold)
          Perc_diff <- Prob_adjust_threshold
        }
        because_msg(lulc_class, "and the percentage difference is >0 then decreasing the probability of the intervention pixels")
        # Decrease the probability of instances above the specified percentile
        # (WR-01(a): `>=`, mirroring Intervention_ptile_mean).
        ix <- Intervention_idx[Intervention_vals >= Intervention_ptile_val]
        normalized[ix, prob := prob + (prob / 100) * -(Perc_diff)]
        # WR-02: clamp only the rows just written, never rows outside this
        # intervention's declared target class and zone.
        normalized[ix, prob := pmin(pmax(prob, 0), 1)]
      } else if (Perc_diff < 0) {
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          # WR-03: the assignment below is -(Prob_adjust_threshold); log that.
          threshold_msg(lulc_class, -(Prob_adjust_threshold))
          Perc_diff <- -(Prob_adjust_threshold)
        }
        because_msg(lulc_class, "and the percentage difference is <0 then increasing the probability of the non-intervention pixels")
        # Increase the probability of instances above the specified percentile
        ix <- Non_intervention_idx[
          Non_Intervention_vals >= Non_intervention_ptile_val
        ]
        normalized[ix, prob := prob + (prob / 100) * abs(Perc_diff)]
        # WR-02: clamp only the rows just written, never rows outside this
        # intervention's declared target class and zone.
        normalized[ix, prob := pmin(pmax(prob, 0), 1)]
      }
    } else if (Prob_adjust_valency == "Increase_inside_decrease_outside") {
      # Simultaneously increase the probability in the intervention area and
      # decrease it in the non-intervention area.
      if (abs(Perc_diff) < Prob_adjust_threshold) {
        threshold_msg(lulc_class, Prob_adjust_threshold)
        Perc_diff <- Prob_adjust_threshold
      }
      because_msg(lulc_class, "increasing the probability of the intervention pixels and decreasing the probability of the non-intervention pixels")

      # Increase the probability of instances above the specified percentile in
      # the intervention area (WR-01(a): `>=`, mirroring the percentile mean).
      ix <- Intervention_idx[Intervention_vals >= Intervention_ptile_val]
      normalized[ix, prob := prob + (prob / 100) * abs(Perc_diff)]
      # WR-02: clamp only the rows just written, never rows outside this
      # intervention's declared target class and zone.
      normalized[ix, prob := pmin(pmax(prob, 0), 1)]

      # Decrease the probability of instances above the specified percentile in the non-intervention area
      ix <- Non_intervention_idx[
        Non_Intervention_vals >= Non_intervention_ptile_val
      ]
      normalized[ix, prob := prob + (prob / 100) * -(abs(Perc_diff))]
      # WR-02: this valency writes two index sets, so it clamps twice; neither
      # clamp may reach a row outside the declared target class and zone.
      normalized[ix, prob := pmin(pmax(prob, 0), 1)]
    }

    # D-19/D-20: one read of the post-adjustment values of the targeted rows,
    # reused for both the delta statistics and rows_changed. `before` was taken
    # over the same `sub_idx` set immediately before this intervention's
    # adjustment, so the delta describes this intervention alone; a later
    # intervention's `before` is this one's output.
    after <- normalized$prob[sub_idx]
    n_changed_k <- .count_prob_changes(before, after)
    st <- .delta_stats(lulc_class, before, after, n_changed_k)
    rm(after)
    delta_parts[[i]] <- attr(st, "delta")
    attr(st, "delta") <- NULL
    stats_rows[[i]] <- st

    rows_changed <- rows_changed + n_changed_k
  }

  list(
    normalized = normalized,
    rows_target = rows_target,
    rows_changed = rows_changed,
    stats = .bind_delta_stats(stats_rows, delta_parts)
  )
}

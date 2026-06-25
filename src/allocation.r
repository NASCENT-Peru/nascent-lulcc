#' Allocation Step: LULC Simulation via Dinamica EGO
#'
#' Orchestrates the allocation of land use/land cover transitions using
#' Dinamica EGO, processing multiple scenarios, timesteps, and regions.
#'
#' @author Ben Black
#'
#'

# testing values
# scenario <- config[["scenario_names"]][1]
# i <- 1
# idx <- 1
# j <- 1

# ---------------------------------------------------------------------------
# Profiling helpers (opt-in via ALLOCATION_PROFILE env var).
# When the env var is unset/FALSE, prof_tic() returns NULL and prof_toc() is a
# no-op — no Sys.time() call, no log line, zero overhead beyond a single
# env-var read. When enabled, prof_toc() emits one `PROFILE ... elapsed=...s
# rss_before=...MB rss_after=...MB rss_delta=+/-...MB peak_rss=...MB
# gc_max_ncells=...MB gc_max_vcells=...MB` line via log_msg (so it lands in
# both the per-region log file and stdout, the latter captured by the SLURM
# .out).
#
# Memory fields (Linux only; NA on Windows / non-/proc systems):
#   rss_before/after  — VmRSS at stage entry/exit (resident memory, includes
#                       native allocations from terra/arrow/xgboost — gc()
#                       alone misses these)
#   peak_rss          — VmHWM since the most recent prof_tic() reset, i.e.
#                       per-stage peak resident memory
#   gc_max_*          — R-managed cell maxes since prof_tic() (R-objects only)
#
# Caveat: external-process stages (e.g. Dinamica EGO) only show R-side RSS
# during the call. Use SLURM's cgroup view (sacct MaxRSS) for total job
# memory including child processes.
#
# Usage:
#   t0 <- prof_tic()
#   ...stage body, with whatever assignments it needs...
#   prof_toc(t0, "region=foo stage=bar", log_file)
.profile_on <- function() {
  isTRUE(as.logical(Sys.getenv("ALLOCATION_PROFILE", unset = "FALSE")))
}

# Portable process memory reader (OBS-01, Plan 01-03).
#
# Returns a list with elements (all in MiB):
#   rss    — current resident set size from `ps::ps_memory_info()$rss`
#   vmhwm  — Linux-only peak resident size (VmHWM since most recent
#            .reset_vmhwm()), NA on non-/proc hosts
#   vsize  — current virtual memory size from `ps::ps_memory_info()$vmem`
#
# Why portable: the previous implementation parsed `/proc/self/status`,
# which is Linux-only and silently returned NA on Windows local dev runs.
# The `ps` package documents `rss` and `vmem` as portable across UNIX and
# Windows, so we use it as the authoritative RSS source. Linux-specific
# VmHWM remains as opt-in enrichment for per-stage peak reporting.
#
# Failure mode: if `ps` is missing (which the pre-flight catches before any
# work starts) or its call fails, we fall back to NA fields so callers
# continue to format profile lines without crashing — but the canonical
# expectation is that pre-flight has already validated `ps` availability.
.read_proc_status <- function() {
  rss <- NA_real_
  vsize <- NA_real_
  if (requireNamespace("ps", quietly = TRUE)) {
    info <- tryCatch(ps::ps_memory_info(), error = function(e) NULL)
    if (!is.null(info)) {
      r <- tryCatch(unname(info[["rss"]]), error = function(e) NA_real_)
      v <- tryCatch(unname(info[["vmem"]]),
                    error = function(e) tryCatch(unname(info[["vms"]]),
                                                  error = function(e) NA_real_))
      if (length(r) && is.numeric(r) && !is.na(r)) rss <- r / (1024 * 1024)
      if (length(v) && is.numeric(v) && !is.na(v)) vsize <- v / (1024 * 1024)
    }
  }

  vmhwm <- NA_real_
  if (file.exists("/proc/self/status")) {
    lines <- readLines("/proc/self/status", warn = FALSE)
    parse_kb <- function(prefix) {
      m <- grep(paste0("^", prefix, ":"), lines, value = TRUE)
      if (!length(m)) {
        return(NA_real_)
      }
      kb <- suppressWarnings(as.numeric(
        regmatches(m[[1]], regexpr("[0-9]+", m[[1]]))
      ))
      if (length(kb) == 0L || is.na(kb)) NA_real_ else kb / 1024
    }
    # Prefer the procfs RSS reading on Linux only when ps did not produce one
    # (so the portable path remains the canonical source).
    if (is.na(rss)) rss <- parse_kb("VmRSS")
    if (is.na(vsize)) vsize <- parse_kb("VmSize")
    vmhwm <- parse_kb("VmHWM")
  }

  list(rss = rss, vmhwm = vmhwm, vsize = vsize)
}

# Reset Linux per-process VmHWM to current VmRSS so the next VmHWM read
# reflects peak-since-now. Best-effort: silently no-ops on platforms without
# /proc/self/clear_refs or if writing fails.
.reset_vmhwm <- function() {
  cf <- "/proc/self/clear_refs"
  if (!file.exists(cf)) {
    return(invisible(FALSE))
  }
  ok <- tryCatch(
    {
      con <- file(cf, "w")
      on.exit(close(con), add = TRUE)
      writeLines("5", con)
      TRUE
    },
    error = function(e) FALSE,
    warning = function(w) FALSE
  )
  invisible(ok)
}

prof_tic <- function() {
  if (!.profile_on()) {
    return(NULL)
  }
  .reset_vmhwm()
  list(
    t = Sys.time(),
    mem = .read_proc_status(),
    gc = gc(reset = TRUE, full = FALSE)
  )
}

prof_toc <- function(t0, tag, log_file = NULL) {
  if (is.null(t0)) {
    return(invisible(NULL))
  }
  dt <- as.numeric(difftime(Sys.time(), t0$t, units = "secs"))
  after <- .read_proc_status()
  gc_after <- gc(full = FALSE)
  rss_delta <- after$rss - t0$mem$rss
  # gc() returns a matrix with columns: used, (Mb), gc trigger, (Mb), max
  # used, (Mb). Both "(Mb)" columns share a name, so we index by position
  # (col 6) to get the max-used MB readout directly without re-deriving it
  # from cell counts.
  ncells_max_mb <- as.numeric(gc_after["Ncells", 6L])
  vcells_max_mb <- as.numeric(gc_after["Vcells", 6L])
  log_msg(
    sprintf(
      paste0(
        "PROFILE %s elapsed=%.3fs rss_before=%.1fMB rss_after=%.1fMB ",
        "rss_delta=%+.1fMB peak_rss=%.1fMB gc_max_ncells=%.1fMB ",
        "gc_max_vcells=%.1fMB"
      ),
      tag,
      dt,
      t0$mem$rss,
      after$rss,
      rss_delta,
      after$vmhwm,
      ncells_max_mb,
      vcells_max_mb
    ),
    log_file
  )
  invisible(list(
    elapsed = dt,
    rss_delta = rss_delta,
    peak_rss = after$vmhwm
  ))
}

# One-shot memory summary line — useful at the end of a region's worker run
# so the post-job summariser doesn't have to scan every stage to find the
# per-region max.
prof_mem_summary <- function(tag, log_file = NULL) {
  if (!.profile_on()) {
    return(invisible(NULL))
  }
  m <- .read_proc_status()
  log_msg(
    sprintf(
      "PROFILE_MEM %s summary rss=%.1fMB peak_rss=%.1fMB",
      tag,
      m$rss,
      m$vmhwm
    ),
    log_file
  )
  invisible(m)
}

pin_native_threads_to_one <- function(verbose = FALSE) {
  Sys.setenv(
    OMP_NUM_THREADS = "1",
    OPENBLAS_NUM_THREADS = "1",
    MKL_NUM_THREADS = "1",
    GOTO_NUM_THREADS = "1",
    GDAL_NUM_THREADS = "1",
    R_DATATABLE_NUM_THREADS = "1"
  )
  if (requireNamespace("RhpcBLASctl", quietly = TRUE)) {
    try(RhpcBLASctl::blas_set_num_threads(1L), silent = !verbose)
    try(RhpcBLASctl::omp_set_num_threads(1L), silent = !verbose)
  }
  if (requireNamespace("data.table", quietly = TRUE)) {
    data.table::setDTthreads(1L)
  }
  if (requireNamespace("arrow", quietly = TRUE)) {
    try(arrow::set_cpu_count(1L), silent = !verbose)
    # IO threads are blocked on disk/network, not CPU — pinning them to 1
    # alongside cpu_count=1 deadlocks Arrow's executor on shared filesystems
    # (Lustre) where IO latency is high enough to trigger backpressure.
  }
  invisible(NULL)
}

prof_cgroup_snapshot <- function(tag, log_file = NULL) {
  paths_v2 <- list(
    current = "/sys/fs/cgroup/memory.current",
    max = "/sys/fs/cgroup/memory.max"
  )
  paths_v1 <- list(
    current = "/sys/fs/cgroup/memory/memory.usage_in_bytes",
    max = "/sys/fs/cgroup/memory/memory.limit_in_bytes"
  )
  paths <- if (file.exists(paths_v2$current)) {
    paths_v2
  } else if (file.exists(paths_v1$current)) {
    paths_v1
  } else {
    return(invisible(NULL))
  }

  read_counter_mb <- function(path) {
    if (!file.exists(path)) {
      return(NA_real_)
    }
    raw <- tryCatch(readLines(path, warn = FALSE), error = function(e) character(0))
    if (!length(raw) || identical(raw[[1]], "max")) {
      return(NA_real_)
    }
    suppressWarnings(as.numeric(raw[[1]]) / 1024^2)
  }

  current_mb <- read_counter_mb(paths$current)
  max_raw <- tryCatch(readLines(paths$max, warn = FALSE), error = function(e) character(0))
  max_mb <- if (!length(max_raw) || identical(max_raw[[1]], "max")) {
    NA_real_
  } else {
    suppressWarnings(as.numeric(max_raw[[1]]) / 1024^2)
  }

  log_msg(
    sprintf(
      "CGROUP %s memory.current=%sMB memory.max=%sMB",
      tag,
      if (is.na(current_mb)) "unknown" else sprintf("%.1f", current_mb),
      if (is.na(max_mb)) "unlimited" else sprintf("%.1f", max_mb)
    ),
    log_file
  )

  invisible(list(current_mb = current_mb, max_mb = max_mb))
}

# ---------------------------------------------------------------------------
# Stage 7 pre-flight gate (Plan 01-03, locked decisions D-01..D-04).
#
# Operator gate that runs before any allocation work starts and aggregates
# every missing prerequisite into ONE actionable failure list. Categories
# checked:
#   - env vars (the contract surface from `get_stage7_runtime_paths()` plus
#     anything else the caller injects)
#   - R packages (pre-flight tests `requireNamespace()`, never installs)
#   - files (model RDS, parquet roots, transition rates, etc.)
#   - Dinamica backend availability (executable on PATH, container artifact
#     for HPC backends)
#
# The function NEVER calls `install.packages()` or modifies the environment;
# environment provisioning belongs in `setup_environments.sh`. Callers MUST
# stop on any non-empty result before `future::plan()` or any region work.
#
# Args:
#   config: optional config list (from get_config()); used to derive default
#     prerequisite expectations when no fixture is supplied.
#   fixture: optional named list with keys `env`, `packages`, `files`,
#     `dinamica` (with sub-keys `backend`, `runtime`, `artifact`). When
#     supplied, the function validates ONLY the fixture-declared expectations
#     so verification can assert the consolidated-failure-list contract
#     without mutating the host. When NULL, the function derives defaults
#     from `config` and the Stage 7 contract.
#
# Returns: character vector of human-readable error lines; empty vector
# means "pre-flight passed".
validate_allocation_runtime <- function(config = NULL, fixture = NULL) {
  errors <- character(0)

  # Resolve the four prerequisite buckets from either the fixture or the
  # contract+config. Fixtures are EXACT (no defaults merged in) so that test
  # callers can isolate single failure modes.
  env_expected <- character(0)
  packages_expected <- character(0)
  files_expected <- character(0)
  dinamica_expected <- NULL

  if (!is.null(fixture)) {
    env_expected <- as.character(fixture[["env"]] %||% character(0))
    packages_expected <- as.character(fixture[["packages"]] %||% character(0))
    files_expected <- as.character(fixture[["files"]] %||% character(0))
    dinamica_expected <- fixture[["dinamica"]]
  } else {
    # Default contract: the Stage 7 path/env vars from Plan 01-01, the
    # prediction-time package set from Plan 01-02 (MEM-06), and a Dinamica
    # backend assertion derived from the env contract.
    env_expected <- c("HPC_SCRATCH_ROOT", "HPC_TMP_ROOT", "TERRA_TEMP",
                      "DINAMICA_EGO_8_HOME")
    packages_expected <- c(
      "ps", "terra", "arrow", "data.table", "future", "furrr",
      "parallelly",
      "processx", "base64enc", "dplyr", "stringr", "yaml", "jsonlite",
      "workflows", "parsnip", "recipes", "ranger", "xgboost",
      "tidypredict", "butcher", "bundle", "qs", "lobstr", "RhpcBLASctl"
    )
    if (!is.null(config)) {
      # Best-effort file expectations from the live config — only include
      # files that the config actually points at, so a partial config does
      # not produce noise.
      maybe <- function(key) {
        v <- config[[key]]
        if (is.character(v) && length(v) == 1L && nzchar(v)) v else NULL
      }
      reg_dir <- config[["reg_dir"]]
      regions_json <- if (is.character(reg_dir) && length(reg_dir) == 1L && nzchar(reg_dir)) {
        file.path(reg_dir, "regions.json")
      } else {
        NULL
      }
      files_expected <- c(
        maybe("ref_grid_path"),
        maybe("lulc_aggregation_path"),
        regions_json
      )
    }
    dinamica_backend <- Sys.getenv("DINAMICA_BACKEND", unset = "auto")
    dinamica_artifact <- Sys.getenv("DINAMICA_EGO_8_HOME", unset = "")
    dinamica_expected <- list(
      backend = dinamica_backend,
      runtime = if (identical(dinamica_backend, "hpc")) "apptainer" else "DinamicaConsole",
      artifact = dinamica_artifact
    )
  }

  # 1. env vars
  for (key in env_expected) {
    val <- Sys.getenv(key, unset = NA_character_)
    if (is.na(val) || !nzchar(val)) {
      errors <- c(errors, sprintf("env: missing or empty %s", key))
    }
  }

  # 2. packages (test-only, never install)
  for (pkg in packages_expected) {
    ok <- suppressWarnings(requireNamespace(pkg, quietly = TRUE))
    if (!isTRUE(ok)) {
      errors <- c(errors, sprintf(
        "package: %s not installed (pre-flight does not install — fix the env file)",
        pkg
      ))
    }
  }

  # 3. files
  for (f in files_expected) {
    if (!nzchar(f)) next
    if (!file.exists(f)) {
      errors <- c(errors, sprintf("file: missing %s", f))
    }
  }

  # 4. Dinamica backend availability
  if (!is.null(dinamica_expected)) {
    backend <- dinamica_expected[["backend"]] %||% "auto"
    runtime <- dinamica_expected[["runtime"]] %||%
      (if (identical(backend, "hpc")) "apptainer" else "DinamicaConsole")
    artifact <- dinamica_expected[["artifact"]] %||% ""

    if (identical(backend, "hpc")) {
      # HPC: container runtime probe + container artifact must exist.
      have_runtime <- any(nzchar(Sys.which(c(runtime, "singularity", "apptainer"))))
      if (!have_runtime) {
        errors <- c(errors, sprintf(
          "dinamica: container runtime '%s' not on PATH (HPC backend)", runtime
        ))
      }
      if (!nzchar(artifact)) {
        errors <- c(errors, "dinamica: HPC backend selected but DINAMICA_EGO_8_HOME (artifact path) is empty")
      } else if (!file.exists(artifact)) {
        errors <- c(errors, sprintf("dinamica: container artifact missing: %s", artifact))
      }
    } else if (identical(backend, "local")) {
      if (!nzchar(Sys.which("DinamicaConsole"))) {
        errors <- c(errors, "dinamica: DinamicaConsole not on PATH (local backend)")
      }
    }
    # backend == "auto" intentionally does no positive probe at pre-flight
    # time; later work selects the backend explicitly.
  }

  errors
}

# Small null-coalesce operator local to this file; avoids importing rlang
# just for its `%||%`.
`%||%` <- function(x, y) if (is.null(x) || (is.atomic(x) && length(x) == 0L)) y else x

parse_allocation_filter_values <- function(env_key) {
  raw <- Sys.getenv(env_key, unset = "")
  if (!nzchar(raw)) {
    return(character(0))
  }
  vals <- trimws(strsplit(raw, ",", fixed = TRUE)[[1]])
  vals[nzchar(vals)]
}

select_allocation_plan <- function() {
  override <- suppressWarnings(as.integer(
    Sys.getenv("ALLOCATION_NUM_WORKERS", unset = NA_character_)
  ))
  workers <- if (!is.na(override) && override > 0L) {
    override
  } else if (requireNamespace("parallelly", quietly = TRUE)) {
    parallelly::availableCores()
  } else {
    max(1L, parallel::detectCores() - 1L)
  }

  forced <- tolower(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", unset = ""))
  if (identical(forced, "sequential")) {
    return(list(strategy = "sequential", workers = 1L))
  }
  if (forced %in% c("multicore", "multisession")) {
    return(list(strategy = forced, workers = workers))
  }
  if (requireNamespace("parallelly", quietly = TRUE) &&
      parallelly::supportsMulticore()) {
    list(strategy = "multicore", workers = workers)
  } else {
    list(strategy = "multisession", workers = workers)
  }
}

filter_allocation_regions <- function(regions) {
  requested <- parse_allocation_filter_values("ALLOCATION_REGION_FILTER")
  if (!length(requested)) {
    return(regions)
  }
  missing <- setdiff(requested, regions$label)
  if (length(missing)) {
    stop(sprintf(
      "ALLOCATION_REGION_FILTER requested unknown region(s): %s",
      paste(missing, collapse = ", ")
    ))
  }
  filtered <- regions[match(requested, regions$label), , drop = FALSE]
  message(sprintf(
    "Smoke filter: restricting regions to %s",
    paste(filtered$label, collapse = ", ")
  ))
  filtered
}

filter_allocation_timesteps <- function(year_starts, year_ends, scenario) {
  year_post_filter <- Sys.getenv("ALLOCATION_YEAR_POST_FILTER", unset = "")
  if (!nzchar(year_post_filter)) {
    return(list(year_starts = year_starts, year_ends = year_ends))
  }

  year_post <- suppressWarnings(as.integer(year_post_filter))
  if (is.na(year_post)) {
    stop("ALLOCATION_YEAR_POST_FILTER must be an integer posterior year")
  }
  idx <- which(year_ends == year_post)
  if (!length(idx)) {
    stop(sprintf(
      "ALLOCATION_YEAR_POST_FILTER=%d is not a valid posterior year for scenario '%s'",
      year_post,
      scenario
    ))
  }
  message(sprintf(
    "Smoke filter: restricting scenario '%s' to posterior year %d",
    scenario,
    year_post
  ))
  list(
    year_starts = year_starts[idx],
    year_ends = year_ends[idx]
  )
}

get_allocation_worker_rss_budget_mb <- function() {
  raw <- Sys.getenv("ALLOCATION_WORKER_RSS_BUDGET_MB", unset = "")
  if (!nzchar(raw)) {
    return(NA_real_)
  }
  suppressWarnings(as.numeric(raw))
}

# Max rows per prediction batch (Phase 3.4 memory optimisation). When set to a
# positive integer, predict_saved_transition_prob() predicts large "from"-class
# transitions in row-batches and accumulates into a preallocated vector, so peak
# transient memory is bounded by the batch size rather than the full row count.
# Unset/empty/<=0 (the default) preserves the original single-shot behaviour
# exactly. Forest-dominated regions (cuenca_del_amazonas, selva_andina) generate
# transitions with tens of millions of viable cells; a value of e.g. 5000000 keeps
# the prediction-time copies small enough to avoid OOM on memory-constrained nodes.
get_allocation_predict_batch_rows <- function() {
  raw <- Sys.getenv("ALLOCATION_PREDICT_BATCH_ROWS", unset = "")
  if (!nzchar(raw)) {
    return(NA_real_)
  }
  val <- suppressWarnings(as.numeric(raw))
  if (is.na(val) || val <= 0) {
    return(NA_real_)
  }
  val
}

# Number of threads the per-transition ranger predict call may use (Phase 3.5,
# Goal 2). This is a SCOPED relaxation: ONLY the ranger predict OpenMP region is
# allowed > 1 thread. pin_native_threads_to_one() (L194) stays fully in force for
# BLAS/OpenBLAS/MKL/GDAL/data.table/arrow — do NOT raise OMP_NUM_THREADS or touch
# arrow::set_cpu_count, RhpcBLASctl, or setDTthreads to grant ranger threads;
# ranger honours an explicit num.threads argument over the OMP env default, so
# the relaxation is one argument on one call, never a global thread-pool change.
#
# Resolution (see 03.5-RESEARCH.md Q3):
#   - cores = SLURM_CPUS_PER_TASK, else parallel::detectCores(logical = TRUE),
#     else 1.
#   - effective_workers = 1 when ALLOCATION_PARALLEL_STRATEGY is "sequential"
#     (the region runs serially in one worker, so prediction gets all cores —
#     the smoke script sets ALLOCATION_NUM_WORKERS=cores even in sequential mode,
#     so dividing by it there would wrongly cap threads at cores/N); otherwise
#     effective_workers = max(1, ALLOCATION_NUM_WORKERS).
#   - Explicit ALLOCATION_PREDICT_NUM_THREADS=N (positive int) -> N; empty/unset
#     -> auto = floor(cores / effective_workers).
#   - The result is hard-clamped to [1, cores %/% effective_workers] so a
#     misconfigured or oversize knob can never oversubscribe the node, and is
#     always a single integer >= 1 (never NA, 0, or > cores).
get_allocation_predict_num_threads <- function() {
  raw <- Sys.getenv("ALLOCATION_PREDICT_NUM_THREADS", unset = "")

  cores <- suppressWarnings(as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = "")))
  if (is.na(cores) || cores < 1L) {
    cores <- tryCatch(
      as.integer(parallel::detectCores(logical = TRUE)),
      error = function(e) 1L
    )
    if (is.na(cores) || cores < 1L) cores <- 1L
  }

  strategy <- tolower(trimws(Sys.getenv("ALLOCATION_PARALLEL_STRATEGY", unset = "")))
  if (identical(strategy, "sequential")) {
    effective_workers <- 1L
  } else {
    effective_workers <- suppressWarnings(
      as.integer(Sys.getenv("ALLOCATION_NUM_WORKERS", unset = "1"))
    )
    if (is.na(effective_workers) || effective_workers < 1L) effective_workers <- 1L
  }

  budget <- max(1L, cores %/% effective_workers)

  if (nzchar(raw)) {
    t <- suppressWarnings(as.integer(raw))
    if (is.na(t) || t < 1L) t <- 1L
  } else {
    t <- budget
  }

  # Hard clamp so a garbage / oversize knob can never oversubscribe:
  # 1 <= t <= cores / effective_workers.
  as.integer(max(1L, min(t, budget)))
}

# Whether generate_probability_maps() reads predictors lazily, per from-class
# (Phase 3.5, Goal 1). Default ON: the eager full-region preload
# (preload_region_predictor_data) is replaced by per-from-class
# load_from_class_predictor_data() reads, cached so each distinct from-class is
# scanned at most once. ALLOCATION_PREDICTOR_LAZY="0"/"false"/"no" reverts to
# the exact eager preload path byte-for-byte — the bisect escape hatch for the
# central falsifiable risk A3 (whether the lazy per-from-class scan beats the
# eager region scan in aggregate). Unset/empty or "1"/"true"/"yes" => lazy.
get_allocation_predictor_lazy <- function() {
  raw <- tolower(trimws(Sys.getenv("ALLOCATION_PREDICTOR_LAZY", unset = "")))
  if (raw %in% c("0", "false", "no", "off")) {
    return(FALSE)
  }
  TRUE
}

load_allocation_class_map <- function(config) {
  lulc_schema <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
  )
  setNames(
    sapply(lulc_schema, function(x) x$value),
    sapply(lulc_schema, function(x) x$class_name)
  )
}

load_allocation_focal_matrices <- function(config) {
  fm <- readRDS(file.path(
    config[["preds_tools_dir"]],
    "neighbourhood_matrices",
    "all_matrices.rds"
  ))
  fm <- unlist(fm, recursive = FALSE)
  names(fm) <- vapply(
    names(fm),
    function(x) stringr::str_split(x, "[.]")[[1]][2],
    character(1)
  )
  fm
}

load_transition_model_file <- function(file_path) {
  if (grepl("\\.qs$", file_path, perl = TRUE)) {
    qs::qread(file_path)
  } else {
    readRDS(file_path)
  }
}

load_allocation_models <- function(region_labels, calibration_period, config,
                                   active_transitions = NULL) {
  class_name_to_value <- load_allocation_class_map(config)
  model_dir <- file.path(config[["transition_model_dir"]], calibration_period)
  models_by_region <- vector("list", length(region_labels))
  names(models_by_region) <- gsub(" ", "_", tolower(region_labels))

  # Pre-build active key set once for all regions (from_val_to_val strings)
  active_key <- if (!is.null(active_transitions)) {
    paste(active_transitions[["from_val"]], active_transitions[["to_val"]], sep = "_")
  } else {
    NULL
  }

  for (region_label in region_labels) {
    region_suffix <- gsub(" ", "_", tolower(region_label))
    model_files <- list.files(
      model_dir,
      pattern = sprintf(".*_%s\\.(qs|rds)$", region_suffix),
      full.names = TRUE
    )
    if (length(model_files) == 0L) {
      stop(sprintf(
        "No fitted model files found for region '%s' in %s",
        region_suffix,
        model_dir
      ))
    }

    model_info <- data.table::data.table(
      file_path = model_files,
      trans_name = sub(
        sprintf("_%s\\.(qs|rds)$", region_suffix),
        "",
        basename(model_files)
      )
    )
    model_info[,
      c("anterior_class", "posterior_class") := data.table::tstrsplit(
        trans_name,
        "-",
        fixed = TRUE
      )
    ]
    model_info[,
      `:=`(
        from_val = as.integer(class_name_to_value[anterior_class]),
        to_val = as.integer(class_name_to_value[posterior_class])
      )
    ]

    # Filter to active transitions before deserializing — avoids loading
    # models for transitions that have zero rate or don't exist in this scenario.
    if (!is.null(active_key)) {
      model_key <- paste(model_info[["from_val"]], model_info[["to_val"]], sep = "_")
      model_info <- model_info[model_key %in% active_key]
      if (nrow(model_info) == 0L) {
        stop(sprintf(
          "No model files matched active transitions for region '%s'",
          region_suffix
        ))
      }
    }

    model_info[, model_obj := lapply(file_path, load_transition_model_file)]
    model_info[, predictor_names := lapply(model_obj, get_saved_transition_predictors)]
    models_by_region[[region_suffix]] <- model_info
  }

  models_by_region
}

write_nhood_tif <- function(anterior_path, pred_name, focal_matrices,
                            class_name_to_value, out_path) {
  if (file.exists(out_path)) return(invisible(out_path))
  anterior <- terra::rast(anterior_path)
  rast <- compute_single_nhood_raster(
    anterior = anterior,
    pred_name = pred_name,
    focal_matrices = focal_matrices,
    class_name_to_value = class_name_to_value
  )
  dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)
  terra::writeRaster(
    rast,
    out_path,
    overwrite = TRUE,
    datatype = "FLT4S",
    gdal = c(
      "COMPRESS=LZW",
      "TILED=YES",
      "BLOCKXSIZE=256",
      "BLOCKYSIZE=256",
      "BIGTIFF=IF_SAFER",
      "NUM_THREADS=1"
    )
  )
  rm(rast, anterior)
  gc(verbose = FALSE)
  out_path
}

prepare_region_nhood_paths <- function(anterior_path, region_models, scenario,
                                       year_post, region_suffix, config,
                                       focal_matrices, class_name_to_value) {
  predictor_names <- unlist(
    region_models[["predictor_names"]],
    recursive = FALSE,
    use.names = FALSE
  )
  nhood_needed <- sort(unique(grep("_nhood_", predictor_names, value = TRUE)))
  if (!length(nhood_needed)) {
    return(setNames(character(0), character(0)))
  }

  cache_dir <- file.path(
    Sys.getenv("TERRA_TEMP", unset = tempdir()),
    "nhood_cache",
    paste0(scenario, "_", year_post, "_", region_suffix)
  )
  out_paths <- setNames(
    file.path(cache_dir, paste0(nhood_needed, ".tif")),
    nhood_needed
  )
  for (pred_name in nhood_needed) {
    write_nhood_tif(
      anterior_path = anterior_path,
      pred_name = pred_name,
      focal_matrices = focal_matrices,
      class_name_to_value = class_name_to_value,
      out_path = out_paths[[pred_name]]
    )
  }
  out_paths
}

prepare_region_worker_inputs <- function(scenario, year_ant, year_post,
                                         current_lulc_path,
                                         regions, region_rast_path,
                                         calibration_period, timestep_dir,
                                         config) {
  class_name_to_value <- load_allocation_class_map(config)

  # Read trans_rates CSVs to build the set of transitions that are actually
  # active for this scenario/year. Models outside this set are never used and
  # loading them wastes ~10GB of RAM for a 6-class region.
  scalar_str <- sprintf("%.1f", config[["selected_scalar"]])
  active_rows <- lapply(regions$label, function(region_label) {
    csv_path <- file.path(
      config[["trans_rate_table_dir"]],
      paste0("simulation-lulc-areas-scalar-", scalar_str, "x"),
      scenario,
      region_label,
      paste0(scenario, "-", region_label, "-trans_rates-", year_ant, ".csv")
    )
    if (!file.exists(csv_path)) return(NULL)
    df <- utils::read.csv(csv_path, check.names = FALSE)
    data.table::data.table(
      from_val = as.integer(df[["From*"]]),
      to_val   = as.integer(df[["To*"]])
    )
  })
  active_transitions <- unique(data.table::rbindlist(active_rows))
  if (nrow(active_transitions) == 0L) active_transitions <- NULL

  t_preload_models <- prof_tic()
  models_list <- load_allocation_models(
    region_labels = regions$label,
    calibration_period = calibration_period,
    config = config,
    active_transitions = active_transitions
  )
  prof_toc(
    t_preload_models,
    sprintf(
      "scenario=%s year_post=%d stage=preload_models models=%d",
      scenario,
      year_post,
      sum(vapply(models_list, nrow, integer(1)))
    )
  )

  focal_matrices <- load_allocation_focal_matrices(config)
  current_lulc <- terra::rast(current_lulc_path)
  region_rast <- terra::rast(region_rast_path)
  region_inputs <- vector("list", nrow(regions))

  t_nhood_precompute <- prof_tic()
  for (idx in seq_len(nrow(regions))) {
    region_label <- regions$label[[idx]]
    region_val <- as.integer(regions$value[[idx]])
    region_suffix <- gsub(" ", "_", tolower(region_label))
    region_work_dir <- file.path(timestep_dir, paste0("region_", region_suffix))
    ensure_dir(region_work_dir)

    lulc_region <- terra::mask(
      current_lulc,
      region_rast,
      maskvalues = region_val,
      inverse = TRUE
    )
    lulc_region <- terra::trim(lulc_region, padding = 0)
    anterior_path <- file.path(region_work_dir, "anterior.tif")
    terra::writeRaster(
      lulc_region,
      anterior_path,
      overwrite = TRUE,
      wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
    )
    rm(lulc_region)
    gc(verbose = FALSE)

    region_models <- models_list[[region_suffix]]
    nhood_paths <- prepare_region_nhood_paths(
      anterior_path = anterior_path,
      region_models = region_models,
      scenario = scenario,
      year_post = year_post,
      region_suffix = region_suffix,
      config = config,
      focal_matrices = focal_matrices,
      class_name_to_value = class_name_to_value
    )

    region_inputs[[idx]] <- list(
      region_label = region_label,
      region_val = region_val,
      region_suffix = region_suffix,
      region_work_dir = region_work_dir,
      anterior_path = anterior_path,
      models_list = region_models,
      nhood_paths = nhood_paths
    )
  }
  prof_toc(
    t_nhood_precompute,
    sprintf(
      "scenario=%s year_post=%d stage=nhood_precompute regions=%d",
      scenario,
      year_post,
      nrow(regions)
    )
  )

  prof_cgroup_snapshot(
    sprintf("scenario=%s year_post=%d stage=parent_baseline", scenario, year_post)
  )
  prof_toc(
    prof_tic(),
    sprintf("scenario=%s year_post=%d stage=parent_baseline", scenario, year_post)
  )

  rm(current_lulc, region_rast, focal_matrices, class_name_to_value)
  gc(verbose = FALSE)
  region_inputs
}

#' Detect if a loaded model object is in the "minimal saved transition model" format
#' (contains only a recipe and a fitted model, without the full workflow structure).
#' This is used to maintain backward compatibility with previously saved models that were stored in a more minimal format.
is_minimal_saved_transition_model <- function(model_obj) {
  is.list(model_obj) &&
    "recipe" %in% names(model_obj) &&
    "model" %in% names(model_obj)
}

#' Restore importance.mode to "none" for ranger models that were saved with importance.mode=NULL (the default in ranger 0.15.1), which causes prediction to fail after loading because the importance.mode field is missing. This is a workaround for previously saved models that were stored with the default importance.mode, and should have no effect on models saved with an explicit importance.mode or with newer versions of ranger where the default is "none".
#' The importance.mode field is required for prediction with ranger models, and if it is missing (as is the case when loading models saved with ranger 0.15.1's default importance.mode=NULL), then prediction will fail with an error about importance.mode being missing. By restoring importance.mode to "none" for loaded models that were saved with the default, we can ensure that prediction works correctly after loading, while still allowing models that were saved with an explicit importance.mode or with newer versions of ranger to retain their original importance.mode settings.
restore_ranger_importance_mode <- function(model_obj) {
  if (
    is.list(model_obj) &&
      !is.null(model_obj$fit) &&
      inherits(model_obj$fit, "ranger") &&
      !length(model_obj$fit$importance.mode)
  ) {
    model_obj$fit$importance.mode <- "none"
  }

  model_obj
}

#' Get the predictor names used in a saved transition model object, handling both the minimal saved format and the full workflow format.
#' This is used to determine which predictor columns need to be loaded from the parquet datasets.
get_saved_transition_predictors <- function(model_obj) {
  predictor_names <- model_obj$predictor_names
  if (!is.null(predictor_names)) {
    return(predictor_names)
  }

  if (is_minimal_saved_transition_model(model_obj)) {
    return(tryCatch(
      setdiff(model_obj$recipe$var_info$variable, "response"),
      error = function(e) character()
    ))
  }

  tryCatch(
    setdiff(
      workflows::extract_recipe(model_obj)$var_info$variable,
      "response"
    ),
    error = function(e) character()
  )
}

subset_saved_transition_data <- function(new_data, predictor_names) {
  if (data.table::is.data.table(new_data)) {
    new_data[, ..predictor_names]
  } else {
    new_data[, predictor_names, drop = FALSE]
  }
}

predict_saved_tidypredict_prob <- function(model_obj, processed_data) {
  if (model_obj$model_type == "tidypredict_glm") {
    linear_pred <- eval(model_obj$prediction_expr, envir = processed_data)
    prob_1 <- 1 / (1 + exp(-linear_pred))
  } else if (model_obj$model_type == "tidypredict_rf") {
    n_trees <- length(model_obj$prediction_expr)
    prob_1 <- numeric(nrow(processed_data))

    for (i in seq_len(n_trees)) {
      tree_result <- eval(
        model_obj$prediction_expr[[i]],
        envir = processed_data
      )
      if (is.character(tree_result)) {
        tree_result <- as.numeric(tree_result == model_obj$response_levels[2])
      }
      prob_1 <- prob_1 + tree_result
    }

    prob_1 <- prob_1 / n_trees
  } else if (model_obj$model_type == "tidypredict_xgboost") {
    if (is.list(model_obj$prediction_expr)) {
      logits <- numeric(nrow(processed_data))
      for (i in seq_along(model_obj$prediction_expr)) {
        logits <- logits +
          eval(model_obj$prediction_expr[[i]], envir = processed_data)
      }
    } else {
      logits <- eval(model_obj$prediction_expr, envir = processed_data)
    }
    prob_1 <- 1 / (1 + exp(-logits))
  } else {
    stop("Unsupported tidypredict model type: ", model_obj$model_type)
  }

  data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1)
}

predict_saved_butchered_prob <- function(model_obj, processed_data) {
  original_model_type <- sub("^butchered_", "", model_obj$model_type)

  # XGBoost models saved via save_minimal_model() are serialized to raw bytes
  # (model_obj$model$fit is NULL, model_obj$model$xgb_raw holds the booster).
  # Reload the booster and predict directly — bypassing parsnip/workflows.
  if (
    original_model_type == "xgboost" &&
      is.list(model_obj$model) &&
      !is.null(model_obj$model$xgb_raw)
  ) {
    xgb_model <- xgboost::xgb.load.raw(model_obj$model$xgb_raw)
    dtest <- xgboost::xgb.DMatrix(data = as.matrix(processed_data))
    raw_predictions <- predict(xgb_model, dtest)
    prob_1 <- 1 / (1 + exp(-raw_predictions))
    return(data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1))
  }

  if (!original_model_type %in% c("glm", "rf", "xgboost")) {
    stop("Unknown butchered model type: ", original_model_type)
  }

  inner_model <- model_obj$model
  if (is.null(inner_model)) {
    stop(sprintf(
      "Butchered model object is missing inner '$model' (type='%s')",
      original_model_type
    ))
  }

  # `processed_data` has already been baked through model_obj$recipe by the
  # caller, so no workflow/recipe reconstruction is needed — and trying to do so
  # fails on recent `workflows` versions because the saved recipe is trained
  # and `workflows::add_recipe()` refuses trained recipes.
  # `inner_model` is a parsnip `model_fit`; predict.model_fit accepts baked data.
  predictions <- tryCatch(
    predict(inner_model, processed_data, type = "prob"),
    error = function(e) {
      stop(sprintf(
        "predict() failed on butchered %s model: %s",
        original_model_type,
        conditionMessage(e)
      ))
    }
  )

  # Normalize to the .pred_<level> column convention expected by callers
  # (caller in predict_saved_transition_prob takes pred_result[[2L]] as prob_1).
  if (
    !is.null(model_obj$response_levels) &&
      ncol(predictions) == length(model_obj$response_levels)
  ) {
    expected <- paste0(".pred_", model_obj$response_levels)
    if (!identical(names(predictions), expected)) {
      if (all(expected %in% names(predictions))) {
        predictions <- predictions[, expected, drop = FALSE]
      } else {
        names(predictions) <- expected
      }
    }
  }

  predictions
}

#' Predict transition probabilities using a saved transition model object, handling both the minimal saved format
#'  and the full workflow format.
#'
#' @param model_obj Saved transition model object (workflow, minimal list, butchered, tidypredict, etc.)
#' @param new_data Data frame / data.table of predictor values
#' @param log_file Optional path to a per-worker log file. When supplied,
#'   progress messages and any errors raised inside this function (or in the
#'   helpers it calls) are written to the log before being re-thrown.
predict_saved_transition_prob <- function(model_obj, new_data, log_file = NULL) {
  # Logs `msg`, then raises an error with the same text. Used so that any
  # failure inside this function (or its helpers) shows up in the worker log
  # next to the surrounding progress messages, instead of only in the top-level
  # stderr where it's easy to miss.
  log_and_stop <- function(msg) {
    log_msg(msg, log_file)
    stop(msg, call. = FALSE)
  }

  log_msg(
    sprintf(
      "        predict_saved_transition_prob: starting (n_rows=%d)",
      NROW(new_data)
    ),
    log_file
  )

  if (is.list(model_obj)) {
    predictor_names <- get_saved_transition_predictors(model_obj)
    log_msg(
      sprintf(
        "        Resolved %d predictor name(s) from saved model",
        length(predictor_names)
      ),
      log_file
    )
    if (length(predictor_names) > 0L) {
      if (!all(predictor_names %in% names(new_data))) {
        missing_predictors <- predictor_names[
          !predictor_names %in% names(new_data)
        ]
        log_and_stop(sprintf(
          "Missing predictor columns in new_data: %s",
          paste(missing_predictors, collapse = ", ")
        ))
      }
    }

    if (!is.null(model_obj$model_type) && model_obj$model_type == "mlr3") {
      log_msg("        Path: mlr3 learner; predict_newdata()", log_file)
      new_data_subset <- subset_saved_transition_data(new_data, predictor_names)

      # Scoped multi-threaded ranger prediction (Phase 3.5, Goal 2). Resolve the
      # thread count ONCE per transition so it is captured by predict_block_prob1's
      # closure and applied identically across batches. This is the only place
      # num.threads > 1 is permitted — pin_native_threads_to_one() stays in force
      # for every other native pool (BLAS/GDAL/arrow/data.table). Logged once so
      # the smoke log proves the relaxation took effect.
      nthreads <- get_allocation_predict_num_threads()
      log_msg(
        sprintf("        predict num.threads=%d", nthreads),
        log_file
      )

      # Predict P(class == "1") for one block of rows. Tries the mlr3 R6 path and
      # falls back to direct prediction on the underlying fitted model when the
      # installed mlr3 version's R6 bindings don't match the stored train_task.
      # `fallback_logged` is mutated in the enclosing scope so the fallback notice
      # is emitted at most once per transition even when called per-batch.
      fallback_logged <- FALSE
      predict_block_prob1 <- function(block) {
        # Apply the resolved thread count to the mlr3 learner (param tagged
        # `threads`, default 1 in mlr3learners) BEFORE predict_newdata so the
        # ranger predict OpenMP region honours it (Pitfall 5). try() so a learner
        # without the param silently no-ops rather than aborting the prediction.
        try(
          model_obj$learner$param_set$set_values(num.threads = nthreads),
          silent = TRUE
        )
        pred_attempt <- tryCatch(
          model_obj$learner$predict_newdata(newdata = block),
          error = function(e) e
        )
        if (!inherits(pred_attempt, "error")) {
          # pred$prob is a matrix; columns named by class label ("0","1") — Risk 6
          return(pred_attempt$prob[, "1"])
        }
        if (!fallback_logged) {
          log_msg(sprintf(
            "        mlr3 predict_newdata() failed (%s); falling back to direct model prediction",
            conditionMessage(pred_attempt)
          ), log_file)
          fallback_logged <<- TRUE
        }
        underlying <- model_obj$learner$model
        lvl_1 <- which(as.character(model_obj$response_levels) == "1")
        if (!length(lvl_1)) lvl_1 <- 2L  # default: second level = positive class

        if (inherits(underlying, "ranger")) {
          # Scoped relaxation (Phase 3.5): the direct-fallback ranger predict must
          # also honour the resolved thread count — setting the learner param above
          # does NOT affect this path (Pitfall 5). Was hardcoded num.threads = 1L.
          raw <- predict(underlying, data = block, num.threads = nthreads)
          # ranger returns probability matrix when probability=TRUE (set by classif.ranger)
          as.numeric(raw$predictions[, lvl_1])
        } else if (inherits(underlying, c("glmnet", "cv.glmnet"))) {
          x_mat <- as.matrix(block[, predictor_names, drop = FALSE])
          s_val <- if (inherits(underlying, "cv.glmnet")) "lambda.min" else
            tryCatch(model_obj$learner$param_set$values$s, error = function(e) 0.01)
          # glmnet type="response" returns P(y == second_level) for binomial
          as.numeric(predict(underlying, newx = x_mat, type = "response", s = s_val))
        } else {
          log_and_stop(sprintf(
            "mlr3 predict_newdata() failed and no direct fallback for model class '%s': %s",
            paste(class(underlying), collapse = "/"),
            conditionMessage(pred_attempt)
          ))
        }
      }

      # Memory-bounded prediction (Phase 3.4): when ALLOCATION_PREDICT_BATCH_ROWS
      # is set, predict in row-batches and accumulate into a preallocated vector so
      # the prediction-time copies (subset matrix + ranger probability matrix) stay
      # bounded by the batch size instead of the full row count, which OOMs the
      # large forest transitions in cuenca_del_amazonas / selva_andina. Unset (the
      # default) keeps the original single-shot path.
      n_rows <- NROW(new_data_subset)
      batch_rows <- get_allocation_predict_batch_rows()
      use_batches <- !is.na(batch_rows) && n_rows > batch_rows

      if (!use_batches) {
        prob_1 <- predict_block_prob1(new_data_subset)
        log_msg(
          if (fallback_logged) {
            "        Path: mlr3 direct fallback — prediction complete"
          } else {
            "        Path: mlr3 — prediction complete"
          },
          log_file
        )
      } else {
        n_batches <- as.integer(ceiling(n_rows / batch_rows))
        log_msg(sprintf(
          "        Batched prediction: n_rows=%d batch_rows=%d n_batches=%d",
          n_rows, as.integer(batch_rows), n_batches
        ), log_file)
        # Row-subset helper that works for both data.table (dt[idx] = rows) and
        # plain data.frame (df[idx, ] = rows; dt[idx, ] would be ambiguous).
        slice_rows <- if (data.table::is.data.table(new_data_subset)) {
          function(idx) new_data_subset[idx]
        } else {
          function(idx) new_data_subset[idx, , drop = FALSE]
        }
        prob_1 <- numeric(n_rows)
        for (b in seq_len(n_batches)) {
          lo <- (b - 1L) * as.integer(batch_rows) + 1L
          hi <- min(b * as.integer(batch_rows), n_rows)
          idx <- seq.int(lo, hi)
          prob_1[idx] <- predict_block_prob1(slice_rows(idx))
        }
        log_msg(sprintf(
          "        Path: mlr3 %s — prediction complete (%d batches)",
          if (fallback_logged) "direct fallback" else "learner",
          n_batches
        ), log_file)
      }

      result <- data.frame(.pred_0 = 1 - prob_1, .pred_1 = prob_1)
      return(result)
    }

    if (
      !is.null(model_obj$model_type) &&
        grepl("^tidypredict_", model_obj$model_type)
    ) {
      log_msg(
        sprintf(
          "        Path: tidypredict (model_type='%s'); subsetting + baking predictors",
          model_obj$model_type
        ),
        log_file
      )
      new_data_processed <- subset_saved_transition_data(
        new_data,
        predictor_names
      )
      preprocessed_data <- tryCatch(
        recipes::bake(model_obj$recipe, new_data_processed),
        error = function(e) {
          log_and_stop(sprintf(
            "recipes::bake() failed for tidypredict model: %s",
            conditionMessage(e)
          ))
        }
      )
      result <- tryCatch(
        predict_saved_tidypredict_prob(model_obj, preprocessed_data),
        error = function(e) {
          log_and_stop(sprintf(
            "predict_saved_tidypredict_prob() failed: %s",
            conditionMessage(e)
          ))
        }
      )
      log_msg("        Path: tidypredict — prediction complete", log_file)
      return(result)
    }

    if (
      !is.null(model_obj$model_type) &&
        grepl("^butchered_", model_obj$model_type)
    ) {
      log_msg(
        sprintf(
          "        Path: butchered (model_type='%s'); subsetting + baking predictors",
          model_obj$model_type
        ),
        log_file
      )
      new_data_processed <- subset_saved_transition_data(
        new_data,
        predictor_names
      )
      preprocessed_data <- tryCatch(
        recipes::bake(model_obj$recipe, new_data_processed),
        error = function(e) {
          log_and_stop(sprintf(
            "recipes::bake() failed for butchered model: %s",
            conditionMessage(e)
          ))
        }
      )
      model_obj$model <- restore_ranger_importance_mode(model_obj$model)
      result <- tryCatch(
        predict_saved_butchered_prob(model_obj, preprocessed_data),
        error = function(e) {
          log_and_stop(sprintf(
            "predict_saved_butchered_prob() failed: %s",
            conditionMessage(e)
          ))
        }
      )
      log_msg("        Path: butchered — prediction complete", log_file)
      return(result)
    }

    if (is_minimal_saved_transition_model(model_obj)) {
      log_msg(
        "        Path: minimal saved transition model; subsetting + baking predictors",
        log_file
      )
      new_data_processed <- subset_saved_transition_data(
        new_data,
        predictor_names
      )
      preprocessed_data <- tryCatch(
        recipes::bake(model_obj$recipe, new_data_processed),
        error = function(e) {
          log_and_stop(sprintf(
            "recipes::bake() failed for minimal saved model: %s",
            conditionMessage(e)
          ))
        }
      )
      model_obj$model <- restore_ranger_importance_mode(model_obj$model)
      result <- tryCatch(
        predict(model_obj$model, preprocessed_data, type = "prob"),
        error = function(e) {
          log_and_stop(sprintf(
            "predict() on minimal saved model failed: %s",
            conditionMessage(e)
          ))
        }
      )
      log_msg(
        "        Path: minimal saved transition model — prediction complete",
        log_file
      )
      return(result)
    }
  }

  if (is.list(model_obj) && "model" %in% names(model_obj)) {
    log_msg("        Path: legacy saved-list model", log_file)
    model_obj$model <- restore_ranger_importance_mode(model_obj$model)
    if (identical(class(model_obj$model), "list")) {
      log_and_stop(paste0(
        "Unsupported saved model format: inner `model` is a plain list with no ",
        "predict() method. Consider re-saving this transition model in the current format."
      ))
    }
    result <- tryCatch(
      predict(model_obj$model, new_data, type = "prob"),
      error = function(e) {
        log_and_stop(sprintf(
          "predict() on legacy saved-list model failed: %s",
          conditionMessage(e)
        ))
      }
    )
    log_msg("        Path: legacy saved-list — prediction complete", log_file)
    return(result)
  }

  if (
    inherits(model_obj, "workflow") &&
      !is.null(model_obj$fit) &&
      !is.null(model_obj$fit$fit)
  ) {
    log_msg("        Path: full workflow model", log_file)
    model_obj$fit$fit <- restore_ranger_importance_mode(model_obj$fit$fit)
  } else {
    log_msg(
      sprintf(
        "        Path: fallback predict() (class='%s')",
        paste(class(model_obj), collapse = "/")
      ),
      log_file
    )
  }

  result <- tryCatch(
    predict(model_obj, new_data, type = "prob"),
    error = function(e) {
      log_and_stop(sprintf(
        "predict() on workflow / fallback path failed: %s",
        conditionMessage(e)
      ))
    }
  )
  log_msg("        Prediction complete", log_file)
  result
}

#' Top-level entry point for the allocation step
#'
#' @param config Configuration list from get_config()
#' @return NULL (called for side effects)
run_allocation <- function(config = get_config()) {
  message("\n========================================")
  message("Starting LULC Allocation Simulations")
  message("========================================\n")

  # Stage 7 operator gate (Plan 01-03, D-01..D-04). One actionable list, then
  # stop BEFORE any future::plan(), worker-log init, or region work runs.
  preflight_errors <- validate_allocation_runtime(config = config)
  if (length(preflight_errors) > 0L) {
    msg <- paste(
      c("Allocation pre-flight failed:", paste0("  - ", preflight_errors)),
      collapse = "\n"
    )
    stop(msg, call. = FALSE)
  }

  # Load regions
  regions <- jsonlite::fromJSON(file.path(config[["reg_dir"]], "regions.json"))
  regions <- filter_allocation_regions(regions)
  region_rast_path <- list.files(
    config[["reg_dir"]],
    pattern = "regions.tif$",
    full.names = TRUE
  )

  for (scenario in config[["scenario_names"]]) {
    t_start <- proc.time()
    message(sprintf("\n--- Scenario: %s ---", scenario))

    run_allocation_for_scenario(
      scenario = scenario,
      regions = regions,
      region_rast_path = region_rast_path,
      config = config
    )

    elapsed <- (proc.time() - t_start)[["elapsed"]]
    message(sprintf(
      "Scenario %s completed in %.1f minutes",
      scenario,
      elapsed / 60
    ))
  }

  message("\n========================================")
  message("All allocation simulations complete")
  message("========================================\n")
}


#' Run allocation for a single scenario across all timesteps
#'
#' @param scenario Scenario name (e.g., "BAU")
#' @param regions Regions data frame with value and label columns
#' @param region_rast_path Path for regions raster
#' @param config Configuration list
#' @return NULL (called for side effects)
run_allocation_for_scenario <- function(
  scenario,
  regions,
  region_rast_path,
  config
) {
  sim_dir <- file.path(config[["simulation_output_dir"]], scenario)
  ensure_dir(sim_dir)

  # Load initial LULC raster for the simulation start year
  start_year <- config[["simulation_start_year"]]
  end_year <- config[["simulation_end_year"]]
  step_length <- config[["step_length"]]

  lulc_files <- list.files(
    config[["aggregated_lulc_dir"]],
    pattern = "\\.tif$",
    full.names = TRUE
  )
  initial_lulc_file <- lulc_files[grepl(as.character(start_year), lulc_files)]
  if (length(initial_lulc_file) == 0) {
    stop(sprintf(
      "No LULC raster found for start year %d in %s",
      start_year,
      config[["aggregated_lulc_dir"]]
    ))
  }

  current_lulc_path <- initial_lulc_file[1]
  message(sprintf("Initial LULC: %s", current_lulc_path))

  # Build timestep pairs from the authoritative explicit schedule (D-11).
  # The rate CSVs (BAU-<region>-trans_rates-<year_ant>.csv) were generated
  # against config[["simulation_year_steps"]] via src/simulation_trans_rates_prep.r,
  # so the allocation loop MUST adopt the same pairs or it will request rate
  # tables for years that were never generated. The validation block below is
  # ported verbatim from src/simulation_trans_rates_prep.r:307-330 (same stop()
  # wording) so a malformed schedule fails fast with the same diagnostic. The
  # scalar step_length read above is retained only as an unused fallback — it is
  # NOT used to construct the pairs (it ends at 2058/9 steps, never reaches 2060).
  year_steps <- config[["simulation_year_steps"]]
  if (is.null(year_steps) || length(year_steps) == 0) {
    stop("config[[\"simulation_year_steps\"]] is missing or empty. Add an explicit ordered list to your config YAML — see 03.2-CONTEXT.md D-01.")
  }
  if (year_steps[1] != start_year) {
    stop(sprintf(
      "simulation_year_steps[1] is %d but simulation_start_year is %d — update one to match the other before continuing.",
      year_steps[1], start_year
    ))
  }
  if (tail(year_steps, 1) != end_year) {
    stop(sprintf(
      "simulation_year_steps ends at %d but simulation_end_year is %d — update one to match the other before continuing.",
      tail(year_steps, 1), end_year
    ))
  }
  if (!all(diff(year_steps) > 0)) {
    stop(sprintf(
      "simulation_year_steps must be strictly increasing — observed diffs: %s",
      paste(diff(year_steps), collapse = ", ")
    ))
  }

  year_starts <- head(year_steps, -1)
  year_ends <- tail(year_steps, -1)
  year_filters <- filter_allocation_timesteps(year_starts, year_ends, scenario)
  year_starts <- year_filters$year_starts
  year_ends <- year_filters$year_ends

  profile_timestep_index <- config[["profile_timestep_index"]]
  if (!is.null(profile_timestep_index)) {
    if (
      profile_timestep_index < 1L ||
        profile_timestep_index > length(year_starts)
    ) {
      stop(sprintf(
        "profile_timestep_index=%d is outside the valid range 1..%d",
        profile_timestep_index,
        length(year_starts)
      ))
    }
    year_starts <- year_starts[profile_timestep_index]
    year_ends <- year_ends[profile_timestep_index]
    message(sprintf(
      "Profile mode: restricting scenario '%s' to timestep index %d (%d -> %d)",
      scenario,
      profile_timestep_index,
      year_starts[[1]],
      year_ends[[1]]
    ))
  }

  # Calibration period: use the last (most recent) data period
  calibration_period <- config[["data_periods"]][length(config[[
    "data_periods"
  ]])]

  for (i in seq_along(year_starts)) {
    year_ant <- year_starts[i]
    year_post <- year_ends[i]
    message(sprintf(
      "\n  Timestep %d/%d: %d -> %d",
      i,
      length(year_starts),
      year_ant,
      year_post
    ))

    current_lulc_path <- run_allocation_one_timestep(
      scenario = scenario,
      year_ant = year_ant,
      year_post = year_post,
      current_lulc_path = current_lulc_path,
      regions = regions,
      region_rast_path = region_rast_path,
      calibration_period = calibration_period,
      sim_dir = sim_dir,
      config = config
    )

    message(sprintf(
      "  Timestep %d -> %d complete: %s",
      year_ant,
      year_post,
      current_lulc_path
    ))
  }
}


#' Run allocation for a single timestep across all regions
#'
#' @param scenario Scenario name
#' @param year_ant Anterior year
#' @param year_post Posterior year
#' @param current_lulc_path Path to current LULC raster
#' @param regions Regions data frame
#' @param region_rast_path Path to regions raster
#' @param calibration_period Calibration period string (e.g., "2018_2022")
#' @param sim_dir Simulation output directory for this scenario
#' @param config Configuration list
#' @return Path to the mosaiced posterior raster
run_allocation_one_timestep <- function(
  scenario,
  year_ant,
  year_post,
  current_lulc_path,
  regions,
  region_rast_path,
  calibration_period,
  sim_dir,
  config
) {
  timestep_dir <- file.path(sim_dir, as.character(year_post))
  ensure_dir(timestep_dir)
  region_inputs <- prepare_region_worker_inputs(
    scenario = scenario,
    year_ant = year_ant,
    year_post = year_post,
    current_lulc_path = current_lulc_path,
    regions = regions,
    region_rast_path = region_rast_path,
    calibration_period = calibration_period,
    timestep_dir = timestep_dir,
    config = config
  )

  # Process each region (parallel via furrr if available)
  posterior_paths <- furrr::future_map(
    region_inputs,
    function(region_input) {
      region_label <- region_input$region_label
      region_val <- region_input$region_val
      region_suffix <- region_input$region_suffix
      region_work_dir <- region_input$region_work_dir
      log_file <- initialize_worker_log(
        file.path(region_work_dir, "worker_logs"),
        region_suffix
      )

      # Breadcrumb state for this worker (Plan 01-03 Task 2; D-06, OBS-02).
      # If the worker dies before reaching the explicit on.exit() below
      # (e.g. SIGKILL), the in-memory state is lost — but lifecycle STATE
      # lines are flushed to log_file at every transition so the latest
      # known state is always durable on disk.
      worker_state_init(
        scenario = scenario,
        region = region_label,
        timestep = year_post,
        log_file = log_file
      )
      sentinel_reason <- "incomplete"
      on.exit({
        # Reachable on R-level errors and on normal exit. Sentinel reason
        # is "ok" only if we set it explicitly at the bottom of the worker.
        worker_state_flush_sentinel(log_file, reason = sentinel_reason)
      }, add = TRUE)

      log_msg(
        sprintf("    Region: %s (ID=%d)", region_label, region_val),
        log_file
      )

      worker_state_set(stage = "region_setup", log_file = log_file)
      t_region_total <- prof_tic()
      t_region_setup <- prof_tic()
      log_msg(
        sprintf(
          paste0(
            "PROFILE region=%s stage=pin_native_threads_to_one ",
            "OMP_NUM_THREADS=%s data.table_threads=%s"
          ),
          region_suffix,
          Sys.getenv("OMP_NUM_THREADS", unset = "unset"),
          if (requireNamespace("data.table", quietly = TRUE)) {
            as.character(data.table::getDTthreads())
          } else {
            "unset"
          }
        ),
        log_file
      )
      worker_budget_mb <- get_allocation_worker_rss_budget_mb()
      if (!is.na(worker_budget_mb)) {
        log_msg(
          sprintf(
            paste0(
              "RSS_BUDGET region=%s budget_mb=%.1f source=",
              "ALLOCATION_WORKER_RSS_BUDGET_MB"
            ),
            region_suffix,
            worker_budget_mb
          ),
          log_file
        )
      }
      log_msg(
        sprintf(
          "PROFILE region=%s stage=preload_models models=%d",
          region_suffix,
          nrow(region_input$models_list)
        ),
        log_file
      )
      log_msg(
        sprintf(
          "PROFILE region=%s stage=nhood_precompute paths=%d",
          region_suffix,
          length(region_input$nhood_paths)
        ),
        log_file
      )
      log_msg(
        sprintf("PROFILE region=%s stage=parent_baseline", region_suffix),
        log_file
      )
      prof_cgroup_snapshot(
        sprintf("region=%s stage=worker_entry", region_suffix),
        log_file
      )
      if (!file.exists(region_input$anterior_path)) {
        stop(log_msg(
          sprintf(
            "Precomputed anterior raster missing for region '%s': %s",
            region_label,
            region_input$anterior_path
          ),
          log_file
        ))
      }
      log_msg(
        sprintf(
          "    Using precomputed anterior raster: %s",
          region_input$anterior_path
        ),
        log_file
      )

      prof_toc(
        t_region_setup,
        sprintf("region=%s stage=region_setup", region_suffix),
        log_file
      )

      # Prepare all Dinamica input files. log_file is threaded both into
      # setup_allocation_inputs() and (D-05, OBS-03) into run_allocation_dinamica()
      # so structured DINAMICA_* breadcrumbs land in this same per-region log.
      worker_state_set(stage = "setup_inputs", log_file = log_file)
      t_setup_inputs <- prof_tic()
      setup_allocation_inputs(
        work_dir = region_work_dir,
        region_label = region_label,
        region_val = region_val,
        scenario = scenario,
        year_ant = year_ant,
        year_post = year_post,
        anterior_path = region_input$anterior_path,
        calibration_period = calibration_period,
        config = config,
        log_file = log_file,
        models_list = region_input$models_list,
        nhood_paths = region_input$nhood_paths
      )
      prof_toc(
        t_setup_inputs,
        sprintf("region=%s stage=setup_inputs", region_suffix),
        log_file
      )

      # Run Dinamica
      worker_state_set(stage = "dinamica_launch", log_file = log_file)
      t_dinamica <- prof_tic()
      posterior_path <- run_allocation_dinamica(
        work_dir = region_work_dir,
        log_file = log_file
      )
      prof_toc(
        t_dinamica,
        sprintf("region=%s stage=dinamica", region_suffix),
        log_file
      )

      # Saturation diagnostic (Phase 3.3 D-05/D-06): per-transition
      # placed-vs-demanded summary. Writes work_dir/saturation_summary.csv
      # and emits one AUDIT stage=saturation line via log_audit_saturation_line().
      # Helper resolves because scripts/run_allocation.r sources
      # src/saturation_diagnostics.r into the worker before src/allocation.r.
      t_saturation <- prof_tic()
      write_saturation_summary(
        work_dir       = region_work_dir,
        scenario       = scenario,
        region_label   = region_label,
        region_suffix  = region_suffix,
        year_ant       = year_ant,
        anterior_path  = region_input$anterior_path,
        posterior_path = posterior_path,
        config         = config,
        log_file       = log_file
      )
      prof_toc(
        t_saturation,
        sprintf("region=%s stage=saturation_summary", region_suffix),
        log_file
      )

      log_msg(
        sprintf("    Completed region: %s (ID=%d)", region_label, region_val),
        log_file
      )

      rm(region_input)
      gc(verbose = FALSE)

      prof_toc(
        t_region_total,
        sprintf("region=%s stage=region_total", region_suffix),
        log_file
      )
      prof_mem_summary(
        sprintf("region=%s", region_suffix),
        log_file
      )

      # Worker reached the end successfully — sentinel will record reason="ok".
      sentinel_reason <- "ok"
      return(posterior_path)
    },
    .options = furrr::furrr_options(seed = TRUE)
  )

  # Mosaic region posteriors back to full extent
  message("    Mosaicing region posteriors...")
  region_rasters <- lapply(posterior_paths, terra::rast)

  # Extend all to the full extent of the original LULC, then merge
  full_extent <- terra::ext(terra::rast(current_lulc_path))
  region_rasters <- lapply(region_rasters, function(r) {
    terra::extend(r, full_extent)
  })

  if (length(region_rasters) == 1) {
    mosaiced <- region_rasters[[1]]
  } else {
    mosaiced <- do.call(terra::merge, region_rasters)
  }

  # Save mosaiced result
  output_path <- file.path(timestep_dir, sprintf("posterior_%d.tif", year_post))
  terra::writeRaster(
    mosaiced,
    output_path,
    overwrite = TRUE,
    wopt = list(datatype = "INT2U", gdal = c("COMPRESS=LZW"))
  )

  rm(region_rasters, mosaiced)
  gc(verbose = FALSE)

  return(output_path)
}


#' Prepare all Dinamica input files in work_dir
#'
#' @param work_dir Working directory for this region/timestep
#' @param region_label Region label string
#' @param region_val Region integer value
#' @param scenario Scenario name
#' @param year_ant Anterior year
#' @param year_post Posterior year
#' @param anterior_path Path to the anterior LULC raster
#' @param calibration_period Calibration period string
#' @param config Configuration list
#' @param log_file Path to the log file for this worker/region
#' @return NULL (called for side effects)
setup_allocation_inputs <- function(
  work_dir,
  region_label,
  region_val,
  scenario,
  year_ant,
  year_post,
  anterior_path,
  calibration_period,
  config,
  log_file,
  models_list,
  nhood_paths
) {
  # 1. Copy transition rates CSV
  scalar_str <- sprintf("%.1f", config[["selected_scalar"]])
  trans_rate_src <- file.path(
    config[["trans_rate_table_dir"]],
    paste0("simulation-lulc-areas-scalar-", scalar_str, "x"),
    scenario,
    region_label,
    paste0(scenario, "-", region_label, "-trans_rates-", year_ant, ".csv")
  )
  if (!file.exists(trans_rate_src)) {
    stop(log_msg(
      sprintf("Transition rate file not found: %s", trans_rate_src),
      log_file
    ))
  }
  trans_rates_dst <- file.path(work_dir, "trans_rates.csv")

  # Single-source filter (Phase 3.3 D-02/D-03): read Stage 4 source, drop
  # persistence (From* == To*) and zero-rate rows, sort by id_trans, then write
  # the filtered df to work_dir. The Stage 4 source CSV on disk is unchanged —
  # only the work_dir copy is filtered. check.names = FALSE preserves the
  # `From*` / `To*` markers Dinamica requires (Pitfall 4 / commit 2c599a2).
  trans_rates_raw <- utils::read.csv(trans_rate_src, check.names = FALSE)
  n_in <- nrow(trans_rates_raw)
  trans_rates_df <- trans_rates_raw[
    trans_rates_raw[["From*"]] != trans_rates_raw[["To*"]] &
      trans_rates_raw[["Rate"]] != 0,
  ]
  trans_rates_df <- trans_rates_df[order(trans_rates_df[["id_trans"]]), ]
  rownames(trans_rates_df) <- NULL
  n_out <- nrow(trans_rates_df)
  utils::write.csv(trans_rates_df, trans_rates_dst, row.names = FALSE)
  log_msg(
    sprintf(
      paste0(
        "AUDIT stage=allocation_filter scenario=%s region=%s year_ant=%d ",
        "input_rows=%d filtered_rows=%d dropped=%d"
      ),
      scenario,
      region_label,
      year_ant,
      n_in,
      n_out,
      n_in - n_out
    ),
    log_file
  )

  # 2. Extract expansion table from allocation params
  alloc_params_path <- file.path(
    config[["calibration_param_dir"]],
    calibration_period,
    region_label,
    "allocation_params.csv"
  )
  if (!file.exists(alloc_params_path)) {
    stop(log_msg(
      sprintf("Allocation params file not found: %s", alloc_params_path),
      log_file
    ))
  }
  alloc_params <- read.csv(alloc_params_path, check.names = FALSE)

  # sort by id_trans
  alloc_params <- alloc_params[order(alloc_params[["id_trans"]]), ]

  # subset alloc_params to only values of id_trans that are present in trans_rates_df
  alloc_params <- alloc_params[
    alloc_params[["id_trans"]] %in% trans_rates_df[["id_trans"]],
  ]

  # Hard-stop if any id_trans values in trans_rates_df are missing from
  # alloc_params (Phase 3.3 D-04 / ALLOC-08). Promoted from warning() to
  # stop() so the run fails loudly rather than silently shipping a truncated
  # expansion/patcher table — the operator must regenerate alloc_params.
  missing_alloc_params <- setdiff(
    trans_rates_df[["id_trans"]],
    alloc_params[["id_trans"]]
  )
  if (length(missing_alloc_params) > 0) {
    stop(log_msg(
      sprintf(
        paste0(
          "Allocation params missing for active id_trans values ",
          "(regenerate alloc_params for region %s period %s): %s"
        ),
        region_label,
        calibration_period,
        paste(missing_alloc_params, collapse = ", ")
      ),
      log_file
    ), call. = FALSE)
  }

  # Defence-in-depth row-order alignment (Phase 3.3 Pitfall 5): reorder
  # alloc_params so its rows align 1:1 by id_trans with trans_rates_df.
  # The downstream expansion_tbl / patcher_tbl slices auto-inherit this order.
  alloc_params <- alloc_params[
    match(trans_rates_df[["id_trans"]], alloc_params[["id_trans"]]),
  ]
  stopifnot(identical(alloc_params[["id_trans"]], trans_rates_df[["id_trans"]]))

  # Expansion table: From*, To*, Perc_expander
  # Dinamica's PercentMatrix type expects fractions [0,1], not percentages [0,100].
  expansion_tbl <- alloc_params[, c("From*", "To*", "Perc_expander")]
  expansion_tbl[["Perc_expander"]] <- expansion_tbl[["Perc_expander"]] / 100
  write.csv(
    expansion_tbl,
    file.path(work_dir, "expansion_table.csv"),
    row.names = FALSE
  )
  log_msg(
    sprintf("  Expansion table written with %d rows", nrow(expansion_tbl)),
    log_file
  )

  # 3. Patcher table: From*, To*, Mean_Patch_Size, Patch_Size_Variance, Patch_Isometry
  patcher_cols <- c(
    "From*",
    "To*",
    "Mean_Patch_Size",
    "Patch_Size_Variance",
    "Patch_Isometry"
  )

  patcher_tbl <- alloc_params[, patcher_cols]
  # Replace NAs/zeros with sensible defaults for unobserved transitions
  patcher_tbl[["Mean_Patch_Size"]] <- ifelse(
    is.na(patcher_tbl[["Mean_Patch_Size"]]) |
      patcher_tbl[["Mean_Patch_Size"]] == 0,
    1,
    patcher_tbl[["Mean_Patch_Size"]]
  )
  patcher_tbl[["Patch_Size_Variance"]] <- ifelse(
    is.na(patcher_tbl[["Patch_Size_Variance"]]),
    0,
    patcher_tbl[["Patch_Size_Variance"]]
  )
  patcher_tbl[["Patch_Isometry"]] <- ifelse(
    is.na(patcher_tbl[["Patch_Isometry"]]),
    0.5,
    patcher_tbl[["Patch_Isometry"]]
  )
  write.csv(
    patcher_tbl,
    file.path(work_dir, "patcher_table.csv"),
    row.names = FALSE
  )
  log_msg(
    sprintf("  Patcher table written with %d rows", nrow(patcher_tbl)),
    log_file
  )

  # 4. Generate probability maps
  generate_probability_maps(
    work_dir = work_dir,
    region_label = region_label,
    region_val = region_val,
    scenario = scenario,
    year_ant = year_ant,
    calibration_period = calibration_period,
    anterior_path = anterior_path,
    trans_rates_df = trans_rates_df,
    config = config,
    log_file = log_file,
    models_list = models_list,
    nhood_paths = nhood_paths
  )
}


#' Pre-load all parquet predictor columns for a region in one Arrow scan.
#'
#' Loads all static and dynamic predictor columns needed by any model for this
#' region in a single Arrow read (filtered by region partition), keyed by
#' cell_id. Callers then do fast in-memory data.table lookups per transition
#' instead of 38+ separate Arrow scans with large cell_id IN filters.
#'
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param preds Character vector of predictor names to load (parquet columns only)
#' @param region_value Integer region partition value
#' @param scenario SSP scenario string for the dynamic partition filter
#' @param log_file Optional path to per-worker log file
#' @return data.table keyed by cell_id with all requested predictor columns
preload_region_predictor_data <- function(
  ds_static,
  ds_dynamic,
  preds,
  region_value,
  scenario,
  log_file = NULL
) {
  preds <- unique(as.character(preds))
  if (length(preds) == 0L) {
    return(data.table::data.table(cell_id = integer()))
  }

  static_cols <- intersect(preds, names(ds_static$schema))
  dyn_cols <- intersect(preds, names(ds_dynamic$schema))

  static_df <- if (length(static_cols) > 0L) {
    ds_static |>
      dplyr::filter(region == !!region_value) |>
      dplyr::select(cell_id, dplyr::all_of(static_cols)) |>
      dplyr::collect() |>
      data.table::as.data.table()
  } else {
    data.table::data.table(cell_id = integer())
  }

  dyn_df <- if (length(dyn_cols) > 0L) {
    ds_dynamic |>
      dplyr::filter(region == !!region_value, scenario == !!scenario) |>
      dplyr::select(cell_id, dplyr::all_of(dyn_cols)) |>
      dplyr::collect() |>
      data.table::as.data.table()
  } else {
    data.table::data.table(cell_id = integer())
  }

  if (nrow(static_df) == 0L && nrow(dyn_df) == 0L) {
    return(data.table::data.table(cell_id = integer()))
  }

  result <- if (nrow(static_df) > 0L && nrow(dyn_df) > 0L) {
    merge(static_df, dyn_df, by = "cell_id", all = TRUE)
  } else if (nrow(static_df) > 0L) {
    static_df
  } else {
    dyn_df
  }

  data.table::setkeyv(result, "cell_id")
  log_msg(
    sprintf(
      "    Region predictor data preloaded: %d cells, %d static cols (%s), %d dynamic cols (%s)",
      nrow(result),
      length(static_cols),
      paste(static_cols, collapse = ", "),
      length(dyn_cols),
      paste(dyn_cols, collapse = ", ")
    ),
    log_file
  )
  result
}


#' Lazily load parquet predictor columns for ONE from-class's sparse cell keys.
#'
#' Per-from-class analogue of preload_region_predictor_data(). Instead of
#' collecting the full region predictor table, this reads only the predictor
#' columns a from-class's transitions need, scoped to that from-class's sparse
#' cell keys via an Arrow `inner_join` against a key table — NOT a
#' `filter(cell_id %in% big_vector)`, which is the known-slow Acero path the
#' L2256-2259 comment warns about (Phase 3.5 RESEARCH Pitfall 1). The result is
#' cached per from_val by the caller so each distinct from-class is scanned at
#' most once.
#'
#' The output is row/column-equivalent to preload_region_predictor_data()
#' restricted to the same keys/columns: static columns filtered by region only,
#' dynamic columns filtered by region + scenario, merged by cell_id with
#' all = TRUE (the same merge the eager preload uses), keyed by cell_id. The
#' downstream keyed left-join `region_pred_dt[J(lookup_keys), nomatch = NA]`
#' (Pitfall 4) operates against this from-class-sized table unchanged, so
#' per-transition cell counts and NA-fill are identical to the eager path.
#'
#' @param ds_static Arrow dataset for static predictors
#' @param ds_dynamic Arrow dataset for dynamic predictors
#' @param cols Character vector of predictor names to load (parquet columns only)
#' @param region_value Integer region partition value
#' @param scenario SSP scenario string for the dynamic partition filter
#' @param ref_cell_keys Integer vector of the from-class's ref_cell_id keys
#' @param log_file Optional path to per-worker log file
#' @return data.table keyed by cell_id with the requested predictor columns,
#'   scoped to the supplied keys. Empty cols or empty keys yield an empty
#'   cell_id-only data.table (no error).
load_from_class_predictor_data <- function(
  ds_static,
  ds_dynamic,
  cols,
  region_value,
  scenario,
  ref_cell_keys,
  log_file = NULL
) {
  cols <- unique(as.character(cols))
  ref_cell_keys <- unique(as.integer(ref_cell_keys))
  if (length(cols) == 0L || length(ref_cell_keys) == 0L) {
    return(data.table::data.table(cell_id = integer()))
  }

  static_cols <- intersect(cols, names(ds_static$schema))
  dyn_cols <- intersect(cols, names(ds_dynamic$schema))

  # Arrow table of the sparse from-class keys. The inner_join is a hash-join
  # Acero pushes into the scan — the FAST path. A bare
  # filter(cell_id %in% ref_cell_keys) on a multi-million-element vector is the
  # slow path the L2256-2259 comment warns about (Pitfall 1). Do NOT replace
  # this with %in%, and do NOT relax arrow threads.
  #
  # arrow_table() infers int32 from R integers, but the prepped parquet stores
  # cell_id as int64. Acero's hash-join requires identical key types — a bare
  # int32 key table errors (or silently matches nothing), which made every lazy
  # run die at the first transition. Cast the key table to each dataset's own
  # cell_id type so the join keys always match (works for int32 or int64).
  make_key_tbl <- function(ds) {
    arrow::arrow_table(cell_id = ref_cell_keys)$cast(
      arrow::schema(cell_id = ds$schema$GetFieldByName("cell_id")$type)
    )
  }

  static_df <- if (length(static_cols) > 0L) {
    ds_static |>
      dplyr::filter(region == !!region_value) |>
      dplyr::select(cell_id, dplyr::all_of(static_cols)) |>
      dplyr::inner_join(make_key_tbl(ds_static), by = "cell_id") |>
      dplyr::collect() |>
      data.table::as.data.table()
  } else {
    data.table::data.table(cell_id = integer())
  }

  dyn_df <- if (length(dyn_cols) > 0L) {
    ds_dynamic |>
      dplyr::filter(region == !!region_value, scenario == !!scenario) |>
      dplyr::select(cell_id, dplyr::all_of(dyn_cols)) |>
      dplyr::inner_join(make_key_tbl(ds_dynamic), by = "cell_id") |>
      dplyr::collect() |>
      data.table::as.data.table()
  } else {
    data.table::data.table(cell_id = integer())
  }

  if (nrow(static_df) == 0L && nrow(dyn_df) == 0L) {
    return(data.table::data.table(cell_id = integer()))
  }

  result <- if (nrow(static_df) > 0L && nrow(dyn_df) > 0L) {
    merge(static_df, dyn_df, by = "cell_id", all = TRUE)
  } else if (nrow(static_df) > 0L) {
    static_df
  } else {
    dyn_df
  }

  data.table::setkeyv(result, "cell_id")
  log_msg(
    sprintf(
      "    From-class predictor data loaded: %d from-class keys -> %d cells, %d static cols (%s), %d dynamic cols (%s)",
      length(ref_cell_keys),
      nrow(result),
      length(static_cols),
      paste(static_cols, collapse = ", "),
      length(dyn_cols),
      paste(dyn_cols, collapse = ", ")
    ),
    log_file
  )
  result
}


#' Generate probability maps for all transitions in a region
#'
#' Loads fitted tidymodels workflows one at a time, predicts transition
#' probabilities at only the sparse set of cells currently in each transition's
#' "from" class, normalizes across transitions per cell, and saves per-transition
#' probability rasters as TIFs.
#'
#' Memory-saving design (vs. a naive "load all models + wide prob columns"
#' approach):
#'   * One fitted model resident at a time; released after predicting.
#'   * Predictor data loaded only for the subset of cells currently in the
#'     relevant "from" class, and only for the columns that model requires.
#'   * Long-format sparse accumulation for normalization (no wide prob_<to>
#'     columns materialized over the full ~42M-cell region raster).
#'
#' @param work_dir Working directory for this region/timestep
#' @param region_label Region label string
#' @param region_val Region integer value
#' @param scenario Scenario name (e.g. "BAU") — mapped to SSP string via
#'   `config$scenario_to_ssp_mapping` before being passed to the dynamic
#'   predictor parquet.
#' @param year_ant Anterior year
#' @param calibration_period Calibration period string
#' @param anterior_path Path to the anterior LULC raster
#' @param trans_rates_df Data frame of transition rates (From*, To*, Rate)
#' @param config Configuration list
#' @param log_file Path to the log file for this worker/region
#' @return Path to the probability_map_dir
generate_probability_maps <- function(
  work_dir,
  region_label,
  region_val,
  scenario,
  year_ant,
  calibration_period,
  anterior_path,
  trans_rates_df,
  config,
  log_file,
  models_list,
  nhood_paths
) {
  prob_map_dir <- file.path(work_dir, "probability_map_dir")
  ensure_dir(prob_map_dir)

  region_suffix <- gsub(" ", "_", tolower(region_label))

  # Scenario -> SSP for dynamic predictor parquet partition filter
  ssp_name <- config[["scenario_to_ssp_mapping"]][[scenario]]
  if (is.null(ssp_name)) {
    stop(log_msg(
      sprintf(
        "No scenario_to_ssp_mapping entry for scenario '%s'",
        scenario
      ),
      log_file
    ))
  }
  #if year_ant is < 2022 then ssp_name == baseline
  if (year_ant <= 2022) {
    ssp_name <- "baseline"
  }

  if (missing(models_list) || !nrow(models_list)) {
    stop(log_msg(
      sprintf(
        "No preloaded transition models available for region '%s'",
        region_suffix
      ),
      log_file
    ))
  }

  model_info <- merge(
    data.table::copy(models_list),
    trans_rates_df[, c("From*", "To*", "id_trans")],
    by.x = c("from_val", "to_val"),
    by.y = c("From*", "To*"),
    all.x = FALSE,
    sort = FALSE
  )

  # Stage 5 boundary audit: compare rate-table row count to fitted-model row
  # count for this region. Any id_trans present in trans_rates_df but absent
  # from model_info is a hard failure (Plan 03.2-02): a missing model means
  # the probability map for that transition cannot be produced, which would
  # silently truncate Dinamica's input set and drop allocation throughput.
  missing_models <- setdiff(
    trans_rates_df[["id_trans"]],
    model_info[["id_trans"]]
  )
  n_rate_rows  <- nrow(trans_rates_df)
  n_model_rows <- nrow(model_info)
  log_msg(sprintf(
    "AUDIT stage=5 region=%s rate_table_rows=%d model_count=%d missing_model_id_trans=%s",
    region_label,
    n_rate_rows,
    n_model_rows,
    if (length(missing_models) > 0L) paste(missing_models, collapse = ",") else "none"
  ), log_file)
  if (length(missing_models) > 0L) {
    stop(log_msg(
      sprintf(
        "generate_probability_maps(): %d id_trans value(s) present in rate table but no fitted model found: %s. Run transition_modelling() to produce missing models or remove the transition from viable_transitions_lists.csv.",
        length(missing_models),
        paste(missing_models, collapse = ", ")
      ),
      log_file
    ))
  }

  # Load the anterior LULC raster for this region
  anterior <- terra::rast(anterior_path)

  # Lightweight sparse index of all non-NA cells: cell_id, x, y, lulc_class,
  # ref_cell_id. This is the only full-extent table held.
  anterior_dt <- data.table::setDT(
    terra::as.data.frame(anterior, cells = TRUE, xy = TRUE, na.rm = TRUE)
  )
  data.table::setnames(
    anterior_dt,
    old = seq_along(anterior_dt),
    new = c("cell_id", "x", "y", "lulc_class")
  )

  if (nrow(anterior_dt) == 0L) {
    warning(log_msg(
      sprintf(
        "No valid cells in anterior raster for region %s",
        region_label
      ),
      log_file
    ))
    return(prob_map_dir)
  }

  # Attach ref_cell_id (national grid) now, once — needed for any model that
  # references parquet predictors. Cost is negligible vs. per-transition
  # recomputation.
  ref_grid <- terra::rast(config[["ref_grid_path"]])
  anterior_dt[,
    ref_cell_id := terra::cellFromXY(ref_grid, cbind(x, y))
  ]
  data.table::setkey(anterior_dt, cell_id)

  # Open parquet datasets lazily (no data pulled until filtered + collected)
  static_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "static"
  )

  dynamic_preds_pq_path <- file.path(
    config[["predictors_prepped_dir"]],
    "parquet_data",
    "dynamic",
    year_ant
  )
  ds_static <- arrow::open_dataset(
    static_preds_pq_path,
    format = "parquet",
    partitioning = arrow::hive_partition(region = arrow::int32())
  )
  ds_dynamic <- arrow::open_dataset(
    dynamic_preds_pq_path,
    format = "parquet",
    partitioning = arrow::hive_partition(
      scenario = arrow::utf8(),
      region = arrow::int32()
    )
  )

  log_msg(
    "    Prepared for probability map generation: preloaded models, anterior cell index, TIF-backed neighbourhood paths and Parquet datasets ready.",
    log_file
  )

  # Predictor read path: lazy per-from-class (default, Phase 3.5 Goal 1) vs.
  # eager full-region preload (escape hatch ALLOCATION_PREDICTOR_LAZY=0). The
  # lazy path bounds per-region predictor RAM by the largest single from-class
  # table instead of the full region table; the eager path is preserved
  # byte-for-byte for bisecting risk A3.
  predictor_lazy <- get_allocation_predictor_lazy()
  log_msg(
    sprintf(
      "    Predictor read path: %s (ALLOCATION_PREDICTOR_LAZY=%s)",
      if (predictor_lazy) "lazy per-from-class" else "eager full-region preload",
      if (predictor_lazy) "1/true (default)" else "0/false"
    ),
    log_file
  )

  if (predictor_lazy) {
    # Lazy path: defer reads to the per-transition loop, cached per from_val so
    # each distinct from-class is scanned at most once. Precompute the
    # column-union each from-class needs (superset over its transitions, minus
    # neighbourhood predictors which are read separately) so the cached read
    # projects exactly those columns. region_pred_dt is populated from the cache
    # inside the loop (RESEARCH Pattern 2).
    col_union_by_from <- split(
      model_info$predictor_names,
      model_info$from_val
    )
    col_union_by_from <- lapply(col_union_by_from, function(lst) {
      flat <- unique(unlist(lst, use.names = FALSE))
      setdiff(flat, grep("_nhood_", flat, value = TRUE))
    })
    from_class_cache <- new.env(parent = emptyenv())
    region_pred_dt <- NULL
  } else {
    # Eager path (escape hatch): pre-load ALL parquet predictor columns for the
    # region in one Arrow scan. This replaces per-transition cell_id %in% Arrow
    # filters (which are very slow for large cell_id vectors) with a single
    # region-partition read followed by fast in-memory data.table keyed lookups
    # per transition.
    all_preds_flat <- unique(unlist(model_info$predictor_names, use.names = FALSE))
    all_parquet_preds <- setdiff(all_preds_flat, grep("_nhood_", all_preds_flat, value = TRUE))
    t_pred_preload <- prof_tic()
    region_pred_dt <- preload_region_predictor_data(
      ds_static = ds_static,
      ds_dynamic = ds_dynamic,
      preds = all_parquet_preds,
      region_value = region_val,
      scenario = ssp_name,
      log_file = log_file
    )
    prof_toc(
      t_pred_preload,
      sprintf("region=%s stage=predictor_preload", region_suffix),
      log_file
    )
  }

  # Map (from_val, to_val) pairs to their trans_rates.csv row index so we can
  # write TIFs with the row-number prefix that Dinamica's probability-map
  # cube expects.
  trans_rates_dt <- data.table::as.data.table(trans_rates_df)
  trans_rates_dt[, row_idx := seq_len(.N)]

  # Per-transition: load model -> predict at sparse from-class cells -> release
  log_msg("    Predicting transition probabilities...", log_file)
  gather <- vector("list", nrow(model_info))
  for (j in seq_len(nrow(model_info))) {
    mi <- model_info[j]
    trans_name <- mi$trans_name
    from_val <- mi$from_val
    to_val <- mi$to_val
    trans_tag <- sprintf(
      "region=%s stage=trans_total from=%d to=%d",
      region_suffix,
      from_val,
      to_val
    )
    t_trans_total <- prof_tic()
    # Update breadcrumb state so a sentinel emitted on SIGKILL records which
    # transition the worker was on (D-06, OBS-02).
    if (exists("worker_state_set", mode = "function", inherits = TRUE)) {
      try(worker_state_set(stage = "predict", transition = trans_name),
          silent = TRUE)
    }
    log_msg(
      sprintf(
        "      Transition %d -> %d: predicting with model '%s'",
        from_val,
        to_val,
        basename(mi$file_path)
      ),
      log_file
    )

    if (is.na(from_val) || is.na(to_val)) {
      warning(log_msg(
        sprintf(
          "Could not map transition '%s' to class values, skipping",
          trans_name
        ),
        log_file
      ))
      prof_toc(t_trans_total, trans_tag, log_file)
      next
    }

    # trans_rates.csv row index (required for TIF filename prefix)
    row_idx_row <- trans_rates_dt[
      `From*` == from_val & `To*` == to_val,
      row_idx
    ]
    if (length(row_idx_row) == 0L) {
      # Not in trans_rates (e.g. zero-rate or not in this scenario) - skip
      prof_toc(t_trans_total, trans_tag, log_file)
      next
    }
    row_idx <- row_idx_row[[1L]]

    # Sparse set of cells currently in this "from" class
    from_idx <- anterior_dt[lulc_class == from_val]
    if (nrow(from_idx) == 0L) {
      prof_toc(t_trans_total, trans_tag, log_file)
      next
    }

    # Lazy path: read this from-class's predictor columns once, cached by
    # from_val, scoped to its sparse cell keys (RESEARCH Pattern 2). region_pred_dt
    # becomes the from-class-sized cached table; the keyed-join block below
    # operates against it unchanged (Pitfall 4). Each distinct from_val triggers
    # exactly one load_from_class_predictor_data() call. Profiled under the
    # predictor_load stage so the lazy read time is attributed.
    if (predictor_lazy) {
      cache_key <- as.character(from_val)
      if (is.null(from_class_cache[[cache_key]])) {
        t_fromclass_load <- prof_tic()
        from_class_cache[[cache_key]] <- load_from_class_predictor_data(
          ds_static = ds_static,
          ds_dynamic = ds_dynamic,
          cols = col_union_by_from[[cache_key]],
          region_value = region_val,
          scenario = ssp_name,
          ref_cell_keys = from_idx$ref_cell_id,
          log_file = log_file
        )
        prof_toc(
          t_fromclass_load,
          sprintf(
            "region=%s stage=predictor_load from=%d to=%d",
            region_suffix,
            from_val,
            to_val
          ),
          log_file
        )
      }
      region_pred_dt <- from_class_cache[[cache_key]]
    }

    t_model_load <- prof_tic()
    fitted_wf <- mi$model_obj[[1L]]
    prof_toc(
      t_model_load,
      sprintf(
        "region=%s stage=model_load from=%d to=%d",
        region_suffix,
        from_val,
        to_val
      ),
      log_file
    )
    preds_needed <- mi$predictor_names[[1L]]
    nhood_needed <- grep("_nhood_", preds_needed, value = TRUE)
    parquet_needed <- setdiff(preds_needed, nhood_needed)

    # Start from_data with the sparse index (copy so we can augment with
    # predictors without mutating anterior_dt).
    from_data <- data.table::copy(from_idx)
    pred_data <- NULL

    if (length(parquet_needed) > 0L) {
      t_predictor_load <- prof_tic()
      avail_cols <- intersect(parquet_needed, setdiff(names(region_pred_dt), "cell_id"))
      if (length(avail_cols) > 0L && nrow(region_pred_dt) > 0L) {
        # Fast in-memory keyed lookup: O(n log N) vs. repeated Arrow scans
        lookup_keys <- from_data$ref_cell_id
        pred_data <- region_pred_dt[J(lookup_keys), nomatch = NA]
        keep_cols <- c("cell_id", avail_cols)
        pred_data <- pred_data[, ..keep_cols]
        data.table::setnames(pred_data, "cell_id", "ref_cell_id")
        from_data <- pred_data[from_data, on = "ref_cell_id"]
      }
      prof_toc(
        t_predictor_load,
        sprintf(
          "region=%s stage=predictor_load from=%d to=%d",
          region_suffix,
          from_val,
          to_val
        ),
        log_file
      )
    }

    if (length(nhood_needed) > 0L) {
      missing_nhood <- setdiff(nhood_needed, names(nhood_paths))
      if (length(missing_nhood) > 0L) {
        stop(log_msg(
          sprintf(
            "Missing precomputed neighbourhood path(s) for region '%s': %s",
            region_label,
            paste(missing_nhood, collapse = ", ")
          ),
          log_file
        ))
      }
      log_msg(
        sprintf(
          "        Reopening neighbourhood rasters from TIF paths for predictors: %s",
          paste(nhood_needed, collapse = ", ")
        ),
        log_file
      )
      t_nhood_extract <- prof_tic()
      nhood_stack <- terra::rast(nhood_paths[nhood_needed])
      nhood_vals <- terra::extract(
        nhood_stack,
        as.matrix(from_data[, .(x, y)])
      )
      nhood_vals <- data.table::as.data.table(nhood_vals)
      if ("ID" %in% names(nhood_vals)) {
        nhood_vals[, ID := NULL]
      }
      nhood_vals[, (nhood_needed) := lapply(.SD, as.integer), .SDcols = nhood_needed]
      from_data[, (nhood_needed) := nhood_vals[, nhood_needed, with = FALSE]]
      prof_toc(
        t_nhood_extract,
        sprintf(
          "region=%s stage=nhood_extract from=%d to=%d",
          region_suffix,
          from_val,
          to_val
        ),
        log_file
      )
    }

    # Predict, then release the model.
    t_predict <- prof_tic()
    pred_result <- predict_saved_transition_prob(
      fitted_wf,
      from_data,
      log_file = log_file
    )
    prof_toc(
      t_predict,
      sprintf(
        "region=%s stage=predict from=%d to=%d",
        region_suffix,
        from_val,
        to_val
      ),
      log_file
    )
    prob_values <- pred_result[[2L]]
    prob_values[is.na(prob_values)] <- 0
    prob_values <- pmax(0, pmin(1, prob_values))

    log_msg(
      "Appending predictions to gather table for normalization...",
      log_file
    )
    gather[[j]] <- data.table::data.table(
      row_idx = row_idx,
      from_val = from_val,
      to_val = to_val,
      cell_id = from_data$cell_id,
      x = from_data$x,
      y = from_data$y,
      prob = prob_values
    )

    rm(fitted_wf, pred_result, from_data, pred_data)
    gc(verbose = FALSE)
    prof_toc(t_trans_total, trans_tag, log_file)
  }

  # Normalize across transitions per cell (long-format, sparse)
  log_msg("    Normalizing probabilities...", log_file)
  normalized <- data.table::rbindlist(gather, use.names = TRUE, fill = TRUE)
  if (nrow(normalized) == 0L) {
    log_msg("    No predictions produced; skipping map writes", log_file)
    return(prob_map_dir)
  }
  normalized[, tot_prob := sum(prob), by = cell_id]
  normalized[tot_prob > 1, prob := prob / tot_prob]
  normalized[, tot_prob := NULL]
  data.table::setkey(normalized, row_idx)

  #todo integrate more recent approach to spatial intervention from NCCS project
  # (placeholder retained from previous implementation)

  # Write one TIF per trans_rates row, preserving the numeric prefix required
  # by Dinamica's CreateCubeOfProbabilityMaps submodel.
  # Prefixing with 001, 002... so these files are sorted the same as the
  # transition, expansion, and patcher tables on all sorts of filesystems.
  log_msg("    Saving probability maps...", log_file)
  t_write_total <- prof_tic()
  for (k in seq_len(nrow(trans_rates_dt))) {
    from_val <- trans_rates_dt[["From*"]][k]
    to_val <- trans_rates_dt[["To*"]][k]
    rate <- trans_rates_dt[["Rate"]][k]
    id_trans <- trans_rates_dt[["id_trans"]][k]

    # Persistence (from_val == to_val) and zero-rate rows are pre-filtered
    # upstream by setup_allocation_inputs() (Phase 3.3 D-02/D-03), so no skip
    # is needed here. The %03d prefix is now gap-free by construction.
    dt_j <- normalized[.(k), nomatch = NULL]
    if (nrow(dt_j) == 0L) {
      # Active transition with no predictions — still write an all-NA TIF at
      # the correct %03d prefix so the alphabetic sequence Dinamica binds to
      # the trans_rates rows stays gap-free (Phase 3.3 Pitfall 2). Dinamica
      # reads the cube and interprets the all-NA layer as zero probability for
      # that transition — no allocation happens for that row, but the row
      # position remains valid and the alignment contract holds.
      tif_path <- file.path(
        prob_map_dir,
        sprintf("%03d_id_trans_%d.tif", k, id_trans)
      )
      r <- terra::setValues(anterior, NA_real_)
      terra::writeRaster(
        r,
        filename = tif_path,
        overwrite = TRUE,
        NAflag = -999
      )
      log_msg(
        sprintf(
          "WARN id_trans=%d row=%d has no predictions; wrote empty TIF",
          id_trans,
          k
        ),
        log_file
      )
      next
    }

    tif_path <- file.path(
      prob_map_dir,
      sprintf("%03d_id_trans_%d.tif", k, id_trans)
    )

    t_raster_write <- prof_tic()
    r <- terra::setValues(anterior, NA_real_)
    r[dt_j[["cell_id"]]] <- dt_j[["prob"]]
    terra::writeRaster(
      r,
      filename = tif_path,
      overwrite = TRUE,
      NAflag = -999
    )
    prof_toc(
      t_raster_write,
      sprintf(
        "region=%s stage=raster_write from=%d to=%d",
        region_suffix,
        from_val,
        to_val
      ),
      log_file
    )
    log_msg(
      sprintf(
        "      Transition %d -> %d (id_trans=%d): %d cells, mean prob=%.4f, saved to %s",
        from_val,
        to_val,
        id_trans,
        nrow(dt_j),
        mean(dt_j[["prob"]]),
        tif_path
      ),
      log_file
    )
  }
  prof_toc(
    t_write_total,
    sprintf("region=%s stage=write_total", region_suffix),
    log_file
  )

  log_msg(sprintf("    Probability maps saved to: %s", prob_map_dir), log_file)
  return(prob_map_dir)
}


#' Compute a single neighbourhood predictor SpatRaster
#'
#' Parses `{class_name}_nhood_{matrix_id}`, applies the named focal matrix to a
#' binary mask of the target class, and returns the resulting SpatRaster.
#' Caller is responsible for caching (this function does no caching itself).
#'
#' @param anterior LULC raster for the current region
#' @param pred_name Neighbourhood predictor name — `{class_name}_nhood_{matrix_id}`
#' @param focal_matrices Named list of focal weight matrices
#'   (matrix_id -> weight matrix)
#' @param class_name_to_value Named integer vector mapping class name -> LULC
#'   class value
#' @return SpatRaster of the focal output (layer name == `pred_name`)
compute_single_nhood_raster <- function(
  anterior,
  pred_name,
  focal_matrices,
  class_name_to_value
) {
  parts <- stringr::str_match(pred_name, "^(.+)_nhood_(.+)$")
  if (is.na(parts[1, 1])) {
    stop(sprintf("Cannot parse nhood predictor name: %s", pred_name))
  }
  class_name <- parts[1, 2]
  matrix_id <- parts[1, 3]

  class_val <- class_name_to_value[class_name]
  if (is.na(class_val)) {
    stop(sprintf(
      "Unknown class '%s' in nhood predictor: %s",
      class_name,
      pred_name
    ))
  }
  if (!matrix_id %in% names(focal_matrices)) {
    stop(sprintf(
      "Unknown matrix '%s' in nhood predictor: %s",
      matrix_id,
      pred_name
    ))
  }

  class_raster <- anterior == class_val
  focal_layer <- terra::focal(
    x = class_raster,
    w = focal_matrices[[matrix_id]],
    na.rm = FALSE,
    expand = TRUE,
    fillvalue = 0
  )
  names(focal_layer) <- pred_name
  focal_layer
}

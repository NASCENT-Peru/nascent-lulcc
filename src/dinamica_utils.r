#' Dinamica EGO Utility Functions
#'
#' Helpers for executing Dinamica EGO models from R.
#' Adapted from evoland-plus R/util_dinamica.R
#'
#' @author Ben Black

# ---------------------------------------------------------------------------
# Phase 1 Plan 04 Task 1 — Unified Dinamica launch contract (D-09, D-10, D-11)
# ---------------------------------------------------------------------------
# `exec_dinamica()` is the only caller-facing Dinamica entrypoint. Backend
# selection (local direct DinamicaConsole vs. HPC apptainer/singularity exec
# against the external `.sif` artifact named by DINAMICA_EGO_8_HOME) happens
# entirely inside `resolve_dinamica_launch()`. No caller outside this file
# should ever know about container details (D-09).
#
# HPC contract (INFRA-01):
#   - On HPC, DINAMICA_EGO_8_HOME is the absolute path to the external
#     Dinamica `.sif` image. The image is treated as an external artifact
#     (D-10) and is consumed verbatim as the image argument to
#     `apptainer exec`/`singularity exec`. No "SIF or wrapper" ambiguity.
#   - The runtime is probed in the order `apptainer` -> `singularity`. Tests
#     and dry-run callers pass `runtime_override` and `probe_runtime = FALSE`
#     so HPC command resolution can be proven on a workstation that has
#     neither runtime installed.
#
# Local contract:
#   - DINAMICA_EGO_8_HOME points at the install directory. `DinamicaConsole`
#     is invoked directly via `processx::run()`. LD_LIBRARY_PATH is set to
#     `<DINAMICA_EGO_8_HOME>/usr/lib` for the child process.
#
# Logging contract (D-07, partial PIPE-07 closure):
#   - Subprocess logs go to `<repo_root>/logs/<timestamp>_dinamica.log` via
#     `resolve_dinamica_log_path()`. The legacy "log next to the model file"
#     placement is gone for both backends.

# Internal: resolved valid backends and runtimes.
.DINAMICA_BACKENDS <- c("auto", "local", "hpc")
.DINAMICA_RUNTIMES_HPC <- c("apptainer", "singularity")

#' Detect which Dinamica backend should be used (D-09)
#'
#' Honours an explicit DINAMICA_BACKEND override; otherwise uses
#' `detect_environment()` from `src/setup.r` to pick "local" or "hpc".
#'
#' @return One of "local" or "hpc".
detect_dinamica_backend <- function() {
  override <- Sys.getenv("DINAMICA_BACKEND", unset = "auto")
  if (!override %in% .DINAMICA_BACKENDS) {
    stop(
      "DINAMICA_BACKEND must be one of: ",
      paste(.DINAMICA_BACKENDS, collapse = ", "),
      "; got '", override, "'.",
      call. = FALSE
    )
  }
  if (identical(override, "local") || identical(override, "hpc")) {
    return(override)
  }
  # auto -> derive from runtime environment
  env <- tryCatch(detect_environment(), error = function(e) "local")
  if (identical(env, "hpc")) "hpc" else "local"
}

#' Probe for an HPC container runtime in the order apptainer -> singularity
#'
#' @return The runtime name ("apptainer" or "singularity") found on PATH.
#' @note Stops with a clear error if neither is available.
.probe_hpc_runtime <- function() {
  for (cmd in .DINAMICA_RUNTIMES_HPC) {
    if (nzchar(Sys.which(cmd))) {
      return(cmd)
    }
  }
  stop(
    "No HPC container runtime found on PATH. ",
    "Tried: ", paste(.DINAMICA_RUNTIMES_HPC, collapse = ", "), ". ",
    "Install apptainer (preferred) or singularity, or override with the ",
    "`runtime_override` argument.",
    call. = FALSE
  )
}

#' Resolve the full Dinamica launch contract without executing anything
#'
#' This is the single source of truth for the local/HPC backend decision,
#' the runtime command, the resolved Dinamica artifact path, the full
#' argument vector, and the central log file path. Both `exec_dinamica()`
#' and the operator-facing smoke test consume this helper instead of
#' deriving their own launch contract.
#'
#' On HPC, `DINAMICA_EGO_8_HOME` is treated as the absolute path to the
#' external `.sif` image (INFRA-01); the resolved args take the apptainer
#' launch shape (D-104):
#' `c("exec", "--home", <staged-home>, "--env", "DINAMICA_EGO_8_TEMP_DIR=<staged-tmp>",`
#' ` <sif_path>, "bash", "-c", "cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model> [flags]")`.
#' The staged home + tmp directories live under `$HPC_SCRATCH_ROOT/dinamica-home`
#' and `$HPC_SCRATCH_ROOT/dinamica-tmp` and are seeded idempotently (D-105).
#' The model path interpolated into the bash -c payload is always absolute
#' via `normalizePath()` (D-106).
#'
#' On local, `DINAMICA_EGO_8_HOME` is the Dinamica install directory; the
#' resolved args are simply the DinamicaConsole flags + model path.
#'
#' @param model_path Path to the `.ego` (or `.ego-decoded`) model file.
#' @param backend One of "auto" (resolve from env), "local", or "hpc".
#' @param disable_parallel Whether to add `-disable-parallel-steps`.
#' @param disable_native_compilation Whether to add `-disable-native-expressions`.
#'   Defaults to TRUE on HPC — the Enhancement Plugin JIT compiler is not
#'   installed in the container, so every step emits a warning without this flag.
#' @param n_processors Integer or NULL. Passes `-processors=N` to Dinamica to
#'   override the auto-detected core count. On HPC, defaults to the value of the
#'   `SLURM_CPUS_PER_TASK` env var so Dinamica's thread pool matches the SLURM
#'   allocation. Pass 0 to let Dinamica use all detected cores.
#' @param log_level Optional `-log-level N` flag.
#' @param runtime_override Optional explicit runtime name. If supplied for
#'   the HPC backend, skips the live PATH probe (use this for verification
#'   on hosts where neither apptainer nor singularity is installed).
#' @param probe_runtime When TRUE (default) and the HPC backend is selected
#'   without a `runtime_override`, probe `apptainer` then `singularity` on
#'   PATH. When FALSE, refuse to probe and require `runtime_override`.
#' @param work_dir Optional working directory used to anchor the central log
#'   path; falls back to `dirname(model_path)`.
#' @return Named list with elements `backend`, `runtime`, `artifact_path`,
#'   `command`, `args`, `log_file`, `env`. `command` is the executable name
#'   to pass to `processx::run()`; `args` is the corresponding argv vector.
#' @note Phase 1.1 — D-104/D-105/D-106. MUST stay in sync with the
#'   `scripts/smoke_test_dinamica.sh` LAUNCH_CMD array.
#' @export
resolve_dinamica_launch <- function(
  model_path,
  backend = "auto",
  disable_parallel = TRUE,
  disable_native_compilation = NULL,
  n_processors = NULL,
  log_level = NULL,
  runtime_override = NULL,
  probe_runtime = TRUE,
  work_dir = NULL
) {
  if (!is.character(model_path) || length(model_path) != 1L || !nzchar(model_path)) {
    stop("resolve_dinamica_launch(): model_path must be a non-empty string.",
         call. = FALSE)
  }
  if (!backend %in% .DINAMICA_BACKENDS) {
    stop(
      "resolve_dinamica_launch(): backend must be one of: ",
      paste(.DINAMICA_BACKENDS, collapse = ", "),
      "; got '", backend, "'.",
      call. = FALSE
    )
  }

  if (identical(backend, "auto")) {
    backend <- detect_dinamica_backend()
  }

  dinamica_home <- Sys.getenv("DINAMICA_EGO_8_HOME", unset = "")
  if (!nzchar(dinamica_home)) {
    stop(
      "DINAMICA_EGO_8_HOME is not set. On HPC this must be the absolute path ",
      "to the external Dinamica .sif image; on local it must be the Dinamica ",
      "install directory. See .env.template and docs/README_HPC.md.",
      call. = FALSE
    )
  }

  # On HPC the Enhancement Plugin JIT compiler is not installed in the container;
  # default TRUE to suppress the per-step warning with the correct flag.
  if (is.null(disable_native_compilation)) {
    disable_native_compilation <- identical(backend, "hpc") ||
      (identical(backend, "auto") && identical(detect_dinamica_backend(), "hpc"))
  }

  # On HPC default to SLURM_CPUS_PER_TASK so Dinamica's thread pool matches
  # the SLURM allocation (avoids oversubscription when cpuset is not enforced).
  if (is.null(n_processors)) {
    slurm_cpus <- suppressWarnings(
      as.integer(Sys.getenv("SLURM_CPUS_PER_TASK", unset = ""))
    )
    if (!is.na(slurm_cpus) && slurm_cpus > 0L) {
      n_processors <- slurm_cpus
    }
  }

  # Pass-through Dinamica flags + model path.
  console_args <- character()
  if (isTRUE(disable_parallel)) {
    console_args <- c(console_args, "-disable-parallel-steps")
  }
  if (isTRUE(disable_native_compilation)) {
    console_args <- c(console_args, "-disable-native-expressions")
  }
  if (!is.null(n_processors)) {
    console_args <- c(console_args, paste0("-processors=", as.integer(n_processors)))
  }
  if (!is.null(log_level)) {
    console_args <- c(console_args, "-log-level", as.character(log_level))
  }
  console_args <- c(console_args, model_path)

  base_dir <- if (!is.null(work_dir) && nzchar(work_dir)) work_dir else dirname(model_path)
  log_file <- resolve_dinamica_log_path(base_dir)

  if (identical(backend, "hpc")) {
    # ---- Phase 1.1 — D-104, D-105, D-106 ----
    # Apptainer launch shape:
    #   apptainer exec --home <staged-home> \
    #                  --env DINAMICA_EGO_8_TEMP_DIR=<staged-tmp> \
    #                  <sif> bash -c \
    #                  'cd /opt/dinamica/usr && bin/DinamicaEGO.sh <abs-model> [flags]'
    # Direct `apptainer exec <sif> DinamicaConsole <model>` is unsupported by
    # the upstream image and produces silent std::exception failures; the
    # `bin/DinamicaEGO.sh` launcher is the only entrypoint that sets the
    # env vars and relative paths the binary needs.
    # MUST stay in sync with scripts/smoke_test_dinamica.sh LAUNCH_CMD array.

    # On HPC the env var is the .sif image path, consumed directly as the
    # container image argument to apptainer/singularity exec (INFRA-01).
    artifact_path <- dinamica_home

    if (!is.null(runtime_override)) {
      if (!runtime_override %in% .DINAMICA_RUNTIMES_HPC) {
        stop(
          "runtime_override must be one of: ",
          paste(.DINAMICA_RUNTIMES_HPC, collapse = ", "),
          "; got '", runtime_override, "'.",
          call. = FALSE
        )
      }
      runtime <- runtime_override
    } else if (isTRUE(probe_runtime)) {
      runtime <- .probe_hpc_runtime()
    } else {
      stop(
        "resolve_dinamica_launch(): probe_runtime=FALSE requires an explicit ",
        "runtime_override on the HPC backend.",
        call. = FALSE
      )
    }

    # D-105 — staged-home + staged-tmp under HPC_SCRATCH_ROOT.
    # Fail fast if unset so we never silently fall back to $HOME (out-of-quota).
    hpc_scratch_root <- Sys.getenv("HPC_SCRATCH_ROOT", unset = "")
    if (!nzchar(hpc_scratch_root)) {
      stop(
        "HPC_SCRATCH_ROOT is not set. Phase 1.1 D-105 requires HPC_SCRATCH_ROOT ",
        "for the apptainer --home and --env DINAMICA_EGO_8_TEMP_DIR staging. ",
        "Source the project .env or export HPC_SCRATCH_ROOT=/cluster/scratch/$USER/nascent-lulcc.",
        call. = FALSE
      )
    }
    staged_home <- file.path(hpc_scratch_root, "dinamica-home")
    staged_tmp  <- file.path(hpc_scratch_root, "dinamica-tmp")
    dir.create(staged_home, recursive = TRUE, showWarnings = FALSE)
    dir.create(staged_tmp,  recursive = TRUE, showWarnings = FALSE)

    # Seed .dinamica_ego_8.conf only if missing (idempotent — see Pitfall 3
    # in 01.1-RESEARCH.md). Atomic write via tempfile + file.rename() so
    # concurrent workers see either the full file or no file at all.
    conf <- file.path(staged_home, ".dinamica_ego_8.conf")
    if (!file.exists(conf)) {
      tmp_conf <- tempfile(tmpdir = staged_home, fileext = ".conf.tmp")
      writeLines(
        c(
          'AlternativePathForR = "/usr/local/bin/Rscript"',
          'ClConfig = "0"',
          'MemoryAllocationPolicy = "1"',
          'RCranMirror = "https://cloud.r-project.org/"',
          'GdalToolsData = "/opt/dinamica/usr/bin/Data/GDAL"'
        ),
        tmp_conf
      )
      file.rename(tmp_conf, conf)
    }
    # Dinamica requires the system conf to exist even if empty (Plan 07 finding).
    system_conf <- file.path(staged_home, ".dinamica_ego_8_system.conf")
    if (!file.exists(system_conf)) file.create(system_conf)

    # D-106 — model path interpolated into the bash -c payload must be
    # absolute (the launcher's relative-path branch is fragile under `cd`).
    abs_model <- normalizePath(model_path, winslash = "/", mustWork = FALSE)

    # D-104 — bash -c payload. `bin/DinamicaEGO.sh` is a thin wrapper around
    # DinamicaConsole and DOES forward DinamicaConsole flags; preserve
    # `console_args` (without the trailing relative model_path), substitute
    # in the absolute model path, and shQuote() it to neutralise shell
    # metacharacters in user-supplied paths (T-01.1-03).
    flags <- character()
    if (isTRUE(disable_parallel)) {
      flags <- c(flags, "-disable-parallel-steps")
    }
    if (isTRUE(disable_native_compilation)) {
      flags <- c(flags, "-disable-native-expressions")
    }
    if (!is.null(n_processors)) {
      flags <- c(flags, paste0("-processors=", as.integer(n_processors)))
    }
    if (!is.null(log_level)) {
      flags <- c(flags, "-log-level", as.character(log_level))
    }
    launcher_argline <- paste(c(flags, shQuote(abs_model)), collapse = " ")
    bash_payload <- sprintf(
      "cd /opt/dinamica/usr && bin/DinamicaEGO.sh %s",
      launcher_argline
    )

    container_args <- c(
      "exec",
      "--home", staged_home,
      "--bind", paste0(hpc_scratch_root, ":", hpc_scratch_root),
      "--env", paste0("DINAMICA_EGO_8_TEMP_DIR=", staged_tmp),
      artifact_path,
      "bash", "-c", bash_payload
    )
    env_vec <- c("current")  # apptainer/singularity inherit env by default
    return(list(
      backend       = "hpc",
      runtime       = runtime,
      artifact_path = artifact_path,
      command       = runtime,
      args          = container_args,
      log_file      = log_file,
      env           = env_vec
    ))
  }

  # Local backend: direct DinamicaConsole invocation.
  artifact_path <- dinamica_home
  ld_lib <- file.path(dinamica_home, "usr", "lib")
  env_vec <- c(
    "current",
    DINAMICA_HOME   = dirname(model_path),
    LD_LIBRARY_PATH = ld_lib
  )
  list(
    backend       = "local",
    runtime       = "DinamicaConsole",
    artifact_path = artifact_path,
    command       = "DinamicaConsole",
    args          = console_args,
    log_file      = log_file,
    env           = env_vec
  )
}

# ---- Phase 1.1 — D-107, D-108 ----
# Post-hoc error-string detection: `DinamicaConsole` / `bin/DinamicaEGO.sh`
# return exit 0 on `std::exception` and on hard parse failures
# (`"terminate called after throwing an instance of 'DFF::Exception'"`).
# Grep the captured log (the SAME artifact `resolve_dinamica_log_path()`
# returns; D-108) for the three canonical error strings and `stop()` on
# any match, regardless of subprocess exit code. This is the only way the
# OBS-02 / Phase 1 SC2 contract ("operator can diagnose any allocation
# failure within minutes") can hold.
# MUST stay in sync with `scripts/smoke_test_dinamica.sh` grep pattern set.
DINAMICA_ERROR_PATTERNS <- c(
  "Dinamica EGO exited with an error",
  "terminate called after throwing",
  "std::exception"
)

#' Inspect a Dinamica subprocess result for post-hoc error strings
#'
#' Internal helper. Reads the captured log (D-108: the SAME teed log file
#' `exec_dinamica()` writes when `write_logfile = TRUE`; falls back to
#' `res$stdout + res$stderr` when `write_logfile = FALSE`) and stops with
#' a clear message if any of the three D-107 patterns match OR if the
#' subprocess exit code is non-zero.
#'
#' @param res A processx::run() result list with `status`, `stdout`, `stderr`.
#' @param write_logfile Logical; whether `exec_dinamica()` was called with
#'   `write_logfile = TRUE` (in which case `logfile_path` is read).
#' @param logfile_path Path to the teed Dinamica log file (or `NULL` when
#'   `write_logfile = FALSE`).
#' @return `invisible(res)` on success; raises `stop()` on any error signal.
#' @keywords internal
.check_dinamica_post_run <- function(res, write_logfile, logfile_path) {
  log_contents <- if (isTRUE(write_logfile) &&
                      !is.null(logfile_path) &&
                      file.exists(logfile_path)) {
    paste(readLines(logfile_path, warn = FALSE), collapse = "\n")
  } else {
    paste(c(res[["stdout"]], res[["stderr"]]), collapse = "\n")
  }

  error_hits <- vapply(
    DINAMICA_ERROR_PATTERNS,
    function(p) grepl(p, log_contents, fixed = TRUE),
    logical(1)
  )

  if (res[["status"]] != 0L || any(error_hits)) {
    matched <- DINAMICA_ERROR_PATTERNS[error_hits]
    stop(
      "Dinamica registered an error",
      if (length(matched)) sprintf(" (matched: %s)", paste(matched, collapse = "; ")) else "",
      if (res[["status"]] != 0L) sprintf(" [exit %d]", res[["status"]]) else "",
      ".\nRerun with echo = TRUE or check logfile: ",
      if (!is.null(logfile_path)) logfile_path else "(no logfile — write_logfile=FALSE)",
      call. = FALSE
    )
  }
  invisible(res)
}

#' Execute a Dinamica .ego file using DinamicaConsole
#'
#' Single caller-facing entrypoint for both local and HPC Dinamica launches.
#' Backend selection happens entirely inside `resolve_dinamica_launch()`
#' (D-09); subprocess logs land under the central `logs/` surface (D-07).
#'
#' @param model_path Path to the .ego model file
#' @param disable_parallel Whether to disable parallel steps (default TRUE)
#' @param log_level Logging level (1-7, default NULL)
#' @param write_logfile bool, write stdout & stderr to a central log file?
#' @param echo bool, direct echo to console?
#' @param backend One of "auto" (default), "local", or "hpc".
#' @param runtime_override Optional runtime override forwarded to
#'   `resolve_dinamica_launch()`. Use only for verification.
#' @param work_dir Optional working directory to anchor central log path.
#' @return invisible processx result
#' @export
exec_dinamica <- function(
  model_path,
  disable_parallel = TRUE,
  disable_native_compilation = NULL,
  n_processors = NULL,
  log_level = NULL,
  write_logfile = TRUE,
  echo = FALSE,
  backend = "auto",
  runtime_override = NULL,
  work_dir = NULL
) {
  launch <- resolve_dinamica_launch(
    model_path                 = model_path,
    backend                    = backend,
    disable_parallel           = disable_parallel,
    disable_native_compilation = disable_native_compilation,
    n_processors               = n_processors,
    log_level                  = log_level,
    runtime_override           = runtime_override,
    probe_runtime              = TRUE,
    work_dir                   = work_dir
  )

  # Pre-flight: verify the actual command exists on PATH before launching.
  if (Sys.which(launch$command) == "") {
    stop(
      sprintf(
        "Dinamica launch command '%s' not found on PATH for backend '%s'. ",
        launch$command, launch$backend
      ),
      if (identical(launch$backend, "hpc"))
        "Install apptainer or singularity on this host."
      else
        "Install Dinamica EGO and ensure DinamicaConsole is on PATH.",
      call. = FALSE
    )
  }

  if (isTRUE(write_logfile)) {
    logfile_path <- launch$log_file
    dir.create(dirname(logfile_path), recursive = TRUE, showWarnings = FALSE)
    message("Logging Dinamica subprocess to ", logfile_path)

    # Run command + args under stdbuf -oL through bash so we can both stream
    # to console and tee to the central log without losing the child exit
    # code. shQuote() preserves arguments containing spaces/paths.
    quoted <- paste(shQuote(c(launch$command, launch$args)), collapse = " ")
    bash_script <- sprintf(
      paste(
        "set -o pipefail;",
        "stdbuf -oL %s 2>&1 |",
        "sed 's/\\x1b\\[[0-9;]*m//g' |",
        "tee '%s';",
        "exit ${PIPESTATUS[0]}"
      ),
      quoted,
      logfile_path
    )
    res <- processx::run(
      command         = "bash",
      args            = c("-c", bash_script),
      error_on_status = FALSE,
      echo            = echo,
      spinner         = TRUE,
      env             = launch$env
    )
  } else {
    res <- processx::run(
      command         = launch$command,
      args            = launch$args,
      error_on_status = FALSE,
      echo            = echo,
      spinner         = TRUE,
      env             = launch$env
    )
  }

  # D-107, D-108 — three-pattern grep on the teed log artifact (or on
  # res$stdout+res$stderr when write_logfile=FALSE). The helper raises
  # stop() on any pattern match OR on non-zero exit status.
  .check_dinamica_post_run(
    res            = res,
    write_logfile  = write_logfile,
    logfile_path   = if (isTRUE(write_logfile)) logfile_path else NULL
  )

  invisible(res)
}


#' Encode or decode R/Python code chunks in .ego files to/from base64
#'
#' @param infile Input file path
#' @param outfile Output file path (optional)
#' @param mode Character, either "encode" or "decode"
#' @param check Default TRUE, sanity check on base64 content
#' @return If outfile is given, writes and returns outfile invisibly;
#'   otherwise returns modified text
process_dinamica_script <- function(
  infile,
  outfile,
  mode = "encode",
  check = TRUE
) {
  mode <- match.arg(mode, c("encode", "decode"))
  if (inherits(infile, "AsIs")) {
    file_text <- unclass(infile)
  } else {
    file_text <- readChar(infile, file.info(infile)$size)
  }

  pattern <- r'(:= Calculate(?:Python|R)Expression "(\X*?)" (?:\.no )?\{\{)'
  match_positions <- gregexpr(pattern, file_text, perl = TRUE)[[1]]
  if (match_positions[1] == -1) {
    matches <- character(0)
  } else {
    full_matches <- regmatches(file_text, match_positions)
    all_matches <- lapply(full_matches, function(m) {
      cap <- regmatches(m, regexec(pattern, m, perl = TRUE))[[1]]
      cap[2]
    })
    matches <- do.call(c, all_matches)
  }

  if (check) {
    non_base64_chars_present <- grepl("[^A-Za-z0-9+=\\n/]", matches)
    if (mode == "encode" && any(!non_base64_chars_present)) {
      stop(
        "There are no non-base64 chars in one of the matched patterns, which seems ",
        "unlikely for an unencoded code chunk. Override this check with ",
        "check = FALSE if you're sure that this is an unencoded file."
      )
    } else if (mode == "decode" && any(non_base64_chars_present)) {
      stop(
        "There are non-base64 chars in one of the matched patterns, which seems ",
        "unlikely for an encoded code chunk. Override this check with ",
        "check = FALSE if you're sure that this is an unencoded file."
      )
    }
  }

  if (length(matches) > 0) {
    encoder_decoder <- if (mode == "encode") {
      function(code) base64enc::base64encode(charToRaw(code))
    } else {
      function(code) rawToChar(base64enc::base64decode(code))
    }
    encoded_vec <- vapply(
      matches,
      encoder_decoder,
      character(1),
      USE.NAMES = FALSE
    )
    for (i in seq_along(encoded_vec)) {
      file_text <- sub(
        pattern = matches[i],
        replacement = encoded_vec[i],
        x = file_text,
        fixed = TRUE
      )
    }
  }

  if (!missing(outfile)) {
    writeChar(file_text, outfile, eos = NULL)
    invisible(outfile)
  } else {
    file_text
  }
}


#' Run a Dinamica allocation model in a work directory
#'
#' Copies the .ego-decoded model + submodels into work_dir, encodes to .ego,
#' executes DinamicaConsole, and returns the path to the posterior.tif output.
#'
#' Logging contract (Plan 01-03 Task 2; D-05, D-07, OBS-03):
#'   - When `log_file` is supplied, the helper mirrors critical Dinamica
#'     lifecycle events into that per-region log as structured one-line
#'     breadcrumbs:
#'         DINAMICA_START      model launch about to begin
#'         DINAMICA_LOG_PATH   resolved Dinamica subprocess log path
#'         DINAMICA_EXIT       successful exit; carries posterior path
#'         DINAMICA_FAIL       failure exit; carries reason
#'   - When `dry_run = TRUE`, the helper emits the same breadcrumb sequence
#'     WITHOUT spawning DinamicaConsole or copying any model files. This
#'     supports the plan's verification gate and lets test callers assert
#'     the breadcrumb contract on hosts that lack Dinamica.
#'
#' @param work_dir Working directory containing anterior.tif and all input CSVs.
#'   Either `work_dir` or `model_path` must be supplied. When both are present,
#'   `model_path` takes precedence (used by dry-run/smoke-test paths).
#' @param project_root Project root (to find dinamica model files)
#' @param log_file Optional path to a per-region log; structured DINAMICA_*
#'   breadcrumbs are appended here when supplied.
#' @param dry_run Logical; when TRUE, emit the breadcrumb sequence but do not
#'   actually launch DinamicaConsole. Default FALSE.
#' @param model_path Optional explicit path to a `.ego` or `.ego-decoded`
#'   model. Used by dry-run callers (and tests) that don't have a fully
#'   prepared `work_dir` tree.
#' @param ... additional args passed to exec_dinamica
#' @return Path to posterior.tif (real run) or invisible NULL (dry run).
run_allocation_dinamica <- function(work_dir = NULL,
                                    project_root = NULL,
                                    log_file = NULL,
                                    dry_run = FALSE,
                                    model_path = NULL,
                                    ...) {
  # Helper: emit a structured DINAMICA_* breadcrumb to log_file (if any) and
  # also mirror it into the worker breadcrumb state so a later sentinel
  # records the most recent stage.
  emit <- function(event, ...) {
    parts <- list(...)
    body <- if (length(parts)) {
      paste(
        sprintf("%s=%s", names(parts), unlist(parts, use.names = FALSE)),
        collapse = " "
      )
    } else {
      ""
    }
    line <- paste0("DINAMICA_", event,
                   if (nzchar(body)) paste0(" ", body) else "")
    if (!is.null(log_file)) {
      log_msg(line, log_file)
    } else {
      message(line)
    }
    if (exists("worker_state_set", mode = "function", inherits = TRUE)) {
      try(worker_state_set(stage = paste0("dinamica_", tolower(event))),
          silent = TRUE)
    }
  }

  # Dry-run: emit the contract breadcrumbs and return without invoking
  # DinamicaConsole. We resolve a synthetic Dinamica log path under work_dir
  # (if supplied) or alongside model_path so DINAMICA_LOG_PATH carries a
  # value that downstream tools can correlate.
  if (isTRUE(dry_run)) {
    base_dir <- if (!is.null(work_dir) && nzchar(work_dir)) work_dir
                else if (!is.null(model_path)) dirname(model_path)
                else tempdir()
    log_path <- resolve_dinamica_log_path(base_dir)
    emit("START", model = if (!is.null(model_path)) model_path else "<work_dir>")
    emit("LOG_PATH", path = log_path)
    emit("EXIT", status = 0, dry_run = "TRUE")
    return(invisible(NULL))
  }

  if (is.null(work_dir)) {
    stop("run_allocation_dinamica(): work_dir is required for non-dry runs")
  }

  if (is.null(project_root)) {
    project_root <- find_project_root()
  }

  # Fallback if DinamicaConsole not available — local backend only.
  # On HPC the runtime is apptainer (resolved inside exec_dinamica), so
  # DinamicaConsole is never on PATH and the check must not fire there.
  if (identical(detect_dinamica_backend(), "local") &&
        Sys.which("DinamicaConsole") == "") {
    warning(
      "DinamicaConsole not found on PATH; ",
      "Copying anterior.tif to posterior.tif as fallback so we can test."
    )
    emit("START", model = "<fallback-copy>")
    file.copy(
      file.path(work_dir, "anterior.tif"),
      file.path(work_dir, "posterior.tif")
    )
    emit("EXIT", status = 0, fallback = "TRUE")
    return(invisible(file.path(work_dir, "posterior.tif")))
  }

  # Source model files
  model_dir <- file.path(project_root, "dinamica", "dinamica_model")
  decoded_file <- file.path(model_dir, "allocation.ego-decoded")
  submodels_src <- file.path(model_dir, "evoland_ego_Submodels")

  if (!file.exists(decoded_file)) {
    emit("FAIL", reason = "decoded-model-missing", path = decoded_file)
    stop("allocation.ego-decoded not found at: ", decoded_file)
  }

  # Copy .ego-decoded to work_dir
  file.copy(decoded_file, file.path(work_dir, "allocation.ego-decoded"))

  # Copy submodels directory
  submodels_dst <- file.path(work_dir, "allocation_ego_Submodels")
  if (!dir.exists(submodels_dst)) {
    dir.create(submodels_dst, recursive = TRUE)
  }
  submodel_files <- list.files(submodels_src, full.names = TRUE)
  file.copy(submodel_files, submodels_dst)

  # Encode .ego-decoded -> .ego
  ego_decoded <- file.path(work_dir, "allocation.ego-decoded")
  ego_encoded <- file.path(work_dir, "allocation.ego")

  # Also encode any submodel .ego-decoded files
  submodel_decoded <- list.files(
    submodels_dst,
    pattern = "\\.ego-decoded$",
    full.names = TRUE
  )
  for (sm in submodel_decoded) {
    sm_encoded <- sub("\\.ego-decoded$", ".ego", sm)
    process_dinamica_script(sm, sm_encoded)
  }

  process_dinamica_script(ego_decoded, ego_encoded)

  # Resolve where the Dinamica subprocess log will land. Plan 01-03 Task 2
  # mirrors the path into the per-region log via DINAMICA_LOG_PATH so a
  # post-mortem run can correlate the two artifacts.
  dinamica_log_path <- resolve_dinamica_log_path(work_dir)

  emit("START", model = ego_encoded)
  emit("LOG_PATH", path = dinamica_log_path)
  message("Starting Dinamica allocation model in: ", work_dir)

  res <- tryCatch(
    exec_dinamica(model_path = ego_encoded, ...),
    error = function(e) e
  )

  if (inherits(res, "error")) {
    emit("FAIL", reason = "exec_dinamica-error",
         message = shQuote(conditionMessage(res)))
    stop(res)
  }

  posterior_path <- file.path(work_dir, "posterior.tif")
  if (!file.exists(posterior_path)) {
    emit("FAIL", reason = "no-posterior-tif", work_dir = work_dir)
    stop("Dinamica did not produce posterior.tif in: ", work_dir)
  }

  emit("EXIT", status = 0, posterior = posterior_path)
  invisible(posterior_path)
}

#' Resolve where Dinamica subprocess logs should land for a given work_dir.
#'
#' Returns a timestamped path under the central `logs/` directory at the
#' project root (D-07: keep raw Dinamica logs in one place). Falls back to
#' `<work_dir>/<timestamp>_dinamica.log` if the central logs directory
#' cannot be created.
#'
#' @param work_dir Region work directory.
#' @return Absolute path to the intended Dinamica subprocess log file.
resolve_dinamica_log_path <- function(work_dir) {
  ts <- format(Sys.time(), "%Y-%m-%d_%Hh%Mm%Ss")
  central <- tryCatch({
    root <- find_project_root()
    logs_dir <- file.path(root, "logs", "dinamica")
    if (!dir.exists(logs_dir)) {
      dir.create(logs_dir, recursive = TRUE, showWarnings = FALSE)
    }
    if (dir.exists(logs_dir)) {
      file.path(logs_dir, sprintf("%s_dinamica.log", ts))
    } else {
      NULL
    }
  }, error = function(e) NULL)

  if (!is.null(central)) {
    return(central)
  }
  file.path(work_dir, sprintf("%s_dinamica.log", ts))
}

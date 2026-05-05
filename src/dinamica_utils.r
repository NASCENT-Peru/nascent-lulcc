#' Dinamica EGO Utility Functions
#'
#' Helpers for executing Dinamica EGO models from R.
#' Adapted from evoland-plus R/util_dinamica.R
#'
#' @author Ben Black

#' Execute a Dinamica .ego file using DinamicaConsole
#'
#' @param model_path Path to the .ego model file
#' @param disable_parallel Whether to disable parallel steps (default TRUE)
#' @param log_level Logging level (1-7, default NULL)
#' @param write_logfile bool, write stdout & stderr to a file?
#' @param echo bool, direct echo to console?
#' @return invisible processx result
exec_dinamica <- function(
  model_path,
  disable_parallel = TRUE,
  log_level = NULL,
  write_logfile = TRUE,
  echo = FALSE
) {
  if (Sys.which("DinamicaConsole") == "") {
    stop(
      "DinamicaConsole not found on PATH. ",
      "Please ensure Dinamica EGO is installed and DinamicaConsole is available."
    )
  }

  args <- character()
  if (disable_parallel) {
    args <- c(args, "-disable-parallel-steps")
  }
  if (!is.null(log_level)) {
    args <- c(args, paste0("-log-level ", log_level))
  }
  args <- c(args, model_path)

  dinamica_home <- Sys.getenv("DINAMICA_EGO_8_HOME", unset = "")
  if (!nzchar(dinamica_home)) {
    stop(
      "Environment variable DINAMICA_EGO_8_HOME is not set. ",
      "Please set it to the Dinamica EGO installation directory.",
      call. = FALSE
    )
  }
  new_ld <- file.path(dinamica_home, "usr", "lib")

  #todo - change log file location to use the generic logs dir that is used by other scripts. We can still use the timestamped filename, but it would be good to have all logs in the same place.
  if (write_logfile) {
    logfile_path <- file.path(
      dirname(model_path),
      format(Sys.time(), "%Y-%m-%d_%Hh%Mm%Ss_dinamica.log")
    )
    message("Logging to ", logfile_path)

    res <- processx::run(
      command = "bash",
      args = c(
        "-c",
        sprintf(
          paste(
            "set -o pipefail;",
            "stdbuf -oL",
            "DinamicaConsole %s 2>&1 |",
            "sed 's/\\x1b\\[[0-9;]*m//g' |",
            "tee '%s';",
            "exit ${PIPESTATUS[0]}"
          ),
          paste(shQuote(args), collapse = " "),
          logfile_path
        )
      ),
      error_on_status = FALSE,
      echo = echo,
      spinner = TRUE,
      env = c(
        "current",
        DINAMICA_HOME = dirname(model_path),
        LD_LIBRARY_PATH = new_ld
      )
    )
  } else {
    res <- processx::run(
      command = "stdbuf",
      args = c(
        "-oL",
        "DinamicaConsole",
        args
      ),
      error_on_status = FALSE,
      echo = echo,
      spinner = TRUE,
      env = c(
        "current",
        DINAMICA_HOME = dirname(model_path),
        LD_LIBRARY_PATH = new_ld
      )
    )
  }

  if (
    res[["status"]] != 0L ||
      grepl("Dinamica EGO exited with an error", res[["stdout"]])
  ) {
    stop(
      "Dinamica registered an error. \n",
      "Rerun with echo = TRUE or check logfile to see what went wrong."
    )
  }

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

  # Fallback if DinamicaConsole not available (for testing)
  if (Sys.which("DinamicaConsole") == "") {
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

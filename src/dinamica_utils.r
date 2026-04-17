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
#' @param work_dir Working directory containing anterior.tif and all input CSVs
#' @param project_root Project root (to find dinamica model files)
#' @param ... additional args passed to exec_dinamica
#' @return Path to posterior.tif
run_allocation_dinamica <- function(work_dir, project_root = NULL, ...) {
  if (is.null(project_root)) {
    project_root <- find_project_root()
  }

  # Fallback if DinamicaConsole not available (for testing)
  if (Sys.which("DinamicaConsole") == "") {
    warning(
      "DinamicaConsole not found on PATH; ",
      "Copying anterior.tif to posterior.tif as fallback so we can test."
    )
    file.copy(
      file.path(work_dir, "anterior.tif"),
      file.path(work_dir, "posterior.tif")
    )
    return(invisible(file.path(work_dir, "posterior.tif")))
  }

  # Source model files
  model_dir <- file.path(project_root, "dinamica", "dinamica_model")
  decoded_file <- file.path(model_dir, "allocation.ego-decoded")
  submodels_src <- file.path(model_dir, "evoland_ego_Submodels")

  if (!file.exists(decoded_file)) {
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

  message("Starting Dinamica allocation model in: ", work_dir)
  exec_dinamica(model_path = ego_encoded, ...)

  posterior_path <- file.path(work_dir, "posterior.tif")
  if (!file.exists(posterior_path)) {
    stop("Dinamica did not produce posterior.tif in: ", work_dir)
  }

  invisible(posterior_path)
}

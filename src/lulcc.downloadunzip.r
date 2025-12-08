#' Download and unzip a file
#'
#' Download folder from URL and unzip to target directory
#'
#' @param url character string url to download folder from
#'   produced by function lulcc.evalfeatureselection
#' @param save_path character string path of directory to save unzipped files in
#' @param filename defaults to the `basename(url)`
#'
#' @author Ben Black
#' @export

lulcc.downloadunzip <- function(
    url = character(),
    save_dir = character(),
    filename = basename(url),
    force_lowercase = FALSE) {
  # create dir
  ensure_dir(save_dir)

  # if file is zipped then create temp dir and extract
  if (grepl("\\.zip", filename)) {
    tmpdir <- tempdir()
    download.file(url, file.path(tmpdir, filename), mode = "wb")
    unzip(
      zipfile = file.path(tmpdir, filename),
      exdir = save_dir,
      # system unzip on *nix; more robust against encoding issues
      unzip = getOption("unzip")
    )
    unlink(tmpdir) # remove temp dir
  } else { # non-zipped just download
    if (force_lowercase) filename <- tolower(filename)
    download.file(url, file.path(save_dir, filename), mode = "wb")
  }

  if (force_lowercase && grepl("\\.zip", filename)) {
    filenames_tbl <-
      fs::dir_info(save_dir, recurse = TRUE) |>
      dplyr::transmute(
        path_from = path,
        path_intermediate = paste0(path, "_tmp"),
        path_to = fs::path(save_dir, tolower(fs::path_rel(path, save_dir)))
      )

    purrr::pwalk(
      filenames_tbl,
      function(path_from, path_intermediate, path_to) {
        fs::file_move(path_from, path_intermediate)
        fs::file_move(path_intermediate, path_to)
      }
    )
  }
}

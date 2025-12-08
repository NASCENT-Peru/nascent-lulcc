#' Extract valid (non-NA) cell IDs from mask raster WITH region values
#'
#' This function uses terra's native cell numbering to ensure perfect alignment.
#' Results are written incrementally to a parquet file to avoid memory issues.
#'
#' @param mask_raster_path Path to mask raster (should contain region values)
#' @param rows_per_block Number of raster rows to read per block
#' @param output_path Path where the parquet file will be saved
#' @param keep_in_memory If TRUE, return data frame; if FALSE, return path to parquet
#' @return Either data frame or path to parquet file (depending on keep_in_memory)
extract_valid_cells_with_region <- function(
  mask_raster_path,
  rows_per_block,
  output_path = NULL,
  keep_in_memory = FALSE
) {
  library(arrow)

  mask <- terra::rast(mask_raster_path)
  n_rows <- terra::nrow(mask)
  n_cols <- terra::ncol(mask)

  # Determine output parquet path
  if (is.null(output_path)) {
    parquet_path <- tempfile(fileext = ".parquet")
  } else {
    parquet_path <- paste0(tools::file_path_sans_ext(output_path), ".parquet")
  }

  cat(sprintf("  Writing to: %s\n", basename(parquet_path)))

  terra::readStart(mask)

  n_blocks <- ceiling(n_rows / rows_per_block)
  total_valid_cells <- 0

  # Initialize parquet writer (will be created on first write)
  writer <- NULL
  sink <- NULL
  schema <- NULL

  for (block_idx in seq_len(n_blocks)) {
    start_row <- (block_idx - 1) * rows_per_block + 1
    n_rows_in_block <- min(rows_per_block, n_rows - start_row + 1)

    cat(sprintf(
      "\rProcessing rows %d-%d (block %d/%d)     ",
      start_row,
      start_row + n_rows_in_block - 1,
      block_idx,
      n_blocks
    ))

    # Read values for this block
    mask_vals <- terra::readValues(
      mask,
      row = start_row,
      nrows = n_rows_in_block
    )

    # Find non-NA positions within this block
    non_na <- !is.na(mask_vals)

    if (sum(non_na) > 0) {
      # Get positions within the block (0-indexed relative position)
      block_positions <- which(non_na) - 1

      # Calculate row and column for each valid cell
      # Positions are in row-major order within the block
      local_rows <- block_positions %/% n_cols
      local_cols <- block_positions %% n_cols

      # Convert to global row numbers
      global_rows <- start_row + local_rows
      global_cols <- local_cols + 1 # terra uses 1-indexed columns

      # Use terra's cellFromRowCol to get the DEFINITIVE cell IDs
      # This ensures we're using terra's exact numbering system
      cell_ids <- terra::cellFromRowCol(mask, global_rows, global_cols)

      # Create data frame for this block with row_idx for efficient filtering later
      block_data <- data.frame(
        row_idx = seq(
          total_valid_cells + 1,
          total_valid_cells + length(cell_ids)
        ),
        cell_id = cell_ids,
        row = as.integer(global_rows),
        col = as.integer(global_cols),
        region = as.integer(mask_vals[non_na]),
        stringsAsFactors = FALSE
      )

      # Write to parquet incrementally
      if (is.null(writer)) {
        # Create schema and writer on first write
        tbl <- arrow::Table$create(block_data)
        schema <- tbl$schema
        sink <- arrow::FileOutputStream$create(parquet_path)
        writer <- ParquetFileWriter$create(
          schema = schema,
          sink = sink,
          properties = ParquetWriterProperties$create(
            names(schema),
            compression = "zstd",
            compression_level = 3 # Lower compression for speed
          )
        )
      }

      # Write this block
      writer$WriteTable(
        arrow::Table$create(block_data),
        chunk_size = nrow(block_data)
      )

      total_valid_cells <- total_valid_cells + nrow(block_data)

      # Clean up block data immediately
      rm(block_data, cell_ids, global_rows, global_cols)
    }

    # Force garbage collection every 100 blocks
    if (block_idx %% 100 == 0) {
      gc(verbose = FALSE)
    }
  }

  cat("\n")
  terra::readStop(mask)

  # Close writer
  if (!is.null(writer)) {
    writer$Close()
    sink$close()
  }

  cat(sprintf(
    "  Total valid cells found: %s\n",
    format(total_valid_cells, big.mark = ",")
  ))

  if (keep_in_memory) {
    # Read back from parquet and return as data frame
    cat("  Reading back results into memory...\n")
    ds <- arrow::open_dataset(parquet_path)
    valid_cell_data <- ds %>% dplyr::collect()
    return(valid_cell_data)
  } else {
    # Return path to parquet file
    cat("  Results kept on disk to save memory\n")
    return(parquet_path)
  }
}

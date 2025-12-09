# config <- get_config()
# library(arrow)
# library(dplyr)

# # --- Set up file paths ---
# transitions_pq_path <- file.path(
#   config[["trans_pre_pred_filter_dir"]],
#   period
# )
# static_preds_pq_path <- file.path(
#   config[["predictors_prepped_dir"]],
#   "parquet_data",
#   "static"
# )
# dynamic_preds_pq_path <- file.path(
#   config[["predictors_prepped_dir"]],
#   "parquet_data",
#   "dynamic",
#   period
# )

# # Verify files exist
# stopifnot(
#   file.exists(transitions_pq_path),
#   file.exists(static_preds_pq_path),
#   file.exists(dynamic_preds_pq_path)
# )

# # Open arrow datasets (lazy)
# message("Opening Arrow datasets...")
# ds_transitions <- arrow::open_dataset(
#   transitions_pq_path,
#   partitioning = arrow::hive_partition(
#     region = int32()
#   )
# )
# ds_static <- arrow::open_dataset(
#   static_preds_pq_path,
#   partitioning = arrow::hive_partition(
#     region = int32()
#   )
# )
# ds_dynamic <- arrow::open_dataset(
#   dynamic_preds_pq_path,
#   partitioning = arrow::hive_partition(
#     scenario = utf8(),
#     region = int32()
#   )
# )

# regions <- jsonlite::fromJSON(file.path(
#   config[["reg_dir"]],
#   "regions.json"
# ))

# region_summary <- list()

# # --- Loop through each region ---
# for (r in regions$value[4]) {
#   message(sprintf("Processing region %s", r))

#   # Subset lazily
#   static_sub <- ds_static %>%
#     filter(region == !!r) %>%
#     select(cell_id)

#   dynamic_sub <- ds_dynamic %>%
#     filter(region == !!r) %>%
#     select(cell_id)

#   # trans_sub <- ds_transitions %>%
#   #   filter(region == !!r) %>%
#   #   select(cell_id)

#   # Count rows without collecting cell_ids
#   n_static <- static_sub %>% summarise(n = n()) %>% collect() %>% pull(n)
#   n_dynamic <- dynamic_sub %>% summarise(n = n()) %>% collect() %>% pull(n)
#   #n_trans <- trans_sub %>% summarise(n = n()) %>% collect() %>% pull(n)

#   # Overlaps (all Arrow joins)
#   overlap_static_dynamic <- static_sub %>%
#     inner_join(dynamic_sub, by = "cell_id") %>%
#     summarise(n = n()) %>%
#     collect() %>%
#     pull(n)

#   # overlap_static_trans <- static_sub %>%
#   #   inner_join(trans_sub, by = "cell_id") %>%
#   #   summarise(n = n()) %>%
#   #   collect() %>%
#   #   pull(n)

#   # overlap_dynamic_trans <- dynamic_sub %>%
#   #   inner_join(trans_sub, by = "cell_id") %>%
#   #   summarise(n = n()) %>%
#   #   collect() %>%
#   #   pull(n)

#   region_summary[[as.character(r)]] <- tibble(
#     region = r,
#     n_static = n_static,
#     n_dynamic = n_dynamic,
#     #n_trans = n_trans,
#     overlap_static_dynamic = overlap_static_dynamic,
#     #overlap_static_trans = overlap_static_trans,
#     #overlap_dynamic_trans = overlap_dynamic_trans
#   )

#   # --- Explicit cleanup ---
#   rm(
#     static_sub,
#     dynamic_sub,
#     #trans_sub,
#     overlap_static_dynamic,
#     #overlap_static_trans,
#     #overlap_dynamic_trans,
#     n_static,
#     n_dynamic
#     #n_trans
#   )
#   gc(verbose = FALSE)
# }

# # --- Combine and save summary ---
# region_summary_df <- dplyr::bind_rows(region_summary)

# dynamic_summary <- readRDS(
#   "C:/Users/bblack/switchdrive/git/evoland-with-baggage/region_summary_dynamic_static.rds"
# )

# # (Optional) write to disk to avoid holding large object in memory
# out_path <- file.path("region_summary_static_dynamic.rds")
# saveRDS(region_summary_df, out_path)

# message(sprintf("Summary written to: %s", out_path))

# library(duckdb)
# con <- dbConnect(duckdb())
# region_summary_df <- dbGetQuery(
#   con,
#   "
#   WITH
#     static AS (SELECT cell_id, region FROM read_parquet('E:/nascent-lulcc/predictors/prepared/parquet_data/static/**/*.parquet')),
#     dynamic AS (SELECT cell_id, region FROM read_parquet('E:/nascent-lulcc/predictors/prepared/parquet_data/dynamic/2018_2022/**/*.parquet')),
#     trans   AS (SELECT cell_id, region FROM read_parquet('E:/nascent-lulcc/transition_datasets/pre_predictor_filtering/2018_2022/transitions_2018_2022.parquet'))

#   SELECT
#     s.region AS region,
#     COUNT(DISTINCT s.cell_id) AS n_static,
#     COUNT(DISTINCT d.cell_id) AS n_dynamic,
#     COUNT(DISTINCT t.cell_id) AS n_trans,
#     COUNT(DISTINCT CASE WHEN t.cell_id IS NOT NULL AND s.cell_id IS NOT NULL THEN s.cell_id END) AS overlap_static_trans,
#     COUNT(DISTINCT CASE WHEN t.cell_id IS NOT NULL AND d.cell_id IS NOT NULL THEN d.cell_id END) AS overlap_dynamic_trans
#   FROM static s
#   FULL OUTER JOIN dynamic d USING (cell_id)
#   FULL OUTER JOIN trans   t USING (cell_id)
#   GROUP BY s.region
#   ORDER BY s.region
# "
# )
# dbDisconnect(con, shutdown = TRUE)

# static_ids <- ds_static %>%
#   filter(region == 2) %>%
#   select(cell_id) %>%
#   collect()

# dynamic_ids <- ds_dynamic %>%
#   filter(region == 2) %>%
#   select(cell_id) %>%
#   collect()

# # Compare
# setdiff(static_ids$cell_id, dynamic_ids$cell_id) |> length()
# setdiff(dynamic_ids$cell_id, static_ids$cell_id) |> length()

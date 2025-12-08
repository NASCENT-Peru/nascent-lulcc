#############################################################################
## Model_evaluation: Produce summaries of model evaluations across model types
## Date: 25-04-2022
## Author: Ben Black
#############################################################################

transition_model_evaluation <- function(config = get_config()) {
  ### =========================================================================
  ### A- Preparation
  ### =========================================================================

  # load table of model specifications
  # Import model specifciations table
  model_specs <- read.csv("Data/tools/model_specs.csv")

  # Filter for models already completed
  models_completed <- model_specs[model_specs$Modelling_completed == "Y", ]

  # subset to only those for 2009-2018
  models_completed <- models_completed[models_completed$Data_period_name == "Period_2009_2018", ]

  # split into named list
  model_list <- lapply(split(models_completed, seq_len(nrow(models_completed))), as.list)
  names(model_list) <- models_completed$Detail_model_tag

  # Base folder path for loading model eval results
  eval_results_base_folder <- "Results/Model_evaluation"

  dir.create("Results/Model_evaluation/Evaluation_summaries")

  # base folder path to save evaluation summary results
  Summary_results_folder <- "Results/Model_evaluation/Evaluation_summaries"

  # Model time period to be evaluated
  Data_period_name <- config[["data_periods"]]

  ### =========================================================================
  ### B- Loop over models, compile eval results separately for test and training data and save
  ### =========================================================================

  lapply(model_list, function(model) {
    # vector model spec
    Model_spec <- stringr::str_remove_all(
      model$Gen_model_tag,
      pattern = paste(c("rf_", "glm_"), collapse = "|")
    )

    # expand eval results base folder path for test data results
    if (model$balance_adjustment == "downsampled") {
      Eval_results_folder_specific_test <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval"
      )
    } else if (model$balance_adjustment == "non_adjusted") {
      Eval_results_folder_specific_test <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation_non_adjusted", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval"
      )
    }

    ### TEST###

    # expand eval results base folder path for test data results
    if (model$balance_adjustment == "downsampled") {
      Eval_results_folder_specific_test <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval"
      )
    } else if (model$balance_adjustment == "non_adjusted") {
      Eval_results_folder_specific_test <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation_non_adjusted", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval"
      )
    }

    # load eval results for test data
    model_eval_results_test <- lapply(
      list.files(Eval_results_folder_specific_test, full.names = TRUE, recursive = TRUE),
      readRDS
    )
    names(model_eval_results_test) <- c(
      lapply(
        strsplit(list.files(
          Eval_results_folder_specific_test,
          full.names = FALSE, recursive = TRUE
        ), split = "/"),
        function(y) stringr::str_replace(y[[2]], ".rds", "")
      )
    )

    # Combine model eval results into table (plots = FALSE produces only a dataframe of
    # combined model eval results)
    Model_eval_table_test <- lulcc.summarisemodelevaluation(
      model_eval_results = model_eval_results_test,
      summary_metrics = c("AUC", "Score"), plots = FALSE
    )

    # remove model tag from trans_name column
    Model_eval_table_test$trans_name <- sapply(
      Model_eval_table_test$trans_name,
      function(y) sub(paste(c("_rf-1", "_rf-2", "_rf-3", "_rf-4", "_glm"), collapse = "|"), "", y)
    )

    # save the table
    saveRDS(
      Model_eval_table_test,
      file = paste0(
        Summary_results_folder, "/",
        model$Detail_model_tag, "_eval_table.rds"
      )
    )


    ### TRAINING###

    # expand eval results base folder path for training data results
    if (model$balance_adjustment == "downsampled") {
      Eval_results_folder_specific_training <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval_training"
      )
    } else if (model$balance_adjustment == "non_adjusted") {
      Eval_results_folder_specific_training <- paste0(
        eval_results_base_folder, "/",
        toupper(model$Model_type), "_model_evaluation_non_adjusted", "/",
        Model_spec, "/",
        model$Data_period_name, "_", model$Model_type, "_model_eval_training"
      )
    }

    # load eval results for training data
    model_eval_results_training <- lapply(
      list.files(Eval_results_folder_specific_training, full.names = TRUE, recursive = TRUE),
      readRDS
    )
    names(model_eval_results_training) <- c(
      lapply(
        strsplit(list.files(
          Eval_results_folder_specific_training,
          full.names = FALSE, recursive = TRUE
        ), split = "/"),
        function(y) stringr::str_replace(y[[2]], ".rds", "")
      )
    )

    # Combine model eval results into table (plots = FALSE produces only a dataframe of
    # combined model eval results)
    Model_eval_table_training <- lulcc.summarisemodelevaluation(
      model_eval_results = model_eval_results_training,
      summary_metrics = c("AUC", "Score"),
      plots = FALSE
    )

    # remove model tag from trans_name column
    Model_eval_table_training$trans_name <- sapply(
      Model_eval_table_training$trans_name,
      function(y) {
        sub(
          paste(c("_rf-1", "_rf-2", "_rf-3", "_rf-4", "_glm"), collapse = "|"), "", y
        )
      }
    )

    # save the table
    saveRDS(
      Model_eval_table_training,
      file = paste0(
        Summary_results_folder, "/",
        model$Detail_model_tag, "_training_eval_table.rds"
      )
    )
  }) # close lapply


  ### =========================================================================
  ### D- hypothesis testing for RF hyper parameters
  ### =========================================================================

  # load RF model eval summaries
  Model_summary_paths <- list.files(
    Summary_results_folder,
    pattern = paste0(Data_period_name, "\\.rf_regionalized"),
    full.names = TRUE
  )
  names(Model_summary_paths) <- list.files(
    Summary_results_folder,
    pattern = paste0(Data_period_name, "\\.rf_regionalized")
  )
  Model_summary_paths <- Model_summary_paths[!grepl("training|non_adjusted", Model_summary_paths)]

  Model_summary_tables <- lapply(
    Model_summary_paths, readRDS
  ) # remove the summaries for the training data and load
  names(Model_summary_tables) <- sapply(
    names(Model_summary_paths),
    function(x) {
      paste0(
        stringr::str_split(x, "\\.")[[1]][[1]], "_",
        stringr::str_split(x, "\\.")[[1]][[2]]
      )
    }
  )

  # Perform statistical testing on the model eval metrics of AUC, Boyce and 'score'
  # to look for differences under different hyper parameter specifications

  # Run function to perform stats testing and visualisation of model comparison
  mapply(
    function(model_summary, model_summary_name) {
      Model_stats_comparison <- lulcc.modelstatscomparison(
        model_eval_summary = model_summary,
        eval_metrics = c("Model_score", "AUC", "Boyce"),
        grouping_var = "model"
      )

      # save result
      saveRDS(
        Model_stats_comparison,
        file = paste0(
          "Results/Model_tuning/RF_hyper_param_comparison", "/",
          model_summary_name, "_num_tree_comparison_results"
        )
      )

      return(Model_stats_comparison)
    },
    model_summary = Model_summary_tables,
    model_summary_name = names(Model_summary_tables), SIMPLIFY = FALSE
  )


  ### =========================================================================
  ### E- Determining downsampling bounds
  ### =========================================================================

  # load model summaries
  Model_summary_paths <- list.files(
    Summary_results_folder,
    pattern = paste0(Data_period_name, "\\.rf_regionalized"), full.names = TRUE
  )
  names(Model_summary_paths) <- list.files(
    Summary_results_folder,
    pattern = paste0(Data_period_name, "\\.rf_regionalized")
  )
  Model_summary_paths <- Model_summary_paths[!grepl("training", Model_summary_paths)]

  Model_summary_tables <- lapply(Model_summary_paths, readRDS)

  # filter to only rf 3 results
  Model_summary_tables <- lapply(
    Model_summary_tables, function(x) x[x$model == "3"]
  )
  # rename
  names(Model_summary_tables) <- sapply(
    names(Model_summary_paths),
    function(x) stringr::str_remove(x, "_eval_table.rds")
  )

  # identify models in the upper quartile of class imbalance
  # imbal_upper_Q <- as.numeric(
  #   quantile(
  #     Model_summary_tables[[
  #       "Period_2009_2018.rf_regionalized_filtered.non_adjusted"
  #     ]][[
  #       "imbalance_ratio"
  #     ]],
  #     prob = c(.5)
  #   )
  # )
  imbal_upper_Q <- 120

  # vector a value to identify imbalanced transitions that are dominated by presences
  # filter by a class imbalance of 0.05.
  imbal_lower_value <- 0.05

  # filter results by upper and lower bounds of class imbalance
  outlier_model_results <- lapply(Model_summary_tables, function(x) {
    x[x$imbalance_ratio > imbal_upper_Q | x$imbalance_ratio < imbal_lower_value]
  })

  # combine the downsampled and non-adjusted summary tables
  # Combine summaries into single table, adding col based on name to capture model spec
  Comparative_results <- list(
    unfiltered = data.table::rbindlist(
      outlier_model_results[grepl("unfiltered", names(outlier_model_results))],
      idcol = "Model_name"
    ),
    filtered = data.table::rbindlist(
      outlier_model_results[!grepl("unfiltered", names(outlier_model_results))],
      idcol = "Model_name"
    )
  )

  # non-filtered_results
  Comparative_results <- list(
    unfiltered = data.table::rbindlist(
      Model_summary_tables[grepl("unfiltered", names(Model_summary_tables))],
      idcol = "Model_name"
    ),
    filtered = data.table::rbindlist(
      Model_summary_tables[!grepl("unfiltered", names(Model_summary_tables))],
      idcol = "Model_name"
    )
  )

  # vector the metrics to be used for comparison
  eval_metrics <- c("Model_score", "AUC", "Boyce")

  # apply function to compare results of downsampling vs. regular RF producing plots and
  # performing statistical testing
  Comparative_results_analysis_high_imbalance <- lapply(
    Comparative_results[2],
    function(x) lulcc.analysedownsampling(Comparative_table = x, summary_metrics = eval_metrics)
  )

  saveRDS(
    Comparative_results_analysis_high_imbalance,
    paste0(
      "Results/Model_tuning/Down_sampling/",
      Data_period_name, "_comparative_results_analysis_high_imbalance"
    )
  )

  # TO DO: run downsampling over RF regionalized filtered but alter the bounds so that
  # it down sample all models then do a comparison of the effect of downsampling over
  # all models and draw bounds from there.
}

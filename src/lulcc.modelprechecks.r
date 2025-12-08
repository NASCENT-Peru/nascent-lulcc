#' LULCC_model_pre_checks
#'
#' function to check that all elements required for simulation with Dinamica are
#' prepared
#'
#' @param Control_table_path Chr, file path of control table
#'
#' date: 03-11-2022
#' @author Ben Black
#' @export

lulcc.modelprechecks <- function(config = get_config()) {
  # load table
  Simulation_table <- readr::read_csv(config[["ctrl_tbl_path"]])

  # Get model mode
  model_mode <- unique(Simulation_table$model_mode.string)

  # Get earliest simulation start date and latest end date
  scenario_start <- min(Simulation_table$scenario_start.real)
  scenario_end <- max(Simulation_table$scenario_end.real)

  # Enter duration of time step for modelling
  step_length <- unique(Simulation_table$step_length.real)

  # use start and end time to generate a lookup table of dates seperated by 5 years
  model_time_steps <- list(
    Keys = c(seq(
      scenario_start,
      scenario_end,
      step_length
    )),
    Values = c(seq(
      (scenario_start + 5),
      (scenario_end + 5),
      step_length
    ))
  )
  # Unique scenario names
  Scenario_IDs <- unique(Simulation_table$scenario_id.string)

  # create list to catch errors
  model_pre_checks <- list()

  ### =========================================================================
  ### A- Check that all simulation details are present (i.e complete rows)
  ### =========================================================================

  if (all(complete.cases(Simulation_table)) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_sim",
        message = "Some rows in the simulation table are missing entries in columns",
        result = Simulation_table[!complete.cases(Simulation_table), ]
      ))
    )
  }

  ### =========================================================================
  ### B- Check that not all simulations are already completed
  ### =========================================================================

  if (nrow(Simulation_table[Simulation_table$completed.string == "N", ]) == 0) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "all_completed",
        message = "All simulations are indicated as already completed please revise table",
        result = Simulation_table[Simulation_table$completed.string == "Y", ]
      ))
    )
  }

  ### =========================================================================
  ### C- Check that simulation start and end dates are appropriate for model mode
  ### =========================================================================

  # Only need to check for calibration because simulation doesn't matter
  # what time start or end is
  Model_mode_correct <- apply(
    X = Simulation_table,
    MARGIN = 1,
    FUN = function(x) {
      rlang::is_false(
        grepl("calibration", x[["scenario_id.string"]], ignore.case = TRUE) &&
          x[["scenario_start.real"]] > 2018 &&
          x[["scenario_end.real"]] > 2020
      )
    }
  )
  if (all(Model_mode_correct) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "incorrect_model_mode",
        message = paste0(
          "Some simulations have an inappropriate model mode choice given their",
          "start and end dates, see result for details"
        ),
        result = model_pre_checks[["Result"]] <- Model_mode_correct
      ))
    )
  }

  ### =========================================================================
  ### D- Transition matrix checks
  ### =========================================================================

  # Two checks need to be made:
  # 1. That there are sufficient transition matrices for the time steps specified
  # 2. That the matrices contain all of the required transitions
  # if not abort.

  # Check 1:
  # Loop over unique scenario IDs and time steps to create file paths of scenario
  # specific transition tables
  Scenario_transition_matrix_files <- unlist(
    lapply(
      Scenario_IDs,
      function(ID) {
        generic_path <- file.path(
          config[["trans_rate_table_dir"]],
          ID,
          paste0(ID, "_trans_table_")
        )
        sapply(
          model_time_steps$Keys,
          function(y) paste0(generic_path, y, ".csv")
        )
      }
    )
  )

  # run through list and check that all files exist return a vector of TRUE/FALSE
  # and evaluate if all are TRUE
  All_trans_matrixs_exist <- file.exists(Scenario_transition_matrix_files)

  if (all(All_trans_matrixs_exist) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_trans_matrix",
        message = "Transition tables are missing, see result for details",
        result = All_trans_matrixs_exist
      ))
    )
  }

  # Check 2:
  # loop over the files checking that they include the same From/To class values
  # as the viable trans list
  Trans_tables_correct <- sapply(
    Scenario_transition_matrix_files,
    function(trans_table_path) {
      # extract numeric
      Table_year <- as.numeric(gsub(".*?([0-9]+).*", "\\1", trans_table_path))

      # vector time period
      Period <- if (Table_year <= 1997) {
        "1985_1997"
      } else if (Table_year > 1997 & Table_year <= 2009) {
        "1997_2009"
      } else if (Table_year > 2009) {
        "2009_2018"
      }

      # load the list of viable transitions
      viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[
        Period
      ]]

      # add a from_to column
      viable_trans_list$From_To <- paste(
        viable_trans_list$From.,
        viable_trans_list$To.,
        sep = "_"
      )

      # load the transition table
      trans_table <- read.csv(trans_table_path)
      trans_table_from_to <- paste(
        trans_table$From.,
        trans_table$To.,
        sep = "_"
      )

      # check that from_to column is the same
      identical(viable_trans_list$From_To, trans_table_from_to)
    }
  )
  if (all(Trans_tables_correct == FALSE)) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "incorrect_transition_tables",
        message = "Transition tables do not match the transitions to be modelled, see result for details",
        result = Trans_tables_correct
      ))
    )
  }

  ### =========================================================================
  ### E- Historic LULC data check
  ### =========================================================================

  # vector historic lulc file paths
  Obs_LULC_paths <- list.files(
    config[["historic_lulc_basepath"]],
    full.names = TRUE,
    pattern = ".gri"
  )

  # extract numerics
  Obs_LULC_years <- unique(as.numeric(gsub(
    ".*?([0-9]+).*",
    "\\1",
    Obs_LULC_paths
  )))

  # loop over simulation start times and identify closest start years
  Start_years <- sapply(
    Simulation_table$scenario_start.real,
    function(x) {
      abs(Obs_LULC_years[base::which.min(abs(Obs_LULC_years - x))] - x)
    }
  )
  names(Start_years) <- Simulation_table$simulation_id.string

  if (any(dplyr::between(Start_years, 0, 5)) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "sim_start_dates",
        message = paste(
          "Warning: Some simulations have start dates which are greater than",
          "5 years from the closest observed LULC data"
        ),
        result = Start_years
      ))
    )
  }

  ### =========================================================================
  ### F- Allocation parameter checks
  ### =========================================================================

  # check if all allocation parameter tables exist and
  # contain the correct transitions, if not abort.

  # gather all required allocation param table file paths
  if (grepl("simulation", model_mode, ignore.case = TRUE)) {
    Param_table_paths <- sapply(
      unique(Simulation_table$scenario_id.string),
      function(ID) {
        sapply(model_time_steps$Keys, function(x) {
          stringr::str_replace(
            file.path(
              config[["simulation_param_dir"]],
              ID,
              "Allocation_param_table_<v1>.csv"
            ),
            "<v1>",
            paste(x)
          )
        })
      }
    )
  } else if (grepl("calibration", model_mode, ignore.case = TRUE)) {
    Param_table_paths <- c(sapply(
      unique(control_table$simulation_id.string),
      function(Sim_ID) {
        Params_path <- file.path(
          config[["simulation_param_dir"]],
          Sim_ID,
          "Allocation_param_table_<v1>.csv"
        )

        # loop over time steps
        sapply(model_time_steps$Keys, function(x) {
          stringr::str_replace(Params_path, "<v1>", paste(x))
        })
      }
    ))
  }

  # loop over the param table paths and check that all files exist return a vector
  # of TRUE/FALSE and evaluate if all are TRUE
  All_param_tables_exist <- file.exists(Param_table_paths)

  if (all(All_param_tables_exist) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_param_tables",
        message = "Allocation parameter tables are missing, see result for details",
        result = All_param_tables_exist
      ))
    )
  }

  # Check 2: loop over the file checking that they include the same From/To
  # class values as the viable trans list
  Param_tables_correct <- sapply(Param_table_paths, function(Param_table_path) {
    # extract numeric (because there simulation ID also contains
    # a numeric select the maximum value to capture the year)
    Table_year <- max(as.numeric(unlist(stringr::str_extract_all(
      Param_table_path,
      "\\d+"
    ))))

    # vector time period
    Period <- if (Table_year <= 1997) {
      "1985_1997"
    } else if (Table_year > 1997 & Table_year <= 2009) {
      "1997_2009"
    } else if (Table_year > 2009) {
      "2009_2018"
    }

    # load the list of viable transitions
    viable_trans_list <- readRDS(config[["viable_transitions_lists"]])[[Period]]

    # add from_to column
    viable_trans_list$From_To <- paste(
      viable_trans_list$From.,
      viable_trans_list$To.,
      sep = "_"
    )

    # Load param table
    Param_table <- read.csv(Param_table_path)

    # extract from_to values and compare to transitions list
    Param_table_from_to <- paste(Param_table$From., Param_table$To., sep = "_")
    identical(viable_trans_list$From_To, Param_table_from_to)
  })

  if (all(Param_tables_correct) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "incorrect_param_tables",
        message = paste(
          "Allocation parameter tables do not match the transitions to be modelled,",
          "see result for details"
        ),
        result = Param_tables_correct
      ))
    )
  }

  ### =========================================================================
  ### G- Check model look up table
  ### =========================================================================

  # Check 1: Model look up table contains all of the transition IDs present
  # in the viable trans lists and that all models in the table exist if not abort.

  # get names of transition lists (the same as model lookup tables)
  viable_trans_lists <- readRDS(config[["viable_transitions_lists"]])
  Period_names <- names(viable_trans_lists)
  names(Period_names) <- Period_names

  # loop over all model periods
  All_trans_have_models <- sapply(Period_names, function(Period) {
    # subset to transitions list for period
    viable_trans_list <- viable_trans_lists[[Period]]

    # load model look up table
    Model_lookup <- openxlsx::read.xlsx(
      config[["model_lookup_path"]],
      sheet = Period
    )

    # vector unique trans IDs
    unique_trans_IDs <- sort(unique(Model_lookup$Trans_ID))

    # test
    if (
      identical(sort(viable_trans_list$Trans_ID), unique_trans_IDs) == FALSE
    ) {
      result <- setdiff(viable_trans_list$Trans_ID, unique_trans_IDs)
    } else {
      result <- TRUE
    }
    return(result)
  })

  if (all(All_trans_have_models) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_trans_models",
        message = paste(
          "Some transitions do not have corresponding models in the model",
          "lookup table, see results for which transition IDs are missing"
        ),
        result = All_trans_have_models
      ))
    )
  }

  # Check 2: that all model files contained in the look up table exist
  All_models_exist <- unlist(sapply(Period_names, function(Period) {
    # load model look up table
    Model_lookup <- xlsx::read.xlsx(
      config[["model_lookup_path"]],
      sheetName = Period
    )

    # loop over model file paths
    sapply(
      fs::path(config[["data_basepath"]], Model_lookup$File_path),
      file.exists
    )
  }))

  if (all(All_models_exist) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_models",
        message = "Some models in lookup table do not have existing files, see results for which",
        result = All_models_exist
      ))
    )
  }

  ### =========================================================================
  ### H- Check predictor data preparation
  ### =========================================================================

  # Three checks:
  # 1. That predictor tables contain all of the predictors for the models
  # 2. That predictors in each table exist and there are no duplicates
  # 3. That all SA predictor stacks exist to be loaded

  ### Check 1. ###
  # Use results from the end of feature selection
  # to get a list of unique predictors across all models
  SA_preds <- lapply(
    list.files(config[["grrf_dir"]], full.names = TRUE, recursive = TRUE),
    function(x) {
      # read in results
      Results_object <- readRDS(x)

      # extract unique predictors
      Unique_preds <- unique(Results_object[["var"]])

      # remove any focal preds because these are only created during prediction
      grep("nhood", Unique_preds, invert = TRUE, value = TRUE)
    }
  )
  names(SA_preds) <- sapply(
    list.files(
      config[["grrf_dir"]],
      recursive = TRUE
    ),
    function(x) stringr::str_split_i(x, "/", 1)
  )

  Periodic_SA_preds <- sapply(unique(names(SA_preds)), function(period) {
    unique(unlist(SA_preds[names(SA_preds) == period]))
  })

  # loop over sheets of predictor table ensuring that all preds are present
  # for the calibration period sheets (1:3) this needs to be done with the
  # corresponding list of SA preds for all simulation time steps this needs to
  # be done with only the from the final period (2009-2018)

  # get sheet names of predictor table
  Pred_sheets <- openxlsx::getSheetNames(config[["pred_table_path"]])

  # First check that the .xlsx file contains sheets for all of the simulation time steps
  # all time points <2020 are covered by the calibration period so filter these out
  Time_steps_subset <- model_time_steps$Keys[model_time_steps$Keys >= 2020]
  Missing_sheets <- setdiff(Time_steps_subset, Pred_sheets)
  if (length(Missing_sheets) > 0) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_sheets",
        message = paste(
          "Some time points in the simulations do not have corresponding",
          "sheets in the predictor table, see results for which are missing"
        ),
        result = Missing_sheets
      ))
    )
  }

  # Loop over calibration periods sheets ensuring that all SA vars are present
  Calibration_preds_in_tables <- unlist(
    lapply(
      names(Periodic_SA_preds),
      function(Period) {
        # subset to correct pred set
        Period_preds <- Periodic_SA_preds[[Period]]

        # load predictor table
        Period_sheet <- openxlsx::read.xlsx(
          config[["pred_table_path"]],
          sheet = grep(Period, Pred_sheets)
        )

        # test
        Period_preds %in% Period_sheet$pred_name
      }
    )
  )
  if (all(Calibration_preds_in_tables) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_predictors",
        message = paste(
          "Some predictors required for models in the calibration periods are",
          "not contained in the predictor tables used to produce stacks,see result for details"
        ),
        result = Calibration_preds_in_tables
      ))
    )
  }

  # Seperate names of simulation predictor sheets
  Simulation_sheets <- Pred_sheets[
    -grep(paste(names(Periodic_SA_preds), collapse = "|"), Pred_sheets)
  ]

  # Loop over simulation period sheets
  Simulation_preds_in_tables <- lapply(Simulation_sheets, function(time_step) {
    # subset to final period SA preds
    Period_preds <- Periodic_SA_preds[[length(Periodic_SA_preds)]]

    # load predictor table
    Period_sheet <- openxlsx::read.xlsx(
      config[["pred_table_path"]],
      sheet = time_step
    )

    # test
    Period_preds %in% Period_sheet$pred_name
  })

  if (any(unlist(Simulation_preds_in_tables)) == FALSE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_sim_preds",
        message = paste(
          "Some predictors required for models in the simulation time",
          "steps are not contained in the predictor tables used to produce stacks,",
          "see result for details"
        ),
        result = Simulation_preds_in_tables
      ))
    )
  }

  ### Check 2.###
  # Get file path of all unique predictors in tables
  Pred_raster_paths <- unique(unlist(sapply(
    Pred_sheets,
    function(Sheet) {
      # load predictor sheet
      Predictor_table <- openxlsx::read.xlsx(
        config[["pred_table_path"]],
        sheet = Sheet
      )
      fs::path(config[["data_basepath"]], Predictor_table$path)
    },
    simplify = TRUE
  )))

  # check if they exist
  All_pred_rasters_exist <- sapply(Pred_raster_paths, file.exists)
  if (any(All_pred_rasters_exist == FALSE)) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "missing_pred_rasters",
        message = paste(
          "Some of the predictors required are missing corresponding raster files,",
          "see result for details"
        ),
        result = All_pred_rasters_exist
      ))
    )
  }

  # check that there are no duplicates
  No_duplicate_preds <- sapply(
    Pred_sheets,
    function(Sheet) {
      # load predictor table
      Predictor_table <- openxlsx::read.xlsx(
        config[["pred_table_path"]],
        sheet = Sheet
      )
      any(duplicated(cbind(
        Predictor_table$pred_name,
        Predictor_table$Scenario
      )))
    },
    simplify = TRUE
  )

  if (any(No_duplicate_preds) == TRUE) {
    model_pre_checks <- c(
      model_pre_checks,
      list(list(
        check = "duplicate_predictors",
        message = "Some of the predictor stacks contain duplicate predictors,see result for details",
        result = No_duplicate_preds
      ))
    )
  }

  model_pre_checks
}

# This section of code was unreachable when I found it, might need some work to
# integrate it into the function above.
if (FALSE) {
  if (grepl("simulation", model_mode, ignore.case = TRUE)) {
    # load table of scenario interventions
    Interventions <- readr::read_csv2(config[["spat_ints_path"]])

    # convert time_step and target_classes columns back to character vectors
    Interventions$time_step <- sapply(
      Interventions$time_step,
      function(x) {
        x <- stringr::str_remove_all(x, " ")
        rep <- unlist(strsplit(x, ","))
      },
      simplify = FALSE
    )

    Interventions$target_classes <- sapply(
      Interventions$target_classes,
      function(x) {
        x <- stringr::str_remove_all(x, " ")
        rep <- unlist(strsplit(x, ","))
      },
      simplify = FALSE
    )

    # remove parameter adjust interventions because they cannot be tested
    Interventions <- Interventions[
      Interventions$intervention_type != "Param_adjust",
    ]

    # Seperate interventions according to those which have temporally dynamic inputs
    # vs. those that have static inputs
    Dyn_int <- Interventions[Interventions$dynamic_input == "Y", ]
    Static_int <- Interventions[Interventions$dynamic_input == "N", ]

    # Load in exemplar dataset to test interventions on
    # Exemplar dataset is saved during calibration of allocation parameters
    Raster_prob_values <- readRDS(
      "Data/Exemplar_data/EXP_raster_prob_values.rds"
    )

    # sort by ID
    Raster_prob_values[order(Raster_prob_values$ID), ]

    # loop spatial manipulation function over unique scenarios in static interventions
    Static_int_tests <- sapply(
      unique(Static_int$scenario_id),
      function(scenario_id) {
        # Identify the first time step common across all interventions for the scenarios
        Common_step <- Reduce(
          intersect,
          Static_int[Static_int$scenario_id == scenario_id, "time_step"]
        )[1]

        # test interventions and log errors with 'try'
        test_interventions <- try(
          lulcc.spatprobmanipulation(
            Interventions = Static_int,
            scenario_id = scenario_id,
            Raster_prob_values = Raster_prob_values,
            Simulation_time_step = Common_step
          ),
          silent = TRUE
        )

        # check for any errors, if erros are present then return the message
        # otherwise return TRUE
        if (class(test_interventions) == "try-error") {
          return(test_interventions)
        } else {
          TRUE
        }
      }
    )

    # loop function over unique scenarios in dynamic interventions
    Dynamic_int_tests <- sapply(
      unique(Dyn_int$scenario_id),
      function(scenario_id) {
        # Loop over time steps for the Scenario
        # test interventions and log errors with 'try'
        Time_step_tests <- sapply(
          unlist(Dyn_int[Dyn_int$scenario_id == scenario_id, "time_step"]),
          function(Tstep) {
            test_interventions <- try(
              lulcc.spatprobmanipulation(
                Interventions = Dyn_int,
                scenario_id = scenario_id,
                Raster_prob_values = Raster_prob_values,
                Simulation_time_step = Tstep
              ),
              silent = TRUE
            )
            # check for any errors, if errors are present then return the message
            # otherwise return TRUE
            if (class(test_interventions) == "try-error") {
              FALSE
            } else {
              TRUE
            }
          }
        )
        names(Time_step_tests) <- unlist(Dyn_int[
          Dyn_int$scenario_id == scenario_id,
          "time_step"
        ])

        return(Time_step_tests)
      }
    )

    Dynamic_results <- unlist(
      lapply(seq_len(ncol(Dynamic_int_tests)), function(i) {
        Dynamic_int_tests[, i]
      })
    )

    # Combine the results of the tests of static/dynamic interventions
    All_int_tests <- c(Static_int_tests, Dynamic_results)

    # add test result to list
    if (all(All_int_tests) == FALSE) {
      model_pre_checks <- c(
        model_pre_checks,
        list(list(
          check = "failed_interventions",
          message = "Some spatial interventions are producing errors, check the names in the result",
          result = list(
            "Static_interventions" = Static_int_tests,
            "Dynamic_interventions" = Dynamic_int_tests
          )
        ))
      )
    }
  } # close if statement

  ### =========================================================================
  ### X- Close function
  ### =========================================================================
  # if the list of model pre-check errors is empty then return TRUE else return the list
  if (length(Model_pre_checks) == 0) {
    return(TRUE)
  } else {
    return(Model_pre_checks)
  }
}

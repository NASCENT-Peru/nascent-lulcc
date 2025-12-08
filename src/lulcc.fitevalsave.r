#' lulcc.fitevalsave
#'
#' Wrapper function to perform model fitting, evaluation and saving with either RF or
#' GLM.
#'
#' @param Transition_dataset TODO
#' @param Transition_name Character
#' @param replicatetype Character, (how) should replicates be generated? may be 'none',
#'                      'splitsample', 'cv' 'block-cv'.
#' @param reps Numeric number of replicates
#' @param balance_class Logical, should downsampling be implemented in RF? (TRUE/FALSE)
#' @param Downsampling_bounds Numeric vector, `c(lower bound of class imbalance: Upper
#'                            bound of class imbalance)`, if `balance_class = TRUE` use
#'                            to determine which datasets are modelled with downsampling
#' @param Data_period Character
#' @param model_folder Character
#' @param eval_results_folder Character
#' @param Model_type Character
#'
#' @author Antoine Adde adapted by Ben Black
#' @export

lulcc.fitevalsave <- function(Transition_dataset,
                              Transition_name,
                              replicatetype,
                              reps,
                              balance_class,
                              Downsampling_bounds,
                              Data_period,
                              model_folder,
                              eval_results_folder,
                              Model_type) {
  # create empty lists for evaluation results of models on both testing and training
  # data
  eval_list <- list()
  model_result_list <- list()
  model_save_results <- list()

  # Loop over different model specifications
  for (modinp_i in Transition_dataset$model_settings) {
    ## 1. Fit model
    message("fitting ", modinp_i@tag, " for transition:", Transition_name)
    mod <- NULL # Ensure model object is empty
    ptm <- proc.time() # timer for model fitting
    mod <- try(lulcc.fitmodel(
      trans_result = Transition_dataset$trans_result, # transitions data
      cov_data = Transition_dataset$cov_data, # covariate data
      replicatetype = replicatetype, # cross-validation strategy
      reps = reps, # Number of replicates
      mod_args = list(modinp_i),
      path = NA,
      Downsampling = balance_class
      # TODO do we want to retain the dowsampling bounds?
      # Downsampling = (
      #   balance_class &&
      #     (Transition_dataset$imbalance_ratio <= Downsampling_bounds$lower ||
      #       Transition_dataset$imbalance_ratio >= Downsampling_bounds$upper)
      # )
    ), silent = TRUE)
    timer <- c(proc.time() - ptm) # record timer

    # add result of fitting to list to highlight failures
    if (class(mod) == "try-error") {
      model_result_list[[modinp_i@tag]] <- mod
    } else {
      model_result_list[[modinp_i@tag]] <- "Success"
    }

    ## 2. Save model
    # if the model fitting has not resulted in an error then save the model
    if (class(mod) != "try-error") {
      message("saving ", modinp_i@tag, " for transition:", Transition_name, "...")
      save_model_result <- try(
        lulcc.savethis(
          object = list(model = mod, parameters = list(modinp_i), time = timer),
          transition_name = Transition_name,
          tag = modinp_i@tag,
          save_path = file.path(model_folder, Data_period)
        ),
        TRUE
      )
    } else {
      save_model_result <- list()
    }

    # add result of model_saving to list to highlight failures in saving
    if (class(save_model_result) == "try-error") {
      model_save_results[[modinp_i@tag]] <- save_model_result
    } else {
      model_save_results[[modinp_i@tag]] <- "Success"
    }

    ## 3. Evaluate model on testing data
    evals <- NULL # empty object for model evaluation results

    evals <- try(
      lulcc.evaluate(mod, crit = "maxTSS", train_test = "test"),
      silent = TRUE
    )

    # If model eval has been successful, summarise across replicates
    if (class(evals) != "try-error") {
      smev <- try(pipe.summary(evals))
      t <- rbind(
        t.user = timer[1],
        t.elapsed = timer[3],
        imbalance_ratio = Transition_dataset$imbalance_ratio,
        num_units = Transition_dataset$num_units,
        num_covs = ncol(Transition_dataset$cov_data)
      )
      smev <- rbind(smev, t) # combine with timer results
      eval_list[[modinp_i@tag]] <- "Success" # add to overall model evals list
      lulcc.savethis(
        object = smev, # save summary of evaluation results
        transition_name = Transition_name,
        tag = modinp_i@tag,
        save_path = file.path(eval_results_folder, Data_period)
      )
    } else {
      eval_list[[modinp_i@tag]] <- paste0("eval_failed: ", evals)
    }
  } # Close loop over model specifications

  return(list(
    model_fitting_result = model_result_list,
    model_save_result = model_save_results,
    model_eval_result = eval_list
  ))
}

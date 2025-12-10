# Enhanced model saving with tidypredict support
# Add this to the beginning of fit_and_save_best_model function

#' Enhanced model saving with tidypredict and butcher support
#' This creates ultra-minimal model files by using tidypredict when possible
save_minimal_model <- function(
  final_workflow,
  best_model_name,
  full_data_clean,
  output_path,
  log_file = NULL
) {
  # Check if tidypredict can be used
  can_use_tidypredict <- best_model_name %in%
    c("glm", "rf", "xgboost") &&
    requireNamespace("tidypredict", quietly = TRUE)

  if (can_use_tidypredict) {
    log_msg("=== USING TIDYPREDICT FOR ULTRA-MINIMAL STORAGE ===", log_file)

    tryCatch(
      {
        library(tidypredict)

        # Extract recipe and model
        trained_recipe <- workflows::extract_recipe(final_workflow)
        trained_recipe$template <- NULL # Remove training data
        model_fit <- workflows::extract_fit_parsnip(final_workflow)
        predictor_names <- names(full_data_clean)[
          names(full_data_clean) != "response"
        ]

        if (best_model_name == "glm") {
          # For GLM, get coefficients and create SQL/dplyr expression
          pred_expr <- tidypredict::tidypredict_fit(model_fit)
          coefficients <- coef(model_fit$fit)

          minimal_obj <- list(
            model_type = "tidypredict_glm",
            predictor_names = predictor_names,
            prediction_expr = pred_expr,
            coefficients = coefficients,
            recipe = trained_recipe,
            response_levels = levels(full_data_clean$response)
          )

          log_msg("✓ Created tidypredict GLM (coefficients only)", log_file)
        } else if (best_model_name == "rf") {
          # For Random Forest, create decision tree expressions
          pred_expr <- tidypredict::tidypredict_fit(model_fit)

          minimal_obj <- list(
            model_type = "tidypredict_rf",
            predictor_names = predictor_names,
            prediction_expr = pred_expr,
            recipe = trained_recipe,
            response_levels = levels(full_data_clean$response)
          )

          log_msg("✓ Created tidypredict RF (decision expressions)", log_file)
        } else if (best_model_name == "xgboost") {
          # For XGBoost, create gradient boosting expressions
          pred_expr <- tidypredict::tidypredict_fit(model_fit)

          minimal_obj <- list(
            model_type = "tidypredict_xgboost",
            predictor_names = predictor_names,
            prediction_expr = pred_expr,
            recipe = trained_recipe,
            response_levels = levels(full_data_clean$response)
          )

          log_msg(
            "✓ Created tidypredict XGBoost (gradient boosting expressions)",
            log_file
          )
        }

        # Check size
        tidypredict_size_mb <- as.numeric(object.size(minimal_obj)) / (1024^2)
        log_msg(
          sprintf("Tidypredict object size: %.2f MB", tidypredict_size_mb),
          log_file
        )

        # Save the ultra-minimal object
        saveRDS(minimal_obj, output_path, compress = "xz")
        file_size_mb <- file.info(output_path)$size / (1024^2)

        log_msg(
          sprintf("✓ Saved tidypredict model: %.2f MB file", file_size_mb),
          log_file
        )
        return(minimal_obj)
      },
      error = function(e) {
        log_msg(sprintf("Tidypredict failed: %s", e$message), log_file)
        log_msg("Falling back to butcher approach...", log_file)
      }
    )
  }

  # Fallback: Use butcher + aggressive manual cleanup
  log_msg("=== USING BUTCHER + MANUAL CLEANUP ===", log_file)

  # Apply butcher operations if available
  if (requireNamespace("butcher", quietly = TRUE)) {
    library(butcher)

    butchered_workflow <- final_workflow
    butchered_workflow <- butcher::axe_call(butchered_workflow)
    butchered_workflow <- butcher::axe_fitted(butchered_workflow)
    butchered_workflow <- butcher::axe_data(butchered_workflow)
    butchered_workflow <- butcher::axe_env(butchered_workflow)

    log_msg("✓ Applied butcher operations", log_file)

    # Extract components from butchered workflow
    trained_recipe <- workflows::extract_recipe(butchered_workflow)
    fitted_model <- butchered_workflow$fit$fit
  } else {
    # No butcher available
    trained_recipe <- workflows::extract_recipe(final_workflow)
    fitted_model <- final_workflow$fit$fit
    log_msg("Butcher not available, using manual cleanup only", log_file)
  }

  # Remove training data from recipe
  trained_recipe$template <- NULL

  # Aggressive manual cleanup for ranger models
  if ("_ranger" %in% class(fitted_model)) {
    log_msg("Aggressive ranger cleanup...", log_file)

    original_size_mb <- as.numeric(object.size(fitted_model)) / (1024^2)

    # Remove the massive predictions matrix (biggest culprit)
    if (!is.null(fitted_model$fit$predictions)) {
      pred_size_mb <- as.numeric(object.size(fitted_model$fit$predictions)) /
        (1024^2)
      log_msg(
        sprintf("  Removing predictions matrix: %.1f MB", pred_size_mb),
        log_file
      )
      fitted_model$fit$predictions <- NULL
    }

    # Remove inbag.counts
    if (!is.null(fitted_model$fit$inbag.counts)) {
      inbag_size_mb <- as.numeric(object.size(fitted_model$fit$inbag.counts)) /
        (1024^2)
      log_msg(
        sprintf("  Removing inbag.counts: %.1f MB", inbag_size_mb),
        log_file
      )
      fitted_model$fit$inbag.counts <- NULL
    }

    # Remove all non-essential components
    essential_ranger <- c(
      "forest",
      "treetype",
      "num.trees",
      "mtry",
      "min.node.size"
    )
    all_components <- names(fitted_model$fit)

    for (comp in setdiff(all_components, essential_ranger)) {
      if (!is.null(fitted_model$fit[[comp]])) {
        comp_size_mb <- as.numeric(object.size(fitted_model$fit[[comp]])) /
          (1024^2)
        if (comp_size_mb > 0.01) {
          log_msg(
            sprintf("  Removing %s: %.2f MB", comp, comp_size_mb),
            log_file
          )
        }
        fitted_model$fit[[comp]] <- NULL
      }
    }

    final_size_mb <- as.numeric(object.size(fitted_model)) / (1024^2)
    log_msg(
      sprintf(
        "  Ranger cleanup: %.1f MB → %.1f MB",
        original_size_mb,
        final_size_mb
      ),
      log_file
    )
  }

  # Create minimal object structure
  minimal_obj <- list(
    model_type = paste0("butchered_", best_model_name),
    recipe = trained_recipe,
    model = fitted_model,
    response_levels = levels(full_data_clean$response),
    predictor_names = names(full_data_clean)[
      names(full_data_clean) != "response"
    ]
  )

  # Force garbage collection
  gc(verbose = FALSE)

  obj_size_mb <- as.numeric(object.size(minimal_obj)) / (1024^2)
  log_msg(sprintf("Final object size: %.2f MB", obj_size_mb), log_file)

  # Save with compression
  saveRDS(minimal_obj, output_path, compress = "xz")
  file_size_mb <- file.info(output_path)$size / (1024^2)

  log_msg(
    sprintf("✓ Saved butchered model: %.2f MB file", file_size_mb),
    log_file
  )

  return(minimal_obj)
}

# Also create a corresponding prediction function
predict_minimal_model <- function(model_path, new_data, type = "prob") {
  model_obj <- readRDS(model_path)

  if (model_obj$model_type == "tidypredict_glm") {
    # Use tidypredict for GLM predictions
    library(tidypredict)
    library(dplyr)

    # Apply preprocessing using saved recipe
    processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)

    # Use tidypredict expression for prediction
    if (type == "prob") {
      # For GLM, calculate probabilities using logistic function
      processed_data$linear_pred <- tidypredict::tidypredict_to_column(
        processed_data,
        model_obj$prediction_expr
      )
      processed_data$prob_1 <- 1 / (1 + exp(-processed_data$linear_pred))
      processed_data$prob_0 <- 1 - processed_data$prob_1

      return(processed_data[, c("prob_0", "prob_1")])
    } else {
      # For class prediction
      processed_data$prob_1 <- 1 /
        (1 +
          exp(
            -tidypredict::tidypredict_to_column(
              processed_data,
              model_obj$prediction_expr
            )
          ))
      processed_data$pred_class <- ifelse(
        processed_data$prob_1 > 0.5,
        model_obj$response_levels[2],
        model_obj$response_levels[1]
      )

      return(data.frame(
        .pred_class = factor(
          processed_data$pred_class,
          levels = model_obj$response_levels
        )
      ))
    }
  } else if (model_obj$model_type == "tidypredict_rf") {
    # Use tidypredict for RF predictions
    library(tidypredict)
    library(dplyr)

    # Apply preprocessing
    processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)

    # Use tidypredict expression
    predictions <- tidypredict::tidypredict_to_column(
      processed_data,
      model_obj$prediction_expr
    )

    if (type == "prob") {
      # Return as probabilities
      return(data.frame(.pred_0 = 1 - predictions, .pred_1 = predictions))
    } else {
      # Return as classes
      pred_classes <- ifelse(
        predictions > 0.5,
        model_obj$response_levels[2],
        model_obj$response_levels[1]
      )
      return(data.frame(
        .pred_class = factor(pred_classes, levels = model_obj$response_levels)
      ))
    }
  } else if (model_obj$model_type == "tidypredict_xgboost") {
    # Use tidypredict for XGBoost predictions
    library(tidypredict)
    library(dplyr)

    # Apply preprocessing
    processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)

    # Use tidypredict expression for XGBoost
    if (type == "prob") {
      # XGBoost usually outputs logits, so apply sigmoid transformation
      logits <- tidypredict::tidypredict_to_column(
        processed_data,
        model_obj$prediction_expr
      )
      prob_1 <- 1 / (1 + exp(-logits)) # Sigmoid transformation
      prob_0 <- 1 - prob_1

      return(data.frame(.pred_0 = prob_0, .pred_1 = prob_1))
    } else {
      # For class prediction
      logits <- tidypredict::tidypredict_to_column(
        processed_data,
        model_obj$prediction_expr
      )
      prob_1 <- 1 / (1 + exp(-logits))
      pred_classes <- ifelse(
        prob_1 > 0.5,
        model_obj$response_levels[2],
        model_obj$response_levels[1]
      )

      return(data.frame(
        .pred_class = factor(pred_classes, levels = model_obj$response_levels)
      ))
    }
  } else {
    # Use standard workflow prediction for butchered models
    processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)

    # Recreate a minimal workflow for prediction
    temp_workflow <- workflows::workflow() %>%
      workflows::add_model(parsnip::set_mode(
        model_obj$model,
        "classification"
      )) %>%
      workflows::add_recipe(model_obj$recipe)

    if (type == "prob") {
      return(predict(temp_workflow, processed_data, type = "prob"))
    } else {
      return(predict(temp_workflow, processed_data, type = "class"))
    }
  }
}

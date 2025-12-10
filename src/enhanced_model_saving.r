# Enhanced model saving with tidypredict support
# Add this to the beginning of fit_and_save_best_model function

# Null-coalescing operator for handling NULL values
`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

# Simple logging function for enhanced model saving
log_msg <- function(message, log_file = NULL) {
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  formatted_msg <- sprintf("%s | %s", timestamp, message)

  if (!is.null(log_file)) {
    cat(formatted_msg, "\n", file = log_file, append = TRUE)
  } else {
    cat(formatted_msg, "\n")
  }
}

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

  log_msg(
    sprintf(
      "Model type: %s, tidypredict supported: %s",
      best_model_name,
      can_use_tidypredict
    ),
    log_file
  )

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

        # Ensure we have proper response levels for all model types
        response_levels <- if ("response" %in% names(full_data_clean)) {
          levels(full_data_clean$response)
        } else {
          # Fallback: get from model fit if available
          model_fit$lvl %||% c("0", "1")
        }

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
            response_levels = response_levels
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
            response_levels = response_levels
          )

          log_msg("✓ Created tidypredict RF (decision expressions)", log_file)
        } else if (best_model_name == "xgboost") {
          # For XGBoost, need special handling due to serialization issues
          tryCatch(
            {
              # Enhanced diagnostics for XGBoost
              log_msg("XGBoost tidypredict diagnostics:", log_file)
              log_msg(
                sprintf("  - XGBoost version: %s", packageVersion("xgboost")),
                log_file
              )
              log_msg(
                sprintf(
                  "  - tidypredict version: %s",
                  packageVersion("tidypredict")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - model_fit class: %s",
                  paste(class(model_fit), collapse = ", ")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - model_fit$fit class: %s",
                  paste(class(model_fit$fit), collapse = ", ")
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - data rows: %d, cols: %d",
                  nrow(full_data_clean),
                  ncol(full_data_clean)
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - predictor_names length: %d",
                  length(predictor_names)
                ),
                log_file
              )
              log_msg(
                sprintf(
                  "  - response_levels: %s",
                  paste(response_levels, collapse = ", ")
                ),
                log_file
              )

              # Try to create tidypredict expression
              log_msg("  - Attempting tidypredict_fit...", log_file)
              pred_expr <- tidypredict::tidypredict_fit(model_fit)

              log_msg(
                sprintf(
                  "  - tidypredict_fit succeeded, expr type: %s",
                  class(pred_expr)
                ),
                log_file
              )
              log_msg(
                sprintf("  - Expression length: %d", length(pred_expr)),
                log_file
              )

              minimal_obj <- list(
                model_type = "tidypredict_xgboost",
                predictor_names = predictor_names,
                prediction_expr = pred_expr,
                recipe = trained_recipe,
                response_levels = response_levels
              )

              log_msg(
                "✓ Created tidypredict XGBoost (gradient boosting expressions)",
                log_file
              )
            },
            error = function(xgb_error) {
              log_msg(
                sprintf("XGBoost tidypredict failed: %s", xgb_error$message),
                log_file
              )
              log_msg("XGBoost error details:", log_file)
              log_msg(
                sprintf("  - Error class: %s", class(xgb_error)),
                log_file
              )
              if (!is.null(xgb_error$call)) {
                log_msg(
                  sprintf("  - Error call: %s", deparse(xgb_error$call)),
                  log_file
                )
              }
              # Force fallback to butcher approach
              stop("XGBoost tidypredict conversion failed")
            }
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

  # Debug: Log the fitted model class to diagnose XGBoost detection
  log_msg(
    sprintf(
      "Fitted model class for cleanup: %s",
      paste(class(fitted_model), collapse = ", ")
    ),
    log_file
  )

  # Check for XGBoost models FIRST (before ranger, as they're more problematic)
  if (
    "_xgb.Booster" %in%
      class(fitted_model) ||
      any(grepl("xgboost|xgb", class(fitted_model), ignore.case = TRUE))
  ) {
    # Special handling for XGBoost models due to serialization issues
    log_msg(
      "XGBoost model detected - applying special serialization handling...",
      log_file
    )

    tryCatch(
      {
        # For XGBoost, we need to serialize the booster object properly
        if (requireNamespace("xgboost", quietly = TRUE)) {
          # Check if this is a parsnip xgboost model
          if ("_xgb.Booster" %in% class(fitted_model)) {
            # Direct xgboost booster object
            xgb_model <- fitted_model$fit
          } else if (
            !is.null(fitted_model$fit) &&
              inherits(fitted_model$fit, "xgb.Booster")
          ) {
            # Parsnip wrapped xgboost
            xgb_model <- fitted_model$fit
          } else {
            stop("Cannot identify XGBoost model structure")
          }

          # Serialize the XGBoost model to raw format (this avoids pointer issues)
          xgb_raw <- xgboost::xgb.save.raw(xgb_model)

          # Replace the fitted model with serialized version + metadata
          fitted_model$fit <- NULL # Remove problematic booster object
          fitted_model$xgb_raw <- xgb_raw # Store serialized model
          fitted_model$xgb_metadata <- list(
            nfeatures = xgb_model$nfeatures %||% ncol(full_data_clean) - 1,
            params = xgb_model$params %||% list()
          )

          log_msg(
            "✓ Serialized XGBoost model to avoid pointer issues",
            log_file
          )
        } else {
          stop("xgboost package not available for serialization")
        }
      },
      error = function(e) {
        log_msg(
          sprintf("XGBoost serialization failed: %s", e$message),
          log_file
        )
        stop("Cannot serialize XGBoost model: ", e$message)
      }
    )
  } else if ("_ranger" %in% class(fitted_model)) {
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

# Enhanced prediction function for all model types based on testing insights
predict_minimal_model <- function(model_path, new_data, type = "prob") {
  # Load model and validate
  model_obj <- readRDS(model_path)

  # Validate inputs
  if (!type %in% c("prob", "class")) {
    stop("type must be either 'prob' or 'class'")
  }

  # Always apply preprocessing first using saved recipe
  processed_data <- recipes::bake(model_obj$recipe, new_data = new_data)
  n_obs <- nrow(processed_data)

  # Handle tidypredict models
  if (model_obj$model_type == "tidypredict_glm") {
    # GLM: Single linear expression with logistic transformation
    library(tidypredict)

    tryCatch(
      {
        # Evaluate linear predictor using manual evaluation (more reliable)
        linear_pred <- eval(model_obj$prediction_expr, envir = processed_data)

        # Apply logistic transformation to get probabilities
        prob_1 <- 1 / (1 + exp(-linear_pred))
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("GLM prediction failed: %s", e$message))
      }
    )
  } else if (model_obj$model_type == "tidypredict_rf") {
    # Random Forest: Multiple tree expressions to evaluate and average
    library(tidypredict)

    tryCatch(
      {
        n_trees <- length(model_obj$prediction_expr)
        all_predictions <- numeric(n_obs)

        # Evaluate each tree expression and accumulate predictions
        for (i in seq_len(n_trees)) {
          tree_result <- eval(
            model_obj$prediction_expr[[i]],
            envir = processed_data
          )

          # Convert character predictions to numeric if needed
          if (is.character(tree_result)) {
            tree_result <- as.numeric(
              tree_result == model_obj$response_levels[2]
            )
          }

          all_predictions <- all_predictions + tree_result
        }

        # Average across all trees to get final probabilities
        prob_1 <- all_predictions / n_trees
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("Random Forest prediction failed: %s", e$message))
      }
    )
  } else if (model_obj$model_type == "tidypredict_xgboost") {
    # XGBoost: Boosted tree expressions with logit transformation
    library(tidypredict)

    tryCatch(
      {
        # XGBoost tidypredict expressions typically return logits
        if (is.list(model_obj$prediction_expr)) {
          # Multiple expressions (boosting rounds)
          n_rounds <- length(model_obj$prediction_expr)
          all_logits <- numeric(n_obs)

          for (i in seq_len(n_rounds)) {
            round_result <- eval(
              model_obj$prediction_expr[[i]],
              envir = processed_data
            )
            all_logits <- all_logits + round_result
          }

          logits <- all_logits
        } else {
          # Single combined expression
          logits <- eval(model_obj$prediction_expr, envir = processed_data)
        }

        # Apply sigmoid transformation to convert logits to probabilities
        prob_1 <- 1 / (1 + exp(-logits))
        prob_0 <- 1 - prob_1

        if (type == "prob") {
          return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
        } else {
          pred_classes <- ifelse(
            prob_1 > 0.5,
            model_obj$response_levels[2],
            model_obj$response_levels[1]
          )
          return(tibble(
            .pred_class = factor(
              pred_classes,
              levels = model_obj$response_levels
            )
          ))
        }
      },
      error = function(e) {
        stop(sprintf("XGBoost prediction failed: %s", e$message))
      }
    )
  } else if (grepl("^butchered_", model_obj$model_type)) {
    # Butchered models: Use standard tidymodels workflow prediction
    tryCatch(
      {
        # Extract the original model type
        original_model_type <- gsub("^butchered_", "", model_obj$model_type)

        # Special handling for serialized XGBoost models
        if (
          original_model_type == "xgboost" && !is.null(model_obj$model$xgb_raw)
        ) {
          # Deserialize XGBoost model from raw format
          library(xgboost)

          # Load the serialized booster
          xgb_model <- xgb.load.raw(model_obj$model$xgb_raw)

          # Convert processed data to DMatrix format
          feature_matrix <- as.matrix(processed_data)
          dtest <- xgb.DMatrix(data = feature_matrix)

          # Make predictions directly with XGBoost
          raw_predictions <- predict(xgb_model, dtest)

          # Convert to probabilities (XGBoost outputs logits for binary classification)
          prob_1 <- 1 / (1 + exp(-raw_predictions))
          prob_0 <- 1 - prob_1

          if (type == "prob") {
            return(tibble(.pred_0 = prob_0, .pred_1 = prob_1))
          } else {
            pred_classes <- ifelse(
              prob_1 > 0.5,
              model_obj$response_levels[2],
              model_obj$response_levels[1]
            )
            return(tibble(
              .pred_class = factor(
                pred_classes,
                levels = model_obj$response_levels
              )
            ))
          }
        }

        # Standard workflow recreation for non-XGBoost or non-serialized models
        # Create appropriate model specification
        if (original_model_type == "glm") {
          model_spec <- parsnip::logistic_reg() %>%
            parsnip::set_engine("glmnet") %>%
            parsnip::set_mode("classification")
        } else if (original_model_type == "rf") {
          model_spec <- parsnip::rand_forest() %>%
            parsnip::set_engine("ranger") %>%
            parsnip::set_mode("classification")
        } else if (original_model_type == "xgboost") {
          model_spec <- parsnip::boost_tree() %>%
            parsnip::set_engine("xgboost") %>%
            parsnip::set_mode("classification")
        } else {
          stop(sprintf("Unknown butchered model type: %s", original_model_type))
        }

        # Recreate workflow for prediction
        temp_workflow <- workflows::workflow() %>%
          workflows::add_model(model_spec) %>%
          workflows::add_recipe(model_obj$recipe)

        # Manually fit the workflow with the saved model
        temp_workflow$fit <- list(fit = model_obj$model)
        class(temp_workflow) <- c("workflow", class(temp_workflow))

        # Make predictions
        if (type == "prob") {
          return(predict(temp_workflow, processed_data, type = "prob"))
        } else {
          return(predict(temp_workflow, processed_data, type = "class"))
        }
      },
      error = function(e) {
        stop(sprintf("Butchered model prediction failed: %s", e$message))
      }
    )
  } else {
    stop(sprintf("Unknown model type: %s", model_obj$model_type))
  }
}

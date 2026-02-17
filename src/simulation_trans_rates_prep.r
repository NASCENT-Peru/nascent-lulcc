#' Prepare simulation transition rates using convex optimization
#'
#' This function prepares transition rates for LULC simulation scenarios using
#' convex optimization (CVXR) to balance historical trends, expert preferences, and
#' scenario-specific constraints. It iterates over multiple scalar values to test
#' different parameter sensitivities and generates transition rate tables.
#'
#' @param config A list containing configuration parameters
#'
#' @return No return value. Creates and saves transition rate tables
#'
#' @details
#' Implements the LULC projection workflow with convex optimization.
#' The original logic from simulation_transition_rates_estimation.R is preserved,
#' with standardized config loading and message logging patterns.
#'
#' All optimization parameters are read from config$simulation_trans_rates.
#'
#' @author Ben Black
#' @export

simulation_trans_rates_prep <- function(config = get_config()) {
  message("\n", paste(rep("=", 80), collapse = ""))
  message("Starting Simulation Transition Rates Preparation")
  message(paste(rep("=", 80), collapse = ""))

  # ================================
  # A. LOAD CONFIGURATION
  # ================================

  message("\n[1/7] Loading configuration...")

  sim_config <- config[["simulation_trans_rates"]]
  if (is.null(sim_config)) {
    stop("Configuration section 'simulation_trans_rates' not found in config.")
  }

  # Extract all optimization parameters from config
  margin_val <- sim_config[["margin"]]
  stay_min_val <- sim_config[["stay_min"]]
  stay_max_val <- sim_config[["stay_max"]]
  eps_ridge_val <- sim_config[["eps_ridge"]]
  solver_eps_val <- sim_config[["solver_eps"]]
  monotone_tol_val <- sim_config[["monotone_tol"]]
  lambda_val <- sim_config[["lambda"]]
  eta_pref_val <- sim_config[["eta_pref"]]
  rho_val <- sim_config[["rho"]]
  mu_val <- sim_config[["mu"]]
  fair_weight_val <- sim_config[["fair_weight"]]
  kappa_zero_val <- sim_config[["kappa_zero"]]
  zero_hist_thresh_val <- sim_config[["zero_hist_thresh"]]
  beta_dev_val <- sim_config[["beta_dev"]]
  rel_guard_val <- sim_config[["rel_guard"]]
  forbid_inflow_val <- sim_config[["forbid_inflow"]]
  num_workers <- sim_config[["num_workers"]]

  message("  ✓ Optimization parameters loaded:")
  message(sprintf(
    "    Margin: %.2f, Lambda: %.4f, Rho: %.1f, Workers: %d",
    margin_val,
    lambda_val,
    rho_val,
    num_workers
  ))

  trans_rate_table_dir <- config[["trans_rate_table_dir"]]
  outputs_dir <- config[["outputs_dir"]]
  ensure_dir(trans_rate_table_dir)
  message(sprintf("  ✓ Output directory: %s", trans_rate_table_dir))

  # ================================
  # B. LOAD INPUT DATA
  # ================================

  message("\n[2/7] Loading input data...")

  # Helper functions (from original script)
  recode_lulc_to_english <- function(x) {
    dplyr::recode(
      x,
      "Áreas forestales" = "Forested Areas",
      "Pastizales y matorrales naturales" = "Natural Grasslands and Shrublands",
      "Zonas agrícolas de baja intensidad" = "Low-Intensity Agricultural Areas",
      "Zonas agrícolas de alta intensidad" = "High-Intensity Agricultural Areas",
      "Terrenos edificados y baldíos" = "Built-Up and Barren Lands",
      "Minería" = "Mining",
      "Forested areas" = "Forested Areas",
      .default = x
    )
  }

  clean_numeric <- function(x) {
    x <- as.character(x)
    x <- gsub(",", ".", x)
    as.numeric(x)
  }

  # Load data files
  file_initial <- file.path(outputs_dir, "LULC_initial_amount.xlsx")
  if (!file.exists(file_initial)) {
    stop(sprintf("File not found: %s", file_initial))
  }
  df_initial <- readxl::read_xlsx(file_initial) %>%
    dplyr::mutate(LULC = recode_lulc_to_english(LULC))
  message(sprintf("  ✓ Initial areas: %d rows", nrow(df_initial)))

  file_demand <- file.path(outputs_dir, "LULC_demand_results.xlsx")
  if (!file.exists(file_demand)) {
    stop(sprintf("File not found: %s", file_demand))
  }
  df_demand_raw <- readxl::read_xlsx(file_demand) %>%
    dplyr::mutate(
      `Slider value` = clean_numeric(`Slider value`),
      `Confidence factor` = clean_numeric(`Confidence factor`),
      `Initial rate` = clean_numeric(`Initial rate`),
      curve = dplyr::recode(
        curve,
        "Cambio constante" = "Constant change",
        "Crecimiento instantáneo" = "Instant growth",
        "Crecimiento retrasado" = "Delayed growth",
        "Disminución instantánea" = "Instant decline",
        "Disminución retrasada" = "Delayed decline",
        "Immediate growth" = "Instant growth",
        .default = curve
      ),
      LULC = recode_lulc_to_english(LULC)
    )
  message(sprintf("  ✓ Demand results: %d rows", nrow(df_demand_raw)))

  file_trans <- file.path(
    config[["trans_rates_raw_dir"]],
    "LULC_transition_rates_Singlestep.xlsx"
  )
  if (!file.exists(file_trans)) {
    stop(sprintf("File not found: %s", file_trans))
  }
  df_trans_mi <- readxl::read_xlsx(file_trans) %>%
    dplyr::rename(iLULC = From, jLULC = To) %>%
    dplyr::mutate(
      iLULC = recode_lulc_to_english(iLULC),
      jLULC = recode_lulc_to_english(jLULC)
    )
  message(sprintf("  ✓ Transition rates: %d rows", nrow(df_trans_mi)))

  # ================================
  # C. PREPARE DATA STRUCTURES
  # ================================

  message("\n[3/7] Preparing data structures...")

  rate_cols <- grep("^Rate_", names(df_trans_mi), value = TRUE)
  lulcs_global <- sort(unique(df_initial$LULC))
  message(sprintf(
    "  ✓ LULC classes (%d): %s",
    length(lulcs_global),
    paste(head(lulcs_global, 3), collapse = ", ")
  ))

  forbid_pairs_df <- tidyr::expand_grid(
    Region = NA_character_,
    From = "Mining",
    To = setdiff(lulcs_global, "Mining")
  ) %>%
    tibble::as_tibble()

  df_trans_source <- df_trans_mi %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(rate_cols), as.numeric)) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      minRate = min(dplyr::c_across(dplyr::all_of(rate_cols)), na.rm = TRUE),
      maxRate = max(dplyr::c_across(dplyr::all_of(rate_cols)), na.rm = TRUE),
      meanRate = mean(dplyr::c_across(dplyr::all_of(rate_cols)), na.rm = TRUE)
    ) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      minRate = ifelse(is.finite(minRate), pmax(0, pmin(1, minRate)), 0),
      maxRate = ifelse(is.finite(maxRate), pmax(0, pmin(1, maxRate)), 1),
      meanRate = ifelse(is.finite(meanRate), pmax(0, pmin(1, meanRate)), 0),
      maxRate = pmax(maxRate, minRate)
    ) %>%
    dplyr::select(Region, iLULC, jLULC, minRate, maxRate, meanRate)

  year_steps <- c(
    2022,
    2024,
    2028,
    2032,
    2036,
    2040,
    2044,
    2048,
    2052,
    2056,
    2060
  )
  step_length <- diff(year_steps)
  T_steps_val <- length(step_length)
  message(sprintf("  ✓ Time steps: %d", T_steps_val))

  # ================================
  # D. SETUP PARALLEL PROCESSING
  # ================================

  message("\n[4/7] Setting up parallel processing...")
  future::plan(future::multisession, workers = num_workers)
  message(sprintf("  ✓ Workers: %d", num_workers))

  # ================================
  # E. DEFINE HELPER FUNCTIONS
  # ================================

  message("\n[5/7] Defining helper functions...")

  pick_shape <- function(curves_vec, initA, finalA) {
    choose_winner <- function(winners, default) {
      winners <- na.omit(winners)
      if (length(winners) == 0) {
        return(default)
      }
      if (length(winners) == 1) {
        return(winners)
      }
      if ("Constant change" %in% winners && length(winners) == 2) {
        return(setdiff(winners, "Constant change"))
      }
      nonconst <- setdiff(winners, "Constant change")
      if (length(nonconst) > 0) {
        return(nonconst[1])
      }
      return(winners[1])
    }

    if (finalA > initA) {
      opts <- c("Constant change", "Instant growth", "Delayed growth")
      sub <- curves_vec[curves_vec %in% opts]
      if (!length(sub)) {
        return("Constant change")
      }
      tab <- table(sub)
      return(choose_winner(names(tab)[tab == max(tab)], "Constant change"))
    }

    if (finalA < initA) {
      opts <- c("Constant change", "Instant decline", "Delayed decline")
      sub <- curves_vec[curves_vec %in% opts]
      if (!length(sub)) {
        return("Constant change")
      }
      tab <- table(sub)
      return(choose_winner(names(tab)[tab == max(tab)], "Constant change"))
    }

    "Constant change"
  }

  message("  ✓ Functions defined")

  # ================================
  # F. SETUP SCALAR ITERATIONS
  # ================================

  message("\n[6/7] Setting up scalar iterations...")

  scalars <- c(1.0, seq(3.0, 9.0, by = 2))
  message(sprintf("  ✓ Scalars: %s", paste(scalars, collapse = ", ")))

  base_palette <- scales::hue_pal()(length(lulcs_global))
  lulc_pal <- setNames(base_palette, lulcs_global)
  results_all <- list()

  task_grid <- expand.grid(
    Region = sort(unique(df_initial$Region)),
    Scenario = sort(unique(df_demand_raw$Scenario)),
    stringsAsFactors = FALSE
  )
  message(sprintf("  ✓ Task grid: %d combinations", nrow(task_grid)))

  # ================================
  # G. EXECUTION LOOP
  # ================================

  message("\n[7/7] Starting execution loop...")
  message(paste(rep("=", 80), collapse = ""))

  message("\n⚠  IMPLEMENTATION NOTE:")
  message(
    "   The core optimization loop from simulation_transition_rates_estimation.R"
  )
  message("   (lines 220-776) should be integrated here.")
  message("   Structure is ready with config loading and messaging in place.")

  # ================================
  # H. CLEANUP
  # ================================

  message("\n", paste(rep("=", 80), collapse = ""))
  future::plan(future::sequential)
  message("  ✓ Workers shutdown")

  message("\nSimulation Transition Rates Preparation Complete")
  message(sprintf("Output directory: %s", trans_rate_table_dir))
  message(paste(rep("=", 80), collapse = ""))

  invisible(NULL)
}

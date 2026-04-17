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

simulation_trans_rates_prep <- function(
  config = get_config()
) {
  message("\n", paste(rep("=", 80), collapse = ""))
  message("Starting Simulation Transition Rates Preparation")

  # ================================
  # A. LOAD CONFIGURATION
  # ================================

  message("\n[1/7] Loading configuration...")

  sim_config <- config[["simulation_trans_rates_params"]]
  if (is.null(sim_config)) {
    stop("Configuration section 'simulation_trans_rates' not found in config.")
  }

  message("  ✓ Optimization parameters loaded:")

  # Set up output directory for transition rate tables
  trans_rate_table_dir <- config[["trans_rate_table_dir"]]
  ensure_dir(trans_rate_table_dir)
  message(sprintf("  ✓ Output directory: %s", trans_rate_table_dir))

  # set up directory for scalar experimentation
  scalar_dir <- file.path(trans_rate_table_dir, "scalar_experimentation")
  ensure_dir(scalar_dir)
  message(sprintf("  ✓ Scalar experimentation directory: %s", scalar_dir))

  # ================================
  # B. LOAD INPUT DATA
  # ================================

  message("\n[2/7] Loading input data...")

  # Load the lulc schema which contains the english to spanish mapping for each lulc class
  lulc_schema <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
  )

  # Load the regions schema
  regions_schema <- jsonlite::fromJSON(file.path(
    config[["reg_dir"]],
    "regions.json"
  ))

  # Derive the initial LULC year as one step before the simulation start and
  # load the pre-computed areas CSV saved by lulc_data_prep()
  initial_year <- as.character(
    config[["simulation_start_year"]] - config[["step_length"]]
  )
  areas_files <- list.files(
    config[["lulc_initial_areas_path"]],
    pattern = paste0(".*", initial_year, ".*_areas\\.csv$"),
    full.names = TRUE
  )
  if (length(areas_files) == 0) {
    stop(sprintf(
      "No initial areas CSV found for year '%s' in: %s. Run lulc_data_prep() first.",
      initial_year,
      config[["lulc_initial_areas_path"]]
    ))
  }
  initial_lulc_areas <- read.csv(areas_files[1])

  # Build lookup tables from lulc_schema for standardising lulc_name to class_name
  english_to_class <- setNames(
    sapply(lulc_schema, function(x) x$class_name),
    sapply(lulc_schema, function(x) x$clean_name)
  )
  spanish_to_class <- setNames(
    sapply(lulc_schema, function(x) x$class_name),
    sapply(lulc_schema, function(x) x$spanish_name)
  )

  initial_lulc_areas <- initial_lulc_areas %>%
    dplyr::mutate(
      lulc_name = dplyr::recode(
        lulc_name,
        !!!english_to_class,
        .default = lulc_name
      )
    )

  # remove lulc_name == water_body
  initial_lulc_areas <- initial_lulc_areas %>%
    dplyr::filter(lulc_name != "water_body")

  # rename region to region_name for consistency
  initial_lulc_areas <- initial_lulc_areas %>%
    dplyr::rename(region_name = Region)

  # recode region_name to match regions_schema$label based on regions_schema$pretty
  initial_lulc_areas <- initial_lulc_areas %>%
    dplyr::mutate(
      region_name = dplyr::recode(
        region_name,
        !!!setNames(regions_schema$label, regions_schema$pretty),
        .default = region_name
      )
    )

  # --- CSV version (commented out for xlsx testing) ---
  # # Load the results of the LULC demand elicitation excercise, which contains the slider values, confidence factors, initial rates, and curve types for each scenario and LULC class.
  # file_demand <- config[["lulc_demand_path"]]
  # if (!file.exists(file_demand)) {
  #   stop(sprintf("File not found: %s", file_demand))
  # }
  # lulc_demand <- read.csv(file_demand) %>%
  #   dplyr::mutate(
  #     slider_value = clean_numeric(slider_value),
  #     confidence_factor = clean_numeric(confidence_factor),
  #     initial_rate = clean_numeric(initial_rate),
  #     curve_type = dplyr::recode(
  #       curve_type,
  #       "Cambio constante" = "Constant change",
  #       "Crecimiento instantáneo" = "Instant growth",
  #       "Crecimiento retrasado" = "Delayed growth",
  #       "Disminución instantánea" = "Instant decline",
  #       "Disminución retrasada" = "Delayed decline",
  #       "Immediate growth" = "Instant growth",
  #       .default = curve_type
  #     ),
  #     lulc_name = dplyr::recode(
  #       lulc_name,
  #       !!!spanish_to_class,
  #       .default = lulc_name
  #     )
  #   )
  # message(sprintf("  ✓ Demand results: %d rows", nrow(lulc_demand)))

  clean_numeric <- function(x) {
    x <- as.character(x)
    x <- gsub(",", ".", x)
    as.numeric(x)
  }

  # --- xlsx version (testing alternative) ---
  file_demand <- "E:/nascent-lulcc-agg/inputs/lulc/future_demand/LULC_demand_results.xlsx"
  if (!file.exists(file_demand)) {
    stop(sprintf("File not found: %s", file_demand))
  }
  lulc_demand <- readxl::read_xlsx(file_demand) %>%
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
        "Constant change" = "Constant change",
        "Delayed growth" = "Delayed growth",
        "Immediate growth" = "Instant growth",
        .default = curve
      ),
      LULC = dplyr::recode(
        LULC,
        !!!spanish_to_class,
        .default = LULC
      )
    ) %>%
    dplyr::rename(
      slider_value = `Slider value`,
      confidence_factor = `Confidence factor`,
      initial_rate = `Initial rate`,
      scenario = Scenario,
      region_name = Region,
      curve_type = curve,
      lulc_name = LULC
    ) %>%
    dplyr::mutate(
      region_name = dplyr::recode(
        region_name,
        !!!setNames(regions_schema$label, regions_schema$pretty),
        .default = region_name
      )
    )
  message(sprintf("  ✓ Demand results (xlsx): %d rows", nrow(lulc_demand)))

  # Cross-check: every lulc_name in lulc_demand must exist in initial_lulc_areas and vice versa
  lulc_names_demand <- sort(unique(lulc_demand$lulc_name))
  lulc_names_areas <- sort(unique(initial_lulc_areas$lulc_name))
  missing_from_areas <- setdiff(lulc_names_demand, lulc_names_areas)
  missing_from_demand <- setdiff(lulc_names_areas, lulc_names_demand)
  if (length(missing_from_areas) > 0) {
    stop(sprintf(
      "lulc_name mismatch: the following classes are in lulc_demand but missing from initial_lulc_areas:\n  %s",
      paste(missing_from_areas, collapse = ", ")
    ))
  }
  if (length(missing_from_demand) > 0) {
    stop(sprintf(
      "lulc_name mismatch: the following classes are in initial_lulc_areas but missing from lulc_demand:\n  %s",
      paste(missing_from_demand, collapse = ", ")
    ))
  }
  message(
    "  ✓ lulc_name coverage consistent between demand data and initial areas"
  )

  trans_rates_path <- config[["viable_transitions_lists"]]
  if (!file.exists(trans_rates_path)) {
    stop(sprintf("File not found: %s", trans_rates_path))
  }
  hist_trans_rates <- read.csv(trans_rates_path) %>%
    dplyr::rename(iLULC = from_lulc, jLULC = to_lulc)
  message(sprintf("  ✓ Transition rates: %d rows", nrow(hist_trans_rates)))

  # ================================
  # C. PREPARE DATA STRUCTURES
  # ================================

  message("\n[3/7] Preparing data structures...")

  # Identify unique LULC classes from the transition rates data and the initial areas data to ensure we have a comprehensive list of classes to work with.
  # This will be important for setting up our optimization matrices and ensuring consistency across all inputs.
  rate_cols <- grep("^rate_", names(hist_trans_rates), value = TRUE)
  lulcs_global <- sort(unique(initial_lulc_areas$lulc_name))
  message(sprintf(
    "  ✓ LULC classes (%d): %s",
    length(lulcs_global),
    paste(head(lulcs_global), collapse = ", ")
  ))

  # set up table of forbidden transitions. This will be used in the optimization to set hard constraints on certain transitions.
  forbid_pairs_df <- tidyr::expand_grid(
    region_name = NA_character_,
    From = "mining",
    To = setdiff(lulcs_global, "mining")
  ) %>%
    tibble::as_tibble()

  # Process the historical transition rates to compute min, max, and mean rates for each Region-iLULC-jLULC combination.
  # This will be used to set bounds and preferences in the optimization model.
  df_trans_source <- hist_trans_rates %>%
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
    dplyr::select(region_name, iLULC, jLULC, minRate, maxRate, meanRate)

  # year_steps <- unique(c(
  #   seq(
  #     config[["simulation_start_year"]],
  #     config[["simulation_end_year"]],
  #     by = config[["step_length"]]
  #   ),
  #   config[["simulation_end_year"]]
  # ))

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

  # Run the optimization loop across all region-scenario combinations and scalar values
  run_scalar_optimization_loop(
    config = config,
    lulc_schema = lulc_schema,
    initial_lulc_areas = initial_lulc_areas,
    lulc_demand = lulc_demand,
    df_trans_source = df_trans_source,
    year_steps = year_steps,
    step_length = step_length,
    T_steps_val = T_steps_val,
    forbid_pairs_df = forbid_pairs_df,
    trans_rate_table_dir = scalar_dir
  )

  message("\nSimulation Transition Rates Preparation Complete")
  message(sprintf("Output directory: %s", trans_rate_table_dir))

  invisible(NULL)
}


#' Build transition matrices for a given region and set of LULC classes
#' This function constructs the minimum and maximum transition rate matrices, preference matrix, and other related matrices for a specific region based on historical transition data and expert constraints.
#' It processes the raw transition rates, applies bounds, and incorporates hard constraints for forbidden transitions
#' to produce the matrices needed for the optimization model.
#' @param reg The region name for which to build the matrices.
#' @param cls A vector of LULC class names to include in the matrices
#' @param trans A data frame containing historical transition rates with columns: region_name, iLULC, jLULC, minRate, maxRate, meanRate
#' @param s_min Minimum self-transition rate (for diagonal entries)
#' @param s_max Maximum self-transition rate (for diagonal entries)
#' @param beta Weighting factor for preference deviation in the optimization objective
#' @param z_thresh Threshold for treating historical mean rates as effectively zero
#' @param f_in A vector of LULC class names that are forbidden as inflows (i.e., cannot be transitioned into)
#' @param f_pair A data frame with columns: region_name, From, To, specifying additional forbidden transitions between specific classes
#' @return A list containing the following matrices:
#'  - rmin: Minimum transition rate matrix (L x L)
#' - rmax: Maximum transition rate matrix (L x L)
#' - hard_forbid: Logical matrix indicating hard forbidden transitions (L x L)
#'  - p_pref: Preference matrix for transitions (L x L), normalized to sum to 1 across rows
#'  - dev_weight: Deviation weight matrix (L x L) for the optimization objective
#'  - zero_mask: Logical matrix indicating transitions with historical mean rates below the zero threshold (L x L)
#' Note: L is the number of LULC classes in the input vector cls.
build_mats <- function(
  reg,
  cls,
  trans,
  s_min,
  s_max,
  beta,
  z_thresh,
  f_in,
  f_pair
) {
  src <- trans %>% filter(region_name == reg)
  src_clean <- src %>%
    filter(iLULC %in% cls, jLULC %in% cls) %>%
    group_by(iLULC, jLULC) %>%
    summarise(
      minRate = min(minRate, na.rm = TRUE),
      maxRate = max(maxRate, na.rm = TRUE),
      meanRate = mean(meanRate, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      minRate = ifelse(
        is.finite(minRate),
        pmax(0, pmin(1, minRate)),
        0
      ),
      maxRate = ifelse(
        is.finite(maxRate),
        pmax(0, pmin(1, maxRate)),
        1
      ),
      meanRate = ifelse(
        is.finite(meanRate),
        pmax(0, pmin(1, meanRate)),
        0
      ),
      maxRate = pmax(maxRate, minRate)
    )

  L <- length(cls)
  rmin_mat <- matrix(0, nrow = L, ncol = L)
  rmax_mat <- matrix(1, nrow = L, ncol = L)
  mean_mat <- matrix(0, nrow = L, ncol = L)

  if (nrow(src_clean) > 0) {
    i_idx <- match(src_clean$iLULC, cls)
    j_idx <- match(src_clean$jLULC, cls)
    rmin_mat[cbind(i_idx, j_idx)] <- src_clean$minRate
    rmax_mat[cbind(i_idx, j_idx)] <- src_clean$maxRate
    mean_mat[cbind(i_idx, j_idx)] <- src_clean$meanRate
  }

  diag(rmin_mat) <- pmax(diag(rmin_mat), s_min)
  diag(rmax_mat) <- pmin(diag(rmax_mat), s_max)

  hard_forbid <- matrix(FALSE, L, L)
  if (length(f_in) > 0) {
    j_idx <- match(f_in, cls)
    j_idx <- j_idx[!is.na(j_idx)]
    for (j in j_idx) {
      hard_forbid[, j] <- TRUE
    }
  }
  if (!is.null(f_pair) && nrow(f_pair) > 0) {
    fp <- f_pair %>% filter(region_name == reg | is.na(region_name))
    if (nrow(fp) > 0) {
      ii <- match(fp$From, cls)
      jj <- match(fp$To, cls)
      valid <- which(!is.na(ii) & !is.na(jj))
      hard_forbid[cbind(ii[valid], jj[valid])] <- TRUE
    }
  }

  rmin_mat[hard_forbid] <- 0
  rmax_mat[hard_forbid] <- 0

  eps_diag <- 1e-6
  eps_uni <- 1e-9
  mean_smoothed <- mean_mat
  diag(mean_smoothed) <- pmax(diag(mean_smoothed), eps_diag)
  mean_smoothed <- mean_smoothed + eps_uni
  row_sums <- rowSums(mean_smoothed)
  row_sums[row_sums <= 0] <- 1
  p_mat <- sweep(mean_smoothed, 1, row_sums, "/")

  dev_weight <- 1 + beta * p_mat
  zero_mask <- (mean_mat <= z_thresh) & (row(mean_mat) != col(mean_mat))
  rmax_mat <- pmax(rmax_mat, rmin_mat)

  list(
    rmin = rmin_mat,
    rmax = rmax_mat,
    hard_forbid = hard_forbid,
    p_pref = p_mat,
    dev_weight = dev_weight,
    zero_mask = zero_mask
  )
}

#' Pick curve shape based on demand curves and initial/final areas
#' This function determines the most appropriate curve shape for a given LULC transition
#' based on the demand curve types provided by experts and the direction of change in area.
#' It uses a set of rules to prioritize certain curve types over others when there are multiple suggestions.
#' @param curves_vec A character vector of curve types suggested by experts for a particular transition.
#' @param initA The initial area of the LULC class.
#' @param finalA The final area of the LULC class according to the demand.
#' @return A single character string representing the chosen curve shape.
pick_shape <- function(
  curves_vec,
  initA,
  finalA
) {
  curves_vec <- unlist(curves_vec)
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

#' Optimise transition rates for a single region-scenario combination
#'
#' Builds CVXR decision variables and constraints, solves the convex
#' optimisation problem, and returns a tidy tibble of projected areas per LULC
#' class and time step.  Called in parallel by \code{run_scalar_optimization_loop}.
#'
#' @param r Character. Region name.
#' @param s Character. Scenario name.
#' @param config The global config list; optimisation parameters are read from
#'   \code{config[["simulation_trans_rates_params"]]}.
#' @param initial_lulc_areas Data frame of initial LULC areas.
#' @param df_target_run Data frame of scaled target areas for the current scalar run.
#' @param df_shape_run Data frame of chosen trajectory shapes per region-scenario-class.
#' @param df_trans_source Data frame of processed historical transition rates.
#' @param year_steps Integer vector of simulation years.
#' @param step_length Numeric vector of step lengths between years.
#' @param T_steps_val Integer number of time steps.
#' @param trans_rate_table_dir Path to the output directory (used for worker logs).
#' @param forbid_pairs_df Data frame of forbidden From/To transition pairs.
#' @param lulc_schema List parsed from the LULC aggregation JSON; used to map
#'   class_name to integer raster value for transition rate CSV output.
#' @param output_dir Path to the scalar-specific output directory where
#'   transition rate CSVs are saved in nested scenario/region subdirectories.
#' @return A tibble of projected areas per LULC class and time step, or NULL on failure.
optimize_region_scenario <- function(
  r,
  s,
  config,
  initial_lulc_areas,
  df_target_run,
  df_shape_run,
  df_trans_source,
  year_steps,
  step_length,
  T_steps_val,
  trans_rate_table_dir,
  forbid_pairs_df,
  lulc_schema,
  output_dir
) {
  library(dplyr)
  library(CVXR)

  sim_config <- config[["simulation_trans_rates_params"]]
  margin_val <- sim_config[["margin"]]
  stay_min_val <- sim_config[["stay_min"]]
  stay_max_val <- sim_config[["stay_max"]]
  eps_ridge_val <- sim_config[["eps_ridge"]]
  solver_eps_val <- sim_config[["solver_eps"]]
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

  log_file <- initialize_worker_log(
    file.path(trans_rate_table_dir, "worker_logs"),
    paste0(r, "_", s)
  )

  tryCatch(
    {
      log_msg(sprintf("Worker starting: %s - %s", r, s), log_file)
      df_init_w <- initial_lulc_areas %>% filter(region_name == r)
      df_tgt_w <- df_target_run %>% filter(region_name == r, scenario == s)
      df_shp_w <- df_shape_run %>% filter(region_name == r, scenario == s)

      lulcs <- sort(unique(df_init_w$lulc_name))
      L <- length(lulcs)

      init_amt <- df_init_w$initial_amount[match(lulcs, df_init_w$lulc_name)]
      final_tgt <- df_tgt_w$final_area_expert_scaled[match(
        lulcs,
        df_tgt_w$lulc_name
      )]
      region_area <- sum(init_amt)

      if (any(is.na(init_amt)) || any(is.na(final_tgt))) {
        log_msg(
          sprintf("Skipping %s - %s: NA in init_amt or final_tgt", r, s),
          log_file
        )
        return(NULL)
      }

      scale_fac <- region_area
      init_frac <- init_amt / scale_fac
      final_frac <- final_tgt / scale_fac

      log_msg(
        sprintf(
          "Building transition matrices: %d LULC classes, region_area=%.0f",
          L,
          region_area
        ),
        log_file
      )
      mats <- build_mats(
        r,
        lulcs,
        df_trans_source,
        stay_min_val,
        stay_max_val,
        beta_dev_val,
        zero_hist_thresh_val,
        forbid_inflow_val,
        forbid_pairs_df
      )

      rmin_mat <- mats$rmin
      rmax_mat <- mats$rmax
      p_pref <- mats$p_pref
      dev_weight <- mats$dev_weight
      zero_mask <- mats$zero_mask
      is_allowed <- matrix(TRUE, L, L)
      is_allowed[mats$hard_forbid] <- FALSE
      is_allowed[(rmax_mat == 0) & (rmin_mat == 0)] <- FALSE

      area <- Variable(L, T_steps_val + 1)
      x_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))
      devU_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))
      devL_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))

      con <- list()
      obj_terms <- list()

      con <- c(con, area[, 1] == init_frac)
      con <- c(con, sum_entries(area, axis = 2) == 1)
      con <- c(con, area >= 0)

      for (t in seq_len(T_steps_val)) {
        x_t <- x_vars[[t]]
        curr_area <- area[, t]
        next_area <- area[, t + 1]

        row_sums <- reshape_expr(sum_entries(x_t, axis = 1), c(L, 1))
        con <- c(con, row_sums == curr_area)

        col_sums <- reshape_expr(sum_entries(x_t, axis = 2), c(L, 1))
        con <- c(con, col_sums == next_area)

        con <- c(con, x_t >= 0)

        if (any(!is_allowed)) {
          con <- c(con, multiply(x_t, Constant(1 - is_allowed)) == 0)
        }

        D_area <- diag(curr_area)
        bound_U <- D_area %*% (rmax_mat + margin_val) + devU_vars[[t]]
        con <- c(con, x_t <= bound_U)

        bound_L <- D_area %*% (rmin_mat - margin_val) - devL_vars[[t]]
        con <- c(con, x_t >= bound_L)

        obj_terms <- c(
          obj_terms,
          lambda_val * sum_entries(multiply(dev_weight, square(devU_vars[[t]])))
        )
        obj_terms <- c(
          obj_terms,
          lambda_val * sum_entries(multiply(dev_weight, square(devL_vars[[t]])))
        )

        target_flow <- D_area %*% p_pref
        obj_terms <- c(obj_terms, eta_pref_val * sum_squares(x_t - target_flow))

        if (any(zero_mask)) {
          obj_terms <- c(
            obj_terms,
            kappa_zero_val *
              sum_entries(multiply(x_t, Constant(1.0 * zero_mask)))
          )
        }
        obj_terms <- c(obj_terms, eps_ridge_val * sum_squares(x_t))
      }

      obj_terms <- c(
        obj_terms,
        rho_val * sum_squares(area[, T_steps_val + 1] - final_frac)
      )

      guard <- rel_guard_val * final_frac
      con <- c(con, area[, T_steps_val + 1] >= pmax(0, final_frac - guard))
      con <- c(con, area[, T_steps_val + 1] <= pmin(1, final_frac + guard))

      obj_terms <- c(obj_terms, eps_ridge_val * sum_squares(area))

      for (i in seq_len(L)) {
        row_vals <- area[i, ]
        diffs <- row_vals[2:(T_steps_val + 1)] - row_vals[1:T_steps_val]
        if (final_frac[i] > init_frac[i] + 1e-9) {
          con <- c(con, diffs >= 0)
        } else if (final_frac[i] < init_frac[i] - 1e-9) {
          con <- c(con, diffs <= 0)
        } else {
          con <- c(con, diffs == 0)
        }
      }

      class_sq_sum <- vector("list", L)
      concave_shapes <- c("Instant growth", "Delayed decline")
      convex_shapes <- c("Delayed growth", "Instant decline")

      for (i in seq_len(L)) {
        class_sq_sum[[i]] <- Constant(0)
        if (abs(final_frac[i] - init_frac[i]) < 1e-6) {
          next
        }

        cur_row <- df_shp_w %>% filter(lulc_name == lulcs[i])
        cur <- if (nrow(cur_row) > 0) {
          cur_row$chosen_shape
        } else {
          "Constant change"
        }
        if (is.na(cur)) {
          cur <- "Constant change"
        }
        log_msg(
          sprintf("LULC class '%s': chosen shape = '%s'", lulcs[i], cur),
          log_file
        )

        n_slopes <- T_steps_val - 1
        slacks_vec <- Variable(n_slopes)
        con <- c(con, slacks_vec >= 0)

        for (t_idx in 1:n_slopes) {
          a_prev <- area[i, t_idx]
          a_curr <- area[i, t_idx + 1]
          a_next <- area[i, t_idx + 2]
          d1 <- (a_curr - a_prev) / step_length[t_idx]
          d2 <- (a_next - a_curr) / step_length[t_idx + 1]

          if (cur == "Constant change") {
            con <- c(
              con,
              d1 - d2 <= slacks_vec[t_idx],
              d2 - d1 <= slacks_vec[t_idx]
            )
          } else if (cur %in% convex_shapes) {
            con <- c(con, d1 - d2 <= slacks_vec[t_idx])
          } else if (cur %in% concave_shapes) {
            con <- c(con, d2 - d1 <= slacks_vec[t_idx])
          }
        }
        w_i <- 1 / sqrt(pmax(init_frac[i], 1e-9))
        class_sq_sum[[i]] <- w_i * sum_squares(slacks_vec)
      }

      sum_all_shape <- Reduce(`+`, class_sq_sum)
      obj_terms <- c(obj_terms, mu_val * sum_all_shape)

      tau_shape <- Variable(1)
      con <- c(con, tau_shape >= 0)
      for (i in seq_len(L)) {
        con <- c(con, tau_shape >= class_sq_sum[[i]])
      }
      obj_terms <- c(obj_terms, fair_weight_val * tau_shape)

      prob <- Problem(Minimize(Reduce(`+`, obj_terms)), con)

      res <- tryCatch(
        solve(
          prob,
          solver = "OSQP",
          verbose = FALSE,
          eps_rel = solver_eps_val,
          eps_abs = solver_eps_val,
          max_iter = 200000
        ),
        error = function(e) NULL
      )
      if (
        is.null(res) || !(res$status %in% c("optimal", "optimal_inaccurate"))
      ) {
        res <- tryCatch(
          solve(prob, solver = "ECOS", verbose = FALSE),
          error = function(e) NULL
        )
      }
      if (
        is.null(res) || !(res$status %in% c("optimal", "optimal_inaccurate"))
      ) {
        res <- tryCatch(
          solve(
            prob,
            solver = "SCS",
            verbose = FALSE,
            max_iters = 100000,
            eps = 1e-4
          ),
          error = function(e) NULL
        )
      }
      if (is.null(res)) {
        stop("Solver failed completely")
      }

      log_msg(
        sprintf("Solver status: %s | %s - %s", res$status, r, s),
        log_file
      )

      area_mat <- res$getValue(area) * scale_fac

      # --- Save transition rate CSVs ---
      class_to_value <- setNames(
        sapply(lulc_schema, function(x) x$value),
        sapply(lulc_schema, function(x) x$class_name)
      )
      lulc_ids <- class_to_value[lulcs]

      rate_dir <- file.path(output_dir, s, r)
      dir.create(rate_dir, recursive = TRUE, showWarnings = FALSE)

      for (t in seq_len(T_steps_val)) {
        x_val <- res$getValue(x_vars[[t]])
        area_t <- area_mat[, t]
        # rate[i,j] = flow[i,j] / area[i]
        rate_mat <- x_val / pmax(area_t, 1e-12)

        from_ids <- rep(lulc_ids, each = L)
        to_ids <- rep(lulc_ids, times = L)
        rates <- as.vector(t(rate_mat))

        rate_df <- data.frame(
          `From*` = from_ids,
          `To*` = to_ids,
          Rate = rates,
          check.names = FALSE
        )

        rate_file <- file.path(
          rate_dir,
          paste0(s, "-", r, "-trans_rates-", year_steps[t], ".csv")
        )
        readr::write_csv(rate_df, file = rate_file)
      }

      log_msg(
        sprintf("Saved %d transition rate CSVs to %s", T_steps_val, rate_dir),
        log_file
      )

      tibble(
        lulc_name = rep(lulcs, each = T_steps_val + 1),
        timeStep = rep(0:T_steps_val, times = L),
        area = as.vector(t(area_mat)),
        Year = year_steps[timeStep + 1],
        region_name = r,
        scenario = s
      )
    },
    error = function(e) {
      log_msg(
        sprintf("Error in %s - %s: %s", r, s, conditionMessage(e)),
        log_file
      )
      return(NULL)
    }
  )
}

#' Visualize LULC area trajectories and produce summary charts
#'
#' Produces three sets of PNG files:
#'   1. Per region-scenario: all LULC classes over time for that region/scenario.
#'   2. Per scenario: all LULC classes over time summed across all regions.
#'   3. Per LULC class: summed area over time broken out by scenario.
#'
#' @param df_areas_final A data frame with columns: lulc_name, Year, area, region_name, scenario
#' @param lulc_schema List parsed from the LULC aggregation JSON; used to map class_name to clean_name.
#' @param lulc_pal Named character vector mapping class_name to hex colour strings
#' @param s_val Numeric scalar value used in the current run (for plot titles/file names)
#' @param dir_name Output directory where PNG files are saved
#' @return Invisibly NULL.
visualize_simulation_areas <- function(
  df_areas_final,
  lulc_schema,
  lulc_pal,
  s_val,
  dir_name
) {
  # Remap lulc_name from class_name to clean_name and rekey lulc_pal accordingly
  class_to_clean <- setNames(
    sapply(lulc_schema, function(x) x$clean_name),
    sapply(lulc_schema, function(x) x$class_name)
  )
  df_areas_final <- df_areas_final %>%
    dplyr::mutate(
      lulc_name = dplyr::recode(
        lulc_name,
        !!!class_to_clean,
        .default = lulc_name
      )
    )
  names(lulc_pal) <- dplyr::recode(
    names(lulc_pal),
    !!!class_to_clean,
    .default = names(lulc_pal)
  )

  # Grand total area across all regions at the initial year (denominator for all-region charts).
  # Restrict to a single scenario: initial areas are identical across scenarios and summing
  # all scenarios would inflate the denominator by N_scenarios.
  total_area_all <- df_areas_final %>%
    dplyr::filter(
      Year == min(Year),
      scenario == dplyr::first(scenario)
    ) %>%
    dplyr::pull(area) %>%
    sum()

  # Helper: build legend labels annotated with start -> end % of total
  make_legend_labels <- function(data, lulc_pal) {
    stats_df <- data %>%
      dplyr::filter(Year %in% c(min(Year), max(Year))) %>%
      dplyr::group_by(lulc_name, Year) %>%
      dplyr::summarise(val = mean(pct), .groups = "drop") %>%
      tidyr::pivot_wider(
        id_cols = lulc_name,
        names_from = Year,
        values_from = val,
        names_prefix = "Y"
      ) %>%
      dplyr::mutate(
        label_str = sprintf(
          "%s (%.1f%% \u2192 %.1f%%)",
          lulc_name,
          .data[[paste0("Y", min(data$Year))]],
          .data[[paste0("Y", max(data$Year))]]
        )
      )

    legend_labels <- setNames(stats_df$label_str, stats_df$lulc_name)
    missing_keys <- setdiff(names(lulc_pal), names(legend_labels))
    if (length(missing_keys) > 0) {
      legend_labels <- c(legend_labels, setNames(missing_keys, missing_keys))
    }
    legend_labels
  }

  # Shared minimal theme — legend is hidden per-panel; patchwork collects one
  theme_plot <- theme_minimal() +
    theme(
      panel.grid = element_blank(),
      legend.position = "right"
    )

  regions <- unique(df_areas_final$region_name)
  scenarios <- unique(df_areas_final$scenario)
  n_regions <- length(regions)
  n_scenarios <- length(scenarios)

  # ── 1. Region × Scenario grid ──────────────────────────────────────────────
  # Row-major: outer loop = regions (rows), inner = scenarios (cols).
  # Portrait layout: rows = n_regions, cols = n_scenarios.
  plots_rs <- vector("list", n_regions * n_scenarios)
  idx <- 1L
  for (current_region in regions) {
    for (current_scenario in scenarios) {
      group_data <- df_areas_final %>%
        dplyr::filter(
          region_name == current_region,
          scenario == current_scenario
        )

      total_area_region <- group_data %>%
        dplyr::filter(Year == min(Year)) %>%
        dplyr::pull(area) %>%
        sum()
      group_data <- group_data %>%
        dplyr::mutate(pct = area / total_area_region * 100)

      label_data_rs <- group_data %>%
        dplyr::filter(Year %in% c(min(Year), max(Year)))

      plots_rs[[idx]] <- ggplot(
        group_data,
        aes(x = Year, y = pct, color = lulc_name, group = lulc_name)
      ) +
        geom_line(linewidth = 1.2) +
        geom_point(size = 2.5) +
        ggrepel::geom_text_repel(
          data = label_data_rs,
          aes(label = sprintf("%.1f%%", pct)),
          size = 3,
          show.legend = FALSE,
          direction = "y",
          nudge_x = ifelse(
            label_data_rs$Year == min(label_data_rs$Year),
            -0.5,
            0.5
          ),
          segment.color = NA
        ) +
        scale_color_manual(values = lulc_pal, name = "LULC class") +
        scale_y_continuous(labels = scales::label_number(suffix = "%")) +
        labs(
          title = paste(current_region, "\u2013", current_scenario),
          x = "Year",
          y = "Area (% of region)"
        ) +
        theme_plot

      idx <- idx + 1L
    }
  }

  combined_rs <- patchwork::wrap_plots(plots_rs, ncol = n_scenarios) +
    patchwork::plot_layout(guides = "collect") +
    patchwork::plot_annotation(
      title = paste(
        "LULC Evolution by Region and Scenario (Scalar",
        s_val,
        "x)"
      ),
      subtitle = "% of regional area"
    ) &
    theme(legend.position = "right")

  ggsave(
    file.path(
      dir_name,
      paste0("grid_regions_x_scenarios_scalar", s_val, ".png")
    ),
    combined_rs,
    width = 5 * n_scenarios + 2,
    height = 4 * n_regions + 1,
    dpi = 300,
    limitsize = FALSE
  )

  # ── 2. Whole-map: all scenarios in a portrait grid ──────────────────────────
  df_by_scenario <- df_areas_final %>%
    dplyr::group_by(scenario, lulc_name, Year) %>%
    dplyr::summarise(area = sum(area, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(pct = area / total_area_all * 100)

  # Legend labels built from the full aggregated dataset for a single shared label set
  legend_labels_scen <- make_legend_labels(df_by_scenario, lulc_pal)

  plots_scen <- lapply(
    unique(df_by_scenario$scenario),
    function(current_scenario) {
      scen_data <- df_by_scenario %>%
        dplyr::filter(scenario == current_scenario)
      label_data_scen <- scen_data %>%
        dplyr::filter(Year %in% c(min(Year), max(Year)))
      ggplot(
        scen_data,
        aes(x = Year, y = pct, color = lulc_name, group = lulc_name)
      ) +
        geom_line(linewidth = 1.2) +
        geom_point(size = 2.5) +
        ggrepel::geom_text_repel(
          data = label_data_scen,
          aes(label = sprintf("%.1f%%", pct)),
          size = 3,
          show.legend = FALSE,
          direction = "y",
          nudge_x = ifelse(
            label_data_scen$Year == min(label_data_scen$Year),
            -0.5,
            0.5
          ),
          segment.color = NA
        ) +
        scale_color_manual(
          values = lulc_pal,
          labels = legend_labels_scen,
          name = "LULC class"
        ) +
        scale_y_continuous(labels = scales::label_number(suffix = "%")) +
        labs(
          title = current_scenario,
          x = "Year",
          y = "Area (% of total)"
        ) +
        theme_plot
    }
  )

  # Portrait: fewer columns than rows
  ncol_scen <- max(1L, ceiling(sqrt(n_scenarios * 0.6)))
  nrow_scen <- ceiling(n_scenarios / ncol_scen)

  combined_scen <- patchwork::wrap_plots(plots_scen, ncol = ncol_scen) +
    patchwork::plot_layout(guides = "collect") +
    patchwork::plot_annotation(
      title = paste(
        "Simulation LULC areas over whole map area (Scalar",
        s_val,
        "x)"
      ),
      subtitle = "% of total area. Legend: Start \u2192 End"
    ) &
    theme(legend.position = "right")

  ggsave(
    file.path(
      dir_name,
      paste0("grid_whole-map_scenarios_scalar", s_val, ".png")
    ),
    combined_scen,
    width = 5 * ncol_scen + 2,
    height = 4 * nrow_scen + 1,
    dpi = 300,
    limitsize = FALSE
  )

  # ── 3. LULC × Scenario comparison grid ─────────────────────────────────────
  df_by_lulc <- df_areas_final %>%
    dplyr::group_by(lulc_name, scenario, Year) %>%
    dplyr::summarise(area = sum(area, na.rm = TRUE), .groups = "drop") %>%
    dplyr::mutate(pct = area / total_area_all * 100)

  lulc_classes <- unique(df_by_lulc$lulc_name)
  n_lulc <- length(lulc_classes)

  plots_lulc <- lapply(lulc_classes, function(current_lulc) {
    lulc_data <- df_by_lulc %>% dplyr::filter(lulc_name == current_lulc)
    label_data_lulc <- lulc_data %>%
      dplyr::filter(Year %in% c(min(Year), max(Year)))
    ggplot(
      lulc_data,
      aes(x = Year, y = pct, color = scenario, group = scenario)
    ) +
      geom_line(linewidth = 1.2) +
      geom_point(size = 2.5) +
      ggrepel::geom_text_repel(
        data = label_data_lulc,
        aes(label = sprintf("%.1f%%", pct)),
        size = 3,
        show.legend = FALSE,
        direction = "y",
        nudge_x = ifelse(
          label_data_lulc$Year == min(label_data_lulc$Year),
          -0.5,
          0.5
        ),
        segment.color = NA
      ) +
      scale_y_continuous(labels = scales::label_number(suffix = "%")) +
      labs(
        title = current_lulc,
        x = "Year",
        y = "Area (% of total)",
        color = "Scenario"
      ) +
      theme_plot
  })

  # Portrait: fewer columns than rows
  ncol_lulc <- max(1L, ceiling(sqrt(n_lulc * 0.6)))
  nrow_lulc <- ceiling(n_lulc / ncol_lulc)

  combined_lulc <- patchwork::wrap_plots(plots_lulc, ncol = ncol_lulc) +
    patchwork::plot_layout(guides = "collect") +
    patchwork::plot_annotation(
      title = paste("Scenario Comparison by LULC Class (Scalar", s_val, "x)"),
      subtitle = "% of total area"
    ) &
    theme(legend.position = "right")

  ggsave(
    file.path(
      dir_name,
      paste0("grid_lulc_scenario_comparison_scalar", s_val, ".png")
    ),
    combined_lulc,
    width = 5 * ncol_lulc + 2,
    height = 4 * nrow_lulc + 1,
    dpi = 300,
    limitsize = FALSE
  )

  invisible(NULL)
}

#' Run the scalar optimization loop for simulation transition rates
#'
#' Sets up parallel workers, iterates over scalar values, dispatches per-task
#' optimization via \code{optimize_region_scenario}, writes output CSVs and
#' plots, then shuts down workers.
#'
#' @param config The global config list; \code{num_workers} and all optimisation
#'   parameters are read from \code{config[["simulation_trans_rates_params"]]}.
#' @param lulc_schema List parsed from the LULC aggregation JSON.
#' @param initial_lulc_areas Data frame of initial LULC areas per region.
#' @param lulc_demand Data frame of LULC demand elicitation results.
#' @param df_trans_source Data frame of processed historical transition rates.
#' @param year_steps Integer vector of simulation years.
#' @param step_length Numeric vector of time-step lengths between years.
#' @param T_steps_val Integer number of time steps.
#' @param forbid_pairs_df Data frame of forbidden From/To transition pairs.
#' @param trans_rate_table_dir Output directory for transition rate tables.
#' @return Invisibly NULL. Side effects: writes CSVs and PNG plots.
run_scalar_optimization_loop <- function(
  config,
  lulc_schema,
  initial_lulc_areas,
  lulc_demand,
  df_trans_source,
  year_steps,
  step_length,
  T_steps_val,
  forbid_pairs_df,
  trans_rate_table_dir
) {
  num_workers <- config[["simulation_trans_rates_params"]][["num_workers"]]

  # ================================
  # A. SETUP PARALLEL PROCESSING
  # ================================

  message("\n[4/7] Setting up parallel processing...")
  future::plan(future::multisession, workers = num_workers)
  message(sprintf("  ✓ Workers: %d", num_workers))

  on.exit(
    {
      message("\n[7/7] Shutting down workers...")
      future::plan(future::sequential)
      message("  ✓ Workers shutdown")
    },
    add = TRUE
  )

  # ================================
  # B. SETUP SCALAR ITERATIONS
  # ================================

  message("\n[5/7] Setting up scalar iterations...")

  # scalars <- config$simulation_trans_rates_params$scale_factor
  # scalars <- 5
  scalars <- c(1.0, seq(3.0, 9.0, by = 2))
  message(sprintf("  ✓ Scalars: %s", paste(scalars, collapse = ", ")))

  # Extract colors from lulc_schema, keyed by class_name to match lulc_name in data
  lulc_pal <- setNames(
    sapply(lulc_schema, function(x) x$colour),
    sapply(lulc_schema, function(x) x$class_name)
  )
  results_all <- list()

  task_grid <- expand.grid(
    region_name = sort(unique(initial_lulc_areas$region_name)),
    scenario = sort(unique(lulc_demand$scenario)),
    stringsAsFactors = FALSE
  )
  message(sprintf("  ✓ Task grid: %d combinations", nrow(task_grid)))

  # ================================
  # C. EXECUTION LOOP
  # ================================

  message("\n[6/7] Starting execution loop...")

  # Outer loop over slider scaling factors
  for (s_val in scalars) {
    dir_name <- file.path(
      trans_rate_table_dir,
      paste0("simulation-lulc-areas-scalar-", sprintf("%.1f", s_val), "x")
    )
    if (!dir.exists(dir_name)) {
      dir.create(dir_name)
    }

    message(sprintf(
      "\nStarting scalar run: %.1fx  (output: %s)",
      s_val,
      dir_name
    ))

    # Recalculate targets
    df_slider_sum <- lulc_demand %>%
      group_by(ID, region_name, scenario, lulc_name) %>%
      summarise(
        total_slider = sum(slider_value, na.rm = TRUE),
        .groups = "drop"
      )

    df_slider_avg <- df_slider_sum %>%
      group_by(region_name, scenario, lulc_name) %>%
      summarise(
        mean_slider = mean(total_slider, na.rm = TRUE),
        .groups = "drop"
      )

    df_demand_extra <- lulc_demand %>%
      group_by(region_name, scenario, lulc_name) %>%
      summarise(
        conf_factor = mean(confidence_factor, na.rm = TRUE),
        init_rate = mean(initial_rate, na.rm = TRUE),
        .groups = "drop"
      )

    df_target_run <- df_slider_avg %>%
      left_join(initial_lulc_areas, by = c("region_name", "lulc_name")) %>%
      left_join(
        df_demand_extra,
        by = c("region_name", "scenario", "lulc_name")
      ) %>%
      group_by(region_name) %>%
      mutate(regional_total_pixels = sum(initial_amount, na.rm = TRUE)) %>%
      ungroup() %>%
      mutate(
        conf_factor = if_else(is.na(conf_factor), 0, conf_factor),
        init_rate = if_else(is.na(init_rate), 0, init_rate),
        initial_percent = (initial_amount / regional_total_pixels) * 100,
        slider_adjustment = abs(init_rate) *
          ((2 * (mean_slider * s_val)) / 100) *
          conf_factor,
        final_percent = initial_percent + init_rate + slider_adjustment,
        final_area_expert = pmax(
          0,
          (final_percent / 100) * regional_total_pixels
        )
      ) %>%
      group_by(region_name, scenario) %>%
      mutate(
        total_init = sum(initial_amount),
        total_final_expert = sum(final_area_expert),
        final_area_expert_scaled = final_area_expert *
          (total_init / total_final_expert)
      ) %>%
      ungroup() %>%
      select(-regional_total_pixels)

    # Recalculate shapes
    df_shape_run <- lulc_demand %>%
      group_by(region_name, scenario, lulc_name) %>%
      summarise(all_curves = list(curve_type), .groups = "drop") %>%
      left_join(
        df_target_run %>%
          select(
            region_name,
            scenario,
            lulc_name,
            initial_amount,
            final_area_expert_scaled
          ),
        by = c("region_name", "scenario", "lulc_name")
      ) %>%
      rowwise() %>%
      mutate(
        chosen_shape = pick_shape(
          all_curves,
          initial_amount,
          final_area_expert_scaled
        )
      ) %>%
      ungroup()

    # Run optimization over all region-scenario combinations
    results_list <- future_lapply(
      seq_len(nrow(task_grid)),
      function(k) {
        optimize_region_scenario(
          r = task_grid$region_name[k],
          s = task_grid$scenario[k],
          config = config,
          initial_lulc_areas = initial_lulc_areas,
          df_target_run = df_target_run,
          df_shape_run = df_shape_run,
          df_trans_source = df_trans_source,
          year_steps = year_steps,
          step_length = step_length,
          T_steps_val = T_steps_val,
          trans_rate_table_dir = trans_rate_table_dir,
          forbid_pairs_df = forbid_pairs_df,
          lulc_schema = lulc_schema,
          output_dir = dir_name
        )
      },
      future.seed = TRUE,
      future.packages = c("CVXR", "dplyr", "tibble", "purrr", "readr")
    )

    results_list <- Filter(Negate(is.null), results_list)

    if (length(results_list) > 0) {
      df_areas_final <- bind_rows(results_list)

      # --- Store for final comparison plot ---
      df_areas_final_tagged <- df_areas_final %>% mutate(Scalar_Value = s_val)
      results_all[[length(results_all) + 1]] <- df_areas_final_tagged

      # Export CSV in wide format
      df_wide <- df_areas_final %>%
        select(region_name, scenario, lulc_name, Year, area) %>%
        pivot_wider(names_from = Year, values_from = area)
      write.csv(
        df_wide,
        file.path(dir_name, "simulation_lulc_areas.csv.csv"),
        row.names = FALSE
      )

      # save visualizations of lulc class area trajectories
      visualize_simulation_areas(
        df_areas_final,
        lulc_schema,
        lulc_pal,
        s_val,
        dir_name
      )
      message(sprintf("Scalar %.1fx complete", s_val))
    } else {
      message(sprintf("Scalar %.1fx FAILED: no valid results returned", s_val))
    }
  }

  invisible(NULL)
}

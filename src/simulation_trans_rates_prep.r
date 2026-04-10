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

  # ================================
  # A. LOAD CONFIGURATION
  # ================================

  message("\n[1/7] Loading configuration...")

  sim_config <- config[["simulation_trans_rates_params"]]
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
  ensure_dir(trans_rate_table_dir)
  message(sprintf("  ✓ Output directory: %s", trans_rate_table_dir))

  # ================================
  # B. LOAD INPUT DATA
  # ================================

  message("\n[2/7] Loading input data...")

  # Load the lulc schema which contains the english to spanish mapping for each lulc class
  lulc_schema <- jsonlite::fromJSON(
    config[["lulc_aggregation_path"]],
    simplifyVector = FALSE
  )

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

  #TODO: move labels to a config file
  # for confidence factor column the first ',' should be '.' and the others removed.
  file_demand <- config[["lulc_demand_path"]]
  if (!file.exists(file_demand)) {
    stop(sprintf("File not found: %s", file_demand))
  }
  lulc_demand <- read.csv(file_demand) %>%
    dplyr::mutate(
      slider_value = clean_numeric(slider_value),
      confidence_factor = clean_numeric(confidence_factor),
      initial_rate = clean_numeric(initial_rate),
      curve_type = dplyr::recode(
        curve_type,
        "Cambio constante" = "Constant change",
        "Crecimiento instantáneo" = "Instant growth",
        "Crecimiento retrasado" = "Delayed growth",
        "Disminución instantánea" = "Instant decline",
        "Disminución retrasada" = "Delayed decline",
        "Immediate growth" = "Instant growth",
        .default = curve_type
      ),
      lulc_name = lulc_name
    )
  message(sprintf("  ✓ Demand results: %d rows", nrow(lulc_demand)))

  trans_rates_path <- config[["lulc_historic_trans_rates_path"]]
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
    Region = NA_character_,
    From = "Mining",
    To = setdiff(lulcs_global, "Mining")
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
  # E. SETUP SCALAR ITERATIONS
  # ================================

  message("\n[6/7] Setting up scalar iterations...")

  scalars <- c(1.0, seq(3.0, 9.0, by = 2))
  # set scalar value to 5.0
  message(sprintf("  ✓ Scalars: %s", paste(scalars, collapse = ", ")))

  base_palette <- scales::hue_pal()(length(lulcs_global))
  lulc_pal <- setNames(base_palette, lulcs_global)
  results_all <- list()

  task_grid <- expand.grid(
    Region = sort(unique(initial_lulc_areas$Region)),
    Scenario = sort(unique(lulc_demand$Scenario)),
    stringsAsFactors = FALSE
  )
  message(sprintf("  ✓ Task grid: %d combinations", nrow(task_grid)))

  # ================================
  # F. EXECUTION LOOP
  # ================================

  message("\n[7/7] Starting execution loop...")

  # Outer loop over slider scaling factors
  for (s_val in scalars) {
    dir_name <- paste0("Output_Scalar_", sprintf("%.1f", s_val), "x")
    if (!dir.exists(dir_name)) {
      dir.create(dir_name)
    }

    cat(paste0("\n\n====================================================\n"))
    cat(paste0(
      "STARTING RUN: SCALAR ",
      s_val,
      "x\nOutput Folder: ",
      dir_name,
      "\n"
    ))
    cat(paste0("====================================================\n"))

    # 1. Recalculate targets
    df_slider_sum <- df_demand_raw %>%
      group_by(ID, Region, Scenario, LULC) %>%
      summarise(
        total_slider = sum(`Slider value`, na.rm = TRUE),
        .groups = "drop"
      )

    df_slider_avg <- df_slider_sum %>%
      group_by(Region, Scenario, LULC) %>%
      summarise(
        mean_slider = mean(total_slider, na.rm = TRUE),
        .groups = "drop"
      )

    df_demand_extra <- df_demand_raw %>%
      group_by(Region, Scenario, LULC) %>%
      summarise(
        conf_factor = mean(`Confidence factor`, na.rm = TRUE),
        init_rate = mean(`Initial rate`, na.rm = TRUE),
        .groups = "drop"
      )

    df_target_run <- df_slider_avg %>%
      left_join(df_initial, by = c("Region", "LULC")) %>%
      left_join(df_demand_extra, by = c("Region", "Scenario", "LULC")) %>%
      group_by(Region) %>%
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
      group_by(Region, Scenario) %>%
      mutate(
        total_init = sum(initial_amount),
        total_final_expert = sum(final_area_expert),
        final_area_expert_scaled = final_area_expert *
          (total_init / total_final_expert)
      ) %>%
      ungroup() %>%
      select(-regional_total_pixels)

    # 2. Recalculate shapes
    df_shape_run <- df_demand_raw %>%
      group_by(Region, Scenario, LULC) %>%
      summarise(all_curves = list(curve), .groups = "drop") %>%
      left_join(
        df_target_run %>%
          select(
            Region,
            Scenario,
            LULC,
            initial_amount,
            final_area_expert_scaled
          ),
        by = c("Region", "Scenario", "LULC")
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

    # 3. Run Parallel Orchestration
    results_list <- future_lapply(
      seq_len(nrow(task_grid)),
      function(k) {
        r <- task_grid$Region[k]
        s <- task_grid$Scenario[k]

        library(dplyr)
        library(CVXR)

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
          src <- trans %>% filter(Region == reg)
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
            fp <- f_pair %>% filter(Region == reg | is.na(Region))
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

        tryCatch(
          {
            # cat(sprintf("Worker solving %s - %s\n", r, s))
            df_init_w <- df_initial %>% filter(Region == r)
            df_tgt_w <- df_target_run %>% filter(Region == r, Scenario == s)
            df_shp_w <- df_shape_run %>% filter(Region == r, Scenario == s)

            lulcs <- sort(unique(df_init_w$LULC))
            L <- length(lulcs)

            init_amt <- df_init_w$initial_amount[match(lulcs, df_init_w$LULC)]
            final_tgt <- df_tgt_w$final_area_expert_scaled[match(
              lulcs,
              df_tgt_w$LULC
            )]
            region_area <- sum(init_amt)

            if (any(is.na(init_amt)) || any(is.na(final_tgt))) {
              return(NULL)
            }

            scale_fac <- region_area
            init_frac <- init_amt / scale_fac
            final_frac <- final_tgt / scale_fac

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
            devU_vars <- lapply(seq_len(T_steps_val), function(t) {
              Variable(L, L)
            })
            devL_vars <- lapply(seq_len(T_steps_val), function(t) {
              Variable(L, L)
            })

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
                lambda_val *
                  sum_entries(multiply(dev_weight, square(devU_vars[[t]])))
              )
              obj_terms <- c(
                obj_terms,
                lambda_val *
                  sum_entries(multiply(dev_weight, square(devL_vars[[t]])))
              )

              target_flow <- D_area %*% p_pref
              obj_terms <- c(
                obj_terms,
                eta_pref_val * sum_squares(x_t - target_flow)
              )

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
            con <- c(
              con,
              area[, T_steps_val + 1] >= pmax(0, final_frac - guard)
            )
            con <- c(
              con,
              area[, T_steps_val + 1] <= pmin(1, final_frac + guard)
            )

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

              cur_row <- df_shp_w %>% filter(LULC == lulcs[i])
              cur <- if (nrow(cur_row) > 0) {
                cur_row$chosen_shape
              } else {
                "Constant change"
              }
              if (is.na(cur)) {
                cur <- "Constant change"
              }

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
              is.null(res) ||
                !(res$status %in% c("optimal", "optimal_inaccurate"))
            ) {
              res <- tryCatch(
                solve(prob, solver = "ECOS", verbose = FALSE),
                error = function(e) NULL
              )
            }
            if (
              is.null(res) ||
                !(res$status %in% c("optimal", "optimal_inaccurate"))
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

            area_mat <- res$getValue(area) * scale_fac

            tibble(
              LULC = rep(lulcs, each = T_steps_val + 1),
              timeStep = rep(0:T_steps_val, times = L),
              area = as.vector(t(area_mat)),
              Year = year_steps[timeStep + 1],
              Region = r,
              Scenario = s
            )
          },
          error = function(e) {
            cat(sprintf("Error in %s - %s: %s\n", r, s, conditionMessage(e)))
            return(NULL)
          }
        )
      },
      future.seed = TRUE,
      future.packages = c("CVXR", "dplyr", "tibble", "purrr")
    )

    results_list <- Filter(Negate(is.null), results_list)

    if (length(results_list) > 0) {
      df_areas_final <- bind_rows(results_list)

      # --- Store for final comparison plot ---
      df_areas_final_tagged <- df_areas_final %>% mutate(Scalar_Value = s_val)
      results_all[[length(results_all) + 1]] <- df_areas_final_tagged

      # Export CSVs
      write.csv(
        df_areas_final,
        file.path(dir_name, "Out_Areas.csv"),
        row.names = FALSE
      )
      df_wide <- df_areas_final %>%
        select(Region, Scenario, LULC, Year, area) %>%
        pivot_wider(names_from = Year, values_from = area)
      write.csv(
        df_wide,
        file.path(dir_name, "Out_Areas_Wide.csv"),
        row.names = FALSE
      )

      # Overview plots per Region-Scenario
      plot_groups <- df_areas_final %>%
        group_by(Region, Scenario) %>%
        group_split()

      for (group_data in plot_groups) {
        current_region <- group_data$Region[1]
        current_scenario <- group_data$Scenario[1]

        # Calculate Stats for Legend Labels
        total_area <- sum(group_data$area[
          group_data$Year == min(group_data$Year)
        ])

        stats_df <- group_data %>%
          filter(Year %in% c(min(Year), max(Year))) %>%
          group_by(LULC, Year) %>%
          summarise(val = sum(area), .groups = "drop") %>%
          mutate(share = (val / total_area) * 100) %>%
          pivot_wider(
            id_cols = LULC,
            names_from = Year,
            values_from = share,
            names_prefix = "Y"
          )

        y_start <- paste0("Y", min(group_data$Year))
        y_end <- paste0("Y", max(group_data$Year))

        stats_df <- stats_df %>%
          mutate(
            label_str = sprintf(
              "%s (%.1f%% -> %.1f%%)",
              LULC,
              .data[[y_start]],
              .data[[y_end]]
            )
          )

        legend_labels <- setNames(stats_df$label_str, stats_df$LULC)

        # Handle missing keys for legend mapping
        missing_keys <- setdiff(names(lulc_pal), names(legend_labels))
        if (length(missing_keys) > 0) {
          temp_add <- setNames(missing_keys, missing_keys)
          legend_labels <- c(legend_labels, temp_add)
        }

        plot_title <- paste(
          "LULC Evolution:",
          current_region,
          "-",
          current_scenario,
          "(Scalar",
          s_val,
          "x)"
        )
        file_name <- paste0(
          current_region,
          "_",
          current_scenario,
          "_evolution.png"
        )

        p <- ggplot(
          group_data,
          aes(x = Year, y = area, color = LULC, group = LULC)
        ) +
          geom_line(linewidth = 1.2) +
          geom_point(size = 2.5, aes(shape = LULC)) +
          scale_color_manual(values = lulc_pal, labels = legend_labels) +
          scale_y_continuous(labels = scales::label_comma()) +
          labs(
            title = plot_title,
            subtitle = "Legend shows share of total area: Start -> End",
            x = "Year",
            y = "Area (pixels)"
          ) +
          theme_light() +
          theme(legend.position = "right")

        ggsave(
          file.path(dir_name, file_name),
          p,
          width = 12,
          height = 8,
          dpi = 300
        )
      }
      cat("Finished Scalar", s_val, "x\n")
    } else {
      cat("Scalar", s_val, "x FAILED completely.\n")
    }
  }

  # ================================
  # G. CLEANUP
  # ================================

  message("\n", paste(rep("=", 80), collapse = ""))
  future::plan(future::sequential)
  message("  ✓ Workers shutdown")

  message("\nSimulation Transition Rates Preparation Complete")
  message(sprintf("Output directory: %s", trans_rate_table_dir))

  invisible(NULL)
}


#' Clean numeric values from character strings
#' This function takes a character input, replaces commas with dots, and converts it to numeric. It is useful for cleaning numeric values that may have been read as characters due to formatting issues (e.g., "1,234" instead of "1.234").
#' @param x A character vector containing numeric values with potential formatting issues.
#' @return A numeric vector with cleaned values.
clean_numeric <- function(x) {
  x <- as.character(x)
  x <- gsub(",", ".", x)
  as.numeric(x)
}


#' Pick curve shape based on demand curves and initial/final areas
#' This function determines the most appropriate curve shape for a given LULC transition
#' based on the demand curve types provided by experts and the direction of change in area.
#' It uses a set of rules to prioritize certain curve types over others when there are multiple suggestions.
#' @param curves_vec A character vector of curve types suggested by experts for a particular transition.
#' @param initA The initial area of the LULC class.
#' @param finalA The final area of the LULC class according to the demand.
#' @return A single character string representing the chosen curve shape.
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

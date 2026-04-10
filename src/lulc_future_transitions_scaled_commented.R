##############################################################################
# SCRIPT: LULC Projection - Scalar Iteration (SELF-CONTAINED PARALLEL)
#         + Iterates over different slider scaling factors ("scalars")
#         + Solves one optimisation problem per Region–Scenario combination
#         + Uses historic transition rates and curve shapes as behavioural priors
##############################################################################

setwd(
  "Z:/NASCENT-Peru/04_workspaces/01_modelling/Future_LULC_transitions/Future_LULC_transitions"
)

# --- Packages ----------------------------------------------------------------
library(readxl) # Excel I/O for initial areas, demand and transitions
library(dplyr) # Data manipulation (filter, mutate, group_by, joins)
library(tidyr) # Reshaping data (pivot_wider, expand_grid)
library(CVXR) # Convex optimisation modelling and solving
library(stringr) # String handling (if needed)
library(ggplot2) # Plotting of LULC trajectories
library(purrr) # Functional tools (map / list-processing)
library(tibble) # Tidy tibble data frames
library(future.apply) # Parallel execution of per-task optimisation

# ================================
# CONFIGURATION
# ================================

# Soft bounds and numerical tolerances
margin_val <- 0.50 # symmetric slack added around historic rate bounds (relaxes min/max by this margin in rate space)
stay_min_val <- 0.10 # global minimum stay rate on the diagonal (minimum share of a class that remains in place)
stay_max_val <- 0.999 # global maximum stay rate on the diagonal (prevents almost-1.0 "locked" stays)
eps_ridge_val <- 1e-8 # small ridge weight on areas and flows for numerical stabilisation and tie-breaking
solver_eps_val <- 1e-5 # base tolerance passed to solvers for convergence criteria
monotone_tol_val <- 1e-3 # tolerance used when enforcing and interpreting monotonicity in share space

# Objective weights (relative priorities of penalties)
lambda_val <- 0.001 # weight for quadratic penalties on bound slack variables (rate-bound violations)
eta_pref_val <- 0.001 # weight for quadratic penalty pulling flows towards historic outflow proportions
rho_val <- 100 # weight for quadratic penalty on final areas versus expert targets
mu_val <- 5 # weight for quadratic penalty on curve shape slack variables
fair_weight_val <- 5 # weight for minimax fairness term on worst per-class shape distortion
kappa_zero_val <- 0.1 # linear penalty weight for flows along historically zero off-diagonal transitions

# Logical / structural configuration
zero_hist_thresh_val <- 1e-5 # threshold below which historic mean rate is treated as "zero" for zero-mask
beta_dev_val <- 1.0 # factor scaling bound-slack penalties by historic edge strength (stronger edges get larger penalties)
rel_guard_val <- 0.05 # relative width of guard band around final targets (allowed deviation from expert targets)
forbid_inflow_val <- c() # list of classes that may not receive inflow (except staying on the diagonal)

# Parallel setup
plan(multisession, workers = 6) # parallel back-end for per Region–Scenario optimisation

# ================================
# 1) Read & Clean Data (Static Inputs)
# ================================

# Map Spanish / variant LULC labels to a single canonical English naming scheme
recode_lulc_to_english <- function(x) {
  dplyr::recode(
    x,
    "Áreas forestales" = "Forested Areas",
    "Pastizales y matorrales naturales" = "Natural Grasslands and Shrublands",
    "Zonas agrícolas de baja intensidad" = "Low-Intensity Agricultural Areas",
    "Zonas agrícolas de alta intensidad" = "High-Intensity Agricultural Areas",
    "Terrenos edificados y baldíos" = "Built-Up and Barren Lands",
    "Minería" = "Mining",
    "Forested areas" = "Forested Areas", # minor English variant
    .default = x
  )
}

# Convert locale-specific numeric strings (comma as decimal separator) to numeric
clean_numeric <- function(x) {
  x <- as.character(x)
  x <- gsub(",", ".", x) # replace comma with dot for decimal
  as.numeric(x)
}

# Input filenames
file_initial <- "LULC_initial_amount.xlsx" # initial LULC areas per Region–LULC
file_demand <- "LULC_demand_results.xlsx" # expert slider settings, curve labels, confidence, initial rate
file_trans_multi <- "LULC_transition_rates_Singlestep.xlsx" # historic multi-period transition rates

# Initial areas per Region–LULC, with harmonised class names
df_initial <- read_xlsx(file_initial) %>%
  mutate(LULC = recode_lulc_to_english(LULC))

# Demand table: sliders, confidence factors, initial rate, curve labels and harmonised LULC names
df_demand_raw <- read_xlsx(file_demand) %>%
  mutate(
    `Slider value` = clean_numeric(`Slider value`), # numeric slider strength per expert input
    `Confidence factor` = clean_numeric(`Confidence factor`), # sensitivity multiplier derived from forecast uncertainty
    `Initial rate` = clean_numeric(`Initial rate`), # baseline change rate from historical trend
    # Harmonise curve labels across Spanish and English variants
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
    LULC = recode_lulc_to_english(LULC)
  )

# Historic transition rates per Region–From–To, with harmonised LULC names
df_trans_mi <- read_xlsx(file_trans_multi) %>%
  rename(iLULC = From, jLULC = To) %>%
  mutate(
    iLULC = recode_lulc_to_english(iLULC),
    jLULC = recode_lulc_to_english(jLULC)
  )

# Identify all columns containing rate samples (e.g. Rate_1986_1990, ...)
rate_cols <- grep("^Rate_", names(df_trans_mi), value = TRUE)

# Global list of canonical LULC classes present in initial table
lulcs_global <- sort(unique(df_initial$LULC))

# Forbidden pairs: Mining is treated as a pure sink, so Mining → anything else is forbidden
forbid_pairs_df <- tidyr::expand_grid(
  Region = NA_character_, # NA = applies to all regions
  From = "Mining",
  To = setdiff(lulcs_global, "Mining") # all other classes except Mining itself
) %>%
  tibble::as_tibble()

# Build historic min / max / mean transition rates per Region–iLULC–jLULC
df_trans_source <- df_trans_mi %>%
  mutate(across(all_of(rate_cols), as.numeric)) %>%
  rowwise() %>%
  mutate(
    # sample-wise statistics across all historic rate columns
    minRate = min(c_across(all_of(rate_cols)), na.rm = TRUE),
    maxRate = max(c_across(all_of(rate_cols)), na.rm = TRUE),
    meanRate = mean(c_across(all_of(rate_cols)), na.rm = TRUE)
  ) %>%
  ungroup() %>%
  mutate(
    # clip to [0, 1] and fall back to neutral defaults if no finite values exist
    minRate = ifelse(is.finite(minRate), pmax(0, pmin(1, minRate)), 0),
    maxRate = ifelse(is.finite(maxRate), pmax(0, pmin(1, maxRate)), 1),
    meanRate = ifelse(is.finite(meanRate), pmax(0, pmin(1, meanRate)), 0),
    maxRate = pmax(maxRate, minRate) # ensure upper bound is never below lower bound
  ) %>%
  select(Region, iLULC, jLULC, minRate, maxRate, meanRate)

# Global time grid for the optimisation (shared across all regions and scenarios)
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
step_length <- diff(year_steps) # length (in years) of each time step
T_steps_val <- length(step_length) # number of transition periods

# Shape selection: map multiple expert curve labels to a single effective shape
pick_shape <- function(curves_vec, initA, finalA) {
  # Resolve ties between candidate shapes using a priority rule
  choose_winner <- function(winners, default) {
    winners <- na.omit(winners)
    if (length(winners) == 0) {
      return(default)
    }
    if (length(winners) == 1) {
      return(winners)
    }
    # If there are exactly two shapes and one is "Constant change", prefer the non-constant one
    if ("Constant change" %in% winners && length(winners) == 2) {
      return(setdiff(winners, "Constant change"))
    }
    # Otherwise prefer any non-constant shape, fall back to first if all are constant
    nonconst <- setdiff(winners, "Constant change")
    if (length(nonconst) > 0) {
      return(nonconst[1])
    }
    return(winners[1])
  }

  # Net increase: restrict to growth-type shapes
  if (finalA > initA) {
    opts <- c("Constant change", "Instant growth", "Delayed growth")
    sub <- curves_vec[curves_vec %in% opts]
    if (!length(sub)) {
      return("Constant change")
    }
    tab <- table(sub)
    for (k in opts) {
      if (!k %in% names(tab)) tab[k] <- 0
    }
    return(choose_winner(names(tab)[tab == max(tab)], "Constant change"))
  }

  # Net decrease: restrict to decline-type shapes
  if (finalA < initA) {
    opts <- c("Constant change", "Instant decline", "Delayed decline")
    sub <- curves_vec[curves_vec %in% opts]
    if (!length(sub)) {
      return("Constant change")
    }
    tab <- table(sub)
    for (k in opts) {
      if (!k %in% names(tab)) tab[k] <- 0
    }
    return(choose_winner(names(tab)[tab == max(tab)], "Constant change"))
  }

  # No net change: enforce a constant trajectory
  "Constant change"
}

# ================================
# 2) Master Execution Loop (Scalars)
# ================================

# List of slider scaling factors; each run multiplies slider strength by s_val
scalars <- seq(3, 9.0, by = 2)

# Cartesian grid of tasks: one optimisation run per Region–Scenario
task_grid <- expand.grid(
  Region = sort(unique(df_initial$Region)),
  Scenario = sort(unique(df_demand_raw$Scenario)),
  stringsAsFactors = FALSE
)

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

  # 1. Recalculate targets for this scalar: final areas implied by slider inputs,
  #    confidence factors, initial rates and the scalar multiplier
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

  # Per-class demand inputs (confidence factor and initial rate) at Region–Scenario–LULC level
  df_demand_extra <- df_demand_raw %>%
    group_by(Region, Scenario, LULC) %>%
    summarise(
      conf_factor = mean(`Confidence factor`, na.rm = TRUE), # sensitivity from forecast uncertainty
      init_rate = mean(`Initial rate`, na.rm = TRUE), # baseline trend-derived rate
      .groups = "drop"
    )

  # Final expert targets per Region–Scenario–LULC for this scalar
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
      # Slider adjustment scaled by scalar s_val, confidence factor and baseline magnitude
      slider_adjustment = abs(init_rate) *
        ((2 * (mean_slider * s_val)) / 100) *
        conf_factor,
      final_percent = initial_percent + init_rate + slider_adjustment,
      # Convert final percentage back to absolute area, enforcing non-negativity
      final_area_expert = pmax(0, (final_percent / 100) * regional_total_pixels)
    ) %>%
    group_by(Region, Scenario) %>%
    mutate(
      # Rescale final expert areas so that total area per Region–Scenario
      # matches the initial regional total exactly
      total_init = sum(initial_amount),
      total_final_expert = sum(final_area_expert),
      final_area_expert_scaled = final_area_expert *
        (total_init / total_final_expert)
    ) %>%
    ungroup() %>%
    select(-regional_total_pixels)

  # 2. Recalculate effective shapes per Region–Scenario–LULC for this scalar
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

  # 3. Run Parallel Orchestration (Self-Contained Logic) – defined in the next chunk
  #    One worker per Region–Scenario, using df_target_run and df_shape_run as inputs
  # 3. Run Parallel Orchestration (Self-Contained Logic)
  results_list <- future_lapply(
    seq_len(nrow(task_grid)),
    function(k) {
      # --- START WORKER LOGIC ---
      # Extract Region–Scenario task for this worker
      r <- task_grid$Region[k]
      s <- task_grid$Scenario[k]

      # Load required packages inside worker environment
      library(dplyr)
      library(CVXR)

      # --- Helper inside worker: builds all rate and preference matrices ---
      build_mats <- function(
        reg, # current region
        cls, # vector of LULC class labels used in this optimisation
        trans, # historic transition table (df_trans_source)
        s_min, # global minimum stay rate
        s_max, # global maximum stay rate
        beta, # scale factor for dev_weight (edge strength)
        z_thresh, # threshold below which mean rate is treated as zero
        f_in, # classes forbidden to receive inflow
        f_pair # explicit From–To forbidden transitions
      ) {
        # Subset historic transitions to this region
        src <- trans %>% filter(Region == reg)

        # Restrict to classes actually used in this optimisation
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

        # Number of classes
        L <- length(cls)

        # Initialise dense matrices for bounds and historic means
        rmin_mat <- matrix(0, nrow = L, ncol = L) # lower rate bounds
        rmax_mat <- matrix(1, nrow = L, ncol = L) # upper rate bounds
        mean_mat <- matrix(0, nrow = L, ncol = L) # mean historic rates

        # Insert historic min / max / mean into the appropriate matrix entries
        if (nrow(src_clean) > 0) {
          i_idx <- match(src_clean$iLULC, cls)
          j_idx <- match(src_clean$jLULC, cls)
          rmin_mat[cbind(i_idx, j_idx)] <- src_clean$minRate
          rmax_mat[cbind(i_idx, j_idx)] <- src_clean$maxRate
          mean_mat[cbind(i_idx, j_idx)] <- src_clean$meanRate
        }

        # Apply global diagonal stay bounds (stay_min, stay_max)
        diag(rmin_mat) <- pmax(diag(rmin_mat), s_min)
        diag(rmax_mat) <- pmin(diag(rmax_mat), s_max)

        # Hard forbid matrix: TRUE where flows are structurally impossible
        hard_forbid <- matrix(FALSE, L, L)

        # Forbid inflows into certain classes (entire column forbidden)
        if (length(f_in) > 0) {
          j_idx <- match(f_in, cls)
          j_idx <- j_idx[!is.na(j_idx)]
          for (j in j_idx) {
            hard_forbid[, j] <- TRUE
          }
        }

        # Apply explicit From–To forbid pairs, global or region-specific
        if (!is.null(f_pair) && nrow(f_pair) > 0) {
          fp <- f_pair %>% filter(Region == reg | is.na(Region))
          if (nrow(fp) > 0) {
            ii <- match(fp$From, cls)
            jj <- match(fp$To, cls)
            valid <- which(!is.na(ii) & !is.na(jj))
            hard_forbid[cbind(ii[valid], jj[valid])] <- TRUE
          }
        }

        # Enforce zero bounds on hard-forbidden entries
        rmin_mat[hard_forbid] <- 0
        rmax_mat[hard_forbid] <- 0

        # Build historic outflow preference matrix p_pref (row-stochastic)
        # Slight smoothing on the diagonal and everywhere to avoid zero rows
        eps_diag <- 1e-6
        eps_uni <- 1e-9
        mean_smoothed <- mean_mat
        diag(mean_smoothed) <- pmax(diag(mean_smoothed), eps_diag)
        mean_smoothed <- mean_smoothed + eps_uni

        row_sums <- rowSums(mean_smoothed)
        row_sums[row_sums <= 0] <- 1
        p_mat <- sweep(mean_smoothed, 1, row_sums, "/") # normalise rows to 1

        # dev_weight scales rate-bound slack penalties for strong historic edges
        dev_weight <- 1 + beta * p_mat

        # zero_mask marks historically near-zero off-diagonal transitions
        zero_mask <- (mean_mat <= z_thresh) & (row(mean_mat) != col(mean_mat))

        # Ensure upper bounds are at least as big as lower bounds everywhere
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

      # --- SOLVER LOGIC ---
      tryCatch(
        {
          cat(sprintf("Worker solving %s - %s\n", r, s))

          # Filter static and scalar-dependent inputs for this Region–Scenario
          df_init_w <- df_initial %>% filter(Region == r)
          df_tgt_w <- df_target_run %>% filter(Region == r, Scenario == s)
          df_shp_w <- df_shape_run %>% filter(Region == r, Scenario == s)

          # LULC backbone and dimension
          lulcs <- sort(unique(df_init_w$LULC))
          L <- length(lulcs)

          # Initial and final expert areas (absolute) in canonical order
          init_amt <- df_init_w$initial_amount[match(lulcs, df_init_w$LULC)]
          final_tgt <- df_tgt_w$final_area_expert_scaled[match(
            lulcs,
            df_tgt_w$LULC
          )]
          region_area <- sum(init_amt)

          # Abort this task if any required data is missing
          if (any(is.na(init_amt)) || any(is.na(final_tgt))) {
            return(NULL)
          }

          # Work in shares during optimisation and rescale later to areas
          scale_fac <- region_area
          init_frac <- init_amt / scale_fac
          final_frac <- final_tgt / scale_fac

          # Build bound and preference matrices for this Region–Scenario–LULC set
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

          rmin_mat <- mats$rmin # lower rate bounds
          rmax_mat <- mats$rmax # upper rate bounds
          p_pref <- mats$p_pref # historic outflow proportions
          dev_weight <- mats$dev_weight # strength-weighted slack scaling
          zero_mask <- mats$zero_mask # historic zero pattern
          is_allowed <- matrix(TRUE, L, L)
          is_allowed[mats$hard_forbid] <- FALSE
          is_allowed[(rmax_mat == 0) & (rmin_mat == 0)] <- FALSE

          # --- Define CVXR decision variables and constraints ---

          # Area shares per class and time step (L x (T_steps_val + 1))
          area <- Variable(L, T_steps_val + 1)

          # Flow matrices and slack variables per time step
          x_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))
          devU_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))
          devL_vars <- lapply(seq_len(T_steps_val), function(t) Variable(L, L))

          con <- list() # constraint list
          obj_terms <- list() # list of objective components

          # Initial condition and total mass conservation over classes
          con <- c(con, area[, 1] == init_frac)
          con <- c(con, sum_entries(area, axis = 2) == 1) # sum over classes = 1 at each time
          con <- c(con, area >= 0)

          # Time step loop: flow conservation, rate bounds, historic preferences
          for (t in seq_len(T_steps_val)) {
            x_t <- x_vars[[t]] # flows at time step t
            curr_area <- area[, t] # shares at start of step t
            next_area <- area[, t + 1] # shares at end of step t

            # Outgoing flows per class must equal its current area (row sums)
            row_sums <- reshape_expr(sum_entries(x_t, axis = 1), c(L, 1))
            con <- c(con, row_sums == curr_area)

            # Incoming flows per class must equal its next-step area (column sums)
            col_sums <- reshape_expr(sum_entries(x_t, axis = 2), c(L, 1))
            con <- c(con, col_sums == next_area)

            # Non-negativity of flows
            con <- c(con, x_t >= 0)

            # Force zero flow on globally or structurally disallowed edges
            if (any(!is_allowed)) {
              con <- c(con, multiply(x_t, Constant(1 - is_allowed)) == 0)
            }

            # Rate bounds with slack variables (upper and lower deviations)
            D_area <- diag(curr_area) # diagonal matrix of current shares

            # Upper bound: x_t <= D_area * (rmax + margin) + devU
            bound_U <- D_area %*% (rmax_mat + margin_val) + devU_vars[[t]]
            con <- c(con, x_t <= bound_U)

            # Lower bound: x_t >= D_area * (rmin - margin) - devL
            bound_L <- D_area %*% (rmin_mat - margin_val) - devL_vars[[t]]
            con <- c(con, x_t >= bound_L)

            # Quadratic penalties on bound slacks, weighted by dev_weight
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

            # Historic outflow preference: pull each row of x_t towards curr_area * p_pref
            target_flow <- D_area %*% p_pref
            obj_terms <- c(
              obj_terms,
              eta_pref_val * sum_squares(x_t - target_flow)
            )

            # Penalise flows along historically zero off-diagonal transitions
            if (any(zero_mask)) {
              obj_terms <- c(
                obj_terms,
                kappa_zero_val *
                  sum_entries(multiply(x_t, Constant(1.0 * zero_mask)))
              )
            }

            # Ridge regularisation on flows of this time step
            obj_terms <- c(obj_terms, eps_ridge_val * sum_squares(x_t))
          }

          # Terminal fit: penalise squared deviation from target final shares
          obj_terms <- c(
            obj_terms,
            rho_val * sum_squares(area[, T_steps_val + 1] - final_frac)
          )

          # Guard band around target final shares (relative tolerance)
          guard <- rel_guard_val * final_frac
          con <- c(
            con,
            area[, T_steps_val + 1] >= pmax(0, final_frac - guard)
          )
          con <- c(
            con,
            area[, T_steps_val + 1] <= pmin(1, final_frac + guard)
          )

          # Ridge regularisation on area variables
          obj_terms <- c(obj_terms, eps_ridge_val * sum_squares(area))
          # --- Monotonicity constraints on area trajectories ---
          for (i in seq_len(L)) {
            # Differences between consecutive time steps for class i
            row_vals <- area[i, ]
            diffs <- row_vals[2:(T_steps_val + 1)] - row_vals[1:T_steps_val]

            # Decide direction based on net change between initial and final share
            if (final_frac[i] > init_frac[i] + 1e-9) {
              # Net increase, enforce non-decreasing path
              con <- c(con, diffs >= 0)
            } else if (final_frac[i] < init_frac[i] - 1e-9) {
              # Net decrease, enforce non-increasing path
              con <- c(con, diffs <= 0)
            } else {
              # No net change, enforce flat path
              con <- c(con, diffs == 0)
            }
          }

          # --- Shape constraints and per class shape distortion ---
          class_sq_sum <- vector("list", L) # stores weighted shape cost per class
          concave_shapes <- c("Instant growth", "Delayed decline")
          convex_shapes <- c("Delayed growth", "Instant decline")

          for (i in seq_len(L)) {
            # Initialise shape cost for class i to zero
            class_sq_sum[[i]] <- Constant(0)

            # Skip shape constraints if class does not change in net terms
            if (abs(final_frac[i] - init_frac[i]) < 1e-6) {
              next
            }

            # Determine chosen shape label for this class in this Region–Scenario
            cur_row <- df_shp_w %>% filter(LULC == lulcs[i])
            cur <- if (nrow(cur_row) > 0) {
              cur_row$chosen_shape
            } else {
              "Constant change"
            }
            if (is.na(cur)) {
              cur <- "Constant change"
            }

            # Number of curvature constraints equals number of interior time points
            n_slopes <- T_steps_val - 1
            # Slack vector for shape deviations at interior steps
            slacks_vec <- Variable(n_slopes)
            con <- c(con, slacks_vec >= 0)

            # Enforce local curvature pattern based on the chosen shape type
            for (t_idx in 1:n_slopes) {
              a_prev <- area[i, t_idx]
              a_curr <- area[i, t_idx + 1]
              a_next <- area[i, t_idx + 2]

              # Discrete slopes across adjacent time intervals
              d1 <- (a_curr - a_prev) / step_length[t_idx]
              d2 <- (a_next - a_curr) / step_length[t_idx + 1]

              if (cur == "Constant change") {
                # Penalise deviations from constant slope
                con <- c(
                  con,
                  d1 - d2 <= slacks_vec[t_idx],
                  d2 - d1 <= slacks_vec[t_idx]
                )
              } else if (cur %in% convex_shapes) {
                # Convex pattern, enforce non-decreasing slopes (d2 >= d1)
                con <- c(con, d1 - d2 <= slacks_vec[t_idx])
              } else if (cur %in% concave_shapes) {
                # Concave pattern, enforce non-increasing slopes (d2 <= d1)
                con <- c(con, d2 - d1 <= slacks_vec[t_idx])
              }
            }

            # Weight shape cost by inverse square root of initial share
            # so smaller classes contribute relatively more to the fairness measure
            w_i <- 1 / sqrt(pmax(init_frac[i], 1e-9))
            class_sq_sum[[i]] <- w_i * sum_squares(slacks_vec)
          }

          # Aggregate weighted shape distortions across classes
          sum_all_shape <- Reduce(`+`, class_sq_sum)
          obj_terms <- c(obj_terms, mu_val * sum_all_shape)

          # Minimax fairness variable tau_shape tracks worst class shape cost
          tau_shape <- Variable(1)
          con <- c(con, tau_shape >= 0)
          for (i in seq_len(L)) {
            con <- c(con, tau_shape >= class_sq_sum[[i]])
          }
          # Add fairness term that penalises the maximum shape distortion
          obj_terms <- c(obj_terms, fair_weight_val * tau_shape)

          # Combine all objective components into a single scalar expression
          prob <- Problem(Minimize(Reduce(`+`, obj_terms)), con)

          # --- Solver cascade: OSQP → ECOS → SCS ---

          # First attempt with OSQP, using configured tolerances
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

          # If OSQP fails or does not report an optimal status, try ECOS
          if (
            is.null(res) ||
              !(res$status %in% c("optimal", "optimal_inaccurate"))
          ) {
            res <- tryCatch(
              solve(prob, solver = "ECOS", verbose = FALSE),
              error = function(e) NULL
            )
          }

          # If ECOS also fails, fall back to SCS with looser tolerances
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

          # Abort this Region–Scenario if no solver succeeded
          if (is.null(res)) {
            stop("Solver failed completely")
          }

          # Extract area trajectories in shares and rescale back to pixel counts
          area_mat <- res$getValue(area) * scale_fac

          # Return tidy table with full time series for this Region–Scenario
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
          # On error, log and return NULL so this task can be filtered out
          cat(sprintf("Error in %s - %s: %s\n", r, s, conditionMessage(e)))
          return(NULL)
        }
      )
      # --- END WORKER LOGIC ---
    },
    future.seed = TRUE,
    future.packages = c("CVXR", "dplyr", "tibble", "purrr")
  )
  # Filter out failed Region–Scenario tasks and keep only successful results
  results_list <- Filter(Negate(is.null), results_list)

  if (length(results_list) > 0) {
    # Combine all Region–Scenario area trajectories into one table for this scalar
    df_areas_final <- bind_rows(results_list)

    # ---------------------------------
    # Export area trajectories (long)
    # ---------------------------------
    write.csv(
      df_areas_final,
      file.path(dir_name, "Out_Areas.csv"),
      row.names = FALSE
    )

    # ---------------------------------
    # Export area trajectories (wide, one column per year)
    # ---------------------------------
    df_wide <- df_areas_final %>%
      select(Region, Scenario, LULC, Year, area) %>%
      pivot_wider(names_from = Year, values_from = area)

    write.csv(
      df_wide,
      file.path(dir_name, "Out_Areas_Wide.csv"),
      row.names = FALSE
    )

    # ---------------------------------
    # Overview plots per Region–Scenario
    # ---------------------------------
    # Group full output by Region and Scenario for multi class overview plots
    plot_groups <- df_areas_final %>%
      group_by(Region, Scenario) %>%
      group_split()

    for (group_data in plot_groups) {
      current_region <- group_data$Region[1]
      current_scenario <- group_data$Scenario[1]

      # Title includes Region, Scenario and scalar multiplier for visual context
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

      # Multi line overview: one line per LULC class over the full time grid
      p <- ggplot(
        group_data,
        aes(x = Year, y = area, color = LULC, group = LULC)
      ) +
        geom_line(linewidth = 1.2) +
        geom_point(size = 2.5, aes(shape = LULC)) +
        scale_y_continuous(labels = scales::label_comma()) +
        labs(
          title = plot_title,
          x = "Year",
          y = "Area (pixels)"
        ) +
        theme_light()

      # Save overview plot into the output folder for this scalar
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
    # If all Region–Scenario tasks failed for this scalar, log and continue
    cat("Scalar", s_val, "x FAILED completely.\n")
  }
}

# Final message after all scalar runs and all Region–Scenario combinations
cat("\nALL RUNS COMPLETE.\n")

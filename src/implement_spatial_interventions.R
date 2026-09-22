#' @title Implement Spatial Interventions on per-transition Probabilities
#' @description
#' Implement all specified spatial interventions on the long-format
#' data.table of per-transition probabilities.
#' @param normalized data.table with columns row_idx, from_val, to_val,
#'   cell_id, x, y, prob. Modified in place.
#' @param anterior SpatRaster of current LULC (reserved for future use).
#' @param trans_rates_dt data.table with From*, To*, id_trans, row_idx
#'   (reserved for future use).
#' @param class_name_to_value named integer vector mapping
#'   lulc_schema class_name to raster integer value.
#' @param interventions_dir Directory containing YAML files with intervention definitions.
#' @param scenario Identifier for the scenario to apply interventions.
#' @param simulation_time_step The time step at which to apply the interventions.
#' @param log_file Path to per-region log file used by log_msg(...).
#' @return The `normalized` data.table with updated probabilities.
implement_spatial_interventions <- function(
  normalized,
  anterior,
  trans_rates_dt,
  class_name_to_value,
  interventions_dir,
  scenario,
  simulation_time_step,
  log_file
) {
  # Load interventions for scenario from YAML file
  Interventions <- yaml::yaml.load_file(file.path(
    interventions_dir,
    paste0(scenario, "_interventions.yml")
  ))

  # filter to Intervention_stage == Allocation
  Current_interventions <- Interventions[sapply(Interventions, function(x) {
    x[["Intervention_stage"]] == "Allocation"
  })]

  # Subset to only interventions for which simulation_time_step is in Time_steps_implemented
  Current_interventions <- Current_interventions[c(sapply(
    Current_interventions,
    function(x) simulation_time_step %in% x$Time_steps_implemented
  ))]

  # If no interventions are found, return the normalized DT unchanged
  if (length(Current_interventions) == 0) {
    log_msg(
      "No interventions found for the specified scenario and time step. Returning original probabilities.",
      log_file
    )
    return(normalized)
  } else {
    log_msg(
      paste(
        "Found",
        length(Current_interventions),
        "allocation stage interventions for scenario",
        scenario,
        "at time step",
        simulation_time_step
      ),
      log_file
    )

    # order interventions by Intervention_ranking putting NAs last
    Current_interventions <- Current_interventions[order(
      sapply(Current_interventions, function(x) {
        if (is.null(x$Intervention_ranking)) {
          return(NA) # Handle cases where Intervention_ranking is NULL
        } else {
          return(as.numeric(x$Intervention_ranking))
        }
      }),
      na.last = TRUE
    )]

    # loop over interventions
    for (intervention in Current_interventions) {
      # Print intervention name
      log_msg(
        paste("Applying intervention:", intervention[["Intervention_ID"]]),
        log_file
      )

      # Translate Transition_target_classes (class_name strings) to integer
      # to_val values via lulc_schema.
      Target_classes <- as.integer(
        class_name_to_value[unlist(intervention[["Transition_target_classes"]])]
      )
      if (any(is.na(Target_classes))) {
        stop(paste(
          "Unknown class_name in Transition_target_classes:",
          paste(intervention[["Transition_target_classes"]], collapse = ", ")
        ))
      }

      # Prepare the Intervention mask
      if (intervention$Mask_type == "Static") {
        # load mask using the Intervention_masks
        Intervention_mask <- terra::rast(intervention[["Intervention_mask"]])
      } else if (intervention$Mask_type == "Dynamic") {
        # subset the list of intervention$Intervention_mask to the current simulation_time_step
        # This assumes that the Intervention_mask is a list with keys corresponding to time steps
        intervention_mask_list <- intervention[["Intervention_mask"]]
        intervention_mask_path <- intervention_mask_list[[
          as.character(simulation_time_step)
        ]]
        if (is.null(intervention_mask_path)) {
          log_msg(
            paste(
              "Dynamic mask has no entry for year",
              simulation_time_step,
              "- skipping intervention."
            ),
            log_file
          )
          next
        }
        # load mask using the Intervention_mask_path appending simulation_time_step
        Intervention_mask <- terra::rast(intervention_mask_path)
      } else {
        stop(paste("Unknown Mask_type:", intervention[["Mask_type"]]))
      }

      # If the Intervention requires filtering by LULC classes then translate
      # the From_lulc_filter class names to integer from_val values, to be
      # passed through to the helpers (which apply the filter on the long DT).
      From_filter_vals <- NULL
      if (
        !is.null(intervention$From_lulc_filter) &&
          length(intervention$From_lulc_filter) > 0 &&
          all(intervention$From_lulc_filter != "None")
      ) {
        From_filter_vals <- as.integer(
          class_name_to_value[unlist(intervention[["From_lulc_filter"]])]
        )
        if (any(is.na(From_filter_vals))) {
          stop(paste(
            "Unknown class_name in From_lulc_filter:",
            paste(intervention[["From_lulc_filter"]], collapse = ", ")
          ))
        }
        log_msg(
          paste(
            "Filtering to only cells that are currently LULC class values:",
            paste(From_filter_vals, collapse = ", ")
          ),
          log_file
        )
      }

      # Apply different functions based on whether the intervention specifies absolute or relative adjustments to probabilities
      # if intervention$Prob_adjust_type == Absolute then apply absolute adjustment function
      if (intervention$Prob_adjust_type == "Absolute") {
        log_msg(
          paste(
            "Applying absolute probability adjustment to cells:",
            intervention[["Prob_adjust_zone"]],
            "the intervention area, adjusting probability values to:",
            intervention[["Prob_adjust_value"]]
          ),
          log_file
        )

        normalized <- absolute_prob_adjust(
          normalized = normalized,
          Prob_adjust_zone = intervention$Prob_adjust_zone,
          Prob_adjust_value = intervention$Prob_adjust_value,
          Target_classes = Target_classes,
          From_filter_vals = From_filter_vals,
          Intervention_mask = Intervention_mask
        )
      } else if (intervention$Prob_adjust_type == "Relative") {
        # convert percentile values to numeric and decimal
        intervention[[
          "Prob_adjust_intervention_percentile"
        ]] <- as.numeric(intervention[[
          "Prob_adjust_intervention_percentile"
        ]]) /
          100
        intervention[[
          "Prob_adjust_non_intervention_percentile"
        ]] <- as.numeric(intervention[[
          "Prob_adjust_non_intervention_percentile"
        ]]) /
          100

        # Apply relative adjustment function
        normalized <- relative_prob_adjust(
          Prob_adjust_valency = intervention[["Prob_adjust_valency"]],
          Prob_adjust_intervention_percentile = intervention[[
            "Prob_adjust_intervention_percentile"
          ]],
          Prob_adjust_non_intervention_percentile = intervention[[
            "Prob_adjust_non_intervention_percentile"
          ]],
          Prob_adjust_threshold = intervention[["Prob_adjust_threshold"]],
          Prob_adjust_zone = intervention[["Prob_adjust_zone"]],
          Target_classes = Target_classes,
          From_filter_vals = From_filter_vals,
          Intervention_mask = Intervention_mask,
          normalized = normalized
        )
      } else {
        stop(paste(
          "Unknown Prob_adjust_type:",
          intervention[["Prob_adjust_type"]]
        ))
      }
    } # end of intervention loop
  }

  # return the updated normalized data.table
  return(normalized)
}

#' @title Perform absolute adjustment of probabilities of change in target classes
#' @description
#' Perform absolute adjustment of probabiltieis of change in target classes
#' either inside or outside an intervention mask.
#' @param normalized A long-format data.table with columns to_val, from_val, x, y, prob.
#' @param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Prob_adjust_value A numeric value to set the probabilities in the target area.
#' @param Target_classes An integer vector of target to_val class values.
#' @param From_filter_vals Optional integer vector of from_val classes to restrict to.
#' @param Intervention_mask A SpatRaster representing the intervention mask.
#' @return The `normalized` data.table with updated probabilities for the target rows.
absolute_prob_adjust <- function(
  normalized,
  Prob_adjust_zone = "Outside",
  Prob_adjust_value = 0.1,
  Target_classes,
  From_filter_vals = NULL,
  Intervention_mask
) {
  # loop over the target classes
  for (lulc_class in Target_classes) {
    cat(paste(
      "Adjusting pixels values of class:",
      lulc_class,
      ",",
      Prob_adjust_zone,
      "mask to:",
      Prob_adjust_value,
      "\n"
    ))

    # Subset to rows of this target class (and from-filter if applicable)
    hit <- normalized$to_val == lulc_class
    if (!is.null(From_filter_vals)) {
      hit <- hit & (normalized$from_val %in% From_filter_vals)
    }
    sub_idx <- which(hit)
    if (length(sub_idx) == 0L) next

    # Sample the intervention mask at the sparse (x, y) points of these rows
    mask_vals <- terra::extract(
      Intervention_mask,
      as.matrix(normalized[sub_idx, .(x, y)])
    )[, 1]

    # If Prob_adjust_zone is Inside, then target the inside-mask rows
    if (Prob_adjust_zone == "Inside") {
      Target_area_idx <- sub_idx[!is.na(mask_vals) & mask_vals == 1L]
    } else if (Prob_adjust_zone == "Outside") {
      # invert the mask to get the non-intersecting area
      Target_area_idx <- sub_idx[is.na(mask_vals) | mask_vals != 1L]
    }

    # Adjust the probabilities in the target area
    ix <- Target_area_idx[normalized$prob[Target_area_idx] > 0]
    normalized[ix, prob := Prob_adjust_value]

    # set any values that are greater than 1 to 1
    normalized[prob > 1, prob := 1]
    # set any values that are less than 0 to 0 excluding NAs
    normalized[!is.na(prob) & prob < 0, prob := 0]
  }

  return(normalized)
}

#' @title Perform relative probability adjustment for target land use classes
#' @description
#' Perform relative probability adjustment for target lulc classes based upon
#' the % difference in average probabilities above specified percentiles for
#' the intervention and non-intervention pixels with the option to specify
#' target pixels as those outside or inside the intervention mask areas
#' @param Prob_adjust_valency A string indicating the valency of the adjustment, either "Increase", "Decrease" or "Increase_inside_decrease_outside".
#' @param Prob_adjust_intervention_percentile A numeric value indicating the percentile for the intervention area.
#' @param Prob_adjust_non_intervention_percentile A numeric value indicating the percentile for the non-intervention area.
#' @param Prob_adjust_threshold A numeric value indicating the threshold for the percentage difference.
#' @param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Target_classes An integer vector of target to_val class values.
#' @param From_filter_vals Optional integer vector of from_val classes to restrict to.
#' @param Intervention_mask A SpatRaster representing the intervention mask.
#' @param normalized A long-format data.table with columns to_val, from_val, x, y, prob.
#' @return The `normalized` data.table with updated probabilities for the target rows.
relative_prob_adjust <- function(
  Prob_adjust_valency,
  Prob_adjust_intervention_percentile,
  Prob_adjust_non_intervention_percentile,
  Prob_adjust_threshold,
  Prob_adjust_zone,
  Target_classes,
  From_filter_vals = NULL,
  Intervention_mask,
  normalized
) {
  # check that of Prob_adjust_valency == Increase_inside_decrease_outside that Prob_adjust_zone is "Inside"
  if (
    Prob_adjust_valency == "Increase_inside_decrease_outside" &&
      Prob_adjust_zone != "Inside"
  ) {
    stop(
      "If Prob_adjust_valency is 'Increase_inside_decrease_outside', then Prob_adjust_zone must be 'Inside'."
    )
  }

  # loop over the target classes
  for (lulc_class in Target_classes) {
    # Subset to rows of this target class (and from-filter if applicable)
    hit <- normalized$to_val == lulc_class
    if (!is.null(From_filter_vals)) {
      hit <- hit & (normalized$from_val %in% From_filter_vals)
    }
    sub_idx <- which(hit)
    if (length(sub_idx) == 0L) next

    # Sample the intervention mask at the sparse (x, y) points of these rows
    mask_vals <- terra::extract(
      Intervention_mask,
      as.matrix(normalized[sub_idx, .(x, y)])
    )[, 1]
    inside_flag <- !is.na(mask_vals) & mask_vals == 1L

    # If Prob_adjust_zone is Inside, then the intervention area is inside the mask and non-intersecting area is outside the mask
    if (Prob_adjust_zone == "Inside") {
      Intervention_idx <- sub_idx[inside_flag]
      Non_intervention_idx <- sub_idx[!inside_flag]
    } else if (Prob_adjust_zone == "Outside") {
      # if the Prob_adjust_zone is Outside, then the intervention area is outside the mask and the non-intersecting area is inside the mask
      Intervention_idx <- sub_idx[!inside_flag]
      Non_intervention_idx <- sub_idx[inside_flag]
    }

    # seperate raster values
    Intervention_vals <- normalized$prob[Intervention_idx]
    Non_Intervention_vals <- normalized$prob[Non_intervention_idx]

    if (length(Intervention_vals) == 0L || length(Non_Intervention_vals) == 0L) {
      next
    }

    # get percentile values
    Intervention_ptile_val <- quantile(
      Intervention_vals[Intervention_vals > 0],
      probs = Prob_adjust_intervention_percentile,
      na.rm = TRUE
    )
    Non_intervention_ptile_val <- quantile(
      Non_Intervention_vals[Non_Intervention_vals > 0],
      probs = Prob_adjust_non_intervention_percentile,
      na.rm = TRUE
    )

    #get the means of the values above the 90th percentile
    Intervention_ptile_mean <- mean(
      Intervention_vals[Intervention_vals >= Intervention_ptile_val],
      na.rm = TRUE
    )
    Non_intervention_ptile_mean <- mean(
      Non_Intervention_vals[
        Non_Intervention_vals >= Non_intervention_ptile_val
      ],
      na.rm = TRUE
    )

    #mean difference
    Mean_diff <- Intervention_ptile_mean - Non_intervention_ptile_mean

    #Average of means
    Average_mean <- (Intervention_ptile_mean + Non_intervention_ptile_mean) / 2

    #calculate percentage difference
    Perc_diff <- (Mean_diff / Average_mean) * 100

    #print the percentage difference for debugging purposes
    cat(
      paste0(
        "The Percentage difference in average probability above the ",
        Prob_adjust_intervention_percentile,
        " and ",
        Prob_adjust_non_intervention_percentile,
        " percentiles of the intervention & non-intervention areas respectively for ",
        lulc_class,
        " is : ",
        Perc_diff
      ),
      "\n"
    )

    # If Prob_adjust_valency == "Increase" then the goal of the intervention is
    # to increase the probability of change for the target land use class in the intervention area
    if (Prob_adjust_valency == "Increase") {
      # However if Perc_diff > 0 this implies that the average probability of change
      # above the specificed percentile in the intervention area is higher than in the
      # non-intervention area and as such it we should increase the probabilities
      # in the intervention area by the % difference.

      # Whereas if Perc_diff < 0 this implies that the average probability of change
      # above the percentile in the intervention area is lower than in the
      # non-intervention area and as such we should decrease the probabilities
      # in the non- intervention area by the % difference.

      if (Perc_diff > 0) {
        #check that Perc_diff is above the Prob_adjust_threshold
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          cat(
            paste0(
              "The Percentage difference is below the threshold for ",
              lulc_class,
              ", setting to threshold value: ",
              Prob_adjust_threshold
            ),
            "\n"
          )
          Perc_diff <- Prob_adjust_threshold
        }

        cat(
          paste(
            "because the Prob_adjust_valency is",
            Prob_adjust_valency,
            "and the percentage difference is >0 then increasing the probability of the intervention pixels"
          ),
          "\n"
        )

        # Increase the probability of instances above the specified percentile
        ix <- Intervention_idx[Intervention_vals > Intervention_ptile_val]
        normalized[ix, prob := prob + (prob / 100) * Perc_diff]
      } else if (Perc_diff < 0) {
        #check that Perc_diff is below the Prob_adjust_threshold
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          cat(
            paste0(
              "The Percentage difference is below the threshold for ",
              lulc_class,
              ", setting to threshold value: ",
              Perc_diff
            ),
            "\n"
          )
          Perc_diff <- -(Prob_adjust_threshold)
        }

        cat(
          paste(
            "because the Prob_adjust_valency is",
            Prob_adjust_valency,
            "and the percentage difference is <0 then decreasing the probability of the non-intervention pixels"
          ),
          "\n"
        )

        # Decrease the probability of instances above the specified percentile
        ix <- Non_intervention_idx[
          Non_Intervention_vals > Non_intervention_ptile_val
        ]
        normalized[ix, prob := prob + (prob / 100) * Perc_diff]
      }
    } else if (Prob_adjust_valency == "Decrease") {
      # If Prob_adjust_valency == "Decrease" then the goal of the intervention is
      # to decrease the probability of change for the target land use class in the intervention area

      # If Perc_diff > 0 this implies that the average probability of change in the
      # intervention area is higher than in the non-intervention area and as such we should
      # decrease the probabilities in the intervention area by the % difference.
      # Whereas if Perc_diff < 0 this implies that the average probability of change in the
      # intervention area is lower than in the non-intervention area and as such we should
      # increase the probabilities in the non-intervention area by the % difference.

      if (Perc_diff > 0) {
        #check that Perc_diff is above the Prob_adjust_threshold
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          cat(
            paste0(
              "The Percentage difference is below the threshold for ",
              lulc_class,
              ", setting to threshold value: ",
              Prob_adjust_threshold
            ),
            "\n"
          )
          Perc_diff <- Prob_adjust_threshold
        }

        cat(
          paste(
            "because the Prob_adjust_valency is",
            Prob_adjust_valency,
            "and the percentage difference is >0 then decreasing the probability of the intervention pixels"
          ),
          "\n"
        )

        # Decrease the probability of instances above the specified percentile
        ix <- Intervention_idx[Intervention_vals > Intervention_ptile_val]
        normalized[ix, prob := prob + (prob / 100) * -(Perc_diff)]
      } else if (Perc_diff < 0) {
        #check that Perc_diff is below the Prob_adjust_threshold
        if (abs(Perc_diff) < Prob_adjust_threshold) {
          cat(
            paste0(
              "The Percentage difference is below the threshold for ",
              lulc_class,
              ", setting to threshold value: ",
              Prob_adjust_threshold
            ),
            "\n"
          )
          Perc_diff <- -(Prob_adjust_threshold)
        }
        cat(
          paste(
            "because the Prob_adjust_valency is",
            Prob_adjust_valency,
            "and the percentage difference is <0 then increasing the probability of the non-intervention pixels"
          ),
          "\n"
        )

        # Increase the probability of instances above the specified percentile
        ix <- Non_intervention_idx[
          Non_Intervention_vals > Non_intervention_ptile_val
        ]
        normalized[ix, prob := prob + (prob / 100) * abs(Perc_diff)]
      }
    } else if (Prob_adjust_valency == "Increase_inside_decrease_outside") {
      # If Prob_adjust_valency == "Increase_inside_decrease_outside" then the goal of the intervention is
      # to simulataneously increase the probability of change for the target land use class in the intervention area
      # and decrease the probability of change for the target land use class in the non-intervention area

      if (abs(Perc_diff) < Prob_adjust_threshold) {
        cat(
          paste0(
            "The Percentage difference is below the threshold for ",
            lulc_class,
            ", setting to threshold value: ",
            Prob_adjust_threshold
          ),
          "\n"
        )
        Perc_diff <- Prob_adjust_threshold
      }

      cat(
        paste(
          "because the Prob_adjust_valency is",
          Prob_adjust_valency,
          "increasing the probability of the intervention pixels and decreasing the probability of the non-intervention pixels"
        ),
        "\n"
      )

      # Increase the probability of instances above the specified percentile in the intervention area
      ix <- Intervention_idx[Intervention_vals > Intervention_ptile_val]
      normalized[ix, prob := prob + (prob / 100) * abs(Perc_diff)]

      # Decrease the probability of instances above the specified percentile in the non-intervention area
      ix <- Non_intervention_idx[
        Non_Intervention_vals > Non_intervention_ptile_val
      ]
      normalized[ix, prob := prob + (prob / 100) * -(abs(Perc_diff))]
    }

    # set any values in prob that are greater than 1 to 1
    normalized[prob > 1, prob := 1]
    # set any values in prob that are less than 0 to 0 excluding NAs
    normalized[!is.na(prob) & prob < 0, prob := 0]
  }

  # return the updated normalized data.table
  return(normalized)
}

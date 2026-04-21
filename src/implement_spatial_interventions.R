#' @title Implement Spatial Interventions on Raster Probability Values
#' @description
#' Implement all specified spatial interventions on raster probability values
#' @param interventions_dir Directory containing YAML files with intervention definitions.
#' @param scenario_ID Identifier for the scenario to apply interventions.
#' @param raster_prob_values Data frame containing raster probability values with columns for coordinates and probabilities.
#' @param simulation_time_step The time step at which to apply the interventions.
#' @param LULC_rat data frame containing LULC class abbreviations and their aggregated IDs, used for filtering based on LULC classes.
#' @return A data frame with updated raster probability values after applying interventions.
implement_spatial_interventions <- function(
  interventions_dir,
  scenario_ID,
  raster_prob_values,
  simulation_time_step,
  LULC_rat,
  Proj = ProjCH
) {
  # Load interventions for scenario from YAML file
  Interventions <- yaml.load_file(file.path(
    interventions_dir,
    paste0(scenario_ID, "_interventions.yml")
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

  # If no interventions are found, return the raster_prob_values unchanged
  if (length(Current_interventions) == 0) {
    warning(
      "No interventions found for the specified scenario and time step. Returning original raster_prob_values."
    )
    return(raster_prob_values)
  } else {
    cat(
      paste(
        "Found",
        length(Current_interventions),
        "allocation stage interventions for scenario",
        scenario_ID,
        "at time step",
        simulation_time_step
      ),
      "\n"
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

    #vector names of columns of probability predictions (matching on Prob_)
    Pred_prob_columns <- grep("Prob_", names(raster_prob_values), value = TRUE)

    #convert probability table to raster stack
    Prob_raster_stack <- c(lapply(Pred_prob_columns, function(x) {
      col_rast <- rast(raster_prob_values[, c("x", "y", x)])
      crs(col_rast) <- Proj # Set the CRS for each raster layer)
      return(col_rast)
    }))
    names(Prob_raster_stack) <- Pred_prob_columns
    cat("Converted raster_prob_values to RasterStack", "\n")

    # if any of the interventions have a value of From_lulc_filter, that is not
    #NULL then create a raster of the current LULC
    if (
      any(sapply(Current_interventions, function(x) {
        val <- x[["From_lulc_filter"]]
        !is.null(val) && length(val) > 0 && any(val != "None")
      }))
    ) {
      # create a raster of the current LULC
      Current_lulc_raster <- rast(raster_prob_values[, c("x", "y", "LULC")])
      names(Current_lulc_raster) <- "Current_LULC"

      # add the crs
      crs(Current_lulc_raster) <- Proj
    }

    # loop over interventions
    for (intervention in Current_interventions) {
      # Print intervention name
      cat(paste(
        "Applying intervention:",
        intervention[["Intervention_ID"]],
        "\n"
      ))

      # Adjust format of Transition_target_classes
      Target_classes <- paste0(
        "Prob_",
        intervention[["Transition_target_classes"]]
      )

      # Prepare the Intervention mask
      if (intervention$Mask_type == "Static") {
        # load mask using the Intervention_masks
        Intervention_mask <- rast(intervention[["Intervention_mask"]])
      } else if (intervention$Mask_type == "Dynamic") {
        # subset the list of intervention$Intervention_mask to the current simulation_time_step
        # This assumes that the Intervention_mask is a list with keys corresponding to time steps
        intervention_mask_list <- intervention[["Intervention_mask"]]
        intervention_mask_path <- unlist(intervention_mask_list[
          names(intervention_mask_list) == simulation_time_step
        ])

        # load mask using the Intervention_mask_path appending simulation_time_step
        Intervention_mask <- rast(intervention_mask_path)
      } else {
        stop(paste("Unknown Mask_type:", intervention[["Mask_type"]]))
      }

      # If the Intervention requires filtering by LULC classes then adjust the mask
      if (
        !is.null(intervention$From_lulc_filter) &&
          length(intervention$From_lulc_filter) > 0 &&
          all(intervention$From_lulc_filter != "None")
      ) {
        # Get the raster value of the From_lulc_filter class from LULC_rat
        LULC_filter_classes <- unlist(LULC_rat[
          LULC_rat$Class_abbreviation %in% intervention[["From_lulc_filter"]],
          "Aggregated_ID"
        ])

        cat(paste(
          "Filtering to only cells that are currently LULC classes:",
          paste(LULC_filter_classes, collapse = ", "),
          "\n"
        ))

        # Filter the Current_lulc_raster to only include the LULC_filter_classes
        LULC_mask <- Current_lulc_raster

        # set all values of LULC_mask that are not in LULC_filter_classes to NA
        LULC_mask[!(values(LULC_mask) %in% LULC_filter_classes)] <- NA

        # Now set all values that are not NA to 1
        LULC_mask[!is.na(LULC_mask)] <- 1

        # Mask the Intervention_mask with the LULC_mask
        Intervention_mask <- terra::mask(Intervention_mask, LULC_mask == 1)
      }

      # if intervention$Intervention_ID is "Agri_maintenance" or "Agri_abandonment"
      # then we need to subset the mask to the most marginal pixels
      if (
        intervention[["Intervention_ID"]] %in%
          c("Agri_maintenance", "Agri_abandonment")
      ) {
        cat(paste(
          "because the intervention is:",
          intervention[["Intervention_ID"]],
          ", subsetting the Intervention mask to pixels 
                    with values >= the upper quartile ofagricultural marginality",
          "\n"
        ))
        # Get the most marginal pixels by calculating the upper quartile value of the Intervention_mask
        # and setting all values that are not equal or greater than to the upper quartile value to NA
        min_value <- quantile(
          values(Intervention_mask),
          probs = 0.75,
          na.rm = TRUE
        )

        # Set all values that are not equal to min_value to NA
        Intervention_mask[values(Intervention_mask) >= min_value] <- NA

        # Set all values that are not NA to 1
        Intervention_mask[!is.na(Intervention_mask)] <- 1
      }

      # Apply different functions based on whether the intervention specifies absolute or relative adjustments to probabilities
      # if intervention$Prob_adjust_type == Absolute then apply absolute adjustment function
      if (intervention$Prob_adjust_type == "Absolute") {
        cat(paste(
          "Applying absolute probability adjustment to cells:",
          intervention[["Prob_adjust_zone"]],
          "the intervention area, adjusting probability values to:",
          intervention[["Prob_adjust_value"]],
          "\n"
        ))

        Prob_raster_stack <- absolute_prob_adjust(
          Prob_raster_stack = Prob_raster_stack,
          Prob_adjust_zone = intervention$Prob_adjust_zone,
          Prob_adjust_value = intervention$Prob_adjust_value,
          Target_classes = Target_classes,
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
        Prob_raster_stack <- relative_prob_adjust(
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
          Intervention_mask = Intervention_mask,
          Prob_raster_stack = Prob_raster_stack
        )
      } else {
        stop(paste(
          "Unknown Prob_adjust_type:",
          intervention[["Prob_adjust_type"]]
        ))
      }
    } # end of intervention loop
  }

  #convert raster stack back to dataframe
  # because terra::as.data.frame() does not handle NA values well,
  # we will loop over the names and convert each layer to df and replace the corresponding column in raster_prob_values
  for (i in names(Prob_raster_stack)) {
    # convert each raster layer to a data frame
    layer_df <- terra::as.data.frame(Prob_raster_stack[[i]], na.rm = FALSE)

    # replace the corresponding column in raster_prob_values
    raster_prob_values[, i] <- layer_df[, i] # assuming the third column is the values
  }

  #return the updated raster_prob_values
  return(raster_prob_values)
}

#' @title Perform absolute adjustment of probabilities of change in target classes
#' @description
#' Perform absolute adjustment of probabiltieis of change in target classes
#' either inside or outside an intervention mask.
#' @param Prob_raster_stack A RasterStack containing layers of probability of change to certain land use classes, layers named Prob_*class*.
#' @param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Prob_adjust_value A numeric value to set the probabilities in the target area.
#' @param Target_classes A vector of land use classes to be targeted by the intervention.
#' @param Intervention_mask A RasterLayer or RasterStack representing the intervention mask.
#' @return A RasterStack with updated probabilities for the target land use classes.
absolute_prob_adjust <- function(
  Prob_raster_stack,
  Prob_adjust_zone = Outside,
  Prob_adjust_value = 0.1,
  Target_classes,
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

    # Subset to target layer
    Target_layer <- Prob_raster_stack[[lulc_class]]
    layer_index <- which(names(Prob_raster_stack) == lulc_class)

    # If Prob_adjust_zone is Inside, then mask the target layer to inside the mask
    if (Prob_adjust_zone == "Inside") {
      Target_area <- terra::mask(Target_layer, Intervention_mask == 1)
    } else if (Prob_adjust_zone == "Outside") {
      # invert the mask to get the non-intersecting area
      Target_area <- terra::mask(
        Target_layer,
        Intervention_mask,
        inverse = TRUE
      )
    }

    # Adjust the probabilities in the target area
    Target_area[values(Target_area) > 0] <- Prob_adjust_value

    # Identify which cells need to have value updated
    ix <- cells(Target_area > 0)

    # Replace values in target raster
    Target_layer[ix] <- Target_area[ix]

    # set any values in Target_layer that are greater than 1 to 1
    Target_layer[Target_layer > 1] <- 1

    # set any values in Target_layer that are less than 0 to 0 excluding NAs
    Target_layer[Target_layer < 0 & !is.na(Target_layer)] <- 0

    # Update the Prob_raster_stack with the modified Target_layer
    Prob_raster_stack[[layer_index]] <- Target_layer
  }

  return(Prob_raster_stack)
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
#'@param Prob_adjust_zone A string indicating the zone for adjustment, either "Inside" or "Outside".
#' @param Target_classes A vector of land use classes to be targeted by the intervention.
#' @return Prob_raster_stack A RasterStack with updated probabilities for the target land use classes.
relative_prob_adjust <- function(
  Prob_adjust_valency,
  Prob_adjust_intervention_percentile,
  Prob_adjust_non_intervention_percentile,
  Prob_adjust_threshold,
  Prob_adjust_zone,
  Target_classes,
  Intervention_mask,
  Prob_raster_stack
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
    # Subset to target layer
    Target_layer <- Prob_raster_stack[[lulc_class]]

    # Identify the layer index in the raster stack
    layer_index <- which(names(Prob_raster_stack) == lulc_class)

    # If Prob_adjust_zone is Inside, then the intervention area is inside the mask and non-intersecting area is outside the mask
    if (Prob_adjust_zone == "Inside") {
      Intervention_area <- terra::mask(Target_layer, Intervention_mask == 1)
      Non_intervention_area <- terra::mask(
        Target_layer,
        Intervention_mask == 1,
        inverse = TRUE
      )
    } else if (Prob_adjust_zone == "Outside") {
      # if the Prob_adjust_zone is Outside, then the intervention area is outside the mask and the non-intersecting area is inside the mask
      Intervention_area <- terra::mask(
        Target_layer,
        Intervention_mask,
        inverse = TRUE
      )
      Non_intervention_area <- terra::mask(Target_layer, Intervention_mask == 1)
    }

    # calculate percentile values of probability for pixels in the
    # intervention area vs. non-intervention area (i.e. intervention - non_intervention)
    #outside the mask

    # seperate raster values
    Intervention_vals <- values(Intervention_area)
    Non_Intervention_vals <- values(Non_intervention_area)

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
        Intervention_area[
          values(Intervention_area) > Intervention_ptile_val
        ] <- Intervention_area[
          values(Intervention_area) > Intervention_ptile_val
        ] +
          (Intervention_area[
            values(Intervention_area) > Intervention_ptile_val
          ] /
            100) *
            Perc_diff

        # Identify which cells need to have value updated
        ix <- cells(Intervention_area > Intervention_ptile_val)

        # Replace values in target raster
        Target_layer[ix] <- Intervention_area[ix]
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
        Non_intervention_area[
          values(Non_intervention_area) > Non_intervention_ptile_val
        ] <-
          Non_intervention_area[
            values(Non_intervention_area) > Non_intervention_ptile_val
          ] +
          (Non_intervention_area[
            values(Non_intervention_area) > Non_intervention_ptile_val
          ] /
            100) *
            Perc_diff

        # Identify which cells need to have value updated
        ix <- cells(Non_intervention_area > Non_intervention_ptile_val)

        # Replace values in target raster
        Target_layer[ix] <- Non_intervention_area[ix]
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
        Intervention_area[values(Intervention_area) > Intervention_ptile_val] <-
          Intervention_area[
            values(Intervention_area) > Intervention_ptile_val
          ] +
          (Intervention_area[
            values(Intervention_area) > Intervention_ptile_val
          ] /
            100) *
            -(Perc_diff)

        # Identify which cells need to have value updated
        ix <- cells(Intervention_area > Intervention_ptile_val)

        # Replace values in target raster
        Target_layer[ix] <- Intervention_area[ix]
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
        Non_intervention_area[
          values(Non_intervention_area) > Non_intervention_ptile_val
        ] <-
          Non_intervention_area[
            values(Non_intervention_area) > Non_intervention_ptile_val
          ] +
          (Non_intervention_area[
            values(Non_intervention_area) > Non_intervention_ptile_val
          ] /
            100) *
            abs(Perc_diff)

        # Identify which cells need to have value updated
        ix <- cells(Non_intervention_area > Non_intervention_ptile_val)

        # Replace values in target raster
        Target_layer[ix] <- Non_intervention_area[ix]
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
      Intervention_area[Intervention_area > Intervention_ptile_val] <-
        Intervention_area[Intervention_area > Intervention_ptile_val] +
        (Intervention_area[Intervention_area > Intervention_ptile_val] / 100) *
          abs(Perc_diff)

      # Identify which cells need to have value updated
      ix <- cells(Intervention_area > Intervention_ptile_val)
      # Replace values in target raster
      Target_layer[ix] <- Intervention_area[ix]

      # Decrease the probability of instances above the specified percentile in the non-intervention area
      Non_intervention_area[
        Non_intervention_area > Non_intervention_ptile_val
      ] <-
        Non_intervention_area[
          Non_intervention_area > Non_intervention_ptile_val
        ] +
        (Non_intervention_area[
          Non_intervention_area > Non_intervention_ptile_val
        ] /
          100) *
          -(abs(Perc_diff))

      # Identify which cells need to have value updated
      ix <- cells(Non_intervention_area > Non_intervention_ptile_val)
      # Replace values in target raster
      Target_layer[ix] <- Non_intervention_area[ix]
    }

    # set any values in Target_layer that are greater than 1 to 1
    Target_layer[Target_layer > 1] <- 1

    # set any values in Target_layer that are less than 0 to 0 excluding NAs
    Target_layer[Target_layer < 0 & !is.na(Target_layer)] <- 0

    # Update the Prob_raster_stack with the modified Target_layer
    Prob_raster_stack[[layer_index]] <- Target_layer
  }

  # return the updated Prob_raster_stack
  return(Prob_raster_stack)
}

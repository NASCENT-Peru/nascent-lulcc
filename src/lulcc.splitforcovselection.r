#' lulcc.splitforcovselection
#'
#' Takes a data frame containing the transition data for one specific transition, splits
#' it into covariates / non-covariates, and enriches these data with collinearity and
#' embedding weights.
#'
#' @param trans_dataset A data frame containing the transition data for one specific
#' transition from class A to class B. Presumed that the last column contains an
#' indicator of whether a transition has occurred (1) or not (0).
#' @param covariate_ids Character vector of column names of the covariates to be split
#' @author Ben Black


lulcc.splitforcovselection <- function(trans_dataset, covariate_ids) {
  names(trans_dataset)[ncol(trans_dataset)] <- "trans_result"
  trans_result <- trans_dataset[, "trans_result", drop = TRUE]

  # Identify the covariate data
  cov_data <- trans_dataset[, c(covariate_ids)]

  # remove cols which only have 1 unique value
  cov_data <- Filter(function(x) (length(unique(x)) > 2), cov_data)

  non_cov_data <- dplyr::select(
    trans_dataset,
    -tidyselect::any_of(covariate_ids)
  )

  # Measure of class imbalance
  # FIXME currently, we're passing in all-NA or all-NULL vectors, leading to warnings.
  # we shoudl avoid processing these altogether.
  imbalance_ratio <- (
    sum(trans_result == min(trans_result)) /
      sum(trans_result == max(trans_result))
  ) # instances of minority class/majority class

  # Next step is to create weight vectors to be used for in both processes of covariate
  # selection Random forests 'classwt' variable can accept class weights as a named
  # vector which is a different form to the weight vector required for GLMs (within
  # collinearity based selection process) as such it makes sense to create a separate
  # weight vector for each process

  # Create weight vector to be used in collinearity based covariate selection

  # trans_result == 1 -> transition occurred, 0 -> no transition
  transitions_vec <- which(trans_result == 1)
  nontransitions_vec <- which(trans_result == 0)

  # create a vector the same length as trans_result and fill it all with 1's
  collin_weights <- rep(1, length(trans_result))

  # calculate the weighting value for the transitions
  trans_weighting <- round(
    length(nontransitions_vec) / length(transitions_vec)
  )
  # calculate the weighting value for the no-transitions
  non_trans_weighting <- round(
    length(transitions_vec) / length(nontransitions_vec)
  )

  # insert the weighting value at the positions of transitions (if the number of
  # transitions is more than the number of no-transitions then the weighting value
  # will be rounded to zero which must be substituted for a 1 for it to work properly
  # in GRRF)
  collin_weights[transitions_vec] <- ifelse(
    length(transitions_vec) < length(nontransitions_vec), trans_weighting, 1
  )
  collin_weights[nontransitions_vec] <- ifelse(
    length(transitions_vec) > length(nontransitions_vec), non_trans_weighting, 1
  )

  # Create weight vector to be used in embedded (grrf) covariate selection
  embed_weights <- c(
    "0" = ifelse(length(transitions_vec) > length(nontransitions_vec), non_trans_weighting, 1),
    "1" = ifelse(length(transitions_vec) < length(nontransitions_vec), trans_weighting, 1)
  )

  w_vectors <- list(
    collin_weights = collin_weights,
    embed_weights = embed_weights
  )

  # group the outputs as a list
  seperated_outputs <- list(
    trans_result = trans_result,
    cov_data = cov_data,
    non_cov_data = non_cov_data,
    collin_weights = w_vectors[["collin_weights"]],
    embed_weights = w_vectors[["embed_weights"]],
    imbalance_ratio = imbalance_ratio,
    num_units = length(trans_result)
  )

  return(seperated_outputs)
}

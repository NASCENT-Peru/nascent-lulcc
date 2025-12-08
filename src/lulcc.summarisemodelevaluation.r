#' lulcc.summarisemodelevaluation
#'
#' Aggregate model evaluation metrics across transitions/bioregions
#'
#' @param model_eval_results TODO
#' @param summary_metrics Vector, names of evaluation metrics to summarize by
#' can include: AUC, AUC.S, RMSE, Boyce, Score, threshold, Sensitivity,
#' Specificity, Accuracy, PPV, NPV, Jaccard, TSS Kappa, SEDI.
#' @param plots Logical, whether to produce summary plots (TRUE or not (FALSE))
#' @returns either a table summarising performance across models or
#' if plots = TRUE a list of the performance table and plots
#' @author Ben Black
#' @export

lulcc.summarisemodelevaluation <- function(model_eval_results, summary_metrics, plots) {
  # 1. Transform list of model eval results to single data frame
  # remove any empty results
  complete_results <- model_eval_results[lapply(model_eval_results, length) > 0]

  lulc_classes <- c(
    "Alp_Past",
    "Closed_Forest",
    "Grassland",
    "Int_AG",
    "Open_Forest",
    "Perm_crops",
    "Shrubland",
    "Urban",
    "Glacier",
    "Static"
  ) # create vector of region names

  Region_names <- c(
    "Jura.",
    "Plateau.",
    "Northern_Prealps.",
    "Southern_Prealps.",
    "Western_Central_Alps.",
    "Eastern_Central_Alps."
  )

  Region_names_regex <- stringr::regex(
    paste(paste(Region_names, sep = "_"), collapse = "|")
  )

  # get list of transition names
  transition_names <- unique(lapply(names(complete_results), function(x) {
    stringr::str_remove_all(
      stringr::str_replace_all(x, Region_names_regex, ""), "_glm"
    )
  }))

  list_trans_evals_data_frame <- lapply(
    complete_results,
    function(x) as.data.frame(t(data.frame(x)))
  )


  list_trans_evals_data_frame_numeric <- lapply(
    list_trans_evals_data_frame,
    function(x) {
      sapply(
        x, function(y) round(as.numeric(y), digits = 3),
        simplify = FALSE
      )
    }
  )

  All_evals_data_frame <- data.table::rbindlist(
    list_trans_evals_data_frame_numeric,
    use.names = TRUE
  )

  rownames(All_evals_data_frame) <- c(names(complete_results))
  All_evals_data_frame <- tibble::rownames_to_column(
    All_evals_data_frame,
    var = "trans_name"
  )

  # add columns for region, transistion and model number
  All_evals_data_frame$Region <- stringr::str_match(
    All_evals_data_frame$trans_name, Region_names_regex
  )
  All_evals_data_frame$transition <- stringr::str_match(
    All_evals_data_frame$trans_name, stringr::regex(paste(transition_names, collapse = "|"))
  )
  All_evals_data_frame$model <- factor(as.numeric(
    regmatches(
      All_evals_data_frame$trans_name,
      gregexpr("[[:digit:]]+", All_evals_data_frame$trans_name)
    )
  ))
  All_evals_data_frame$initial_lulc <- stringr::str_match(
    All_evals_data_frame$transition,
    paste(lulc_classes, collapse = "|")
  )
  All_evals_data_frame$final_lulc <- lapply(
    stringr::str_match_all(
      All_evals_data_frame$transition,
      paste(lulc_classes, collapse = "|")
    ), function(x) x[2, ]
  )
  All_evals_data_frame$Model_score <- rowMeans(
    All_evals_data_frame[, c("AUC.S", "Boyce")],
    na.rm = TRUE
  )

  if (plots == TRUE) {
    # 2. Summarise by Bioregion
    Data_by_region <- split(All_evals_data_frame, All_evals_data_frame$Region)

    regional.model.eval.plot <- function(regional_data, region_name, eval_metric) {
      # create plot
      regional_plot <-
        ggplot2::ggplot(
          regional_data, ggplot2::aes_string(
            color = "model",
            y = eval_metric,
            x = "transition",
            group = "model"
          )
        ) +
        ggplot2::geom_point() +
        ggplot2::theme(
          axis.text.x = ggplot2::element_text(
            size = 9, angle = 90
          )
        ) +
        ggplot2::scale_fill_brewer(palette = "Dark2") +
        ggplot2::labs(title = region_name) +
        ggplot2::theme(
          text = ggplot2::element_text(
            family = "Times New Roman"
          ),
          plot.title = ggplot2::element_text(
            size = ggplot2::rel(1.1), hjust = 0.5
          ),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black")
        )
      return(regional_plot)
    }


    # Produce plots for each bioregional summary table under each summary metric
    Bioregion_plots_list <- list()
    for (i in summary_metrics) {
      Bioregion_summary_plots <- mapply(
        regional.model.eval.plot,
        regional_data = Data_by_region,
        region_name = names(Data_by_region),
        eval_metric = i,
        SIMPLIFY = FALSE
      )

      Bioregion_combined_plot <- gridExtra::grid.arrange(
        grobs = Bioregion_summary_plots
      )

      Bioregion_plots_list[[i]] <- Bioregion_combined_plot
    }


    # 2. Aggregating by transitions

    # create scatter plot for each eval metric supplied in 'summary metric' argument
    # against transitions with coloured by model number and shape by region
    Transitions_plots_list <- list()

    for (i in summary_metrics) {
      transition_plot <-
        ggplot2::ggplot(
          All_evals_data_frame, ggplot2::aes_string(y = i, x = "transition", color = "model")
        ) +
        ggplot2::geom_point() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(size = 9, angle = 90)) +
        ggplot2::scale_fill_brewer(palette = "Dark2") +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.border = ggplot2::element_blank(),
          panel.background = ggplot2::element_blank(),
          panel.grid = ggplot2::element_blank(),
          panel.spacing.x = ggplot2::unit(0, "line"),
          axis.text = ggplot2::element_text(colour = "black")
        )
      transition_plot_facet <- transition_plot + ggplot2::facet_grid(
        . ~ initial_lulc,
        scales = "free_x"
      )
      Transitions_plots_list[[paste(i)]] <- transition_plot_facet
    }

    # 3. aggregate by initial LULC

    Initial_lulc_plots_list <- list()

    for (i in summary_metrics) {
      initial_lulc_plot <-
        ggplot2::ggplot(
          All_evals_data_frame,
          ggplot2::aes_string(y = i, x = "initial_lulc", shape = "model", color = "Region")
        ) +
        ggplot2::geom_point() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(size = 9, angle = 90)) +
        ggplot2::scale_fill_brewer(palette = "Dark2") +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black")
        )
      Initial_lulc_plots_list[[paste(i)]] <- initial_lulc_plot
    }

    # 4. aggregate by final LULC

    Final_lulc_plots_list <- list()

    for (i in summary_metrics) {
      final_lulc_plot <- ggplot2::ggplot(
        All_evals_data_frame,
        ggplot2::aes_string(
          y = i, x = "final_lulc",
          shape = "model", color = "Region"
        )
      ) +
        ggplot2::geom_point() +
        ggplot2::theme(axis.text.x = ggplot2::element_text(size = 9, angle = 90)) +
        ggplot2::scale_fill_brewer(palette = "Dark2") +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black")
        )
      Final_lulc_plots_list[[paste(i)]] <- final_lulc_plot
    }


    # Class imbalance vs summary metric

    Class_imbalance_plots_list <- list()

    for (i in summary_metrics) {
      class_imbalance_plot <- ggplot2::ggplot(
        All_evals_data_frame, ggplot2::aes_string(
          y = i, x = "class_imbalance", shape = "model", color = "num_units"
        )
      ) +
        ggplot2::geom_point() +
        ggplot2::scale_color_gradient() +
        ggplot2::xlim(NA, 1200) +
        ggplot2::theme(axis.text.x = ggplot2::element_text(size = 9, angle = 90)) +
        ggplot2::scale_fill_brewer(palette = "Dark2") +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black")
        )
      Class_imbalance_plots_list[[paste(i)]] <- class_imbalance_plot
    }

    # 5. format output

    names(output) <- list(
      Bioregion_plots_list = Bioregion_plots_list,
      Transitions_plots_list = Transitions_plots_list,
      tabular_output = All_evals_data_frame,
      Initial_lulc_plots_list = Initial_lulc_plots_list,
      Final_lulc_plots_list = Final_lulc_plots_list,
      Class_imbalance_plots_list = Class_imbalance_plots_list
    )
    return(output)
  } else {
    tabular_output <- All_evals_data_frame
    return(tabular_output)
  } # close loop over plots
} # close function

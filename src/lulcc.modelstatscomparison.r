#' lulcc.modelstatscomparison
#'
#' Check assumptions and perform hypothesis testingfor differences in model eval metrics
#' between different model specification
#'
#' @param model_eval_summary Dataframe summary of model performance
#' @param eval_metrics Vector, names of evaluation metrics to summarize by
#' can include: AUC, AUC.S, RMSE, Boyce, Score, threshold, Sensitivity,
#' Specificity, Accuracy, PPV, NPV, Jaccard, TSS Kappa, SEDI.
#' @param grouping_var Character, name of column to group results by
#' @returns List of model comparison results objects: stats test results, plots
#' @author Ben Black
#' @export

lulcc.modelstatscomparison <- function(model_eval_summary, eval_metrics, grouping_var) {
  comparison_output <- list()

  for (i in seq_along(eval_metrics)) { # loop over model eval metrics

    # identify names of transitions with NAs for eval metric
    trans_to_be_removed <- model_eval_summary$trans_name[
      which(is.na(model_eval_summary[[eval_metrics[i]]]))
    ]
    model_eval_summary <- subset(
      model_eval_summary, !(trans_name %in% trans_to_be_removed)
    )

    model_formula <- as.formula(paste0(eval_metrics[i], "~", grouping_var)) # vector model formula

    # Checking assumptions

    # check number of models being compared to determine assumptiosn to be checked and
    # statistical tests
    # 2 models:
    # Assumptions: Normality and homoscedasticity of variance
    # Test: dependent T-test

    #>2 models
    # Assumptions: Normality and Sphericity
    # Test: repeated measures ANOVA or Friedman test

    if (length(table(model_eval_summary[, .SD, .SDcols = grouping_var])) == 2) {
      # Assumptions: Normality and homoscedasticity of variance

      # Shapiro-wilks test for normality of differences between models
      # compute the difference in model performance between each specification
      grouped_dep_var <-
        model_eval_summary[
          , .SD,
          .SDcols = c(
            grouping_var,
            "trans_name",
            eval_metrics[i],
            "num_covs", "imbalance_ratio", "num_units"
          )
        ] |>
        tidyr::pivot_wider(names_from = 1, values_from = c(3, 4, 5, 6))
      grouped_dep_var <- data.frame(
        grouped_dep_var %>%
          dplyr::mutate(model_diff = .[[2]] - .[[3]]) %>%
          dplyr::mutate(pred_diff = .[[5]] - .[[4]])
      )
      grouped_dep_var$num_units <- grouped_dep_var[
        grepl("num_units", colnames(grouped_dep_var))
      ][, 1]
      grouped_dep_var$imbalance_ratio <- grouped_dep_var[
        grepl("imbalance_ratio", colnames(grouped_dep_var))
      ][, 1]

      grouped_dep_var <- grouped_dep_var[
        ,
        c("trans_name", "model_diff", "pred_diff", "num_units", "imbalance_ratio")
      ]

      diffs <- grouped_dep_var$model_diff # convert just the model difference values to numeric

      # Shapiro-Wilk normality test for the differences
      Shapiro_result <- shapiro.test(diffs)

      # Hypothesis testing
      if (Shapiro_result$p.value > 0.05) {
        print("residuals are normally distributed using paired sample t-test")

        # Compute t-test
        res <- t.test(model_formula, data = model_eval_summary, paired = TRUE)
        test_type <- "parametric"
      } else {
        print("residuals are non-normally distributed using paired samples Wilcoxon test")
        res <- wilcox.test(model_formula, data = model_eval_summary, paired = TRUE)
        test_type <- "nonparametric"
      }


      # produce violin plot of results of paired-sample t-test
      violin_plot <- ggstatsplot::ggwithinstats(
        data = model_eval_summary,
        x = Model_name,
        y = !!eval_metrics[i],
        type = test_type,
        xlab = "Model specification", ## label for the x-axis
        ylab = stringr::str_replace(eval_metrics[i], "_", " "),
        pairwise.comparisons = FALSE,
        pairwise.display = "significant",
        sample.size.label = F,
        package = "yarrr", ## package from which color palette is to be taken
        palette = "info2", ## choosing a different color palette
        ggplot.component = list(
          ggplot2::scale_y_continuous(limits = c(0.0, 1), breaks = seq(from = 0, to = 1, by = 0.1)),
          ggplot2::theme(
            text = ggplot2::element_text(family = "Times New Roman"),
            plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
            axis.line = ggplot2::element_line(1),
            panel.background = ggplot2::element_blank(),
            axis.text = ggplot2::element_text(colour = "black"),
            axis.text.x = ggplot2::element_text(size = 8)
          )
        )
      )

      # Cleveland Dot Plot of model eval metric vs. number of variables
      Dot_plot <- ggplot2::ggplot(data = model_eval_summary) +
        ggplot2::geom_point(
          ggplot2::aes_string(x = "num_preds", y = eval_metrics[i], colour = "Model_name"),
          alpha = 0.7
        ) +
        ggplot2::geom_line(
          inherit.aes = FALSE,
          ggplot2::aes_string(x = "num_preds", y = eval_metrics[i], group = "trans_name"),
          linetype = 1,
          alpha = 0.1
        ) +
        ggplot2::labs(
          y = if (eval_metrics[i] == "Model_score") {
            "Model score (avg. AUC and Boyce)"
          } else {
            eval_metrics[i]
          },
          x = "Number of predictors",
          colour = "Model specifciation:"
        ) +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black"),
          axis.text.x = ggplot2::element_text(size = 7),
          legend.title = ggplot2::element_text(size = 8, face = "bold"),
          legend.text = ggplot2::element_text(size = 8),
          legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
          legend.title.align = 0.5,
          legend.position = "bottom"
        )


      # Scatter plot of distribution of differences in model eval metric vs. change in
      # number of variables
      Diff_plot_pred <- ggplot2::ggplot(grouped_dep_var) +
        ggplot2::geom_point(
          ggplot2::aes(x = pred_diff, y = model_diff),
          alpha = 0.7, colour = "darkolivegreen4", size = 3
        ) +
        ggplot2::labs(
          y = bquote(
            Delta ~ "Average " ~ .(
              stringr::str_replace(eval_metrics[i], "_", " ")
            ) ~ " between specifcations"
          ), # use bquote expression to include greek symbol Delta, object and string
          x = expression(paste(Delta, " number of predictors")),
          colour = "Model specifciation:"
        ) +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black"),
          axis.text.x = ggplot2::element_text(size = 7),
          legend.title = ggplot2::element_text(size = 8, face = "bold"),
          legend.text = ggplot2::element_text(size = 8),
          legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
          legend.title.align = 0.5,
          legend.position = "bottom"
        )


      Diff_plot_size <- ggplot2::ggplot(grouped_dep_var) +
        ggplot2::geom_point(
          ggplot2::aes(x = num_units, y = model_diff),
          alpha = 0.7, colour = "darkolivegreen4", size = 3
        ) +
        ggplot2::labs(
          y = bquote(
            Delta ~ "Average " ~ .(
              stringr::str_replace(eval_metrics[i], "_", " ")
            ) ~ " between training and test sets"
          ), # use bquote expression to include greek symbol Delta, object and string
          x = "Number of instances in dataset",
          colour = "Model specifciation:"
        ) +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black"),
          axis.text.x = ggplot2::element_text(size = 7),
          legend.title = ggplot2::element_text(size = 8, face = "bold"),
          legend.text = ggplot2::element_text(size = 8),
          legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
          legend.title.align = 0.5,
          legend.position = "bottom"
        )

      # append outputs to list
      comparison_output[[eval_metrics[i]]] <- list(
        model_summary_table = model_eval_summary,
        Shapiro_result = Shapiro_result,
        stat_test_result = res,
        test_type = test_type,
        violin_plot = violin_plot,
        Diff_plot_pred_change = Diff_plot_pred,
        Diff_plot_train_test = Diff_plot_size,
        Dot_plot = Dot_plot
      )
    } else if (length(table(model_eval_summary[, .SD, .SDcols = grouping_var])) > 2) {
      # Assumptions: Outliers, Normality and Sphericity

      # outlier detection
      outliers <- model_eval_summary |>
        dplyr::group_by(!!grouping_var) |>
        rstatix::identify_outliers(eval_metrics[i])
      data.frame(outliers)

      # remove outliers to conduct hypothesis testing with vs. without them
      model_eval_summary_noutliers <- subset(
        model_eval_summary, !(trans_name %in% outliers$trans_name)
      )

      # Shapiro-wilks test for normality of residuals
      Shapiro_result <- shapiro.test(residuals(lm(model_formula, data = model_eval_summary)))

      # Sphericity: The anova_test function automatically performs the Mauchly test
      # for Sphericity and corrects the results accordingly

      # Hypothesis testing
      if (Shapiro_result$p.value > 0.05) {
        print("residuals are normally distributed using repeated measures ANOVA")

        aov_res <- rstatix::anova_test(
          data = model_eval_summary, dv = eval_metrics[i], wid = trans_name, within = Model_name
        )
        res <- rstatix::get_anova_table(aov_res)

        aov_res_nout <- rstatix::anova_test(
          data = model_eval_summary_noutliers,
          dv = eval_metrics[i], wid = trans_name, within = Model_name
        )
        res_noutliers <- rstatix::get_anova_table(aov_res_nout)

        test_type <- "parametric"

        # post-hoc testing
        if (res$p < 0.05) {
          post_hoc <- statsExpressions::pairwise_comparisons(
            model_eval_summary,
            x = Model_name,
            y = eval_metrics[i],
            subject.id = trans_name,
            type = test_type,
            paired = TRUE,
            p.adjust.method = "bonferroni",
            k = 3L
          )

          post_hoc_noutliers <- statsExpressions::pairwise_comparisons(
            model_eval_summary_noutliers,
            x = Model_name,
            y = eval_metrics[i],
            subject.id = trans_name,
            type = test_type,
            paired = TRUE,
            p.adjust.method = "bonferroni",
            k = 3L
          )
        } else if (res$p > 0.05) {
          post_hoc <- "non-significant result in hypothesis test no need for post-hoc"
          post_hoc_noutliers <- "non-significant result in hypothesis test no need for post-hoc"
        } # close post-hoc testing
      } else if (Shapiro_result$p.value < 0.05) { # close non-parametric hypothesis testing

        print("data does not meet the assumptions for parametric repeated measures
(because we are dealing with 'paired samples') one-way ANOVA so the Friedman test is appropriate")

        # Friedman test requires complete blocks i.e. all records for all groups
        # because some models may be missing we need to remove these transitions
        friedman_formula <- as.formula(paste0(eval_metrics[i], " ~ ", grouping_var, "|trans_name"))

        res <- rstatix::friedman_test(friedman_formula, data = model_eval_summary)
        test_type <- "nonparametric"

        res_noutliers <- rstatix::friedman_test(
          friedman_formula,
          data = model_eval_summary_noutliers
        )

        # Post-Hoc testing
        if (res$p < 0.05) {
          # perform post-hoc testing using Conover's all pairs comparisons tests
          post_hoc <- PMCMRplus::frdAllPairsConoverTest(
            y = model_eval_summary[[eval_metrics[i]]], groups = model_eval_summary[[grouping_var]],
            blocks = model_eval_summary$trans_name, p.adjust.method = "bonf"
          )

          post_hoc_noutliers <- PMCMRplus::frdAllPairsConoverTest(
            y = model_eval_summary_noutliers[[eval_metrics[i]]],
            groups = model_eval_summary_noutliers[[grouping_var]],
            blocks = model_eval_summary_noutliers$trans_name, p.adjust.method = "bonf"
          )
        } else {
          post_hoc <- "non-significant result in hypothesis test no need for post-hoc"
          post_hoc_noutliers <- "non-significant result in hypothesis test no need for post-hoc"
        } # close post-hoc testing
      } # close non-parametric hypothesis testing

      # produce violin plot of results eith repeated measures ANOVA or the Friedman test
      # flexible due to vector of test_type
      # However for the Friedman test ggstatsplot defaults to the Durbin-Conover test
      # for post-hoc which is not desirable; Instead swap this for the results of the
      # Conover's all pairs comparison tests

      if (test_type == "parametric") {
        violin_with_pairwise <- ggstatsplot::ggwithinstats(
          data = model_eval_summary,
          x = Model_name,
          y = !!eval_metrics[i],
          type = test_type,
          xlab = "Model specification", ## label for the x-axis
          ylab = stringr::str_replace(eval_metrics[i], "_", " "),
          pairwise.comparisons = TRUE,
          sample.size.label = F,
          package = "yarrr", ## package from which color palette is to be taken
          palette = "info2", ## choosing a different color palette
          ggplot.component = list(
            ggplot2::scale_y_continuous(
              limits = c(0.0, 1), breaks = seq(from = 0, to = 1, by = 0.1)
            ),
            ggplot2::theme(
              text = ggplot2::element_text(family = "Times New Roman"),
              plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
              axis.line = ggplot2::element_line(1),
              panel.background = ggplot2::element_blank(),
              axis.text = ggplot2::element_text(colour = "black"),
              axis.text.x = ggplot2::element_text(size = 8)
            )
          )
        )
      } else {
        violin_with_pairwise <- ggstatsplot::ggwithinstats(
          data = model_eval_summary,
          x = !!grouping_var,
          y = !!eval_metrics[i],
          type = test_type,
          xlab = "Model specification", ## label for the x-axis
          ylab = stringr::str_replace(eval_metrics[i], "_", " "),
          pairwise.comparisons = FALSE,
          sample.size.label = FALSE,
          results.subtitle = FALSE,
          package = "yarrr", ## package from which color palette is to be taken
          palette = "info2", ## choosing a different color palette
          ggplot.component = list(
            ggplot2::theme(
              text = ggplot2::element_text(family = "Times New Roman"),
              plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
              axis.line = ggplot2::element_line(1),
              panel.background = ggplot2::element_blank(),
              axis.text = ggplot2::element_text(colour = "black"),
              axis.text.x = ggplot2::element_text(size = 8)
            )
          ),
          point.args = list(
            alpha = 0.4,
            position = ggplot2::position_jitter(w = 0.05, h = 0, seed = 123)
          )
        )

        if (res$p < 0.05) {
          # clean post-hoc results
          post_hoc_results <- post_hoc$p.value # extract the p.value matrix
          ph_df <- data.table::melt(
            post_hoc_results,
            na.rm = TRUE,
            value.name = "p_value"
          ) # matrix to df
          ph_df$groups <- paste0(
            "c(\"", ph_df$Var1, "\" , \" ", ph_df$Var2, "\")"
          ) # get groups column right
          ph_plot_res <- ph_df |> dplyr::arrange(Var1) # sort by the first column
          ph_plot_res$asterisk_label <- sapply(ph_plot_res$p_value, function(x) {
            if (x <= 0.01) {
              "**"
            } else {
              "*"
            }
          }) # add a column for asterisk reporting
          ph_plot_res$y_pos <- seq(1, (1 + (nrow(ph_plot_res) - 1) * 0.095), by = 0.095)
          ph_plot_res <- dplyr::filter(
            ph_plot_res, p_value < 0.05
          ) # filter out non-significant entries


          violin_with_pairwise <- violin_with_pairwise + ggsignif::geom_signif(
            inherit.aes = FALSE,
            data = ph_plot_res,
            ggplot2::aes(
              xmin = Var1,
              xmax = Var2,
              annotations = asterisk_label, y_position = y_pos, group = groups
            ),
            textsize = 4, tip_length = 0.01, vjust = 0.5, manual = T
          )
        }
      } # close else chunk of plot creation


      # create scatter plot of model performance with a 0.7 threshold line on

      # vector value for threshold value
      thresh <- NULL
      label_y <- NULL
      thresh <- if (eval_metrics[i] == "AUC") {
        0.7
      } else {
        0.4
      }
      label_y <- thresh - 0.05

      # calculate number of models below threshold
      models_below_thresh <- model_eval_summary[model_eval_summary[[eval_metrics[i]]] < thresh, ]
      num_below_thresh <- models_below_thresh |> dplyr::count(Model_name)
      num_below_thresh$clean_name <- sapply(num_below_thresh[, 1], function(x) {
        stringr::str_split(stringr::str_remove(x, ".downsampled"), "\\.")[[1]][2]
      })
      num_below_thresh$label <- with(
        num_below_thresh, paste0(num_below_thresh$clean_name, " (", num_below_thresh[, 2], ")")
      )


      model_performance_scatter <- ggplot2::ggplot(model_eval_summary) +
        ggplot2::geom_jitter(
          ggplot2::aes_string(
            x = grouping_var, y = eval_metrics[i], colour = grouping_var
          ),
          alpha = 0.7, size = 3, width = 0.25
        ) +
        ghibli::scale_colour_ghibli_d("PonyoMedium") +
        ggplot2::geom_hline(yintercept = thresh, linetype = "dashed", color = "darkred") +
        ggplot2::scale_color_manual(
          name = "Model specification (Number of models below threshold)",
          values = ghibli_palettes$PonyoMedium[1:4],
          labels = c(num_below_thresh$label)
        ) +
        ggplot2::labs(
          y = paste0("Average ", stringr::str_replace(eval_metrics[i], "_", " ")),
          x = "Model specification"
        ) +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black"),
          axis.text.x = ggplot2::element_text(size = 7),
          legend.title = ggplot2::element_text(size = 8, face = "bold"),
          legend.text = ggplot2::element_text(size = 8),
          legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
          legend.title.align = 0.5,
          legend.position = "right"
        )

      model_performance_scatter_labelled <-
        ggplot2::ggplot(model_eval_summary) +
        ggplot2::geom_jitter(
          ggplot2::aes_string(
            x = grouping_var, y = eval_metrics[i],
            colour = grouping_var
          ),
          alpha = 0.7, size = 3, width = 0.25
        ) +
        ghibli::scale_colour_ghibli_d("PonyoMedium") +
        ggplot2::geom_segment(
          ggplot2::aes(x = 0, xend = 4.2, y = thresh, yend = thresh),
          linetype = "dashed", color = "black"
        ) +
        ggplot2::geom_label(
          data = num_below_thresh,
          ggplot2::aes(
            x = Model_name, label = n, y = (label_y - 0.15)
          ), family = "Times New Roman"
        ) +
        ggplot2::scale_color_manual(
          name = "Model specification",
          values = ghibli_palettes$PonyoMedium[1:4],
          labels = c(num_below_thresh$clean)
        ) +
        ggplot2::labs(
          y = paste0("Average ", stringr::str_replace(eval_metrics[i], "_", " ")),
          x = "Model specification"
        ) +
        ggplot2::theme(
          text = ggplot2::element_text(family = "Times New Roman"),
          plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
          axis.line = ggplot2::element_line(1),
          panel.background = ggplot2::element_blank(),
          axis.text = ggplot2::element_text(colour = "black"),
          legend.title = ggplot2::element_text(size = 10, face = "bold"),
          legend.text = ggplot2::element_text(size = 10),
          legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
          legend.position = c(0.8, 0.1)
        ) +
        ggplot2::guides(colour = ggplot2::guide_legend(
          title.position = "top", title.hjust = 0.5
        ))

      if (eval_metrics[i] == "AUC") {
        model_performance_scatter_labelled <-
          ggplot2::ggplot(model_eval_summary) +
          ggplot2::geom_jitter(
            ggplot2::aes_string(
              x = grouping_var, y = eval_metrics[i], colour = grouping_var
            ),
            alpha = 0.7, size = 3, width = 0.25
          ) +
          ghibli::scale_colour_ghibli_d("PonyoMedium") +
          ggplot2::geom_segment(
            ggplot2::aes(x = 0, xend = 4.2, y = 0.7, yend = 0.7),
            linetype = "dashed",
            color = "black"
          ) +
          ggplot2::geom_label(
            data = num_below_thresh,
            ggplot2::aes(x = Model_name, label = n, y = 0.55),
            family = "Times New Roman"
          ) +
          ggplot2::scale_color_manual(
            name = "Model specification",
            values = ghibli_palettes$PonyoMedium[1:4],
            labels = c(num_below_thresh$clean)
          ) +
          ggplot2::labs(
            y = paste0("Average ", stringr::str_replace(eval_metrics[i], "_", " ")),
            x = "Model specification"
          ) +
          ggplot2::theme(
            text = ggplot2::element_text(family = "Times New Roman"),
            plot.title = ggplot2::element_text(size = ggplot2::rel(1.1), hjust = 0.5),
            axis.line = ggplot2::element_line(1),
            panel.background = ggplot2::element_blank(),
            axis.text = ggplot2::element_text(colour = "black"),
            legend.title = ggplot2::element_text(size = 10, face = "bold"),
            legend.text = ggplot2::element_text(size = 10),
            legend.key = ggplot2::element_rect(fill = "white", colour = "white"),
            legend.position = c(0.8, 0.1)
          ) +
          ggplot2::guides(colour = ggplot2::guide_legend(title.position = "top", title.hjust = 0.5))
      }

      # append outputs to list
      comparison_output[[eval_metrics[i]]] <- list(
        model_summary_table = model_eval_summary,
        Shapiro_result = Shapiro_result,
        stat_test_result = res,
        post_hoc_results = post_hoc,
        test_type = test_type,
        violin_plot = violin_with_pairwise,
        model_performance_scatter = model_performance_scatter,
        model_performance_scatter_labelled = model_performance_scatter_labelled,
        minus_outliers_results = list(stat_test = res_noutliers, post_hoc = post_hoc_noutliers),
        below_threshold_models = models_below_thresh
      )
    } # close else if chunk
  } # close for loop over eval_metrics
  return(comparison_output)
} # close function

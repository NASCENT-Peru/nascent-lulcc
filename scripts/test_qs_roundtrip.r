#!/usr/bin/env Rscript
# Quick diagnostic: test qs vs rds round-trip for mlr3 ranger learner

suppressPackageStartupMessages({
  library(qs)
  library(mlr3)
  library(mlr3learners)
  library(ranger)
})

qs_path <- "E:/nascent-lulcc-agg/outputs/transition_models/2018_2022/water_body-mining_selva_andina.qs"

cat("=== Loading .qs model ===\n")
m <- qs::qread(qs_path)
cat("model_type:     ", m$model_type, "\n")
cat("predictor_names:", paste(m$predictor_names, collapse = ", "), "\n")
cat("learner class:  ", class(m$learner)[[1L]], "\n")

# Synthetic fixture — same structure as training data, no NAs
fixture <- as.data.frame(matrix(0.5, nrow = 5L, ncol = length(m$predictor_names)))
names(fixture) <- m$predictor_names

cat("\n=== predict_newdata on qs-loaded learner ===\n")
tryCatch({
  pred <- m$learner$predict_newdata(newdata = fixture)
  cat("OK — prob[1:3,]:\n")
  print(head(pred$prob, 3L))
}, error = function(e) {
  cat("ERROR:", conditionMessage(e), "\n")
})

# Now test: save as rds, reload, predict
rds_path <- tempfile(fileext = ".rds")
cat("\n=== Saving as .rds and re-loading ===\n")
saveRDS(m, rds_path)
m2 <- readRDS(rds_path)
cat("learner class (rds):", class(m2$learner)[[1L]], "\n")

tryCatch({
  pred2 <- m2$learner$predict_newdata(newdata = fixture)
  cat("OK — prob[1:3,]:\n")
  print(head(pred2$prob, 3L))
}, error = function(e) {
  cat("ERROR:", conditionMessage(e), "\n")
})

cat("\nDone.\n")

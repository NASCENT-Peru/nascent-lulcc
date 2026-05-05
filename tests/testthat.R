# testthat entry point for the nascent-lulcc package
# Run with: Rscript -e 'testthat::test_dir("tests/testthat")'

library(testthat)

test_dir(file.path(dirname(sys.frame(1)$ofile %||% "."), "testthat"))

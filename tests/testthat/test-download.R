test_that("download_comex works", {
  fl <- c("IMP_TOTAIS_CONFERENCIA.csv",
  "EXP_TOTAIS_CONFERENCIA.csv")
  res <- comexstatr:::download_comex(fl)
  expect_equal(basename(res), fl)
})

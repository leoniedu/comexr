library(testthat)
#library(comexr)
library(lubridate)
library(dplyr)

# Test data
test_data <- tibble(
  date = seq(ymd("2023-01-01"), ymd("2023-12-01"), by = "month"),
  qt_stat = c(100, 150, 200, 80, 120, 90, 110, 140, 180, 95, 130, 105),
  kg_net = qt_stat * 10,
  fob_usd = qt_stat * 100,
  freight_usd = qt_stat * 5,
  insurance_usd = qt_stat * 2
)

# Tests for comex_sum

test_that("comex_sum aggregates correctly", {
  result <- comex_sum(test_data)
  expect_equal(result$qt_stat, sum(test_data$qt_stat))
  expect_equal(result$kg_net, sum(test_data$kg_net))
  expect_equal(result$fob_usd, sum(test_data$fob_usd))
})

test_that("comex_sum handles custom column selection", {
  result <- comex_sum(test_data, x = c("qt_", "fob_"))
  expect_equal(names(result), c("qt_stat", "fob_usd"))
  expect_equal(result$qt_stat, sum(test_data$qt_stat))
  expect_equal(result$fob_usd, sum(test_data$fob_usd))
})

# Tests for comex_roll

test_that("comex_roll calculates rolling sums", {
  result <- comex_roll(test_data, k = 3)
  expect_equal(result$qt_stat_3[3], sum(test_data$qt_stat[1:3]))
  expect_true(all(is.na(result$qt_stat_3[1:2])))  # Incomplete windows should be NA
})

test_that("comex_roll handles custom column selection and window size", {
  result <- comex_roll(test_data, x = c("kg_", "freight_"), k = 6)
  expect_equal(names(result), c(names(test_data), "kg_net_6", "freight_usd_6"))
  expect_equal(result$kg_net_6[6], sum(test_data$kg_net[1:6]))
})


# Tests for ncm

test_that("ncm cleans NCM codes correctly", {
  expect_equal(ncm("0101.2100"), "01012100")
  expect_equal(ncm("20-01.10-00"), "20011000")
  expect_equal(ncm("abc123def456", checkncm = FALSE), "123456")
  expect_equal(ncm("abc123def45678"), "12345678")
})

test_that("ncm validates NCM codes", {
  expect_error(ncm("1234567"), "Not all NCMs valid or NA!")
  expect_error(ncm("abc1"), "Not all NCMs valid or NA!")
})

test_that("ncm handles NA values and empty strings", {
  expect_equal(ncm(c("01012100", NA, "")), c("01012100", NA, NA))
})

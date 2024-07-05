cdircomex <- path.expand(rappdirs::user_cache_dir("comexstatr"))
ddircomex <- file.path(rappdirs::user_data_dir("comexstatr"))

#' Merge and Format NCM Datasets from Comexstat
#'
#' This function retrieves multiple NCM (Nomenclatura Comum do Mercosul) datasets from the `comexstat` package,
#' merges them into a single data frame, and converts all columns to character format.
#'
#' @param files A character vector specifying the names of the NCM datasets to retrieve from `comexstat`.
#'   Defaults to `c("ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade")`.
#'
#' @return A merged data frame containing all specified NCM datasets, with all columns converted to character format.
#'
#' @details
#' This function streamlines the process of working with multiple NCM datasets from the `comexstat` package. It performs the following steps:
#'
#' 1. **Retrieval:** Retrieves the specified NCM datasets using the `comexstat` function.
#' 2. **Merging:** Combines all retrieved datasets into a single data frame using left joins.
#' 3. **Format Conversion:** Converts all columns in the merged data frame to character format for consistent data manipulation.
#'
#' The function suppresses messages and warnings during the retrieval and merging steps to provide a cleaner output.
#'
#' @examples
#' \dontrun{
#' # Merge and format default NCM datasets
#' merged_ncms <- ncms()
#'
#' # Merge specific NCM datasets (e.g., only "ncm" and "ncm_cuci")
#' merged_ncms <- ncms(files = c("ncm", "ncm_cuci"))
#' }
#'
#' @export
ncms <- function(files = c("ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade")) {
  suppressMessages(suppressWarnings({
    ncms_list <- purrr::map(files, comexstat)
    ncms_merged <- Reduce(dplyr::left_join, ncms_list)
    ncms_merged |>
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
  }))
}



#' Rewrites the data read from cache directory into partitioned files.
#' @noRd
#' @return the directory where the partition is written to.
#' @details The downloaded files are one for the exports and other for imports, with data for the entire period available (1997-).
#'
#' This function reads those files using arrow by calling function comexstat_raw. It then writes to the data directory the partitioned files.
# comexstat_rewrite <- function() {
#   ## write partitioned data
#   ddir_partition <- file.path(ddircomex, "comexstat_partition")
#   unlink(ddir_partition, recursive = TRUE)
#   dir.create(ddir_partition, showWarnings = FALSE)
#   comexstat_ncm() |>
#     ## data will be written using the partitions
#     ## in the group_by statement
#     dplyr::group_by(direction, year) |>
#     arrow::write_dataset(ddir_partition, format = "parquet")
#   ddir_partition
# }


#' Create a data frame with a co_ano_mes_m column with an id every m months
#' @noRd
#' @param m number of months to aggregate
#' @param data data frame or outpur of comexstat with co_ano and co_mes columns
#'
#' @return tibble with columns: co_ano, co_mes, co_ano_mes_m
#'
#' @examples
#' ym(m=6, comexstat()|>filter(co_ano>=2021))
# ym <- function(m, data=comexstat()) {
#   mm <- data|>
#     dplyr::distinct(co_ano, co_mes)|>
#     dplyr::arrange(desc(co_ano), desc(co_mes))|>
#     dplyr::collect()|>
#     head(1) |>
#     dplyr::mutate(co_ano_mes=lubridate::make_date(co_ano, co_mes,1))
#   md <- dplyr::tibble(
#     co_ano_mes=seq.Date(from=mm$co_ano_mes[1]-base::months(m-1), to=mm$co_ano_mes[1], by="month"))|>
#     dplyr::mutate(co_ano=lubridate::year(co_ano_mes),
#            co_mes=lubridate::month(co_ano_mes)
#            )
#   md
# }

#' Create id (co_ano_mes_m) by m months
#' @noRd
#' @param m number of months
#'
#' @return tibble with id variables: co_ano_mes_m, co_pais, fluxo, co_ncm; and sums of qt_estat, vl_cif, vl_fob
#'
#' @details Since it outputs every possible combination of co_ano_mes_m, co_pais, fluxo and co_ncm, it can be very large, and time consuming.
#'
# comexstat_m <- function(m=12, data=comextat()) {
#   ## fix, allow choosing id variables
#   ymm <- ym(m, data=data)
#   ##aggregate by months
#   res0 <- data|>
#     dplyr::inner_join(ymm)
#   u_ncm <- res0|>dplyr::distinct(co_ncm)|>dplyr::collect()
#   u_pais <- res0|>dplyr::distinct(co_pais)|>dplyr::collect()
#   u_data <- ymm|>dplyr::distinct(co_ano_mes_m)
#   g <- tidyr::expand_grid(u_data, u_pais, fluxo=c("imp", "exp"), u_ncm)
#   res <- res0|>
#     dplyr::group_by(co_ano_mes_m, co_pais, co_ncm, fluxo)|>
#     dplyr::summarise(qt_estat=sum(qt_estat), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob), kg_liquido=sum(kg_liquido), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob))
#   g|>
#     dplyr::left_join(res, copy=TRUE)|>
#     dplyr::mutate_if(is.numeric, tidyr::replace_na, 0)|>
#     dplyr::mutate(vl_cif=dplyr::if_else(fluxo=="imp", vl_cif, NA_real_))
# }



## complete with implicit zeros
# comexstat_complete <- function(data=comexstat(),k=12) {
#   require(rlang)
#   ## get the full date range
#   range_dates <- data|>
#     dplyr::ungroup()|>
#     dplyr::summarise(min_date=min(co_ano_mes), max_date=max(co_ano_mes))|>
#     dplyr::collect()
#   ## expected months
#   seq_d <- seq.Date(from = range_dates$min_date, to=range_dates$max_date, by="month")
#   ## throws an error if any day!=1
#   stopifnot(all(lubridate::day(seq_d)==1))
#   df <- data|>
#     dplyr::group_by(co_ano_mes, .add=TRUE)|>
#     #dplyr::summarise(vl_fob=sum(vl_fob, na.rm=TRUE), vl_cif=sum(vl_cif, na.rm=TRUE), kg_liquido=sum(kg_liquido, na.rm=TRUE), qt_estat=sum(qt_estat, na.rm=TRUE), .groups = "keep")|>
#     #dplyr::mutate(across(where(is.numeric), tidyr::replace_na))|>
#     dplyr::summarise(across(where(is.numeric), sum), .groups = "keep")|>
#     collect()
#   g1 <- dplyr::group_vars(df)
#   df|>
#     dplyr::ungroup()|>
#     tidyr::complete(co_ano_mes=seq_d , !!!data_syms(g1|>setdiff("co_ano_mes")) #,fill = list(vl_fob=0, vl_cif=0, kg_liquido=0, qt_estat=0)
#                     )|>
#     group_by(!!!data_syms(g1))
# }


#' Aggregate Comex Data by Summing Columns with Matching Prefixes
#'
#' This function aggregates a Comex (Brazilian trade) dataset by calculating the sum of columns whose names
#' start with any of the specified prefixes.
#'
#' @param data A data frame or tibble containing Comex data.
#' @param x A character vector specifying the column name prefixes to match and sum.
#'   Defaults to `c("qt_stat", "kg_net", "fob_", "freight_", "insurance_", "cif_")`, which captures common columns
#'   related to quantities, weights, and various costs.
#'
#' @return A tibble with one row containing the sum of each matched column. The column names in
#'   the result are the same as the input column names.
#'
#' @details
#' This function simplifies the process of aggregating multiple related variables in Comex data by allowing
#' you to specify prefixes instead of listing each column individually. For example, the default prefix `"fob_"`
#' would match columns like `fob_usd`, `fob_brl`, etc., and sum them all together.
#'
#' @examples
#' # Create a sample Comex dataset
#' comex_data <- data.frame(
#'   qt_stat = c(100, 250, 80),
#'   kg_net = c(5000, 12000, 3500),
#'   fob_usd = c(15000, 38000, 10000),
#'   fob_brl = c(75000, 190000, 50000),
#'   freight_usd = c(2000, 5000, 1500)  # Additional column with 'fob_' prefix
#' )
#'
#' # Aggregate columns starting with default prefixes
#' summary_data <- comex_sum(comex_data)
#' summary_data
#'
#' # Aggregate only columns starting with 'qt_' and 'fob_'
#' summary_data <- comex_sum(comex_data, x = c("qt_", "fob_"))
#' summary_data
#'
#' @export
comex_sum <- function(data, x = c("qt_stat", "kg_net", "fob_", "freight_", "insurance_", "cif_")) {
  data %>%
    summarise(across(starts_with(x), sum, .names = "{.col}"))
}

#' Calculate Rolling Sums for Comex Data
#'
#' This function computes rolling sums over a specified window for selected columns in a Comex (Brazilian trade) dataset. It operates on columns that start with the specified prefixes.
#'
#' @param data A data frame or tibble containing Comex data, with a `date` column.
#' @param x A character vector specifying the prefixes of the column names for which to calculate rolling sums.
#'   Defaults to `c("qt_stat", "kg_net", "fob_", "freight_", "insurance_", "cif_")`, which captures common columns
#'   related to quantities, weights, and various costs.
#' @param k An integer specifying the window size (in months) for the rolling sum calculation. Defaults to 12.
#'
#' @return A modified version of the input `data`, with new columns added for each selected column.
#'   The new column names are of the format "{original_col_name}_{k}", where `k` is the window size.
#'   These new columns contain the rolling sums for the corresponding original columns.  Rows with
#'   incomplete windows (less than `k` months of data) will have NA values in the new columns.
#'
#' @details
#' This function uses the `slider` package's `slide_index_dbl` function to efficiently calculate rolling sums.
#'
#' The rolling sum for each date is calculated by summing the values from the current date up to `k-1` months prior.
#' Since the `.complete` argument in `slide_index_dbl` is set to `TRUE`, the function will only calculate rolling sums
#' for dates where there are at least `k` months of prior data available. Rows with incomplete windows will have `NA` values.
#'
#' @examples
#' #' # Create sample Comex data
#' set.seed(123)
#' comex_data <- tibble(
#'   date = seq(from = ymd("2022-01-01"), to = ymd("2023-12-01"), by = "month"),
#'   qt_stat = rpois(24, lambda = 100),
#'   fob_usd = runif(24, min = 500, max = 2000)
#' )
#' # Example usage with default prefixes and window size of 12 months:
#' rolled_data <- comex_roll(comex_data)
#'
#' # Calculate 6-month rolling sums for columns starting with 'qt_' and 'fob_':
#' rolled_data <- comex_roll(comex_data, x = c("qt_", "fob_"), k = 6)
#' head(rolled_data,10)
#'
#' @export
comex_roll <- function(data, x=c("qt_stat", "kg_net", "fob_", "freight_", "insurance_", "cif_"), k=12) {
  data|>
    dplyr::arrange(date)|>
    dplyr::collect()|>
    dplyr::mutate(dplyr::across(
      tidyselect::starts_with(x), ~slider::slide_index_dbl(.x = .x,
                                                           .before = months(k-1),
                                                           .complete = TRUE,
                                                           .f = function(z) sum(z), .i = date), .names = "{.col}_{k}"))
}


#' Standardize and Validate NCM Codes
#'
#' This function cleans and optionally validates NCM (Nomenclatura Comum do Mercosul) codes.
#'
#' @param x A character vector containing NCM codes.
#' @param checkncm A logical flag indicating whether to perform NCM validation. Defaults to `TRUE`.
#'
#' @return A character vector of cleaned NCM codes.
#'   * Non-numeric characters are removed.
#'   * Empty strings are converted to `NA`.
#'   * If `checkncm` is `TRUE`, an error is raised if any non-NA values do not have exactly 8 characters.
#'
#' @details
#' NCM codes are standardized to contain only digits (0-9). This is important for consistency in data analysis and comparison.
#'
#' The optional validation step ensures that the cleaned NCM codes adhere to the expected format of 8 digits.
#'
#' @examples
#' # Clean and validate valid NCM codes
#' ncm(c("01012100", "02011000", "invalid code"))  # Raises an error due to "invalid code"
#'
#' # Clean without validation
#' ncm(c("01012100", "02011000", "invalid code"), checkncm = FALSE)
#' # Returns: c("01012100", "02011000", NA)
#'
#' @export
ncm <- function(x, checkncm = TRUE) {
  x <- gsub("[^0-9]", "", x)
  x[nchar(x) == 0] <- NA_character_
  if (checkncm) {
    if (!all(nchar(x %>% na.omit()) == 8)) {
      stop("Not all NCMs valid or NA!")
    }
  }
  x
}

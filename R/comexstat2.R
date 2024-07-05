#' Download and processes deflators (CPI/USA, IPCA/Brazil, Exchange rate BRL/USD)
#'
#' @param updated
#'
#' @return A data frame with columns co_ano_mes (date), ipca (monthly inflation from Brazil), ipca_i (monthly inflation from Brazil indexed such as 1997-01-01 is 1), cpi index (monthly inflation from USA).
#' @export
#'
get_deflators <- function(updated = Sys.Date(), na_omit = FALSE) {
  # Function to get deflators (CPI, IPCA, and exchange rates)
  # Get IPCA data and join with exchange rate data (brlusd)
  res <- get_ipca() |>
    dplyr::full_join(get_brlusd(), by = "date") |>
    # Join with CPI data
    dplyr::full_join(get_cpi(), by = "date") |>
    # Rename 'date' column to 'co_ano_mes'
    dplyr::rename(co_ano_mes = date) |>
    # Sort by 'co_ano_mes'
    dplyr::arrange(co_ano_mes)
  # Remove rows with missing values if na_omit is TRUE
  if (na_omit) res <- na.omit(res)
  return(res)
}



#' Read and Standardize Comexstat Data
#'
#' @description This function reads a specified Comexstat (Brazilian trade statistics) dataset
#'   from a local CSV file, cleans the column names, and applies standardized column renaming.
#'
#' @param table A character string specifying the name of the Comexstat table to read.
#' @param dir The directory containing the Comexstat data file (defaults to `comexstatr:::cdircomex`).
#' @param extension The file extension of the Comexstat data file (defaults to ".csv").
#' @param ... Additional arguments passed to `readr::read_csv2`, such as `col_types`, `na`, etc.
#'
#' @return A tibble (data frame) containing the specified Comexstat data with standardized column names.
#'
#' @examples
#' \dontrun{
#' # Read the "ncm" table from the default directory
#' ncm_data <- comex("ncm")
#'
#' # Read the "ncm_cgce" table from a specific directory ("my_data")
#' ncm_cgce_data <- comex("ncm_cgce", dir = "my_data")
#'
#' # Read a table with a different extension (e.g., .txt)
#' other_data <- comex("other_table", extension = ".txt")
#'
#' }
#'
#' @export
comex <- function(table, dir = comexstatr:::cdircomex, extension = ".csv", ...) {
  readr::read_csv2(
    file.path(dir, paste0(table, extension)),
    locale = readr::locale(encoding = "latin1"),
    ...
  ) |>
    janitor::clean_names() |>
    comex_rename()
}


comex_rename <- function(x) {
  lookup <- c(year = "co_ano",
              month = "co_mes",
              country_code = "co_pais",
              country_name = "no_pais_ing",
              block_code = "co_bloco",
              block_name = "no_bloco_ing",
              qt_stat = "qt_estat",
              kg_net="kg_liquido",
              fob_usd="vl_fob",
              freight_usd="vl_frete",
              insurance_usd="vl_seguro",
              number_of_lines="numero_linhas",
              file="arquivo"
  )
  x|>
    dplyr::rename_with(tolower)|>
    dplyr::rename(dplyr::any_of(lookup))|>
    dplyr::mutate(across(dplyr::any_of(c("country_code")), as.integer))|>
    dplyr::mutate(across(dplyr::any_of(c("ncm")), as.character))|>
    dplyr::mutate(across(dplyr::any_of(c("qt_stat", "kg_net", "fob_usd", "freight_usd", "insurance_usd")), bit64::as.integer64))
}

comex_check <- function() {
  cached <- comex_ncm()|>
    dplyr::group_by(year, direction)|>
    dplyr::mutate(number_of_lines=1)|>
    dplyr::summarise(across(c(qt_stat, kg_net, fob_usd, freight_usd, insurance_usd,number_of_lines), sum))|>
    dplyr::collect()
  cached_hs4 <- comex_hs4()|>
    dplyr::group_by(year, direction)|>
    dplyr::mutate(number_of_lines=1)|>
    dplyr::summarise(across(c(fob_usd, number_of_lines), sum))|>
    dplyr::collect()
  conf <- comex("imp_totais_conferencia")|>
    dplyr::bind_rows(comex("exp_totais_conferencia"))|>
    dplyr::mutate(direction=dplyr::if_else(grepl("IMP", file), "imp", "exp"))
  checked <- conf|>dplyr::anti_join(cached, by = join_by(year, qt_stat, kg_net, fob_usd, freight_usd, insurance_usd, number_of_lines, direction))
  if (nrow(checked)>0) {
    toprint <- checked|>dplyr::inner_join(cached, by = join_by(year, direction), suffix = c("_check", "_cached"))
    print(toprint|>dplyr::select(sort(names(toprint)))|>t())
    stop("Conference file mismatch NCM with downloaded data!")
  }
  checked_hs4 <- conf|>dplyr::anti_join(cached_hs4, by = join_by(year, fob_usd, direction))
  if (nrow(checked_hs4)>0) stop("Conference file mismatch HS4 with downloaded data!")
  print("All clear!")
}

#' Open ComexStat HS4 Trade Dataset
#'
#' This function opens the ComexStat HS4 trade data as an Arrow Dataset. The data is assumed to be located in the ComexStat data directory and to have been downloaded using the `comex_download()` function.
#'
#' @return An Arrow Dataset containing combined import and export HS4 trade data. The dataset has the following columns:
#'   - `year`: Year (integer)
#'   - `month`: Month (integer)
#'   - `hs4`: 4-digit HS product code (integer)
#'   - `country_code`: Country code (integer)
#'   - `state`: Brazilian state abbreviation (string)
#'   - `mun_code`: Municipality code (integer)
#'   - `kg_net`: Net weight in kilograms (integer64)
#'   - `fob_usd`: FOB value in US dollars (integer64)
#'   - `direction`: Trade direction, either "exp" (export) or "imp" (import)
#'
#' @details This function assumes that the HS4 data files are named according to the following pattern:
#'   - Imports: "imp_[year]_mun.csv" (e.g., "imp_2023_mun.csv")
#'   - Exports: "exp_[year]_mun.csv" (e.g., "exp_2023_mun.csv")
#'
#' @examples
#' \dontrun{
#' # Open the ComexStat HS4 dataset:
#' hs4_dataset <- comexstat_hs4()
#'
#' # Explore the dataset:
#' print(hs4_dataset)
#' }
#'
#' @export
comex_hs4 <- function() {
  # Define schema for HS4 data
  # FIX: how to set schema including hive data
  # schema_hs4 <- arrow::schema(
  #   #direction = arrow::string(),
  #   year = arrow::int32(),
  #   month = arrow::int32(),
  #   hs4 = arrow::string(),
  #   country_code = arrow::int32(),
  #   state_abb = arrow::string(),
  #   mun_code = arrow::int32(),
  #   kg_net = arrow::int64(),
  #   fob_usd = arrow::int64()
  # )
  # Open import and export HS4 datasets
  hs4 <- arrow::open_delim_dataset(
    sources = file.path(comexstatr:::cdircomex, "hs4"),
    delim = ";",
    #partitioning = arrow::schema(direction = arrow::string()),
    #schema = schema_hs4,
    skip = 0)|>comex_rename()
  hs4
}

#' Open ComexStat NCM Trade Dataset
#'
#' This function opens the ComexStat NCM (8-digit product code) trade data as an Arrow Dataset. The data is assumed to be located in the  ComexStat data directory and to have been downloaded using the `comex_download()` function.
#'
#' @return An Arrow Dataset containing combined import and export NCM trade data, with an additional `direction` column indicating "exp" (export) or "imp" (import). The dataset has the following columns:
#'   - `year`: Year (integer)
#'   - `month`: Month (integer)
#'   - `ncm`: 8-digit NCM product code (string)
#'   - `unit_code`: Unit of measurement code (integer)
#'   - `country_code`: Country code (integer)
#'   - `state_abb`: Brazilian state abbreviation (string)
#'   - `transp_mode_code`: Transportation mode code (integer)
#'   - `urf_code`: Customs clearance unit code (integer)
#'   - `qt_stat`: Statistical quantity (integer64)
#'   - `kg_net`: Net weight in kilograms (integer64)
#'   - `fob_usd`: FOB value in US dollars (integer64)
#'   - `freight_usd`: Freight value in US dollars (integer64, only for imports)
#'   - `insurance_usd`: Insurance value in US dollars (integer64, only for imports)
#'   - `direction`: Trade direction, either "exp" (export) or "imp" (import) (derived)
#'
#' @details This function assumes that the NCM data files are named according to the following pattern:
#'   - Imports: "imp_[year].csv" (e.g., "imp_2023.csv")
#'   - Exports: "exp_[year].csv" (e.g., "exp_2023.csv")
#'
#'   It infers the trade direction based on the presence or absence of the `freight_usd` column (present only in imports).
#'
#' @examples
#' \dontrun{
#' library(dplyr)
#' # Open the ComexStat NCM dataset:
#' ncm_dataset <- comex_ncm()
#'
#' # Explore the dataset:
#' print(ncm_dataset)
#' ncm_dataset|>group_by(year, direction)|>summarise(fob_usd=sum(fob_usd), freight_usd=sum(freight_usd))|>mutate(p=freight_usd/fob_usd)|>collect()
#' comex_ncm()|>group_by(country_code)|>summarise(fob_usd=sum(fob_usd, na.rm=TRUE), freight_usd=sum(freight_usd))|>mutate(p=freight_usd/fob_usd)|>arrange(desc(p))|>collect()
#' }
#'
#' @export
comex_ncm <- function(check=FALSE) {
  # Define schemas for export and import NCM data
  schema_exp_ncm <- arrow::schema(
    year = arrow::int32(),
    month = arrow::int32(),
    ncm = arrow::string(),
    unit_code = arrow::int32(),
    country_code = arrow::int32(),
    state_abb = arrow::string(),
    transp_mode_code = arrow::int32(),
    urf_code = arrow::int32(),
    qt_stat = arrow::int64(),
    kg_net = arrow::int64(),
    fob_usd = arrow::int64()
  )

  schema_imp_ncm <- schema_exp_ncm$AddField(
    schema_exp_ncm |> length(),
    field = arrow::field("freight_usd", arrow::int64())
  )

  schema_imp_ncm <- schema_imp_ncm$AddField(
    schema_exp_ncm |> length() + 1,
    field = arrow::field("insurance_usd", arrow::int64())
  )

  # Open import and export NCM datasets
  imp_sources <- dir(file.path(comexstatr:::cdircomex, "ncm", "direction=imp"),
  pattern = "imp_[0-9]+.csv", full.names = TRUE)
  if (check) {
    for (i in imp_sources) arrow::open_delim_dataset(
      sources = i,
      delim = ";",
      schema = schema_imp_ncm,
      skip = 1
    )|>dplyr::count(year)|>dplyr::collect()
  }
  imp_ncm <- arrow::open_delim_dataset(
    sources = imp_sources,
    delim = ";",
    schema = schema_imp_ncm,
    skip = 1
  )
  exp_sources <- dir(file.path(comexstatr:::cdircomex, "ncm", "direction=exp"),
                     pattern = "exp_[0-9]+.csv", full.names = TRUE)
  if (check) {
    for (i in exp_sources) arrow::open_delim_dataset(
      sources = i,
      delim = ";",
      schema = schema_exp_ncm,
      skip = 1
    )|>dplyr::count(year)|>dplyr::collect()
  }
  exp_ncm <- arrow::open_delim_dataset(
    sources = exp_sources,
    delim = ";",
    schema = schema_exp_ncm,
    skip = 1
  )
  # Combine imports and exports, adding direction column
  df <- arrow::open_dataset(list(imp_ncm, exp_ncm)) |>
    dplyr::mutate(direction = if_else(is.na(freight_usd), "exp", "imp"),
                  date=lubridate::make_date(year, month),
                  cif_usd=fob_usd+freight_usd+insurance_usd)
  df
}





#' Deflate and Convert Comexstat Data
#'
#' This function deflates USD-denominated values in Comexstat data (trade statistics)
#' using specified deflators (CPI for USD, IPCA for BRL after converting using the exchange rate of the date of the statistics)
#'
#' @param data A data frame or tibble containing Comexstat data.
#' @param basedate An optional date object specifying the base date for deflation.
#'   If `NULL`, the latest available date in the `deflators` data is used.
#' @param deflators A data frame containing deflator time series data, including columns
#'   `cpi` (Consumer Price Index), `ipca_i` (Brazilian IPCA index), and `date`. Defaults
#'   to the `get_deflators()` function's output with missing values removed.
#'
#' @return A modified version of the input `data`, with the following changes:
#'   * New columns for deflated USD values (e.g., `fob_usd_deflated`, `cif_usd_deflated`).
#'   * New columns for BRL values based on the exchange rate and USD values (e.g., `fob_brl`, `cif_brl`).
#'   * New columns for deflated BRL values using the IPCA deflator (e.g., `fob_brl_deflated`).
#'
#' @details
#' The function performs the following steps:
#' 1. Handles missing values in `deflators`, issuing a warning if present.
#' 2. Determines the base date for deflation, either from `basedate` input or the latest date in `deflators`.
#' 3. Calculates deflation ratios (`cpi_r`, `ipca_r`) for each date relative to the base date.
#' 4. Joins the `deflators` data with the input `data` based on `date`.
#' 5. Deflates the USD-denominated columns (`fob_usd`, `cif_usd`, etc.) using `cpi_r`.
#' 6. Converts USD values to BRL based on the exchange rate in the `brlusd` column.
#' 7. Deflates the BRL values using `ipca_r`.
#' 8. Arranges the resulting data by `date`.
#'
#' @examples
#' \dontrun{
#' # Deflate using default data and latest base date
#' deflated_data <- comex_deflate()
#'
#' # Deflate with a specific base date (e.g., 2023-12-31)
#' deflated_data <- comex_deflate(basedate = as.Date("2023-12-31"))
#' }
#'
#' @export
comex_deflate <- function(data, basedate = NULL, deflators = get_deflators(na_omit = TRUE)) {
  # Remove any rows with missing values from the deflators dataset
  deflators_complete <- deflators |> na.omit()

  # Issue a warning if there were missing values in the original deflators dataset
  if (nrow(deflators_complete) != nrow(deflators)) warning("Missing data in deflators.")

  # Helper function to extract the base value and date for a given deflator
  get_base <- function(x, date, basedate = NULL) {
    # If no basedate is provided, use the latest available date with a non-missing value for the deflator
    if (is.null(basedate)) {
      basedate <- max(date[!is.na(x)])
    }
    # Check if there's exactly one matching basedate
    r <- date == basedate
    if (sum(r) != 1) stop("Incomplete deflators.")
    # Return a tibble with the base value and basedate
    dplyr::tibble(x = x[r], basedate = as.Date(basedate))
  }
  # Sort the deflators by date
  deflators_0 <- deflators |> dplyr::arrange(date)
  # Get base CPI and IPCA values
  base_cpi <- get_base(x = deflators_0$cpi, date = deflators_0$date, basedate = basedate)
  base_ipca <- with(deflators_0, get_base(x = ipca_i, date = date, basedate = basedate))
  # Calculate deflation ratios and add basedate columns
  deflators_1 <- deflators_0 |>
    dplyr::mutate(
      cpi_r = base_cpi$x / cpi,         # CPI deflation ratio
      cpi_basedate = base_cpi$basedate, # CPI basedate
      ipca_r = base_ipca$x / ipca_i,     # IPCA deflation ratio
      ipca_basedate = base_ipca$basedate # IPCA basedate
    )
  # Join the deflated data with the original dataset and perform calculations
  data |>
    dplyr::left_join(deflators_1, by = c("date"), copy = TRUE) |>
    collect() |>
    # Deflate USD values by CPI
    dplyr::mutate(dplyr::across(any_of(c("fob_usd", "cif_usd", "freight_usd", "insurance_usd")),
                         function(x) x * cpi_r, .names = "{col}_deflated")) |>
    # Convert USD values to BRL using the exchange rate on the statistic date
    dplyr::mutate(dplyr::across(any_of(c("fob_usd", "cif_usd", "freight_usd", "insurance_usd")),
                         function(x) x * brlusd, .names = "{col}_brl")) |>
    # Clean up column names
    dplyr::rename_with(function(x) gsub("_usd_brl$", "_brl", x)) |>
    # Deflate BRL values by IPCA
    dplyr::mutate(dplyr::across(any_of(c("fob_brl", "cif_brl", "freight_brl", "insurance_brl")),
                         function(x) x * ipca_r, .names = "{col}_deflated")) |>
    # Sort by date
    dplyr::arrange(date)
}

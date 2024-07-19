#' Download and processes deflators (CPI/USA, IPCA/Brazil, Exchange rate BRL/USD)
#'
#' @param updated
#'
#' @return A data frame with columns date, ipca (monthly inflation from Brazil), ipca_i (monthly inflation from Brazil indexed such as 1997-01-01 is 1), cpi index (monthly inflation from USA).
#' @export
#'
get_deflators <- function(updated = Sys.Date(), na_omit = FALSE) {
    # Function to get deflators (CPI, IPCA, and exchange rates) Get IPCA data and join with exchange rate data
    # (brlusd)
    res <- get_ipca() |>
        dplyr::full_join(get_brlusd(), by = "date") |>
        # Join with CPI data
    dplyr::full_join(get_cpi(), by = "date") |>
        # Sort by 'date'
    dplyr::arrange(date)
    # Remove rows with missing values if na_omit is TRUE
    if (na_omit)
        res <- na.omit(res)
    return(res)
}



#' Read and Standardize Comexstat Data
#'
#' @description This function reads a specified Comexstat (Brazilian trade statistics) dataset
#'   from a local CSV file, cleans the column names, and applies standardized column renaming.
#'
#' @param table A character string specifying the name of the Comexstat table to read.
#' @param dir The directory containing the Comexstat data file (defaults to `comexr:::cdircomex`).
#' @param extension The file extension of the Comexstat data file (defaults to '.csv').
#' @param ... Additional arguments passed to `readr::read_csv2`, such as `col_types`, `na`, etc.
#'
#' @return A tibble (data frame) containing the specified Comexstat data with standardized column names.
#'
#' @examples
#' \dontrun{
#' # Read the 'ncm' table from the default directory
#' ncm_data <- comex('ncm')
#'
#' # Read the 'ncm_cgce' table from a specific directory ('my_data')
#' ncm_cgce_data <- comex('ncm_cgce', dir = 'my_data')
#'
#' # Read a table with a different extension (e.g., .txt)
#' other_data <- comex('other_table', extension = '.txt')
#'
#' }
#'
#' @export
comex <- function(table, dir = comexr:::cdircomex, extension = ".csv", ...) {
    ## fix get col_types from ...
    cols_spec <- NULL
    if (table %in% "imp_totais_conferencia") {
    cols_spec <- readr::cols(
      ARQUIVO = readr::col_character(),
      CO_ANO = readr::col_double(),
      QT_ESTAT = readr::col_double(),
      KG_LIQUIDO = readr::col_double(),
      VL_FOB = readr::col_double(),
      VL_FRETE = readr::col_double(),
      VL_SEGURO = readr::col_double(),
      NUMERO_LINHAS = readr::col_double()
    )
    }
    if (table %in% "exp_totais_conferencia") {
    cols_spec <- readr::cols(
      ARQUIVO = readr::col_character(),
      CO_ANO = readr::col_double(),
      QT_ESTAT = readr::col_double(),
      KG_LIQUIDO = readr::col_double(),
      VL_FOB = readr::col_double(),
      NUMERO_LINHAS = readr::col_double()
    )
    }
    if (table %in% c("imp_totais_conferencia_mun", "exp_totais_conferencia_mun")) {
      cols_spec <- readr::cols(
        ARQUIVO = readr::col_character(),
        CO_ANO = readr::col_double(),
        KG_LIQUIDO = readr::col_double(),
        VL_FOB = readr::col_double(),
        NUMERO_LINHAS = readr::col_double()
      )
    }
    readr::read_csv2(file.path(dir, paste0(table, extension)), locale = readr::locale(encoding = "latin1", decimal_mark = ","),
        col_types = cols_spec, ...) |>
        janitor::clean_names() |>
        comex_rename()
}

comex_rename <- function(x) {
    lookup <- c(year = "co_ano", month = "co_mes", country_code = "co_pais", country_name = "no_pais_ing", block_code = "co_bloco", hs4="sh4", "mun_code"="co_mun", "state_abb"="sg_uf_mun",
        block_name = "no_bloco_ing", qt_stat = "qt_estat", kg_net = "kg_liquido", fob_usd = "vl_fob", freight_usd = "vl_frete",
        insurance_usd = "vl_seguro", number_of_lines = "numero_linhas", file = "arquivo")
    x |>
        dplyr::rename_with(tolower) |>
        dplyr::rename(dplyr::any_of(lookup)) |>
        dplyr::mutate(across(dplyr::any_of(c("country_code")), as.integer)) |>
        dplyr::mutate(across(dplyr::any_of(c("ncm")), as.character)) |>
        dplyr::mutate(across(dplyr::any_of(c("qt_stat", "kg_net", "fob_usd", "freight_usd", "insurance_usd")), bit64::as.integer64))
}

#' Check Consistency Between Comex Data and Conference Files
#'
#' This function verifies the consistency between downloaded Comex data and the corresponding conference files provided by the official source (MDIC). It checks if the aggregated totals for specific columns match between the two sources.
#'
#' @param years A numeric vector specifying the years to check. If `NULL` (default), all available years are checked.
#' @param directions A character vector specifying the directions of trade to check: 'imp' (imports) and/or 'exp' (exports). If `NULL` (default), both directions are checked.
#' @param type A character string indicating the type of Comex data: 'ncm' (Nomenclatura Comum do Mercosul) or 'hs4' (Harmonized System 4-digit). Defaults to 'ncm'.
#'
#' @return No direct return value. The function prints a message indicating whether the check passed ('All clear!') or if there are mismatches between the data and conference files. If mismatches are found, a table displaying the discrepancies is printed and an error is raised.
#'
#' @details
#' This function performs the following checks:
#'
#' 1. **Input Validation:** Ensures that the `type` argument is valid ('ncm' or 'hs4') and has a length of 1.
#' 2. **Data Retrieval:** Reads the relevant Comex data (based on the `type`) and the corresponding conference files.
#' 3. **Filtering:** Filters the data and conference files based on the specified `years` and `directions` if provided.
#' 4. **Aggregation:** Aggregates the Comex data by `year` and `direction` and calculates sums for relevant columns.
#' 5. **Comparison:** Compares the aggregated sums from the Comex data with the totals in the conference files.
#' 6. **Mismatch Reporting:** If any mismatches are found, a table highlighting the differences is printed to the console.
#' 7. **Error Handling:** If mismatches exist, the function raises an error to stop further analysis.
#'
#' @examples
#' \dontrun{
#' # Check consistency for all NCM data from 2020 onwards
#' comex_check(years = 2020:2023)
#'
#' # Check consistency for only import data (NCM)
#' comex_check(directions = 'imp')
#'
#' # Check consistency for HS4 data in 2022
#' comex_check(years = 2022, type = 'hs4')
#' }
#'
#' @export
comex_check <- function(years = NULL, directions = NULL, type = "ncm") {
    # Input validation
    stopifnot(type %in% c("ncm", "hs4"), length(type) == 1)  # Ensure valid type and single value

    # Determine file names based on type
    files <- c("imp_totais_conferencia", "exp_totais_conferencia")
    if (type == "hs4")
        files <- paste0(files, "_mun")

    # Read and combine conference data
    conf_data <- dplyr::bind_rows(lapply(files, comex)) |>
        dplyr::mutate(direction = dplyr::if_else(grepl("IMP", file), "imp", "exp"))

    # Get cached data and filter based on years and directions
    cached_data <- get(paste0("comex_", type))()
    if (is.null(years)) {
     years <-  cached_data|>dplyr::distinct(year)|>dplyr::collect()|>dplyr::pull(year)
    }
    cached_data <- cached_data |>
      dplyr::filter(year %in% years)
    conf_data <- conf_data |>
      dplyr::filter(year %in% years)
    if (!is.null(directions)) {
        cached_data <- cached_data |>
            dplyr::filter(direction %in% directions)
        conf_data <- conf_data |>
            dplyr::filter(direction %in% directions)
    }

    # Calculate summaries for comparison
    cached_summary <- cached_data |>
        dplyr::group_by(year, direction) |>
        dplyr::mutate(number_of_lines = 1) |>
        comex_sum(x = c("qt_stat", "kg_net", "fob_usd", "freight_usd", "insurance_usd", "number_of_lines")) |>
        dplyr::collect()

    # Find differences between conference and cached data
    join_cols <- intersect(names(conf_data), names(cached_summary))
    checked <- conf_data |>
        dplyr::anti_join(cached_summary, by = join_cols)

    if (nrow(checked) > 0) {
        toprint <- checked |>
            dplyr::left_join(cached_summary, by = c("year", "direction"), suffix = c("_check", "_cached"))

        print(t(toprint |>
            dplyr::select(sort(names(toprint)))))
        stop("Conference file mismatch with downloaded data!")
    }

    # Success message if no mismatches
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
#'   - `direction`: Trade direction, either 'exp' (export) or 'imp' (import)
#'
#' @details This function reads HS4 parquet files in the user data directory.
#'
#' @examples
#' \dontrun{
#' # Open the ComexStat HS4 dataset:
#' hs4_dataset <- comexstat_hs4()
#'
#' }
#'
#' @export
comex_hs4 <- function() {
  arrow::open_dataset(sources = file.path(comexr:::ddircomex, "hs4"))
}



comex_hs4_raw <- function() {
    # Define schema for HS4 data FIX: how to set schema including hive data schema_hs4 <- arrow::schema(
    # #direction = arrow::string(), year = arrow::int32(), month = arrow::int32(), hs4 = arrow::string(),
    # country_code = arrow::int32(), state_abb = arrow::string(), mun_code = arrow::int32(), kg_net =
    # arrow::int64(), fob_usd = arrow::int64() ) Open import and export HS4 datasets
    hs4 <- arrow::open_delim_dataset(sources = file.path(comexr:::cdircomex, "hs4"), delim = ";", skip = 0)  |>
        comex_rename()
    hs4
}

#' Open ComexStat NCM Trade Dataset
#'
#' This function opens the ComexStat NCM (8-digit product code) trade data as an Arrow Dataset. The data is assumed to be located in the  ComexStat data directory and to have been downloaded and processed using the `comex_download()` function.
#'
#' @return An Arrow Dataset containing combined import and export NCM trade data, with an additional `direction` column indicating 'exp' (export) or 'imp' (import). The dataset has the following columns:
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
#'   - `direction`: Trade direction, either 'exp' (export) or 'imp' (import) (derived)
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
comex_ncm <- function() {
  arrow::open_dataset(sources = file.path(comexr:::ddircomex, "ncm"))
}

comex_ncm_raw <- function(check = FALSE) {
    # Define schemas for export and import NCM data
    schema_exp_ncm <- arrow::schema(year = arrow::int32(), month = arrow::int32(), ncm = arrow::string(), unit_code = arrow::int32(),
        country_code = arrow::int32(), state_abb = arrow::string(), transp_mode_code = arrow::int32(), urf_code = arrow::int32(),
        qt_stat = arrow::int64(), kg_net = arrow::int64(), fob_usd = arrow::int64())

    schema_imp_ncm <- schema_exp_ncm$AddField(schema_exp_ncm |>
        length(), field = arrow::field("freight_usd", arrow::int64()))

    schema_imp_ncm <- schema_imp_ncm$AddField(schema_exp_ncm |>
        length() + 1, field = arrow::field("insurance_usd", arrow::int64()))

    # Open import and export NCM datasets
    imp_sources <- dir(file.path(comexr:::cdircomex, "ncm", "direction=imp"), pattern = "imp_[0-9]+.csv", full.names = TRUE)
    imp_ncm <- arrow::open_delim_dataset(sources = imp_sources, delim = ";", schema = schema_imp_ncm, skip = 1)
    exp_sources <- dir(file.path(comexr:::cdircomex, "ncm", "direction=exp"), pattern = "exp_[0-9]+.csv", full.names = TRUE)
    exp_ncm <- arrow::open_delim_dataset(sources = exp_sources, delim = ";", schema = schema_exp_ncm, skip = 1)
    # Combine imports and exports, adding direction column
    df <- arrow::open_dataset(list(imp_ncm, exp_ncm)) |>
        dplyr::mutate(direction = dplyr::if_else(is.na(freight_usd), "exp", "imp"), date = lubridate::make_date(year, month),
            cif_usd = fob_usd + freight_usd + insurance_usd)
    df
}





#' Deflate and Convert Comexstat Data
#'
#' This function deflates USD-denominated values
#' using specified deflators (CPI for USD, IPCA for BRL after converting using the exchange rate of the date of the statistics)
#'
#' @param data A data frame or tibble containing data to be deflated.
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
#' 5. Deflates the USD-denominated columns (those ending with _usd, such as `fob_usd`, `cif_usd`, etc.) using `cpi_r`.
#' 6. Converts USD values to BRL based on the exchange rate in the `brlusd` column.
#' 7. Deflates columns with BRL values (those ending with _brl)  using `ipca_r`.
#' 8. Arranges the resulting data by `date`.
#'
#' @examples
#' \dontrun{
#' }
#'
#' @export
comex_deflate <- function(data, basedate = NULL, deflators = get_deflators(na_omit = TRUE)) {
    # Remove any rows with missing values from the deflators dataset
    deflators_complete <- deflators |>
        na.omit()

    # Issue a warning if there were missing values in the original deflators dataset
    if (nrow(deflators_complete) != nrow(deflators))
        warning("Missing data in deflators.")

    # Helper function to extract the base value and date for a given deflator
    get_base <- function(x, date, basedate = NULL) {
        # If no basedate is provided, use the latest available date with a non-missing value for the deflator
        if (is.null(basedate)) {
            basedate <- max(date[!is.na(x)])
        }
        # Check if there's exactly one matching basedate
        r <- date == basedate
        if (sum(r) != 1)
            stop("Incomplete deflators.")
        # Return a tibble with the base value and basedate
        dplyr::tibble(x = x[r], basedate = as.Date(basedate))
    }
    # Sort the deflators by date
    deflators_0 <- deflators |>
        dplyr::arrange(date)
    # Get base CPI and IPCA values
    base_cpi <- get_base(x = deflators_0$cpi, date = deflators_0$date, basedate = basedate)
    base_ipca <- with(deflators_0, get_base(x = ipca_i, date = date, basedate = basedate))
    # Calculate deflation ratios and add basedate columns
    deflators_1 <- deflators_0 |>
        dplyr::mutate(cpi_r = base_cpi$x/cpi, cpi_basedate = base_cpi$basedate, ipca_r = base_ipca$x/ipca_i, ipca_basedate = base_ipca$basedate  # IPCA basedate
)
    # Join the deflated data with the original dataset and perform calculations
    data |>
        dplyr::left_join(deflators_1, by = c("date"), copy = TRUE) |>
        dplyr::collect() |>
        # Deflate USD values by CPI
    dplyr::mutate(dplyr::across(dplyr::ends_with(c("_usd")), function(x) x *
        cpi_r, .names = "{col}_deflated")) |>
        # Convert USD values to BRL using the exchange rate on the statistic date
    dplyr::mutate(dplyr::across(ends_with(c("_usd")), function(x) x *
        brlusd, .names = "{col}_brl")) |>
        # Clean up column names
    dplyr::rename_with(function(x) gsub("_usd_brl$", "_brl", x)) |>
        # Deflate BRL values by IPCA
    dplyr::mutate(dplyr::across(ends_with(c("_brl")), function(x) x *
        ipca_r, .names = "{col}_deflated")) |>
        # Sort by date
    dplyr::arrange(date)
}

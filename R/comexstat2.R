
comexstat2 <- function(table, ...) {
  read_comex <- function(name, dir=ddircomex, extension=".csv") {
    file.path(dir, paste0(name, extension)) |>
      read1_comex() |>
      suppressMessages()
  }
  read1_comex <- function(fname) {
    fname |>
      readr::read_csv2(locale = readr::locale(encoding="latin1")) |>
      suppressMessages() |>
      janitor::clean_names()
  }
  read_comex(table, ...)|>
    comexstat_rename()
}

comexstat_rename <- function(x) {
  lookup <- c(year = "co_ano",
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
    dplyr::mutate(across(dplyr::any_of(c("qt_stat", "kg_net", "fob_usd", "freight_usd", "insurance_usd")), bit64::as.integer64))
}

comexstat2_check <- function() {
  cached <- comexstat_ncm()|>
    dplyr::group_by(year, direction)|>
    dplyr::mutate(number_of_lines=1)|>
    dplyr::summarise(across(c(qt_stat, kg_net, fob_usd, freight_usd, insurance_usd,number_of_lines), sum))%>%
    dplyr::collect()
  cached_hs4 <- comexstat_hs4()|>
    dplyr::group_by(year, direction)|>
    dplyr::mutate(number_of_lines=1)|>
    dplyr::summarise(across(c(fob_usd, number_of_lines), sum))%>%
    dplyr::collect()
  conf <- comexstat2("imp_totais_conferencia")|>
    dplyr::bind_rows(comexstat2("exp_totais_conferencia"))|>
    dplyr::mutate(direction=dplyr::if_else(grepl("IMP", file), "imp", "exp"))
  checked <- conf|>dplyr::anti_join(cached, by = join_by(year, qt_stat, kg_net, fob_usd, freight_usd, insurance_usd, number_of_lines, direction))
  if (nrow(checked)>0) stop("Conference file mismatch NCM with downloaded data!")
  checked_hs4 <- conf|>dplyr::anti_join(cached_hs4, by = join_by(year, fob_usd, direction))
  if (nrow(checked_hs4)>0) stop("Conference file mismatch HS4 with downloaded data!")
  print("All clear!")
}

#' Open ComexStat HS4 Trade Dataset
#'
#' This function opens the ComexStat HS4 trade data as an Arrow Dataset. The data is assumed to be located in the ComexStat data directory and to have been downloaded using the `comexstat_download2()` function.
#'
#' @return An Arrow Dataset containing combined import and export HS4 trade data. The dataset has the following columns:
#'   - `year`: Year (integer)
#'   - `month`: Month (integer)
#'   - `hs4`: 4-digit HS product code (integer)
#'   - `country_code`: Country code (integer)
#'   - `state_abb`: Brazilian state abbreviation (string)
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
comexstat_hs4 <- function() {
  # Define schema for HS4 data
  # FIX: how to set schema including hive data
  # schema_hs4 <- arrow::schema(
  #   #direction = arrow::string(),
  #   year = arrow::int32(),
  #   month = arrow::int32(),
  #   hs4 = arrow::int32(),
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
    skip = 0)|>comexstat_rename()
  hs4
}

#' Open ComexStat NCM Trade Dataset
#'
#' This function opens the ComexStat NCM (8-digit product code) trade data as an Arrow Dataset. The data is assumed to be located in the  ComexStat data directory and to have been downloaded using the `comexstat_download2()` function.
#'
#' @return An Arrow Dataset containing combined import and export NCM trade data, with an additional `direction` column indicating "exp" (export) or "imp" (import). The dataset has the following columns:
#'   - `year`: Year (integer)
#'   - `month`: Month (integer)
#'   - `ncm`: 8-digit NCM product code (integer)
#'   - `unit_code`: Unit of measurement code (integer)
#'   - `country_code`: Country code (integer)
#'   - `state_abb_ncm`: Brazilian state abbreviation (string)
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
#' ncm_dataset <- comexstat_ncm()
#'
#' # Explore the dataset:
#' print(ncm_dataset)
#' ncm_dataset|>group_by(year, direction)|>summarise(fob_usd=sum(fob_usd), freight_usd=sum(freight_usd))|>mutate(p=freight_usd/fob_usd)%>%collect()
#' comexstat_ncm()%>%group_by(country_code)%>%summarise(fob_usd=sum(fob_usd, na.rm=TRUE), freight_usd=sum(freight_usd))%>%mutate(p=freight_usd/fob_usd)%>%arrange(desc(p))%>%collect()
#' }
#'
#' @export
comexstat_ncm <- function() {
  # Define schemas for export and import NCM data
  schema_exp_ncm <- arrow::schema(
    year = arrow::int32(),
    month = arrow::int32(),
    ncm = arrow::int32(),
    unit_code = arrow::int32(),
    country_code = arrow::int32(),
    state_abb_ncm = arrow::string(),
    transp_mode_code = arrow::int32(),
    urf_code = arrow::int32(),
    qt_stat = arrow::int64(),
    kg_net = arrow::int64(),
    fob_usd = arrow::int64()
  )

  schema_imp_ncm <- schema_exp_ncm$AddField(
    schema_exp_ncm %>% length(),
    field = arrow::field("freight_usd", arrow::int64())
  )

  schema_imp_ncm <- schema_imp_ncm$AddField(
    schema_exp_ncm %>% length() + 1,
    field = arrow::field("insurance_usd", arrow::int64())
  )

  # Open import and export NCM datasets
  imp_ncm <- arrow::open_delim_dataset(
    sources = dir(file.path(comexstatr:::cdircomex, "ncm", "direction=imp"),
                  pattern = "imp_[0-9]+.csv", full.names = TRUE),
    delim = ";",
    schema = schema_imp_ncm,
    skip = 1
  )

  exp_ncm <- arrow::open_delim_dataset(
    sources = dir(file.path(comexstatr:::cdircomex, "ncm", "direction=exp"),
                  pattern = "exp_[0-9]+.csv", full.names = TRUE),
    delim = ";",
    schema = schema_exp_ncm,
    skip = 1
  )
  # Combine imports and exports, adding direction column
  df <- arrow::open_dataset(list(imp_ncm, exp_ncm)) %>%
    dplyr::mutate(direction = if_else(is.na(freight_usd), "exp", "imp"))
  df
}


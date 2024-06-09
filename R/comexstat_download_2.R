' Download ComexStat Trade Data
#'
#' This function downloads Brazilian international trade data (ComexStat) from the Ministry of Economy's website. It supports various data types, years, and trade directions.
#'
#' @param years Numeric vector of years for which to download data (default: 1997:1998).
#' @param directions Character vector indicating trade directions: "imp" for imports, "exp" for exports (default: c("imp", "exp")).
#' @param types Character vector specifying data types: "ncm" for 8-digit product codes, "hs4" for 4-digit product codes (default: c("ncm", "hs4")).
#' @param download_aux Logical indicating whether to download auxiliary data files (default: TRUE).
#' @param cache Logical indicating whether to resume incomplete downloads from previous attempts (default: TRUE).
#' @param .progress Logical indicating whether to display a progress bar during downloads (default: TRUE).
#' @param n_tries Integer specifying the maximum number of download attempts
#'
#' @return Downloads data files to the ComexStat data directory. Returns invisibly a logical vector indicating the success of each download.
#'
#' @details The downloaded data files will be saved in CSV format. The file names will be structured as follows:
#'    - For HS4 data: "<direction>_<year>_mun.csv" (e.g., "imp_1997_mun.csv")
#'    - For NCM data: "<direction>_<year>.csv" (e.g., "exp_1998.csv")
#'
#' @examples
#' \dontrun{
#' # Download import and export data for 1997-1998 in both NCM and HS4 formats:
#' comexstat_download()
#'
#' # Download only import data for 2020-2022 in NCM format:
#' comexstat_download(years = 2020:2022, directions = "imp", types = "ncm")
#' }
#'
#' @export
comexstat_download <- function(years=2023:2024,
                                directions=c("imp", "exp"),
                                types=c("ncm", "hs4"),
                                download_aux=TRUE,
                                cache=TRUE,
                                .progress=TRUE, n_tries=30) {
  sapply(file.path(comexstatr:::cdircomex, directions), dir.create(showWarnings = TRUE, recursive=TRUE))
  stopifnot(all(types%in%c("hs4", "ncm")))
  todownload <- tidyr::crossing(tibble::tibble(year=years), tibble::tibble(direction=directions), tibble::tibble(type=types))|>
    dplyr::mutate(
      dir=file.path(comexstatr:::cdircomex,
                    type,
                    paste0("direction=", direction)),
      path=file.path(dir,
                     dplyr::if_else(type=="hs4",
                                                  paste0(tolower(direction),"_", year,"_mun.csv"),
                                    paste0(tolower(direction),"_", year,".csv"))),
      url=dplyr::if_else(type=="hs4",
                         glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/{toupper(direction)}_{year}_MUN.csv"),
                         glue::glue("https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/{toupper(direction)}_{year}.csv")))
  purrr::walk(todownload$dir%>%unique, dir.create, showWarnings=FALSE)
  if (download_aux) {
    aux_data <- tibble::tibble(url=c(
      "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv",
      "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv",
      "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv",
      # "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_COMPLETA.zip",
      # "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_COMPLETA.zip",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CUCI.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CGCE.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv",
      "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv"))%>%
      dplyr::mutate(path=file.path(comexstatr:::cdircomex,basename(url) |>
                                     tolower()))
    todownload <- todownload|>dplyr::bind_rows(aux_data)
  }
  todownload|>
    dplyr::mutate(ok=FALSE) -> todownload

  j <- 1
  while ((j<=n_tries)&any(!todownload$ok)) {
    if (j>1) print(glue::glue("Try #{j}!"))
    res <- curl::multi_download(todownload$url,
                                destfiles = todownload$path, progress = .progress,
                                resume = cache, timeout = 100, multiplex = TRUE)
    todownload$ok <- (res$success%in%TRUE)
    j <- j+1
  }
  ## delete urls not found
  todelete <- res|>dplyr::filter(!grepl("balanca.economia.gov.br", url))%>%pull(destfile)
  unlink(todelete)
  if (length(todelete)>0) warning(glue::glue("{length(todelete)} urls not found."))
  if (any(!todownload$ok)) stop("Error downloading data.")
}

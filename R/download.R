#' Internal function that downloads comextat data
#'
#' @noRd
#' @param filenames file to download, all for everything
#' @param outdir where to download the files to
#' @param ... arguments sent to the base download.file function
#'
#' @return paths to the files downloaded
#'
download_comex <- function(filenames, outdir=ddircomex, replace=TRUE, ...) {
  urls <- c(
    "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_COMPLETA.zip",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_COMPLETA.zip",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CUCI.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CGCE.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv")
  names(urls) <- basename(urls) |>
    tolower()
  if (filenames[1]=="all") {
    filenames <- names(urls)
  }
  dfile <- function(..., destfile, replace=TRUE) {
    if(file.exists(destfile) & !replace) return(TRUE)  else download.file(..., destfile=destfile)
  }
  res <- lapply(filenames, function(.x) dfile(url=urls[tolower(.x)], destfile = file.path(outdir, .x), replace=replace, ...))
  file.path(outdir, filenames)
}

#' Download raw data from Comexstat
#'
#' @param force_download Force downloading data, even if it is (apparently) current.
#' @param ... arguments sent to the download.file function
#' @return NULL
#'
#' @export
#'
#' @details
#'
#' Depending on local cofiguration and network properties, you might need to set the download methods and other parameters, which are sent to the download.file function.
#'
#'
#' @examples
#' \dontrun{
#' comexstat_download()
#' ## might need something like this if you get ssl errors.
##' comexstat_download(method="wget", extra="--no-check-certificate")
##' ## or this, if the default method does not work
##' comexstat_download(method="libcurl")
##' ## or this, if it times out
##' options(timeout=100)
#' }
comexstat_download <- function(..., force_download=FALSE) {
  message("Downloading data from Comexstat...")
  memoise::forget(ncms)
  memoise::forget(comexstat)
  memoise::forget(ym)
  dir.create(cdircomex, showWarnings = FALSE, recursive = TRUE)
  dir.create(ddircomex, showWarnings = FALSE, recursive = TRUE)
  check_imp_down <- download_comex(filenames = "imp_totais_conferencia.csv", outdir = cdircomex, ...)
  check_imp <- check_imp_down |>
    read1_comex()
  local_imp <- tryCatch(comexstat("imp_totais_conferencia"), error = function(e) dplyr::tibble())
  if (force_download | (!setequal(local_imp, check_imp))) {
    tryCatch({
    message("downloading files ... can take a while ...")
    ds <- download_comex("all", ...)
    message("Unzipping files...")
    zip::unzip(
      grep("exp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    zip::unzip(
      grep("imp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    comexstat_rewrite()
    comexstat_check()
    }, error=function(e) {
      print("error downloading file ... ")
      unlink(file.path(ddircomex,"imp_totais_conferencia.csv"))
      })
  }
}


#' Checks the downloaded raw totals to the totals file
#'
#' @return
#'
comexstat_check <- function() {
  ccalc <- comexstat() |>
    dplyr::group_by(co_ano, fluxo) |>
    dplyr::summarise(
      vl_seguro=sum(vl_seguro, na.rm=TRUE),
      kg_liquido=sum(kg_liquido, na.rm=TRUE),
      vl_fob=sum(vl_fob, na.rm=TRUE),
      vl_frete=sum(vl_frete, na.rm=TRUE),
      vl_seguro=sum(vl_seguro, na.rm=TRUE),
      numero_linhas=n()
    ) |>
    dplyr::collect()
  ccheck <- dplyr::bind_rows(
    comexstat("imp_totais_conferencia") |>
      dplyr::mutate(fluxo="imp"),
    comexstat("exp_totais_conferencia") |>
      dplyr::mutate(fluxo="exp"))
  res <- ccheck |>
    dplyr::left_join(ccalc, by=c('co_ano', "fluxo")) |>
    dplyr::group_by(fluxo) |>
    dplyr::summarise(vl_fob=sum(abs(vl_fob.x-vl_fob.y)),
              vl_seguro=sum(abs(vl_seguro.x-vl_seguro.y)),
              kg_liquido=sum(abs(kg_liquido.x-kg_liquido.y)),
              vl_frete=sum(abs(vl_frete.x-vl_frete.y)),
              vl_seguro=sum(abs(vl_seguro.x-vl_seguro.y)),
              numero_linhas=sum(abs(numero_linhas.x-numero_linhas.y))
              )
  expected <- structure(list(fluxo = c("exp", "imp"), vl_fob = c(0, 0), vl_seguro = c(NA,0), kg_liquido = c(0, 0), vl_frete = c(NA, 0), numero_linhas = c(0, 0)), class = c("tbl_df", "tbl", "data.frame"), row.names = c(NA, -2L))
  if (!dplyr::setequal(expected,res)) stop("Results not matching check (conferencia) file.")
}

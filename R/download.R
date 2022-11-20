download_comex <- function(filenames, outdir=ddircomex, method="auto", extra=NULL) {
  urls <- c(
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
  if (filenames=="all") {
    filenames <- names(urls)
  }
  res <- purrr::map(filenames, ~download.file(url=urls[.x], destfile = file.path(outdir, .x), method=method, extra=extra))
  file.path(outdir, filenames)
}

#' Download raw data from Comexstat
#'
#' @param force_download Force downloading data, even if it is (apparently) current.
#'
#' @return NULL
#' @export
#'
#' @examples
#' \dontrun{ comexstat_download() }
comexstat_download <- function(force_download=FALSE,method="auto", extra=NULL) {
  msg("Downloading data from Comexstat...")
  check_imp_down <- download_comex("imp_totais_conferencia.csv", outdir = cdircomex, method=method, extra=extra)
  check_imp <- check_imp_down |>
    read1_comex()
  local_imp <- tryCatch(read_comex("imp_totais_conferencia"), error = function(e) dplyr::tibble())
  if (force_download | (!setequal(local_imp, check_imp))) {
    msg("downloading files ... can take a while ...")
    ds <- download_comex("all", method=method, extra=extra)
    msg("Unzipping files...")
    zip::unzip(
      grep("exp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    zip::unzip(
      grep("imp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    comexstat_rewrite()
    comexstat_check()
  }
  msg("Downloading done!")
  ddir
}


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
    read_comex("imp_totais_conferencia") |>
      dplyr::mutate(fluxo="imp"),
    read_comex("exp_totais_conferencia") |>
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

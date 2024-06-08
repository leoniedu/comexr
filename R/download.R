#' Internal function that downloads comextat data
#'
#' @noRd
#' @param filenames file to download, all for everything
#' @param outdir where to download the files to
#' @param resume resume partial downloads?
#' @param ... arguments sent to the base download.file function
#'
#' @return paths to the files downloaded
#'
download_comex <- function(filenames, outdir=ddircomex, replace=TRUE, resume=TRUE, timeout=600, ...) {
  if (resume) n_try <- 10 else n_try <- 1
  urls <- c(
    "https://balanca.economia.gov.br/balanca/bd/tabelas/URF.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/VIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_COMPLETA.zip",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_COMPLETA.zip",
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
  res <- data.frame(success=FALSE)
  j <- 1
  while ((j<=n_try)&any(!res$success)) {
    res <- curl::multi_download(urls[tolower(filenames)], destfiles = file.path(outdir, tolower(filenames)), progress = TRUE,
                                resume = resume, timeout = 10000, ...)
    j <- j+1
  }
  #res <- lapply(filenames, function(.x) dfile(url=urls[tolower(.x)], destfile = file.path(outdir, .x), replace=replace, ...))
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
#'#'
#'
#' @examples
#' \dontrun{
#' comexstat_download()
comexstat_download <- function(..., force_download=FALSE, timeout=600, replace=TRUE, resume=FALSE) {
  message("Downloading data from Comexstat...")
  memoise::forget(ncms)
  memoise::forget(ym)
  dir.create(cdircomex, showWarnings = FALSE, recursive = TRUE)
  dir.create(ddircomex, showWarnings = FALSE, recursive = TRUE)
  check_imp_down <- download_comex(filenames = "imp_totais_conferencia.csv", outdir = cdircomex, replace=TRUE, ...)
  check_exp_down <- download_comex(filenames = "exp_totais_conferencia.csv", outdir = cdircomex, replace=TRUE, ...)
  check_imp <- comexstat("imp_totais_conferencia", dir=cdircomex)
  check_exp <- comexstat("exp_totais_conferencia", dir=cdircomex)
  # check_imp <- check_imp_down |>
  #   read1_comex()
  local_imp <- purrr::possibly(comexstat, otherwise=dplyr::tibble())("imp_totais_conferencia")
  local_exp <- purrr::possibly(comexstat, otherwise=dplyr::tibble())("exp_totais_conferencia")
  ok <- setequal(dplyr::bind_rows(local_imp, local_exp), dplyr::bind_rows(check_imp, check_exp))
  if (any(force_download,!ok,(!purrr::possibly(comexstat_check, FALSE)()))) {
    tryCatch({
    message("downloading files ... can take a while ...")
    ds <- download_comex("all", ..., replace=replace, resume=resume)
    message("Unzipping files...")
    browser()
    zip::unzip(
      grep("exp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    zip::unzip(
      grep("imp_completa", ds, value = TRUE)
      , exdir = cdircomex)
    comexstat_rewrite()
    comexstat_check()
    }, error=function(e) {
      #unlink(file.path(ddircomex,"imp_totais_conferencia.csv"))
      stop("error downloading file ... ")
      })
  }
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
  ok <- dplyr::setequal(expected,res)
  if (!ok) {
    #browser()
    warning("Results not matching check (conferencia) file.")
  }
  ok
}


get_cpi <- function() {
  path<-"https://fred.stlouisfed.org/graph/fredgraph.csv?id=CPIAUCSL"
  cpi_0 <- read.csv(path, header = FALSE, col.names = c("date", "cpi"), skip = 1)
  transform(cpi_0, date=as.Date(date))|>
    dplyr::filter(date>=as.Date("1997-01-01"))
}

get_ipca <- function() {
  ipca0 <- rbcb::get_series(433, start_date="1997-01-01")
  names(ipca0)[2] <- "ipca"
  ipca <- transform(ipca0[order(ipca0$date),],ipca_i=cumprod(1+ipca/100))
  ipca
}



get_brlusd <- function(from="1997-01-01", to=NULL) {
  if (is.null(to)) {
    lastmonth <- {
    m <- Sys.Date()
    lubridate::day(m) <- 1
    m-1
    }
  } else lastmonth <- to
  brlusd0 <- rbcb::get_currency("USD", from, lastmonth)
  brlusd <- brlusd0|>
    dplyr::group_by(date=lubridate::make_date(lubridate::year(date),lubridate::month(date)))|>
    dplyr::summarise(brlusd=(mean(bid)+mean(ask))/2)
  brlusd
}


#' Download and processes deflators (CPI/USA, IPCA/Brazil, Exchange rate BRL/USE)
#'
#' @param updated
#'
#' @return A data frame with columns co_ano_mes (date), ipca (monthly inflation from Brazil), ipca_i (monthly inflation from Brazil indexed such as 1997-01-01 is 1), cpi index (monthly inflation from USA).
#' @export
#'
get_deflators <- function(updated=Sys.Date(), na_omit=FALSE) {
  res <- get_ipca()|>
    dplyr::full_join(get_brlusd(), by= "date")|>
    dplyr::full_join(get_cpi(), by= "date")|>
    dplyr::arrange(date)
  if (na_omit) res <- na.omit(res)
  res
}




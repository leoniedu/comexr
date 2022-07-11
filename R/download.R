#' Download raw data from Comexstat
#'
#' Este comando descarga la base de datos completa como un unico archivo zip que
#' se descomprime para crear la base de datos local. Si no quieres descargar la
#' base de datos en tu home, ejecuta usethis::edit_r_environ() para crear la
#' variable de entorno comexstat2017_DIR con la ruta.
#'
#' @param force_download Force downloading data, even if it is (apparently) current.
#'
#' @return NULL
#' @export
#'
#' @examples
#' \dontrun{ comexstat_download_raw() }
comexstat_download_raw <- function(force_download=FALSE) {
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  msg("Downloading data from Comexstat...")
  require(pins)
  require(dplyr)
  comexstat_check_urls <- c(
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_TOTAIS_CONFERENCIA.csv",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_TOTAIS_CONFERENCIA.csv"
  )
  names(comexstat_check_urls) <- basename(comexstat_check_urls) %>%
    tolower() %>%
    tools::file_path_sans_ext()
  comexstat_urls <- c(
    comexstat_check_urls,
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/IMP_COMPLETA.zip",
    "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/ncm/EXP_COMPLETA.zip",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS_BLOCO.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CUCI.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_ISIC.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_CGCE.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/PAIS.csv",
    "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM_UNIDADE.csv"
  )
  names(comexstat_urls) <- basename(comexstat_urls) %>%
    tolower() %>%
    tools::file_path_sans_ext()
  comexstat_board_url <- board_url(comexstat_urls)
  comexstat_board <- board_local(versioned = FALSE)
  check_imp <- pin_download(board = comexstat_board_url, name = "imp_totais_conferencia") %>% read.csv2()
  local_imp <- tryCatch(pin_download(board = comexstat_board, name = "imp_totais_conferencia") %>% read.csv2(), error = function(e) tibble())
  if (force_download | (!setequal(local_imp, check_imp))) {
    msg("downloading files ... can take a while ...")
    ds <- purrr::map(comexstat_board_url$urls %>% names(), ~ pin_download(board = comexstat_board_url, name = .x))
    names(ds) <- names(comexstat_board_url$urls)
    purrr::walk(names(ds), ~ pin_upload(comexstat_board, name = .x, paths = ds[[.x]]))
    msg("Unzipping files...")
    unzip(comexstat_board %>% pin_download("exp_completa"), exdir = cdir)
    unzip(comexstat_board %>% pin_download("imp_completa"), exdir = cdir)
  }
}




#' Descarga los archivos tsv/shp desde GitHub
#' @noRd
get_gh_release_file <- function(repo, tag_name = NULL, dir = tempdir(),
                                overwrite = TRUE) {
  releases <- httr::GET(
    paste0("https://api.github.com/repos/", repo, "/releases")
  )
  httr::stop_for_status(releases, "buscando versiones")

  releases <- httr::content(releases)

  if (is.null(tag_name)) {
    release_obj <- releases[1]
  } else {
    release_obj <- purrr::keep(releases, function(x) x$tag_name == tag_name)
  }

  if (!length(release_obj)) stop("No se encuenta una version disponible \"",
                                 tag_name, "\"")

  if (release_obj[[1]]$prerelease) {
    msg("Estos datos aun no se han validado.")
  }

  download_url <- release_obj[[1]]$assets[[1]]$url
  filename <- basename(release_obj[[1]]$assets[[1]]$browser_download_url)
  out_path <- normalizePath(file.path(dir, filename), mustWork = FALSE)
  response <- httr::GET(
    download_url,
    httr::accept("application/octet-stream"),
    httr::write_disk(path = out_path, overwrite = overwrite),
    httr::progress()
  )
  httr::stop_for_status(response, "downloading data")

  attr(out_path, "ver") <- release_obj[[1]]$tag_name
  return(out_path)
}

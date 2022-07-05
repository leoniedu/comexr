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
    ds <- purrr::map(comexstat_board_url$urls %>% names(), ~ pin_download(board = comexstat_board_url, name = .x))
    names(ds) <- names(comexstat_board_url$urls)
    purrr::walk(names(ds), ~ pin_upload(comexstat_board, name = .x, paths = ds[[.x]]))
    msg("Unzipping files...")
    unzip(comexstat_board %>% pin_download("exp_completa"), exdir = cdir)
    unzip(comexstat_board %>% pin_download("imp_completa"), exdir = cdir)
  }
}


#' @export
comexstat_stage <- function(year_min_ncm=2019, year_min_ncm_country=2019) {
  ymin <- year_min_ncm
  yminp <- year_min_ncm_country
  require(arrow)
  require(dplyr)
  require(rappdirs)
  require(tictoc)
  require(pins)
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  comexstat_board <- board_local(versioned = FALSE)
  tic()
  ## In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange().
  comexstat_schema_e <- schema(
    field("CO_ANO", int16()),
    field("CO_MES", int8()),
    field("CO_NCM", string()),
    field("CO_UNID", string()),
    field("CO_PAIS", string()),
    field("SG_UF_NCM", string()),
    field("CO_VIA", string()),
    field("CO_URF", string()),
    field("QT_ESTAT", double()),
    field("KG_LIQUIDO", double()),
    field("VL_FOB", double())
  )
  comexstat_schema_i <- comexstat_schema_e
  comexstat_schema_i <- comexstat_schema_i$AddField(11, field = field("VL_FRETE", double()))
  comexstat_schema_i <- comexstat_schema_i$AddField(12, field = field("VL_SEGURO", double()))
  ## export
  fname <- file.path(cdir, "EXP_COMPLETA.csv")
  cnames <- read.csv2(fname, nrows = 3) %>%
    janitor::clean_names() %>%
    names()
  df_e <- open_dataset(
    fname,
    delim = ";",
    format = "text",
    schema = comexstat_schema_e,
    read_options = CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
  )
  fname <- file.path(cdir, "IMP_COMPLETA.csv")
  cnames <- read.csv2(fname, nrows = 3) %>%
    janitor::clean_names() %>%
    names()
  df_i <- open_dataset(
    fname,
    delim = ";",
    format = "text",
    schema = comexstat_schema_i,
    read_options = CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
  )
  ## bind together imports and exports
  df <- open_dataset(list(df_i, df_e)) %>%
    rename_with(tolower) %>%
    mutate(fluxo = if_else(is.na(vl_frete), "exp", "imp"))
  ## write partitioned data
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df %>%
    filter(co_ano>=ymin)%>%
    group_by(co_ano, fluxo) %>%
    write_dataset(ddir_partition, format = "parquet")
  toc()
  tic()
  ## partition by pais
  ddir_partition <- file.path(comexstat_path(), "comexstat_pais")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df %>%
    filter(co_ano >= yminp) %>%
    group_by(co_pais, fluxo) %>%
    write_dataset(ddir_partition, format = "parquet")
  toc()
}

comexstat_arrow <- function(con_comex) {
  ## Load full arrow dataset created by "stage"
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  df_f <- open_dataset(
    ddir_partition,
    format = "parquet"
  )
  duckdb_register_arrow(con_comex, "comexstat_arrow", df_f)
  ## Load  arrow dataset created by "stage" partition by pais
  ddir_partition <- file.path(comexstat_path(), "comexstat_pais")
  df_f <- open_dataset(
    ddir_partition,
    format = "parquet"
  )
  duckdb_register_arrow(con_comex, "comexstat_pais_arrow", df_f)
}



#' @export
comexstat_create_db <- function(overwrite=FALSE) {
  require(pins)
  require(DBI)
  require(arrow)
  require(duckdb)
  require(dplyr)
  comexstat_board <- board_local(versioned = FALSE)
  con_comex <- comexstat_connect(overwrite = overwrite)
  comexstat_arrow(con_comex)
  ##
  msg("Checking calculated totals with the supplied totals dataset  ... ")
  bind_rows(
    comexstat_board %>%
      pin_download("exp_totais_conferencia") %>%
      read.csv2() %>%
      mutate(fluxo = "exp"),
    comexstat_board %>%
      pin_download("imp_totais_conferencia") %>%
      read.csv2() %>%
      mutate(fluxo = "imp")
  ) %>%
    rename_with(tolower) -> totais

  totais_tocheck <- suppressWarnings(tbl(con_comex, "comexstat_arrow") %>%
    group_by(co_ano, fluxo) %>%
    mutate(numero_linhas = 1) %>%
    summarise(across(c(numero_linhas, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro), sum)) %>%
    collect())

  check <- left_join(totais_tocheck, totais %>% select(-arquivo), by = c("co_ano", "fluxo")) %>%
    transmute(check = qt_estat.y
              - qt_estat.x + vl_fob.y - vl_fob.x +
                numero_linhas.y - numero_linhas.x, check2 = if_else(fluxo == "imp", vl_frete.y - vl_frete.x + vl_seguro.y - vl_seguro.x, 0)) %>%
    filter((check > 0) | (check2 > 0))

  ## runs out of memory fast!
  # totais_tocheck_pais <- tbl(con_comex, "comexstat_pais_arrow") %>%
  #   group_by(co_ano, fluxo) %>%
  #   mutate(numero_linhas = 1) %>%
  #   summarise(across(c(numero_linhas, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro), sum)) %>%
  #   collect()
  #
  # check_pais <- left_join(totais_tocheck_pais, totais %>% select(-arquivo), by = c("co_ano", "fluxo")) %>%
  #   transmute(check = qt_estat.y
  #             - qt_estat.x + vl_fob.y - vl_fob.x +
  #               numero_linhas.y - numero_linhas.x, check2 = if_else(fluxo == "imp", vl_frete.y - vl_frete.x + vl_seguro.y - vl_seguro.x, 0)) %>%
  #   filter((check > 0) | (check2 > 0))
  #
  #
  # stopifnot(nrow(check_pais) == 0)

  ## load auxiliary tables
  purrr::walk(c(
    "ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade", "pais",
    "pais_bloco"
  ), ~ {
    pin_download(comexstat_board, name = .x) %>%
      data.table::fread(encoding = "Latin-1", colClasses = "character") %>%
      janitor::clean_names() %>%
      dbWriteTable(con_comex, name = .x, ., overwrite = TRUE)
  })

  ## check that co_unid is unique by co_ncm
  check_unid <- tbl(con_comex, "comexstat_arrow") %>%
    distinct(co_ncm, co_unid) %>%
    count(co_ncm) %>%
    filter(n > 1) %>%
    collect()
  stopifnot(nrow(check_unid) == 0)

  ## ncms
  dbSendQuery(comexstat_connect(), "create or replace table ncms as select * from ncm left join ncm_cgce using (co_cgce_n3) left join ncm_cuci using (co_cuci_item) left join ncm_isic using (co_isic_classe) left join ncm_unidade using (co_unid)")
  purrr::walk(c("ncm_cgce", 'ncm', "ncm_cuci", "ncm_isic", "ncm_unidade"), ~ dbRemoveTable(comexstat_connect(), name=.x))
  msg("Local database created and/or checked!")
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

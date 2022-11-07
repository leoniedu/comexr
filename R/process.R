#' @export
comexstat <- function(rewrite=TRUE) {
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  ddir <- file.path(comexstat_path(), "comexstat_partition")
  comexstat_board <- pins::board_local(versioned = FALSE)
  ## In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange().
  if (rewrite) {
    comexstat_schema <- arrow::schema(
      arrow::field("co_ano", arrow::int32()),
      arrow::field("co_mes", arrow::int32()),
      arrow::field("co_ncm", arrow::string()),
      arrow::field("fluxo", arrow::string()),
      arrow::field("co_pais", arrow::string()),
      arrow::field("co_unid", arrow::string()),
      arrow::field("qt_estat", double()),
      arrow::field("kg_liquido", double()),
      arrow::field("vl_fob", double()),
      arrow::field("vl_frete", double()),
      arrow::field("vl_seguro", double()))
    res <- arrow::open_dataset(
      ddir,
      format = "parquet",schema = comexstat_schema
    )|>dplyr::rename_with(tolower)
    res
  } else {
    comexstat_schema_e <- arrow::schema(
      arrow::field("CO_ANO", arrow::int32()),
      arrow::field("CO_MES", arrow::int32()),
      arrow::field("CO_NCM", arrow::string()),
      arrow::field("CO_UNID", arrow::string()),
      arrow::field("CO_PAIS", arrow::string()),
      arrow::field("SG_UF_NCM", arrow::string()),
      arrow::field("CO_VIA", arrow::string()),
      arrow::field("CO_URF", arrow::string()),
      arrow::field("QT_ESTAT", double()),
      arrow::field("KG_LIQUIDO", double()),
      arrow::field("VL_FOB", double())
    )
    comexstat_schema_i <- comexstat_schema_e
    comexstat_schema_i <- comexstat_schema_i$AddField(11, field = arrow::field("VL_FRETE", double()))
    comexstat_schema_i <- comexstat_schema_i$AddField(12, field = arrow::field("VL_SEGURO", double()))
    ## export
    fname <- file.path(cdir, "EXP_COMPLETA.csv")
    cnames <- read.csv2(fname, nrows = 3) |>
      janitor::clean_names() |>
      names()
    df_e <- arrow::open_dataset(
      fname,
      delim = ";",
      format = "text",
      schema = comexstat_schema_e,
      read_options = arrow::CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
    )
    fname <- file.path(cdir, "IMP_COMPLETA.csv")
    cnames <- read.csv2(fname, nrows = 3) |>
      janitor::clean_names() |>
      names()
    df_i <- arrow::open_dataset(
      fname,
      delim = ";",
      format = "text",
      schema = comexstat_schema_i,
      read_options = arrow::CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
    )
    ## bind together imports and exports
    df <- arrow::open_dataset(list(df_i, df_e)) |>
      dplyr::rename_with(tolower) |>
      dplyr::mutate(fluxo = if_else(is.na(vl_frete), "exp", "imp"))
    df
  }
}

#' @export
ncms <- function() {
  suppressWarnings({
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  comexstat_board <- pins::board_local()
  ## write auxiliary data
  ncms_list <- purrr::map(c("ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade"),~
               pins::pin_download(.x, board=comexstat_board) |> readr::read_csv2(locale = readr::locale(encoding="latin1")) |> janitor::clean_names())
  ncms_merged <- Reduce(dplyr::left_join, ncms_list)
  ncms_merged
  }) |> suppressMessages()
}

#' @export
pais <- function() {
  suppressWarnings({
    cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
    comexstat_board <- pins::board_local()
    comexstat_board |>
      pins::pin_download("pais") |>
      readr::read_csv2(locale = readr::locale(encoding="latin1")) |>
      janitor::clean_names()
  }) |> suppressMessages()
}

#' @export
pais_bloco <- function() {
  suppressWarnings({
    cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
    comexstat_board <- pins::board_local(versioned = FALSE)
    comexstat_board |>
      pins::pin_download("pais_bloco") |>
      readr::read_csv2(locale = readr::locale(encoding="latin1")) |>
      janitor::clean_names()
  }) |> suppressMessages()
}

#' @export
comexstat_rewrite <- function() {
  df <- comexstat(rewrite = FALSE)
  ## write partitioned data
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df |>
    dplyr::group_by(co_ano, fluxo) |>
    arrow::write_dataset(ddir_partition, format = "parquet")
  ddir_partition
}

#' @export
comexstat_raw <- function(rewrite=TRUE) {
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  ddir <- file.path(comexstat_path(), "comexstat_partition")
  comexstat_board <- pins::board_local(versioned = FALSE)
  ## In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange().
  if (rewrite) {
    comexstat_schema <- arrow::schema(
      arrow::field("co_ano", arrow::int16()),
      arrow::field("co_mes", arrow::int8()),
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
      arrow::field("CO_ANO", arrow::int16()),
      arrow::field("CO_MES", arrow::int8()),
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
  df <- comexstat_raw(rewrite = FALSE)
  ## write partitioned data
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df |>
    dplyr::group_by(co_ano, fluxo) |>
    arrow::write_dataset(ddir_partition, format = "parquet")
  ddir_partition
}



#' @export
comexstat_stage <- function(year_min_ncm=2019, year_min_ncm_country=2019) {
  ymin <- year_min_ncm
  yminp <- year_min_ncm_country
  #require(arrow)
  #require(dplyr)
  #require(rappdirs)
  #require(tictoc)
  #require(pins)
  cdir <- path.expand(rappdirs::user_cache_dir("comexstatr"))
  comexstat_board <- pins::board_local(versioned = FALSE)
  tic()
  ## In the current release, arrow supports the dplyr verbs mutate(), transmute(), select(), rename(), relocate(), filter(), and arrange().
  comexstat_schema_e <- arrow::schema(
    arrow::field("CO_ANO", arrow::int16()),
    arrow::field("CO_MES", arrow::int8()),
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
  df <- arrow::open_dataset(list(df_i, df_e))  |>
    dplyr::rename_with(tolower) |>
    dplyr::mutate(fluxo = if_else(is.na(vl_frete), "exp", "imp"))
  ## write partitioned data
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df |>
    dplyr::filter(co_ano>=ymin)|>
    dplyr::group_by(co_ano, fluxo) |>
    arrow::write_dataset(ddir_partition, format = "parquet")
  toc()
  tic()
  ## partition by pais
  ddir_partition <- file.path(comexstat_path(), "comexstat_pais")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df |>
    dplyr::filter(co_ano >= yminp) |>
    dplyr::mutate(co_ano_mes=as.Date(paste(co_ano,co_mes,"01", sep="-")))|>
    dplyr::group_by(co_pais, fluxo, co_ano_mes, co_ncm)|>
    dplyr::summarise(qt_estat=sum(qt_estat),
              kg_liquido=sum(kg_liquido),
              vl_fob=sum(vl_fob),
              vl_frete=sum(vl_frete),
              vl_seguro=sum(vl_seguro)
    )|>
    dplyr::group_by(co_pais, fluxo) |>
    dplyr::arrange(co_ncm, co_ano_mes)|>
    arrow::write_dataset(ddir_partition, format = "parquet", partitioning = c("fluxo","co_pais"))
  toc()
}




#' @export
comexstat_arrow <- function(con_comex) {
  require(arrow)
  require(duckdb)
  ## Load full arrow dataset created by "stage"
  ddir_partition <- file.path(comexstat_path(), "comexstat_partition")
  df_f <- open_dataset(
    ddir_partition,
    format = "parquet"
  )
  duckdb_register_arrow(con_comex, "comexstat_arrow", df_f)
  ## Load  arrow dataset created by "stage" partition by pais
  ddir_partition <- file.path(comexstat_path(), "comexstat_pais")
  comexstat_schema <- arrow::schema(
    arrow::field("co_ano_mes", date32()),
    arrow::field("co_ncm", arrow::string()),
    arrow::field("fluxo", arrow::string()),
    arrow::field("co_pais", arrow::string()),
    arrow::field("qt_estat", double()),
    arrow::field("kg_liquido", double()),
    arrow::field("vl_fob", double()),
    arrow::field("vl_frete", double()),
    arrow::field("vl_seguro", double()))
  df_f <- open_dataset(
    ddir_partition,
    format = "parquet", schema = comexstat_schema
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
  totais <- bind_rows(
    comexstat_board |>
      pin_download("exp_totais_conferencia") |>
      read.csv2() |>
      mutate(fluxo = "exp"),
    comexstat_board |>
      pin_download("imp_totais_conferencia") |>
      read.csv2() |>
      mutate(fluxo = "imp")
  ) |>
    dplyr::rename_with(tolower)

  totais_tocheck <- suppressWarnings(tbl(con_comex, "comexstat_arrow") |>
                                       group_by(co_ano, fluxo) |>
                                       mutate(numero_linhas = 1) |>
                                       summarise(across(c(numero_linhas, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro), sum)) |>
                                       collect())

  check <- left_join(totais_tocheck, totais |> select(-arquivo), by = c("co_ano", "fluxo")) |>
    transmute(check = qt_estat.y
              - qt_estat.x + vl_fob.y - vl_fob.x +
                numero_linhas.y - numero_linhas.x, check2 = if_else(fluxo == "imp", vl_frete.y - vl_frete.x + vl_seguro.y - vl_seguro.x, 0)) |>
    filter((check > 0) | (check2 > 0))

  ## runs out of memory fast!
  # totais_tocheck_pais <- tbl(con_comex, "comexstat_pais_arrow") |>
  #   group_by(co_ano, fluxo) |>
  #   mutate(numero_linhas = 1) |>
  #   summarise(across(c(numero_linhas, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro), sum)) |>
  #   collect()
  #
  # check_pais <- left_join(totais_tocheck_pais, totais |> select(-arquivo), by = c("co_ano", "fluxo")) |>
  #   transmute(check = qt_estat.y
  #             - qt_estat.x + vl_fob.y - vl_fob.x +
  #               numero_linhas.y - numero_linhas.x, check2 = if_else(fluxo == "imp", vl_frete.y - vl_frete.x + vl_seguro.y - vl_seguro.x, 0)) |>
  #   filter((check > 0) | (check2 > 0))
  #
  #
  # stopifnot(nrow(check_pais) == 0)

  ## load auxiliary tables
  purrr::walk(c(
    "ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade", "pais",
    "pais_bloco"
  ), ~ {
    r <- pin_download(comexstat_board, name = .x) |>
      read.csv2(fileEncoding = "latin1", colClasses = "character") |>
      janitor::clean_names()
    DBI::dbWriteTable(con_comex, name = .x, value = r, overwrite = TRUE)
  })

  ## check that co_unid is unique by co_ncm
  check_unid <- tbl(con_comex, "comexstat_arrow") |>
    distinct(co_ncm, co_unid) |>
    count(co_ncm) |>
    filter(n > 1) |>
    collect()
  stopifnot(nrow(check_unid) == 0)

  ## ncms
  dbSendQuery(comexstat_connect(), "create or replace table ncms as select * from ncm left join ncm_cgce using (co_cgce_n3) left join ncm_cuci using (co_cuci_item) left join ncm_isic using (co_isic_classe) left join ncm_unidade using (co_unid)")
  purrr::walk(c("ncm_cgce", 'ncm', "ncm_cuci", "ncm_isic", "ncm_unidade"), ~ DBI::dbRemoveTable(comexstat_connect(), name=.x))
  msg("Local database created and/or checked!")
}


#' @export
comexstat_process <- function(year_min_ncm=2019, year_min_ncm_country=2019, mem_limit_gb=4, threads=2) {
  require(feather)
  require(arrow)
  require(duckdb)
  require(tictoc)
  require(pins)
  library(rlang)
  require(glue)
  require(dplyr)
  ymin <- year_min_ncm%||%2017
  yminp <- year_min_ncm_country%||%2017
  tic()
  con_comex <- comexstat_connect()
  # using arrow to minimize memory usage.
  ## Might have to change this in order to work
  dbSendQuery(con_comex, glue("PRAGMA memory_limit='{mem_limit_gb}GB'"))
  dbSendQuery(con_comex, glue("PRAGMA threads={threads}"))

  # ano, mes, fluxo, pais ---------------------------------------------------
  comexstat_arrow(con_comex)
  msg("Creating ncm/country/month level dataset  ... ")
  ## rolling sum by month, direction (imports/exports) and country
  ## comexstat_pais_arrow  already summarised (sum) by co_ano_mes, co_pais, co_ncm, fluxo
  #dbSendQuery(con_comex, paste("create or replace table comexstat_pais_arrow as select make_date(co_ano, co_mes, 1) as co_ano_mes, co_pais, co_ncm, fluxo,
#sum(vl_fob) as vl_fob, sum(qt_estat) as qt_estat, sum(kg_liquido) as kg_liquido, sum(vl_frete) as vl_frete, sum(vl_seguro) as vl_seguro from comexstat_pais_arrow  where co_ano>=", yminp, "  group by co_ncm, co_ano_mes, co_pais, fluxo"))
  ## combination all co_ncm, co_pais, co_ano_mes, fluxo
  sqlcomb <- paste0("
create or replace table co_ano_mes_pais_ncm as select
co_ano_mes,
co_ncm,
co_pais,
fluxo
from
(select distinct co_ano_mes  from comexstat_pais_arrow),
(select min(co_ano_mes) co_ano_mes_min, co_ncm, co_pais, fluxo from comexstat_pais_arrow group by co_pais, co_ncm, fluxo)
WHERE co_ano_mes>=co_ano_mes_min
")
  tmp <- dbSendQuery(con_comex, sqlcomb)



  sql <- paste('create or replace view comexstat_12_sum_p as SELECT  co_ano_mes, co_pais, co_ncm, fluxo,
  sum("vl_fob") OVER one as vl_fob_12_sum,
  sum("qt_estat") OVER one as qt_estat_12_sum,
  sum("kg_liquido") OVER one as kg_liquido_12_sum,
  sum("vl_frete") OVER one as vl_frete_12_sum,
  sum("vl_seguro") OVER one as vl_seguro_12_sum
   from co_ano_mes_pais_ncm left join comexstat_pais_arrow
   using (co_ano_mes, co_ncm, co_pais, fluxo)
  WINDOW one as (
  PARTITION BY "co_ncm", "co_pais", "fluxo"
   ORDER BY "co_ano_mes" ASC
   RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING
   AND INTERVAL 0 MONTHS FOLLOWING)
')
  dbSendQuery(con_comex, sql)

  #  create (persist) table by ano, mes, pais
  sql <- glue::glue(
    "create or replace table comexstatp as
select comexstat_12_sum_p.*,
vl_fob, vl_frete, vl_seguro,
vl_fob+vl_frete+vl_seguro as vl_cif,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum
                             from
                             comexstat_12_sum_p
                             left join comexstat_pais_arrow using (co_ncm, co_ano_mes, co_pais, fluxo)
                             where vl_fob_12_sum is not null
                             and vl_fob_12_sum>0"
  )
  dbSendQuery(con_comex, sql)


  sql <- glue::glue(
    "create or replace view comexstat_pais_arrowv as
select comexstat_pais_arrow.*, ncms.no_ncm_por as no_ncm, pais.no_pais as pais, ncms.co_unid, ncms.sg_unid as unidade
                             from
                             comexstat_pais_arrow
                             left join pais using(co_pais)
                             left join ncms using(co_ncm)"
  )
  dbSendQuery(con_comex, sql)
  tbl(con_comex, "comexstat_pais_arrow") |> count(co_ano_mes)

  toc()

  # ano, mes, fluxo ---------------------------------------------------------
  tic()
  msg("Creating ncm/month level dataset  ... ")



  ## agregado por mes e fluxo
  dbSendQuery(con_comex, paste("create or replace view comexstat_sel as select make_date(co_ano, co_mes, 1) as co_ano_mes, co_ncm, fluxo,
sum(vl_fob) as vl_fob, sum(qt_estat) as qt_estat, sum(kg_liquido) as kg_liquido, sum(vl_frete) as vl_frete, sum(vl_seguro) as vl_seguro from comexstat_arrow  where co_ano>=", ymin, "group by co_ano_mes, co_ncm, fluxo "))


  ## combination all co_ncm, co_ano_mes, fluxo
  sqlcomb <- paste0("
create or replace table co_ano_mes_ncm as select
co_ano_mes,
co_ncm,
fluxo
from
(select distinct co_ano_mes  from comexstat_sel),
(select min(co_ano_mes) co_ano_mes_min, co_ncm, fluxo from comexstat_sel group by co_ncm, fluxo)
WHERE co_ano_mes>=co_ano_mes_min
")
  tmp <- dbSendQuery(con_comex, sqlcomb)

  sql <- paste('create or replace view comexstat_12_sum as SELECT  co_ano_mes, co_ncm, fluxo,
  sum("vl_fob") OVER one as vl_fob_12_sum,
  sum("qt_estat") OVER one as qt_estat_12_sum,
  sum("kg_liquido") OVER one as kg_liquido_12_sum,
  sum("vl_frete") OVER one as vl_frete_12_sum,
  sum("vl_seguro") OVER one as vl_seguro_12_sum
   from co_ano_mes_ncm left join comexstat_sel
   using (co_ano_mes, co_ncm, fluxo)
  WINDOW one as (
  PARTITION BY "co_ncm", "fluxo"
   ORDER BY "co_ano_mes" ASC
   RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING
   AND INTERVAL 0 MONTHS FOLLOWING)
')
  dbSendQuery(con_comex, sql)


  #  create (persist) table by ano, mes, pais
  sql <- glue(
    "create or replace table comexstat as
select comexstat_12_sum.*,
vl_fob, vl_frete, vl_seguro,
vl_fob+vl_frete+vl_seguro as vl_cif,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum
                             from
                             comexstat_12_sum
                             left join comexstat_sel using (co_ncm, co_ano_mes, fluxo)
                             where vl_fob_12_sum is not null
                             and vl_fob_12_sum>0"
  )
  dbSendQuery(con_comex, sql)


  sql <- glue(
    "create or replace view comexstatv as
select comexstat.*, ncms.no_ncm_por as no_ncm, ncms.co_unid, ncms.sg_unid as unidade
                             from
                             comexstat
                             left join ncms using(co_ncm)"
  )
  dbSendQuery(con_comex, sql)

  sql <- "create or replace view comexstat12_raw as
  (select co_ano, make_date(co_ano, 12, 1) as co_ano_mes, co_ncm, co_pais, fluxo,
    sum(qt_estat) qt_estat_12_sum,
    sum(kg_liquido) kg_liquido_12_sum,
    sum(vl_fob) vl_fob_12_sum,
    sum(vl_frete) vl_frete_12_sum,
    sum(vl_seguro) vl_seguro_12_sum,
    sum(vl_fob + vl_frete +vl_seguro) vl_cif_12_sum
  from  comexstat_arrow
    where co_ano< (select max(co_ano) from comexstat_arrow)
    group by co_ano, co_ncm, co_pais, fluxo)
union all
  (select date_part('year', co_ano_mes) as co_ano, co_ano_mes, co_ncm, co_pais, fluxo,
    qt_estat_12_sum,
    kg_liquido_12_sum,
    vl_fob_12_sum,
    vl_frete_12_sum,
    vl_seguro_12_sum,
    vl_cif_12_sum
  from  comexstatp
    where co_ano>= (select max(co_ano) from comexstat_arrow));


create or replace view comexstatvp as
  select comexstat12_raw.*, pais.no_pais as pais,
  ncms.no_ncm_por as no_ncm,
  ncms.sg_unid, ncms.no_unid
  from
    comexstat12_raw
    left join pais using(co_pais)
    left join ncms using(co_ncm);
  "
  dbSendQuery(con_comex, sql)


  dbRemoveTable(con_comex, "co_ano_mes_ncm")
  dbRemoveTable(con_comex, "co_ano_mes_pais_ncm")
  toc()
  dbDisconnect(con_comex)
}



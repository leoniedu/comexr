cdircomex <- path.expand(rappdirs::user_cache_dir("comexstatr"))
ddircomex <- file.path(rappdirs::user_data_dir("comexstatr"))


#' Reads files with comexdata in the cache directory
#'
#' @noRd
#' @details The downloaded files are one for the exports and other for imports, with data for the entire period available (1997-).
#'
#' This function reads those files using arrow. It is used by comexstat_rewrite.
#'
#' @return arrow::open_dataset
#'
comexstat_raw <- function() {
  comexstat_schema_e <- arrow::schema(
    arrow::field("CO_ANO", arrow::string()),
    arrow::field("CO_MES", arrow::string()),
    arrow::field("CO_NCM", arrow::string()),
    arrow::field("CO_UNID", arrow::string()),
    arrow::field("CO_PAIS", arrow::string()),
    arrow::field("SG_UF_MUN", arrow::string()),
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
  fname <- file.path(cdircomex, "EXP_COMPLETA.csv")
  cnames <- read.csv2(fname, nrows = 3) |>
    janitor::clean_names() |>
    names()
  ##FIX: col names coming with different names on raw data
  cnames <- dplyr::if_else(cnames=='sg_uf_ncm', "sg_uf_mun", cnames)
  df_e <- arrow::open_dataset(
    fname,
    delim = ";",
    format = "text",
    schema = comexstat_schema_e,
    read_options = arrow::CsvReadOptions$create(skip_rows = 1, column_names = toupper(cnames))
  )
  fname <- file.path(cdircomex, "IMP_COMPLETA.csv")
  cnames <- read.csv2(fname, nrows = 3) |>
    janitor::clean_names() |>
    names()
  ##FIX: col names coming with different names on raw data
  cnames <- dplyr::if_else(cnames=='sg_uf_ncm', "sg_uf_mun", cnames)
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



#' Reads comexstat data from the data directory
#'
#' @return tibble available columns are
#' co_ano: aa,
#' co_mes, co_ncm, fluxo, co_pais, sg_uf_mun, co_via, co_urf, co_unid, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro, vl_cif
#' @export
#' @details After successfully running comexstat_download(), comexstat data will be in the data directory. This function reads the data using arrow, allowing fast read, particularly when reading subsets of the data.
#'
#' It defaults to the trade data (imports and exports values) using arrow. For this case you might have to use collect() to actually read the data.
#' Note that arrow only accepts a subset of the dplyr functions. The remaining tables are read from the original csv files.
#'
#' @examples
#' comexstat_download()
#' comexstat()|>filter(co_ano>2017)|>group_by(co_ano, fluxo)|>summarise(vl_fob_bi=sum(vl_fob)/1e9)|>arrange(co_ano)|>collect
comexstat <- function(table="trade", ...) {
  if (table=="trade") {
    comexstat_schema <- arrow::schema(
      #arrow::field("co_ano", arrow::int32()),
      #arrow::field("co_mes", arrow::int32()),
      arrow::field("co_ano_mes", arrow::date32()),
      arrow::field("co_ncm", arrow::string()),
      arrow::field("fluxo", arrow::string()),
      arrow::field("co_pais", arrow::string()),
      arrow::field("sg_uf_mun", arrow::string()),
      arrow::field("co_via", arrow::string()),
      arrow::field("co_urf", arrow::string()),
      arrow::field("co_unid", arrow::string()),
      arrow::field("qt_estat", double()),
      arrow::field("kg_liquido", double()),
      arrow::field("vl_fob", double()),
      arrow::field("vl_frete", double()),
      arrow::field("vl_seguro", double()))
    fe <- file.exists(file.path(comexstatr:::ddircomex, "comexstat_partition"))
    if (!fe) stop("Please, first download and process data using the comexstat_download function.")
    res <- arrow::open_dataset(
      file.path(ddircomex, "comexstat_partition"),
      format = "parquet",schema = comexstat_schema
    )#|>dplyr::rename_with(tolower)
    res |>
      dplyr::mutate(vl_cif=vl_fob+vl_frete+vl_seguro,
                    co_ano=as.character(lubridate::year(co_ano_mes)),
                    co_mes=as.character(lubridate::month(co_ano_mes))
                    )|>
      dplyr::select(co_ano_mes, co_ano, co_mes, everything())
  } else {
    read_comex(table, ...)|>
      dplyr::mutate(across(c(tidyselect::any_of(c("co_ano", "co_mes"))), as.character))
  }
}



#' Reads and merges all comexstat files with information about  NCM
#'
#' @return data.frame/tibble
#' @export
#' @details reads the following files:  ncm, ncm_cgce, ncm_cuci, ncm_isic, ncm_unidade then joins them into a single tibble
#' @examples ncms()
ncms <- function() {
  suppressMessages(suppressWarnings({
    ncms_list <- purrr::map(c("ncm", "ncm_cgce", "ncm_cuci", "ncm_isic", "ncm_unidade"),comexstat)
    ncms_merged <- Reduce(dplyr::left_join, ncms_list)
    ncms_merged |>
      dplyr::mutate(dplyr::across(dplyr::everything(), as.character))
}))
}

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

#' Rewrites the data read from cache directory into partitioned files.
#' @noRd
#' @return the directory where the partition is written to.
#' @details The downloaded files are one for the exports and other for imports, with data for the entire period available (1997-).
#'
#' This function reads those files using arrow by calling function comexstat_raw. It then writes to the data directory the partitioned files.
comexstat_rewrite <- function() {
  df <- comexstat_raw()|>
    dplyr::mutate(co_ano_mes=lubridate::make_date(co_ano, co_mes), co_ano=NULL, co_mes=NULL)|>
    dplyr::select(co_ano_mes, everything())
  ## write partitioned data
  ddir_partition <- file.path(ddircomex, "comexstat_partition")
  unlink(ddir_partition, recursive = TRUE)
  dir.create(ddir_partition, showWarnings = FALSE)
  df |>
    ## data will be written using the partitions
    ## in the group_by statement
    dplyr::group_by(fluxo, co_ano_mes) |>
    arrow::write_dataset(ddir_partition, format = "parquet")
  ddir_partition
}


#' Create a data frame with a co_ano_mes_m column with an id every m months
#' @noRd
#' @param m number of months to aggregate
#' @param data data frame or outpur of comexstat with co_ano and co_mes columns
#'
#' @return tibble with columns: co_ano, co_mes, co_ano_mes_m
#'
#' @examples
#' ym(m=6, comexstat()|>filter(co_ano>=2021))
ym <- function(m, data=comexstat()) {
  mm <- data|>
    dplyr::distinct(co_ano, co_mes)|>
    dplyr::arrange(desc(co_ano), desc(co_mes))|>
    dplyr::collect()|>
    head(1) |>
    dplyr::mutate(co_ano_mes=lubridate::make_date(co_ano, co_mes,1))
  md <- dplyr::tibble(
    co_ano_mes=seq.Date(from=mm$co_ano_mes[1]-base::months(m-1), to=mm$co_ano_mes[1], by="month"))|>
    dplyr::mutate(co_ano=lubridate::year(co_ano_mes),
           co_mes=lubridate::month(co_ano_mes)
           )
  md
}

#' Create id (co_ano_mes_m) by m months
#' @noRd
#' @param m number of months
#'
#' @return tibble with id variables: co_ano_mes_m, co_pais, fluxo, co_ncm; and sums of qt_estat, vl_cif, vl_fob
#'
#' @details Since it outputs every possible combination of co_ano_mes_m, co_pais, fluxo and co_ncm, it can be very large, and time consuming.
#'
comexstat_m <- function(m=12, data=comextat()) {
  ## fix, allow choosing id variables
  ymm <- ym(m, data=data)
  ##aggregate by months
  res0 <- data|>
    dplyr::inner_join(ymm)
  u_ncm <- res0|>dplyr::distinct(co_ncm)|>dplyr::collect()
  u_pais <- res0|>dplyr::distinct(co_pais)|>dplyr::collect()
  u_data <- ymm|>dplyr::distinct(co_ano_mes_m)
  g <- tidyr::expand_grid(u_data, u_pais, fluxo=c("imp", "exp"), u_ncm)
  res <- res0|>
    dplyr::group_by(co_ano_mes_m, co_pais, co_ncm, fluxo)|>
    dplyr::summarise(qt_estat=sum(qt_estat), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob), kg_liquido=sum(kg_liquido), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob))
  g|>
    dplyr::left_join(res, copy=TRUE)|>
    dplyr::mutate_if(is.numeric, tidyr::replace_na, 0)|>
    dplyr::mutate(vl_cif=dplyr::if_else(fluxo=="imp", vl_cif, NA_real_))
}



#' Deflate comexstat series
#'
#' @param data trade data to deflate
#' @param deflators data.frame with deflators
#' @param basedate base date to deflate to.
#'
#' @return deflated data.
#' @export
#'
comexstat_deflated <- function(data=comexstat(), basedate=NULL, deflators=get_deflators(na_omit = TRUE)) {
  deflators_complete <- deflators|>na.omit()
  if (nrow(deflators_complete)!=nrow(deflators)) stop("Missing data in deflators.")
  get_base <- function(x, date, basedate=NULL) {
    if (is.null(basedate)) {
      basedate <- max(date[!is.na(x)])
    }
    r <- date==basedate
    if (sum(r)!=1) stop()
    dplyr::tibble(x=x[r], basedate=as.Date(basedate))
  }
  deflators_0 <- deflators|>
    dplyr::arrange(co_ano_mes)
  base_cpi <- with(deflators_0, get_base(x=cpi, date=co_ano_mes, basedate=basedate))
  base_ipca <- with(deflators_0, get_base(x=ipca_i, date=co_ano_mes, basedate=basedate))
  deflators_1 <- deflators_0|>
    dplyr::mutate(
      #co_ano=as.integer(lubridate::year(date)),
      #co_mes=as.integer(lubridate::month(date)),
      cpi_r=base_cpi$x/cpi,
      cpi_basedate=base_cpi$basedate,
      ipca_r=base_ipca$x/ipca_i,
      ipca_basedate=base_ipca$basedate
    )
  data|>
    ## right join so as get up to the last data
    dplyr::right_join(deflators_1, by=c("co_ano_mes"))|>
    dplyr::mutate(
      #date=lubridate::make_date(co_ano, co_mes),
      vl_fob_constant_usd=vl_fob*(cpi_r),
      vl_fob_constant_brl=vl_fob*brlusd*ipca_r,
      vl_cif_constant_usd=vl_cif*(cpi_r),
      vl_cif_constant_brl=vl_cif*brlusd*ipca_r,
    )
}

## complete with implicit zeros
comexstat_complete <- function(data=comexstat(),k=12) {
  require(rlang)
  ## get the full date range
  range_dates <- data|>
    dplyr::ungroup()|>
    dplyr::summarise(min_date=min(co_ano_mes), max_date=max(co_ano_mes))|>
    dplyr::collect()
  ## expected months
  seq_d <- seq.Date(from = range_dates$min_date, to=range_dates$max_date, by="month")
  ## throws an error if any day!=1
  stopifnot(all(lubridate::day(seq_d)==1))
  df <- data|>
    dplyr::group_by(co_ano_mes, .add=TRUE)|>
    #dplyr::summarise(vl_fob=sum(vl_fob, na.rm=TRUE), vl_cif=sum(vl_cif, na.rm=TRUE), kg_liquido=sum(kg_liquido, na.rm=TRUE), qt_estat=sum(qt_estat, na.rm=TRUE), .groups = "keep")|>
    #dplyr::mutate(across(where(is.numeric), tidyr::replace_na))|>
    dplyr::summarise(across(where(is.numeric), sum), .groups = "keep")|>
    collect()
  g1 <- dplyr::group_vars(df)
  df%>%
    dplyr::ungroup()|>
    tidyr::complete(co_ano_mes=seq_d , !!!data_syms(g1%>%setdiff("co_ano_mes")) #,fill = list(vl_fob=0, vl_cif=0, kg_liquido=0, qt_estat=0)
                    )%>%
    group_by(!!!data_syms(g1))
}


comex_roll <- function(.x) {
  slider::slide_index_dbl(.x = .x,
                          .before = months(k-1),
                          .complete = TRUE,
                          .f = function(z) sum(z, na.rm=TRUE), .i = co_ano_mes, .names = "{.col}_roll_{k}")
}


ncm <- function(x) {
  gsub("[^0-9]", "", x)
}

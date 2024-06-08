cdircomex <- path.expand(rappdirs::user_cache_dir("comexstatr"))
ddircomex <- file.path(rappdirs::user_data_dir("comexstatr"))

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



#' Rewrites the data read from cache directory into partitioned files.
#' @noRd
#' @return the directory where the partition is written to.
#' @details The downloaded files are one for the exports and other for imports, with data for the entire period available (1997-).
#'
#' This function reads those files using arrow by calling function comexstat_raw. It then writes to the data directory the partitioned files.
# comexstat_rewrite <- function() {
#   df <- comexstat_raw()|>
#     dplyr::mutate(co_ano_mes=lubridate::make_date(co_ano, co_mes), co_ano=NULL, co_mes=NULL)|>
#     dplyr::select(co_ano_mes, everything())
#   ## write partitioned data
#   ddir_partition <- file.path(ddircomex, "comexstat_partition")
#   unlink(ddir_partition, recursive = TRUE)
#   dir.create(ddir_partition, showWarnings = FALSE)
#   df |>
#     ## data will be written using the partitions
#     ## in the group_by statement
#     dplyr::group_by(fluxo, co_ano_mes) |>
#     arrow::write_dataset(ddir_partition, format = "parquet")
#   ddir_partition
# }


#' Create a data frame with a co_ano_mes_m column with an id every m months
#' @noRd
#' @param m number of months to aggregate
#' @param data data frame or outpur of comexstat with co_ano and co_mes columns
#'
#' @return tibble with columns: co_ano, co_mes, co_ano_mes_m
#'
#' @examples
#' ym(m=6, comexstat()|>filter(co_ano>=2021))
# ym <- function(m, data=comexstat()) {
#   mm <- data|>
#     dplyr::distinct(co_ano, co_mes)|>
#     dplyr::arrange(desc(co_ano), desc(co_mes))|>
#     dplyr::collect()|>
#     head(1) |>
#     dplyr::mutate(co_ano_mes=lubridate::make_date(co_ano, co_mes,1))
#   md <- dplyr::tibble(
#     co_ano_mes=seq.Date(from=mm$co_ano_mes[1]-base::months(m-1), to=mm$co_ano_mes[1], by="month"))|>
#     dplyr::mutate(co_ano=lubridate::year(co_ano_mes),
#            co_mes=lubridate::month(co_ano_mes)
#            )
#   md
# }

#' Create id (co_ano_mes_m) by m months
#' @noRd
#' @param m number of months
#'
#' @return tibble with id variables: co_ano_mes_m, co_pais, fluxo, co_ncm; and sums of qt_estat, vl_cif, vl_fob
#'
#' @details Since it outputs every possible combination of co_ano_mes_m, co_pais, fluxo and co_ncm, it can be very large, and time consuming.
#'
# comexstat_m <- function(m=12, data=comextat()) {
#   ## fix, allow choosing id variables
#   ymm <- ym(m, data=data)
#   ##aggregate by months
#   res0 <- data|>
#     dplyr::inner_join(ymm)
#   u_ncm <- res0|>dplyr::distinct(co_ncm)|>dplyr::collect()
#   u_pais <- res0|>dplyr::distinct(co_pais)|>dplyr::collect()
#   u_data <- ymm|>dplyr::distinct(co_ano_mes_m)
#   g <- tidyr::expand_grid(u_data, u_pais, fluxo=c("imp", "exp"), u_ncm)
#   res <- res0|>
#     dplyr::group_by(co_ano_mes_m, co_pais, co_ncm, fluxo)|>
#     dplyr::summarise(qt_estat=sum(qt_estat), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob), kg_liquido=sum(kg_liquido), vl_cif=sum(vl_cif), vl_fob=sum(vl_fob))
#   g|>
#     dplyr::left_join(res, copy=TRUE)|>
#     dplyr::mutate_if(is.numeric, tidyr::replace_na, 0)|>
#     dplyr::mutate(vl_cif=dplyr::if_else(fluxo=="imp", vl_cif, NA_real_))
# }



## complete with implicit zeros
# comexstat_complete <- function(data=comexstat(),k=12) {
#   require(rlang)
#   ## get the full date range
#   range_dates <- data|>
#     dplyr::ungroup()|>
#     dplyr::summarise(min_date=min(co_ano_mes), max_date=max(co_ano_mes))|>
#     dplyr::collect()
#   ## expected months
#   seq_d <- seq.Date(from = range_dates$min_date, to=range_dates$max_date, by="month")
#   ## throws an error if any day!=1
#   stopifnot(all(lubridate::day(seq_d)==1))
#   df <- data|>
#     dplyr::group_by(co_ano_mes, .add=TRUE)|>
#     #dplyr::summarise(vl_fob=sum(vl_fob, na.rm=TRUE), vl_cif=sum(vl_cif, na.rm=TRUE), kg_liquido=sum(kg_liquido, na.rm=TRUE), qt_estat=sum(qt_estat, na.rm=TRUE), .groups = "keep")|>
#     #dplyr::mutate(across(where(is.numeric), tidyr::replace_na))|>
#     dplyr::summarise(across(where(is.numeric), sum), .groups = "keep")|>
#     collect()
#   g1 <- dplyr::group_vars(df)
#   df%>%
#     dplyr::ungroup()|>
#     tidyr::complete(co_ano_mes=seq_d , !!!data_syms(g1%>%setdiff("co_ano_mes")) #,fill = list(vl_fob=0, vl_cif=0, kg_liquido=0, qt_estat=0)
#                     )%>%
#     group_by(!!!data_syms(g1))
# }


# comex_roll <- function(.x) {
#   slider::slide_index_dbl(.x = .x,
#                           .before = months(k-1),
#                           .complete = TRUE,
#                           .f = function(z) sum(z, na.rm=TRUE), .i = co_ano_mes, .names = "{.col}_roll_{k}")
# }


# ncm <- function(x) {
#   gsub("[^0-9]", "", x)
# }

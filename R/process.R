#' @export
comexstat_process <- function(year_min_ncm=2019, year_min_ncm_country=2019, mem_limit_gb=4, threads=4) {
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
  dbSendQuery(con_comex, paste("create or replace table comexstat_selp as select make_date(co_ano, co_mes, 1) as co_ano_mes, co_pais, co_ncm, fluxo,
sum(vl_fob) as vl_fob, sum(qt_estat) as qt_estat, sum(kg_liquido) as kg_liquido, sum(vl_frete) as vl_frete, sum(vl_seguro) as vl_seguro from comexstat_pais_arrow  where co_ano>=", yminp, "  group by co_ncm, co_ano_mes, co_pais, fluxo"))


  ## combination all co_ncm, co_pais, co_ano_mes, fluxo
  sqlcomb <- paste0("
create or replace table co_ano_mes_pais_ncm as select
co_ano_mes,
co_ncm,
co_pais,
fluxo
from
(select distinct co_ano_mes  from comexstat_selp),
(select min(co_ano_mes) co_ano_mes_min, co_ncm, co_pais, fluxo from comexstat_selp group by co_pais, co_ncm, fluxo)
WHERE co_ano_mes>=co_ano_mes_min
")
  tmp <- dbSendQuery(con_comex, sqlcomb)



  sql <- paste('create or replace view comexstat_12_sum_p as SELECT  co_ano_mes, co_pais, co_ncm, fluxo,
  sum("vl_fob") OVER one as vl_fob_12_sum,
  sum("qt_estat") OVER one as qt_estat_12_sum,
  sum("kg_liquido") OVER one as kg_liquido_12_sum,
  sum("vl_frete") OVER one as vl_frete_12_sum,
  sum("vl_seguro") OVER one as vl_seguro_12_sum
   from co_ano_mes_pais_ncm left join comexstat_selp
   using (co_ano_mes, co_ncm, co_pais, fluxo)
  WINDOW one as (
  PARTITION BY "co_ncm", "co_pais", "fluxo"
   ORDER BY "co_ano_mes" ASC
   RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING
   AND INTERVAL 0 MONTHS FOLLOWING)
')
  dbSendQuery(con_comex, sql)

  #  create (persist) table by ano, mes, pais
  sql <- glue(
    "create or replace table comexstatp as
select comexstat_12_sum_p.*,
vl_fob, vl_frete, vl_seguro,
vl_fob+vl_frete+vl_seguro as vl_cif_imp,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum_imp
                             from
                             comexstat_12_sum_p
                             left join comexstat_selp using (co_ncm, co_ano_mes, co_pais, fluxo)
                             where vl_fob_12_sum is not null
                             and vl_fob_12_sum>0"
  )
  dbSendQuery(con_comex, sql)


  sql <- glue(
    "create or replace view comexstatpv as
select comexstatp.*, ncms.no_ncm_por as no_ncm, pais.no_pais as pais, ncms.co_unid, ncms.sg_unid as unidade
                             from
                             comexstatp
                             left join pais using(co_pais)
                             left join ncms using(co_ncm)"
  )
  dbSendQuery(con_comex, sql)
  tbl(con_comex, "comexstatp") %>% count(co_ano_mes)

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
vl_fob+vl_frete+vl_seguro as vl_cif_imp,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum_imp
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

  dbRemoveTable(con_comex, "co_ano_mes_ncm")
  dbRemoveTable(con_comex, "co_ano_mes_pais_ncm")
  toc()
  dbDisconnect(con_comex)
}

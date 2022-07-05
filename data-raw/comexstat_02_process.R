library(feather)
library(arrow)
library(duckdb)
library(tictoc)
library(pins)
tic()

con_comex <- comexstat_duck_1(overwrite = TRUE)



# ano, mes, fluxo, pais ---------------------------------------------------


## rolling sum by month, direction (imports/exports) and country
dbSendQuery(con_comex, paste("create or replace view comexstat_vp as select make_date(co_ano, co_mes, 1) as co_ano_mes, co_pais, co_ncm, fluxo,
sum(vl_fob) as vl_fob, sum(qt_estat) as qt_estat, sum(kg_liquido) as kg_liquido, sum(vl_frete) as vl_frete, sum(vl_seguro) as vl_seguro from comexstat_pais_arrow  group by co_ncm, co_ano_mes, co_pais, fluxo"))


## combination all co_ncm, co_pais, co_ano_mes, fluxo
sqlcomb <- paste0("
create or replace table co_ano_mes_pais_ncm as select
co_ano_mes,
co_ncm,
co_pais,
fluxo
from
(select distinct co_ano_mes  from comexstat_vp),
(select min(co_ano_mes) co_ano_mes_min, co_ncm, co_pais, fluxo from comexstat_vp group by co_pais, co_ncm, fluxo)
WHERE co_ano_mes>=co_ano_mes_min
")
tmp <- dbSendQuery(con_comex, sqlcomb)



sql <- paste('create or replace view comexstat_12_sum_p as SELECT  co_ano_mes, co_pais, co_ncm, fluxo,
  sum("vl_fob") OVER one as vl_fob_12_sum,
  sum("qt_estat") OVER one as qt_estat_12_sum,
  sum("kg_liquido") OVER one as kg_liquido_12_sum,
  sum("vl_frete") OVER one as vl_frete_12_sum,
  sum("vl_seguro") OVER one as vl_seguro_12_sum
   from co_ano_mes_pais_ncm left join comexstat_vp
   using (co_ano_mes, co_ncm, co_pais, fluxo)
  WINDOW one as (
  PARTITION BY "co_ncm", "co_pais", "fluxo"
   ORDER BY "co_ano_mes" ASC
   RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING
   AND INTERVAL 0 MONTHS FOLLOWING)
')
dbSendQuery(con_comex, sql)

#  create (persist) table by ano, mes, pais
sql <- paste(
  "create or replace table comexstat12p as
select comexstat_12_sum_p.*, ncms.no_ncm_por as no_ncm, pais.no_pais as pais, ncms.co_unid, ncms.sg_unid as unidade,
vl_fob+vl_frete+vl_seguro as vl_cif_imp,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum_imp
                             from
                             comexstat_12_sum_p
                             left join comexstat_vp using (co_ncm, co_ano_mes, co_pais, fluxo)
                             left join pais using(co_pais)
                             left join ncms using(co_ncm)
                             where vl_fob_12_sum is not null
                             and vl_fob_12_sum>0
                             and co_ano_mes>=make_date(",
  ## 12 month rolling sum, so better start analysis 1 year after ymin
  yminp + 1, ",1,1)"
)
dbSendQuery(con_comex, sql)


tbl(con_comex, "comexstat12p") %>% count(co_ano_mes)

toc()






# ano, mes, fluxo ---------------------------------------------------------
tic()



## agregado por mes e fluxo
dbSendQuery(con_comex, paste("create or replace view comexstat_v as select make_date(co_ano, co_mes, 1) as co_ano_mes, co_ncm, fluxo,
sum(vl_fob) as vl_fob, sum(qt_estat) as qt_estat, sum(kg_liquido) as kg_liquido, sum(vl_frete) as vl_frete, sum(vl_seguro) as vl_seguro from comexstat_arrow  group by co_ano_mes, co_ncm, fluxo"))


## combination all co_ncm, co_ano_mes, fluxo
sqlcomb <- paste0("
create or replace table co_ano_mes_ncm as select
co_ano_mes,
co_ncm,
fluxo
from
(select distinct co_ano_mes  from comexstat_v),
(select min(co_ano_mes) co_ano_mes_min, co_ncm, fluxo from comexstat_v group by co_ncm, fluxo)
WHERE co_ano_mes>=co_ano_mes_min
")
tmp <- dbSendQuery(con_comex, sqlcomb)

sql <- paste('create or replace view comexstat_12_sum as SELECT  co_ano_mes, co_ncm, fluxo,
  sum("vl_fob") OVER one as vl_fob_12_sum,
  sum("qt_estat") OVER one as qt_estat_12_sum,
  sum("kg_liquido") OVER one as kg_liquido_12_sum,
  sum("vl_frete") OVER one as vl_frete_12_sum,
  sum("vl_seguro") OVER one as vl_seguro_12_sum
   from co_ano_mes_ncm left join comexstat_v
   using (co_ano_mes, co_ncm, fluxo)
  WINDOW one as (
  PARTITION BY "co_ncm", "fluxo"
   ORDER BY "co_ano_mes" ASC
   RANGE BETWEEN INTERVAL 11 MONTHS PRECEDING
   AND INTERVAL 0 MONTHS FOLLOWING)
')
dbSendQuery(con_comex, sql)



sql <- paste(
  "create or replace table comexstat12 as
select comexstat_12_sum.*, ncms.no_ncm_por as no_ncm, ncms.sg_unid as unidade,
vl_fob+vl_frete+vl_seguro as vl_cif_imp,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum_imp
                             from
                             comexstat_12_sum
                             left join comexstat_v using (co_ncm, co_ano_mes, fluxo)
                             left join ncms using(co_ncm)
                             where vl_fob_12_sum is not null
                             and co_ano_mes>=make_date(",
  ## 12 month rolling sum, so better start analysis 1 year after ymin
  ymin + 1, ",1,1)"
)
gc()
dbSendQuery(con_comex, sql)


tbl(con_comex, "comexstat12p") %>% count(co_ano_mes)
# comex_pais_ano_mes <- dnow

sql <- paste(
  "create or replace table comexstat as
select co_ncm,  ncms.no_ncm_por as no_ncm, ncms.co_unid, ncms.sg_unid as unidade,
vl_fob+vl_frete+vl_seguro as vl_cif_imp,
vl_fob_12_sum+vl_frete_12_sum+vl_seguro_12_sum as vl_cif_12_sum_imp
                             from
                             comexstat_12_sum
                             left join comexstat_v using (co_ncm, co_ano_mes, fluxo)
                             left join ncms using(co_ncm)
                             where vl_fob_12_sum is not null
                             and vl_fob_12_sum>0
                             and co_ano_mes>=make_date(",
  ## 12 month rolling sum, so better start analysis 1 year after ymin
  ymin + 1, ",1,1)"
)
dbSendQuery(con_comex, sql)

toc()


dbDisconnect(con_comex)

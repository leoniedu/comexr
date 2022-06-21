library(feather)
library(arrow)
library(duckdb)
library(tictoc)
library(pins)
tic()

con_comex <- comexstat_duck_1(overwrite = TRUE)



comexstat_board <- board_local(versioned = FALSE)


con_comex <- comexstat_duck_1(overwrite = TRUE)
## Might have to change this in order to work
dbSendQuery(con_comex, "PRAGMA memory_limit='4GB'")
dbSendQuery(con_comex, "PRAGMA threads=4")

## Load full arrow dataset created by "stage"
ddir_partition <- file.path(ddir, "comexstat_partition")
df_f <- open_dataset(
  ddir_partition,
  format = "feather"
)
duckdb_register_arrow(con_comex, "comexstat_arrow", df_f)


## Load  arrow dataset created by "stage" partition by pais
ddir_partition <- file.path(ddir, "comexstat_pais")
df_f <- open_dataset(
  ddir_partition,
  format = "feather"
)
duckdb_register_arrow(con_comex, "comexstat_pais_arrow", df_f)


## check calculated totals with the totals dataset
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

totais_tocheck <- tbl(con_comex, "comexstat_arrow") %>%
  group_by(co_ano, fluxo) %>%
  mutate(numero_linhas = 1) %>%
  summarise(across(c(numero_linhas, qt_estat, kg_liquido, vl_fob, vl_frete, vl_seguro), sum)) %>%
  collect()

check <- left_join(totais_tocheck, totais %>% select(-arquivo), by = c("co_ano", "fluxo")) %>%
  transmute(check = qt_estat.y
  - qt_estat.x + vl_fob.y - vl_fob.x +
    numero_linhas.y - numero_linhas.x, check2 = if_else(fluxo == "imp", vl_frete.y - vl_frete.x + vl_seguro.y - vl_seguro.x, 0)) %>%
  filter((check > 0) | (check2 > 0))

stopifnot(nrow(check) == 0)


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


## ncm
ncms <- tbl(con_comex, "ncm") %>%
  left_join(tbl(con_comex, "ncm_cgce")) %>%
  left_join(tbl(con_comex, "ncm_cuci")) %>%
  left_join(tbl(con_comex, "ncm_isic")) %>%
  left_join(tbl(con_comex, "ncm_unidade")) %>%
  collect()
dbWriteTable(con_comex, "ncms", ncms, overwrite = TRUE)


## check that co_unid is unique by co_ncm
check_unid <- tbl(con_comex, "comexstat_arrow") %>%
  distinct(co_ncm, co_unid) %>%
  count(co_ncm) %>%
  filter(n > 1) %>%
  collect()
stopifnot(nrow(check_unid) == 0)



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

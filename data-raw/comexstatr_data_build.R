library(comexstatr)
library(tictoc)
comexstat_download_raw(rewrite = TRUE)

ymin <- 0
yminc <- lubridate::year(Sys.Date())-1
comexstat_stage(year_min_ncm=ymin, year_min_ncm_country=yminc)



comexstat_create_db(overwrite=TRUE)
tic()
comexstat_process(year_min_ncm=ymin, year_min_ncm_country=yminc, threads = 2, mem_limit_gb = 8)
toc()

source("data-raw/comexstatr_data_write.R", echo=TRUE)
stop()


tmp <- map(dbListTables(con), ~ try(tbl(con, .x)%>%head%>%collect))
names(tmp) <- dbListTables(con)

## export comexstatv


library(comexstatr)
library(tictoc)
library(dplyr)
library(arrow)
tic()
con <- comexstat_connect()


## blocos
blocos_sel <- c("22","111","1","3","9", "5", "51", "105", "41")

pais_sel <-
  tbl(con, "comexstat_arrow")%>%
  filter(co_ano>=2017)%>%
  anti_join(
    tbl(con, "pais_bloco")%>%
      filter(co_bloco%in%blocos_sel)%>%
      distinct(co_pais))%>%
  group_by(co_pais)%>%
  summarise(vl_fob=sum(vl_fob))%>%
  filter(!is.na(vl_fob))%>%
  arrange(desc(vl_fob))%>%
  head(10)%>%
  pull(co_pais)


top_blocos <- tbl(con, "pais_bloco")%>%
  left_join(tbl(con, "pais"))%>%
  mutate(bloco=
           case_when(
             co_pais%in%pais_sel ~ no_pais,
             co_bloco%in%blocos_sel ~ no_bloco,
             TRUE ~ NA_character_
           ))%>%
  filter(!is.na(bloco))

stopifnot(0==(top_blocos%>%count(co_pais)%>%filter(n>1)%>%collect%>%nrow))




pb <- tbl(con, "comexstat12")%>%
  left_join(top_blocos, by='co_pais')%>%
  group_by(bloco, co_ano_mes, fluxo)%>%
  summarise(vl_fob_12_sum_bi=sum(vl_fob_12_sum)/1e9)%>%
  group_by(co_ano_mes, fluxo)%>%
  mutate(p=vl_fob_12_sum_bi/sum(vl_fob_12_sum_bi))


pb <- pb%>%collect

library(ggplot2)
qplot(co_ano_mes, p, geom='line', data=pb%>%group_by(bloco)%>%filter(any(p>.25)), color=bloco) + facet_wrap(~fluxo)

qplot(co_ano_mes, vl_fob_12_sum_bi, geom='line', data=pb%>%group_by(bloco)%>%filter(any(p>.1)), color=bloco) + facet_wrap(~fluxo)

dnow <- tbl(con, "comexstat12")%>%
  filter(co_ncm=="07032090", co_ano_mes>=as.Date("2017-01-01"))%>%
  group_by(co_ncm, fluxo, co_ano_mes)%>%
  summarise(across(c(qt_estat_12_sum:vl_cif_12_sum), sum))%>%collect

qplot(co_ano_mes, vl_fob_12_sum/1e6, data=
        dnow, geom='line', color=fluxo) +
  labs("Valor FOB - USD Milhões")



ddir_partition <- file.path(comexstatr:::comexstat_path(), "comexstat12")
unlink(ddir_partition, recursive = TRUE)
dir.create(ddir_partition, showWarnings = FALSE)
df <- tbl(con, "comexstat12")%>%collect
toc()
tic()
df %>%
  group_by(co_ano, fluxo) %>%
  write_dataset(ddir_partition, format = "parquet")
toc()

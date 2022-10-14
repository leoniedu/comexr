library(comexstatr)
library(tictoc)
library(dplyr)

##downloading
tic()
p <- comexstat_download_raw(rewrite = TRUE, force_download = FALSE)
toc()


## summarise by ncm and year
tic()
cstat <- comexstat_raw(rewrite = TRUE)
cstat_ncm_year <- cstat%>%
  dplyr::filter(co_ano>2017)%>%
  dplyr::group_by(co_ano, co_ncm, fluxo)%>%
  dplyr::summarise(vl_fob=sum(vl_fob))%>%dplyr::collect()%>%
  dplyr::arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)
toc()


tic()
cstat <- comexstat_raw(rewrite = TRUE)
cstat_year <- cstat%>%
  dplyr::group_by(co_ano, fluxo)%>%
  dplyr::summarise(vl_fob_bi=sum(vl_fob)/1e9)%>%dplyr::collect()%>%
  dplyr::arrange(fluxo, co_ano)
tail(cstat_year)
toc()


## summarise by ncm and year
## with rewrite
tic()
cstat <- arrow::open_dataset(p)
cstat_ncm_year <- cstat%>%
  dplyr::filter(co_ano>2017)%>%
  dplyr::group_by(co_ano, co_ncm, fluxo)%>%
  dplyr::summarise(vl_fob=sum(vl_fob))%>%dplyr::collect()%>%
  dplyr::arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)
toc()


## summarise by pais
## with rewrite
tic()
cstat <- arrow::open_dataset(p)
cstat_pais_year <- cstat%>%
  #dplyr::filter(co_ano>2017)%>%
  dplyr::group_by(co_pais, fluxo)%>%
  dplyr::summarise(vl_fob=sum(vl_fob))%>%dplyr::collect()%>%
  dplyr::arrange(desc(vl_fob))
head(cstat_pais_year)
toc()

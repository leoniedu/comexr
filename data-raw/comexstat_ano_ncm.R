library(comexstatr)
library(tictoc)

##downloading
tic()
p <- comexstat_download_raw(rewrite = TRUE)
toc()


## summarise by ncm and year
tic()
cstat <- comexstat_raw()
cstat_ncm_year <- cstat%>%
  dplyr::filter(co_ano>2017)%>%
  dplyr::group_by(co_ano, co_ncm, fluxo)%>%
  dplyr::summarise(vl_fob=sum(vl_fob))%>%dplyr::collect()%>%
  dplyr::arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)
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

library(comexstatr)
library(tictoc)

##downloading
tic()
comexstat_download_raw()
toc()


## summarise by ncm and year
tic()
cstat <- comexstat_raw()
cstat_ncm_year <- cstat%>%
  filter(co_ano>=2021)%>%
  group_by(co_ano, co_ncm, fluxo)%>%
  summarise(vl_fob=sum(vl_fob))%>%collect%>%
  arrange(co_ano, co_ncm, fluxo)
head(cstat_ncm_year)

library(comexstatr)
library(tictoc)
library(dplyr)

##downloading
tic()
p <- comexstat_download_raw(rewrite = TRUE, force_download = FALSE)
toc()


## summarise by ncm and year
tic()
cstat <- comexstat()
cstat_ncm_year <- cstat%>%
  dplyr::filter(co_ano>2017)%>%
  dplyr::group_by(co_ano, co_ncm, fluxo)%>%
  dplyr::summarise(vl_fob=sum(vl_fob))%>%
  dplyr::collect()%>%
  dplyr::arrange(co_ano, co_ncm, fluxo)
tail(cstat_ncm_year)
toc()

##aggregate 12 most recent months
ym <- comexstat()%>%
  distinct(co_ano, co_mes)%>%
  arrange(-co_ano, -co_mes)%>%
  collect%>%
  mutate(id=rep(1:(n()/12), each=12)[1:n()])%>%
  group_by(id)%>%
  mutate(co_ano_mes_12=lubridate::make_date(co_ano[1], co_mes[1]))%>%
  ungroup%>%
  select(-id)


msul <- pais_bloco()%>%
  filter(co_bloco==111)%>%
  pull(co_pais)
eu <- pais_bloco()%>%
  filter(co_bloco==22)%>%
  pull(co_pais)

pb <- pais()%>%
  mutate(parceiro=
           case_when(co_pais%in%msul ~ "Mercosul",
                     co_pais%in%eu ~ "União Europeia",
                     co_pais%in%"249" ~ "Estados Unidos",
                     co_pais%in%"160" ~ "China",
                     co_pais%in%"361" ~ "Índia",
                     co_pais%in%"158" ~ "Chile",
                     TRUE ~ "Outros"
                     ))




tmp <- cstat%>%
  #filter(co_ano>2020)%>%
  inner_join(ym, by=c("co_ano", "co_mes"))%>%
  left_join(pb, by="co_pais")%>%
  group_by(co_ano_mes_12, parceiro, fluxo)%>%
  summarise(vl_fob=sum(vl_fob), kg_liquido=sum(kg_liquido))%>%
  collect%>%
  group_by(parceiro, fluxo)%>%
  arrange(co_ano_mes_12)%>%
  mutate(vl_fob_rel=vl_fob/vl_fob[1],
         kg_liquido_rel=kg_liquido/kg_liquido[1]
         )


qplot(co_ano_mes_12, vl_fob_rel/kg_liquido_rel, data=tmp, color=parceiro, geom="line") + facet_wrap(~fluxo)





tic()
cstat <- comexstat(rewrite = TRUE)
cstat_year <- cstat%>%
  dplyr::group_by(co_ano, fluxo)%>%
  dplyr::summarise(vl_fob_bi=sum(vl_fob)/1e9)%>%dplyr::collect()%>%
  dplyr::arrange(fluxo, co_ano)
tail(cstat_year)
toc()


